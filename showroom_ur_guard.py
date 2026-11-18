"""Самовывоз закрывает офис, а не онлайн-менеджер (пункт 4 плана Кати, 13.11.2026).

Что происходило. Сделка розницы с типом доставки «Самовывоз из офиса» или «Самовывоз из
шоурума» по правилу 2 `office_transfer` из «Успешно реализовано» розницы сразу едет в
«Успешно реализовано» Офиса - а на этом этапе интеграция МойСклада создаёт отгрузку, то
есть товар списывается. Это правильно, когда клиент уже пришёл, оплатил и забрал товар:
так закрывает сделку офис. Но если в УР сделку перевёл онлайн-менеджер, клиента в офисе
ещё не было, а товар уже списан.

Что делаем. Перед переносом спрашиваем журнал событий amo, кто перевёл сделку в УР.
Онлайн-МОП (Игорь, Егор, «красный телефон» - тот же список, что у сторожа перемещений) →
в Офис не переносим, возвращаем сделку на этап, откуда её перевели (решение Кати 13.11:
«возвращаем в этап, откуда перевёл менеджер»), и пишем в чат ОП:
«@менеджер некорректное действие в CRM: … попытка перевода офисной сделки в УР».

Почему здесь, а не в сторожe перемещений. Перенос идёт по вебхуку за секунды - сторож с
опросом раз в 45 секунд опоздал бы, и отгрузка уже была бы создана.

Если журнал не ответил или события ещё нет (amo пишет его чуть позже вебхука) - ждём
несколько секунд и спрашиваем снова; так и не узнали автора - переносим как раньше и пишем
в лог (сбой журнала не должен останавливать закрытие сделок офисом).
⚠️ Возврат в «Новый лид» заново запускает раздачу лидов - так велела Катя, в уведомлении
об этом не пишем отдельно.
Флаг `OFFICE_TRANSFER_SHOWROOM_GUARD`: пусто/0 - выключено (по умолчанию), `dry` - режим отчёта
(в лог «остановила бы», перенос идёт как раньше), `1` - боевой режим.
"""
from __future__ import annotations

import asyncio
import logging
import os

import alerts
import amo_service
import telegram_bot
import tg_recipients
from stage_guard_rules import Config
from waybill_config import (
    DELIVERY_PICKUP_MARKERS,
    FIELD_DELIVERY_TYPE,
    MANAGER_NAMES,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_TANGEMSHOP,
    STATUS_SUCCESS,
    WAZZUP_TG_HANDLES,
)

logger = logging.getLogger("uvicorn")

_MODE = os.getenv("OFFICE_TRANSFER_SHOWROOM_GUARD", "").strip().lower()
SHOWROOM_GUARD_ENABLED = _MODE in ("1", "dry", "notify")
SHOWROOM_GUARD_DRY_RUN = _MODE == "dry"
# `notify` - живое уведомление в чат ОП без действия: сделка уезжает в Офис как раньше
# (Катя 13.11: «живые алерты на всё» на неделю проверки).
SHOWROOM_GUARD_NOTIFY_ONLY = _MODE == "notify"
# Сколько раз и с какой паузой искать событие перехода (amo пишет журнал чуть позже вебхука).
LOOKUP_ATTEMPTS = int(os.getenv("SHOWROOM_GUARD_LOOKUP_ATTEMPTS", "3"))
LOOKUP_PAUSE_S = float(os.getenv("SHOWROOM_GUARD_LOOKUP_PAUSE_S", "2"))

GUARDED_PIPELINES = (PIPELINE_CLEVER_MAIN, PIPELINE_TANGEMSHOP)
EVENT_KEY = "showroom_ur_guard"


def applies(lead: dict) -> bool:
    """Наш случай: флаг включён, сделка розницы/TangemShop в УР, доставка - наш самовывоз."""
    if not SHOWROOM_GUARD_ENABLED:
        return False
    if int(lead.get("pipeline_id") or 0) not in GUARDED_PIPELINES:
        return False
    if int(lead.get("status_id") or 0) != STATUS_SUCCESS:
        return False
    text = str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").lower()
    return any(marker in text for marker in DELIVERY_PICKUP_MARKERS)


async def _last_entry_to_won(lead_id: int, pipeline_id: int) -> dict | None | bool:
    """Последнее событие «перевели в УР этой воронки». None - журнал не ответил,
    False - ответил, но такого события нет."""
    data = await amo_service._do_get("/api/v4/events", [
        ("filter[type]", "lead_status_changed"),
        ("filter[entity]", "lead"),
        ("filter[entity_id]", str(lead_id)),
        ("limit", "50"),
    ])
    if data is None:
        return None
    best = None
    for ev in (data.get("_embedded") or {}).get("events") or []:
        after = ((ev.get("value_after") or [{}])[0].get("lead_status") or {})
        if int(after.get("id") or 0) != STATUS_SUCCESS or int(after.get("pipeline_id") or 0) != pipeline_id:
            continue
        if best is None or int(ev.get("created_at") or 0) > int(best.get("created_at") or 0):
            best = ev
    return best or False


async def check(lead: dict) -> str | None:
    """None - переносим как обычно. Строка - перенос остановлен, это исход для лога."""
    if not applies(lead):
        return None
    lead_id = int(lead["id"])
    pipeline_id = int(lead["pipeline_id"])

    ev = None
    for attempt in range(LOOKUP_ATTEMPTS):
        ev = await _last_entry_to_won(lead_id, pipeline_id)
        if ev:
            break
        if attempt + 1 < LOOKUP_ATTEMPTS:
            await asyncio.sleep(LOOKUP_PAUSE_S)
    if not ev:
        logger.warning("showroom_ur_guard %s: автор перехода в УР не найден (%s) - переносим как раньше",
                       lead_id, "журнал не ответил" if ev is None else "события нет")
        return None

    user_id = int(ev.get("created_by") or 0)
    if user_id not in Config.from_env().online_mops:
        return None

    before = ((ev.get("value_before") or [{}])[0].get("lead_status") or {})
    back_pipeline, back_status = int(before.get("pipeline_id") or 0), int(before.get("id") or 0)
    if SHOWROOM_GUARD_DRY_RUN:
        logger.info("showroom_ur_guard %s [отчёт]: УР с самовывозом поставил онлайн-МОП %s - остановила бы "
                    "перенос и вернула в %s/%s; переносим как раньше", lead_id, user_id, back_pipeline, back_status)
        return None
    tag = WAZZUP_TG_HANDLES.get(user_id) or MANAGER_NAMES.get(user_id) or "менеджер"
    link = alerts.lead_link(lead_id)
    if SHOWROOM_GUARD_NOTIFY_ONLY:
        await _notify(lead_id, tag, link, "Режим проверки: сделка уехала в Офис как раньше и товар "
                      "списан. Самовывоз закрывает офис, когда клиент забрал товар.", lead)
        logger.info("showroom_ur_guard %s [уведомление]: УР с самовывозом поставил онлайн-МОП %s, "
                    "переносим как раньше", lead_id, user_id)
        return None
    res = await amo_service.patch_lead(lead_id, pipeline_id=back_pipeline, status_id=back_status) \
        if back_pipeline and back_status else {"ok": False}
    returned = bool(res.get("ok"))
    if returned:
        try:
            await amo_service.add_note(
                lead_id, f"{MANAGER_NAMES.get(user_id) or tag} - ЗАПРЕЩЕННОЕ ДЕЙСТВИЕ: перевод сделки "
                "с самовывозом в «Успешно реализовано». Сделка возвращена.")
        except Exception:
            logger.exception("showroom_ur_guard %s: примечание в сделку не записалось", lead_id)
    tail = ("Сделка возвращена на прежний этап. Самовывоз закрывает офис, когда клиент забрал товар."
            if returned else "Вернуть сделку не получилось - верните руками, в Офис она не перенесена.")
    await _notify(lead_id, tag, link, tail, lead)
    logger.info("showroom_ur_guard %s: УР онлайн-МОПа %s остановлен, возврат %s/%s - %s",
                lead_id, user_id, back_pipeline, back_status, "ок" if returned else "НЕ УДАЛСЯ")
    return "blocked-showroom-online-mop" if returned else "blocked-showroom-rollback-failed"


FIELD_MS_ORDER_NUMBER = 576697   # «№ Заказа» МойСклада
FIELD_COMPOSITION = 576703       # «Состав заказа» (пишет мост МойСклада, с «Итого»)


def details(lead: dict | None) -> list[str]:
    """Строки подробностей: сделка, заказ, сумма, доставка, состав."""
    if not lead:
        return []
    head = [f"«{lead.get('name') or 'без названия'}»"]
    order = str(amo_service.get_custom_field_value(lead, FIELD_MS_ORDER_NUMBER) or "").strip()
    if order:
        head.append(f"заказ №{order}")
    if lead.get("price"):
        head.append(f"{int(lead['price']):,} ₽".replace(",", " "))
    out = ["Сделка: " + ", ".join(head)]
    delivery = str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").strip()
    if delivery:
        out.append(f"Доставка: {delivery}")
    comp = " ".join(str(amo_service.get_custom_field_value(lead, FIELD_COMPOSITION) or "").split())
    if comp:
        out.append(f"Состав: {comp[:300]}" + ("…" if len(comp) > 300 else ""))
    return out


async def _notify(lead_id: int, tag: str, link: str, tail: str, lead: dict | None = None) -> None:
    text = "\n".join([f"⛔ {tag} некорректное действие в CRM: попытка перевода офисной сделки в УР."]
                     + details(lead) + [tail, link])
    try:
        d = alerts.decide(EVENT_KEY, legacy_text=text,
                          values={"теги": tag, "ссылка_на_сделку": link},
                          chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID,
                          parse_mode="HTML")
        if d is not None:
            await telegram_bot.send_alert(d.text, **d.send_kwargs())
    except Exception:
        logger.exception("showroom_ur_guard %s: уведомление не ушло", lead_id)
