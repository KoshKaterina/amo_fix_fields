"""Новый лид в Академии → сообщение в топик УВЕДОМЛЕНИЯ с тегом Гладкова (Катя 08.09.2026).

Сделка встала на этап «Входящий лид» воронки «Академия» — Саша (РОП) должен увидеть её
в тот же момент, а не найти вечером в списке. Уведомление короткое: кто клиент, куда
звонить, ссылка на сделку.

────────────────────────────── что считается новым лидом ──────────────────────────────
ЛЮБОЕ попадание сделки на этап «Входящий лид» (выбор Кати 08.09.2026): и сделка, созданная
сразу там, и переведённая туда с другого этапа. Для Саши это одно и то же событие — лид,
которого раньше в работе не было, — и различать их в интерфейсе нечем.

Отсюда два следствия, которых нет у соседа showroom_alert:
  • гейта по возрасту сделки НЕТ — он отсекал бы ровно перевод старой сделки;
  • дедуп живёт ОКНОМ, а не навсегда: эхо вебхука и «подёргали этап туда-сюда» гасятся
    в пределах ACADEMY_LEAD_ALERT_DEDUP_H, а честный повторный заход через неделю
    уведомит снова.

Взамен возраста от массового прогона по старью защищает часовой лимит: больше
ACADEMY_LEAD_ALERT_HOUR_LIMIT сообщений за час — молчим до конца окна, сказав об этом
одной строкой в чат. Урок showroom_alert 07.08.2026: чужой прогон по паре десятков сделок
насыпает в топик пачку, и топик перестают читать.

Сделка ВСЕГДА перечитывается из amo перед отправкой: вебхук говорит о моменте, а
отправляем мы через ACADEMY_LEAD_ALERT_DELAY_S (поля и контакт дозаписываются не разом).
За эту паузу лид могли увести дальше по воронке — тогда молчим, а отметку дедупа снимаем:
следующий настоящий заход на этап должен дойти.

Дедуп переживает рестарт: список лежит в /app/var (постоянный том контейнера).
"""

import asyncio
import json
import logging
import os
import time

import amo_service
import telegram_bot
import alerts
from api import BASE_URL
from tg_recipients import ACADEMY_ALERT_TAG, NOTIFY_CHAT_ID, NOTIFY_THREAD_ID
from waybill_config import (
    ACADEMY_LEAD_ALERT_DEDUP_H,
    ACADEMY_LEAD_ALERT_DELAY_S,
    ACADEMY_LEAD_ALERT_ENABLED,
    ACADEMY_LEAD_ALERT_HOUR_LIMIT,
    FIELD_PHONE,
    PIPELINE_ACADEMY,
    STATUS_ACADEMY_INBOUND_LEAD,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
# lead_id (строкой) → время последней отправки. Не set, как у showroom_alert: дедуп тут
# с окном, а без времени окно не посчитать.
_seen: dict[str, float] = {}
_SEEN_CAP = 5000
_SEEN_PATH = os.getenv("ACADEMY_LEAD_ALERT_SEEN_PATH", "/app/var/academy_lead_alert_seen.json")
_seen_loaded = False

# Часовое окно: времена отправок за последний час и отметка, что про перебор уже сказали.
_sent_times: list[float] = []
_burst_notified = False


def _load_seen() -> None:
    """Поднять дедуп с диска. Файла нет / битый — начинаем с пустого."""
    global _seen_loaded
    if _seen_loaded:
        return
    _seen_loaded = True
    try:
        with open(_SEEN_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for key, ts in data.items():
                try:
                    _seen[str(key)] = float(ts)
                except (TypeError, ValueError):
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("Академия-алерт: не прочитался %s — дедуп с нуля", _SEEN_PATH)


def _save_seen() -> None:
    """Сбросить дедуп на диск. Не удался — не беда, в памяти он остаётся."""
    try:
        os.makedirs(os.path.dirname(_SEEN_PATH), exist_ok=True)
        tmp = f"{_SEEN_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_seen, f)
        os.replace(tmp, _SEEN_PATH)
    except Exception:
        logger.exception("Академия-алерт: не записался %s", _SEEN_PATH)


def _forget_stale(now: float) -> None:
    """Выбросить записи старше окна дедупа и подрезать список, если он разросся."""
    window = ACADEMY_LEAD_ALERT_DEDUP_H * 3600
    for key in [k for k, ts in _seen.items() if now - ts > window]:
        _seen.pop(key, None)
    if len(_seen) > _SEEN_CAP:
        for key in sorted(_seen, key=lambda k: _seen[k])[: len(_seen) - _SEEN_CAP]:
            _seen.pop(key, None)


def _is_new(lead_id) -> bool:
    """True — про эту сделку в текущем окне ещё не писали (писать).
    False — эхо вебхука, повтор после рестарта или тот же лид, подёрганный по этапам."""
    _load_seen()
    now = time.time()
    _forget_stale(now)
    key = str(lead_id)
    if key in _seen:
        return False
    _seen[key] = now
    _save_seen()
    return True


def _unsee(lead_id) -> None:
    """Снять отметку: отправки не было (лид уже увели с этапа, amo молчит, лимит).
    Без этого следующий честный заход на «Входящий лид» промолчал бы всё окно."""
    if _seen.pop(str(lead_id), None) is not None:
        _save_seen()


def _budget_ok() -> bool:
    """Влезаем ли в часовой лимит. Перебор — один раз говорим об этом в чат и молчим."""
    global _burst_notified
    now = time.time()
    _sent_times[:] = [ts for ts in _sent_times if now - ts < 3600]
    if len(_sent_times) < ACADEMY_LEAD_ALERT_HOUR_LIMIT:
        if not _sent_times:
            _burst_notified = False
        return True
    return False


def is_inbound_lead(lead: dict) -> bool:
    """Сделка ПРЯМО СЕЙЧАС стоит на «Входящем лиде» воронки Академии?

    Проверяется по сделке, а не по вебхуку: между вебхуком и отправкой проходит пауза,
    и за неё лид могли увести дальше."""
    if not lead:
        return False
    if str(lead.get("pipeline_id")) != str(PIPELINE_ACADEMY):
        return False
    return str(lead.get("status_id")) == str(STATUS_ACADEMY_INBOUND_LEAD)


def notify_bg(lead_id, pipeline_id, status_id) -> None:
    """Разбор вебхука `/lead_change`: сделка Академии встала на «Входящий лид».

    Зовётся на КАЖДОМ изменении любой сделки, поэтому здесь только сравнения и словарь.
    Сеть и чтение amo — в фоне, в `_apply`."""
    if not ACADEMY_LEAD_ALERT_ENABLED:
        return
    if lead_id is None or pipeline_id is None or status_id is None:
        return
    if str(pipeline_id) != str(PIPELINE_ACADEMY):
        return
    if str(status_id) != str(STATUS_ACADEMY_INBOUND_LEAD):
        return
    if not _is_new(lead_id):
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        # Ждём, пока источник лида дозапишет поля и привяжет контакт.
        if ACADEMY_LEAD_ALERT_DELAY_S:
            await asyncio.sleep(ACADEMY_LEAD_ALERT_DELAY_S)

        try:
            lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        except Exception:
            logger.exception("Академия-алерт: не прочиталась сделка %s", lead_id)
            _unsee(lead_id)
            return

        if not is_inbound_lead(lead):
            logger.info(
                "Академия-алерт: сделка %s уже не на «Входящем лиде» — молчим", lead_id,
            )
            _unsee(lead_id)
            return

        if not _budget_ok():
            _unsee(lead_id)
            await _report_burst()
            return

        client, phone = await _client_card(lead)
        text = _build_message(lead_id, lead, client, phone)
        name = ((lead or {}).get("name") or "").strip()
        d = alerts.decide(
            "academy_lead", legacy_text=text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID, lead=lead,
            values={
                "теги": ACADEMY_ALERT_TAG,
                "телефон": phone or "",
                "клиент": client or "",
                "сделка": name if name and name != client else "",
                "ссылка_на_сделку": alerts.lead_link(lead_id),
            },
        )
        if d is None:
            logger.info("Академия-алерт: событие выключено в панели (сделка %s)", lead_id)
            return
        ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
        if ok:
            _sent_times.append(time.time())
        else:
            # Не дошло — пусть следующий вебхук по этой сделке попробует снова.
            _unsee(lead_id)
        logger.info(
            "Академия-алерт: %s (сделка %s, клиент %s)",
            "отправлен" if ok else "НЕ отправлен", lead_id, client or "—",
        )
    except Exception:
        logger.exception("Академия-алерт: ошибка на сделке %s", lead_id)
        _unsee(lead_id)


async def _report_burst() -> None:
    """Перебор часового лимита: одно предупреждение за окно, дальше тишина.
    Молчать совсем нельзя — иначе пропажа уведомлений выглядит как поломка бота."""
    global _burst_notified
    if _burst_notified:
        return
    _burst_notified = True
    logger.warning(
        "Академия-алерт: часовой лимит %s исчерпан — уведомления приглушены",
        ACADEMY_LEAD_ALERT_HOUR_LIMIT,
    )
    d = alerts.decide(
        "academy_lead_burst",
        legacy_text=(
            f"🎓 Лидов в Академию за час пришло больше {ACADEMY_LEAD_ALERT_HOUR_LIMIT}, "
            "дальше уведомления приглушены до конца часа. Похоже на массовый перенос сделок, "
            "стоит заглянуть в воронку."
        ),
        chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
        values={"лимит": ACADEMY_LEAD_ALERT_HOUR_LIMIT},
    )
    if d is None:
        return
    await telegram_bot.send_alert(d.text, **d.send_kwargs())


async def _client_card(lead: dict) -> tuple[str | None, str | None]:
    """Имя и телефон клиента. ⚠️ Во вложенных контактах сделки amo отдаёт только id и
    is_main — ни имени, ни телефона там нет, контакт надо дочитывать отдельно."""
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    if not contacts:
        return None, None
    main = next((c for c in contacts if c.get("is_main")), contacts[0])
    try:
        contact = await amo_service.get_contact_by_id(main.get("id"))
    except Exception:
        logger.exception("Академия-алерт: контакт %s не прочитался", main.get("id"))
        return None, None
    if not contact:
        return None, None
    name = (contact.get("name") or "").strip() or None
    phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    phone = str(phone).strip() if phone else None
    return name, phone


def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(lead_id, lead: dict, client, phone) -> str:
    """Без ID в тексте (правило Кати 03.08.2026) и без точек посередине (26.08.2026)."""
    lines = [
        "🎓 Новый лид в Академии",
        ACADEMY_ALERT_TAG,
    ]
    if phone:
        lines.append(f"📞 {_esc(phone)}")
    if client:
        lines.append(f"👤 {_esc(client)}")
    name = ((lead or {}).get("name") or "").strip()
    if name and name != client:
        lines.append(f"📝 {_esc(name)}")
    lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
