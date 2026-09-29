"""Уведомление ответственному о действии клиента Академии.

Срабатывает только если webhook amo прямо перечислил одно из двух целевых полей
контакта. Это защищает от рассылки по исторически заполненным карточкам при любом
постороннем изменении. В клиентские чаты модуль ничего не отправляет.

На одно касание уходит ОДНО сообщение, даже если изменились оба поля: раньше цикл
слал по сообщению на поле. Плюс дедуп по паре «поле - значение» в рамках сделки:
бот шлёт несколько касаний подряд, а склейка дублей контактов заставляет amo
отчитаться обо всех полях разом, и один человек собирал до девяти сообщений.
Имя клиента в ключ дедупа НЕ входит намеренно: у склеенных карточек оно разное
(«Руслан» и «Ruslan»), а действие одно и то же.
"""

import asyncio
import html
import json
import logging
import os
import time

import alerts
import amo_service
import telegram_bot
from alerts import lead_link
from tg_recipients import (
    ACADEMY_NOTIFY_THREAD,
    NOTIFY_CHAT_ID,
    NOTIFY_THREAD_ID,
    academy_mentions_for,
)
from waybill_config import (
    ACADEMY_CUTOVER_TS,
    ACADEMY_INTENT_ALERT_COALESCE_S,
    ACADEMY_INTENT_ALERT_ENABLED,
    ACADEMY_PANEL_FALLBACK_AMO_ID,
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_MANAGER_ACTION,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")
_bg_tasks: set[asyncio.Task] = set()
_relevant = {FIELD_ACADEMY_EVENT_REGISTRATION, FIELD_ACADEMY_MANAGER_ACTION}

# «сделка:поле:значение» → время отправки. Ключ по СДЕЛКЕ, а не по контакту:
# у склеенного человека контактов два, и по contact_id повторы не схлопнулись бы.
# Копилка склейки: сделка → {rows: {поле: значение}, lead, contact}. Живёт только в памяти
# и только на время окна - терять тут нечего, а рестарт посреди окна отдаст одно сообщение
# со следующего касания.
_pending: dict = {}
_flush_tasks: dict = {}

_seen: dict[str, float] = {}
_SEEN_CAP = 5000
_SEEN_PATH = os.getenv(
    "ACADEMY_INTENT_ALERT_SEEN_PATH", "/app/var/academy_intent_alert_seen.json")
_seen_loaded = False


def _dedup_window() -> float:
    """Окно дедупа в секундах. По умолчанию три минуты (решение Кати 29.09.2026)."""
    try:
        return max(0.0, float(os.getenv("ACADEMY_INTENT_ALERT_DEDUP_S", "180")))
    except (TypeError, ValueError):
        return 180.0


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
        logger.exception("Академия-действие: не прочитался %s — дедуп с нуля", _SEEN_PATH)


def _save_seen() -> None:
    """Сбросить дедуп на диск. Не удался — не беда, в памяти он остаётся."""
    try:
        os.makedirs(os.path.dirname(_SEEN_PATH), exist_ok=True)
        tmp = f"{_SEEN_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_seen, f)
        os.replace(tmp, _SEEN_PATH)
    except Exception:
        logger.exception("Академия-действие: не записался %s", _SEEN_PATH)


def _forget_stale(now: float) -> None:
    """Выбросить записи старше окна и подрезать словарь, если он разросся."""
    window = _dedup_window()
    for key in [k for k, ts in _seen.items() if now - ts > window]:
        _seen.pop(key, None)
    if len(_seen) > _SEEN_CAP:
        for key in sorted(_seen, key=lambda k: _seen[k])[: len(_seen) - _SEEN_CAP]:
            _seen.pop(key, None)


def _key(lead_id, field_id: int, value: str) -> str:
    return f"{lead_id}:{field_id}:{value}"


def _is_new(lead_id, field_id: int, value: str) -> bool:
    """True — про это же значение в окне ещё не писали (писать).
    False — повтор: эхо вебхука, следующее касание бота или склейка карточек."""
    _load_seen()
    now = time.time()
    _forget_stale(now)
    key = _key(lead_id, field_id, value)
    if key in _seen:
        return False
    _seen[key] = now
    _save_seen()
    return True


def _unsee(lead_id, field_id: int, value: str) -> None:
    """Снять отметку: отправки не было (Telegram отказал, бот выключен).
    Без этого следующее честное касание промолчало бы всё окно."""
    if _seen.pop(_key(lead_id, field_id, value), None) is not None:
        _save_seen()


def on_contact_change(contact_id, changed_field_ids: set[int]) -> None:
    if not ACADEMY_INTENT_ALERT_ENABLED or contact_id is None:
        return
    changed = _relevant.intersection(changed_field_ids or set())
    if not changed:
        return
    task = asyncio.create_task(process(contact_id, changed))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _lead_ids(contact: dict) -> list[int]:
    out = []
    for item in ((contact.get("_embedded") or {}).get("leads")) or []:
        try:
            out.append(int(item["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return out


async def _academy_lead(contact: dict) -> dict | None:
    candidates = []
    for lead_id in _lead_ids(contact):
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if (
            lead
            and str(lead.get("pipeline_id")) == str(PIPELINE_ACADEMY)
            and ACADEMY_CUTOVER_TS
            and int(lead.get("created_at") or 0) >= ACADEMY_CUTOVER_TS
        ):
            candidates.append(lead)
    if not candidates:
        return None
    return max(candidates, key=lambda row: int(row.get("updated_at") or 0))


def _thread() -> int | None:
    """Ветка «Уведомления академии», а нет её номера - общий топик УВЕДОМЛЕНИЯ.

    Тот же приём, что в `academy_lead_alert`: номер приходит из окружения, и до его
    появления уведомление обязано ходить туда, куда ходило раньше, а не пропасть.
    """
    return ACADEMY_NOTIFY_THREAD or NOTIFY_THREAD_ID


def _mentions(lead: dict) -> str:
    """Тег: ник ответственного, а нет его в карте - Гладков.

    ⚠️ 29.09.2026 заменено с `mentions_for`. У той фолбэк - вся смена РОЗНИЦЫ, и на
    действие клиента Академии будили розничных менеджеров. Частный случай «ответственный
    Гладков» тоже ушёл: он не в розничной карте, поэтому общий фолбэк даёт его тег сам.
    """
    return academy_mentions_for(lead.get("responsible_user_id"))


def _client_name(contact: dict) -> str:
    return str(contact.get("name") or "").strip() or "Клиент без имени"


def _label(field_id: int) -> str:
    if field_id == FIELD_ACADEMY_EVENT_REGISTRATION:
        return "Запись на мероприятие"
    return "Действие клиента"


def _message(lead: dict, contact: dict, rows: list[tuple[int, str]]) -> str:
    """Одно сообщение на касание: сколько полей изменилось, столько строк внутри."""
    lines = [
        "🎓 <b>Действие клиента в Академии</b>",
        html.escape(_mentions(lead)),
        f"👤 {html.escape(_client_name(contact))}",
    ]
    for field_id, value in rows:
        lines.append(f"🔹 <b>{html.escape(_label(field_id))}:</b> {html.escape(value)}")
    lines.append(f"🔗 {lead_link(lead.get('id'))}")
    return "\n".join(lines)


async def process(contact_id, changed_field_ids: set[int]) -> str:
    """Разбор одного вебхука: прочитать контакт, найти сделку, положить строки в копилку.

    Само сообщение уходит из `flush` - через окно склейки. Причина: бот заполняет поля по
    одному, каждое своим вебхуком, и 29.09.2026 в 15:06 два поля дали два сообщения с
    разницей в две секунды. Утренний дедуп такое не склеивает - он гасит ПОВТОР одной и той
    же пары «поле, значение», а тут пары разные.
    """
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if not contact:
        return "no_contact"
    lead = await _academy_lead(contact)
    if not lead:
        return "no_academy_lead"
    rows: dict[int, str] = {}
    for field_id in sorted(_relevant.intersection(changed_field_ids)):
        value = str(amo_service.get_custom_field_value(contact, field_id) or "").strip()
        if value:
            rows[field_id] = value
    if not rows:
        logger.info(
            "Академия-действие: контакт %s, сделка %s, нечего слать",
            contact_id, lead.get("id"),
        )
        return "empty"

    lead_id = lead.get("id")
    # Копилка по СДЕЛКЕ, а не по контакту: у склеенного человека контактов два, и по
    # contact_id касания одного и того же человека в одну копилку не попали бы.
    box = _pending.setdefault(lead_id, {"rows": {}, "lead": lead, "contact": contact})
    box["rows"].update(rows)
    box["lead"] = lead
    box["contact"] = contact          # имя берём из последнего увиденного контакта

    if ACADEMY_INTENT_ALERT_COALESCE_S <= 0:
        return await flush(lead_id)
    if lead_id not in _flush_tasks:
        task = asyncio.create_task(_flush_later(lead_id))
        _flush_tasks[lead_id] = task
        task.add_done_callback(lambda _t, lid=lead_id: _flush_tasks.pop(lid, None))
    logger.info(
        "Академия-действие: контакт %s, сделка %s, строк в копилке %s, ждём склейку %.0f с",
        contact_id, lead_id, len(box["rows"]), ACADEMY_INTENT_ALERT_COALESCE_S,
    )
    return "buffered"


async def _flush_later(lead_id) -> None:
    try:
        await asyncio.sleep(ACADEMY_INTENT_ALERT_COALESCE_S)
        await flush(lead_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Академия-действие: склейка по сделке %s упала", lead_id)


async def flush(lead_id) -> str:
    """Отправить накопленное по сделке одним сообщением.

    ⚠️ Дедуп применяется ЗДЕСЬ, а не при накоплении. Проверь его раньше - повтор внутри
    окна отметил бы пару отправленной, а само сообщение ещё не ушло.
    """
    box = _pending.pop(lead_id, None)
    if not box or not box["rows"]:
        return "empty"
    lead, contact = box["lead"], box["contact"]
    contact_id = contact.get("id")
    rows = sorted(box["rows"].items())
    fresh = [(fid, val) for fid, val in rows if _is_new(lead_id, fid, val)]
    if not fresh:
        logger.info(
            "Академия-действие: контакт %s, сделка %s, повтор в окне — молчим",
            contact_id, lead_id,
        )
        return "duplicate"

    # Событие настраивается с экрана панели (Катя 29.09.2026): чат, текст и получателей
    # решает каталог, а не этот модуль. До этого уведомление шло напрямую в топик
    # УВЕДОМЛЕНИЯ и в каталоге его не было - управлять им было негде.
    by_field = {fid: val for fid, val in fresh}
    d = alerts.decide(
        "academy_intent", legacy_text=_message(lead, contact, fresh), parse_mode="HTML",
        chat_id=NOTIFY_CHAT_ID, thread_id=_thread(), lead=lead,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "теги": _mentions(lead),
            "клиент": _client_name(contact),
            # По переменной на поле, а не одной строкой: так в шаблоне у каждой строки
            # свой заголовок жирным, а пустая строка выпадает сама. Одной переменной
            # разметка внутри значения приехала бы экранированной.
            "запись_на_мероприятие": by_field.get(FIELD_ACADEMY_EVENT_REGISTRATION, ""),
            "действие_клиента": by_field.get(FIELD_ACADEMY_MANAGER_ACTION, ""),
            "ссылка_на_сделку": lead_link(lead_id),
        },
    )
    if d is None:
        # Выключено в панели. Отметки дедупа СНИМАЕМ: включат обратно - следующее
        # честное касание должно дойти, а не молчать до конца окна.
        for fid, val in fresh:
            _unsee(lead_id, fid, val)
        logger.info(
            "Академия-действие: событие выключено в панели (контакт %s, сделка %s)",
            contact_id, lead_id,
        )
        return "disabled"

    ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
    if not ok:
        for fid, val in fresh:
            _unsee(lead_id, fid, val)
    logger.info(
        "Академия-действие: контакт %s, сделка %s, строк %s из %s, отправлено %s",
        contact_id, lead_id, len(fresh), len(rows), int(bool(ok)),
    )

    if ok:
        # Вторым каналом - лента панели, лично ответственному (ТЗ Кати 29.09.2026).
        plain, url = alerts.strip_link(d.text)
        alerts.panel_notify_bg(
            kind="academy_intent", level="info",
            title="Действие клиента в Академии",
            body=plain, url=url,
            # Ключ тот же, по чему дедупим в чате: повторная доставка не всплывёт второй раз.
            dedupe_key=f"academy_intent:{lead_id}:" + ";".join(f"{f}={v}" for f, v in fresh),
            amo_user_id=lead.get("responsible_user_id"),
            fallback_amo_user_id=ACADEMY_PANEL_FALLBACK_AMO_ID,
        )
    return "sent" if ok else "empty_or_failed"
