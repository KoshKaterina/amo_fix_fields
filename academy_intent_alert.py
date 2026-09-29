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

import amo_service
import telegram_bot
from alerts import lead_link
from tg_recipients import ACADEMY_ALERT_TAG, NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    ACADEMY_CUTOVER_TS,
    ACADEMY_INTENT_ALERT_ENABLED,
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_MANAGER_ACTION,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")
_bg_tasks: set[asyncio.Task] = set()
_relevant = {FIELD_ACADEMY_EVENT_REGISTRATION, FIELD_ACADEMY_MANAGER_ACTION}

# «сделка:поле:значение» → время отправки. Ключ по СДЕЛКЕ, а не по контакту:
# у склеенного человека контактов два, и по contact_id повторы не схлопнулись бы.
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


def _mentions(lead: dict) -> str:
    # В Академии Саша может быть ответственным, хотя из общей розничной карты он
    # временно исключён. Для его карточек используем отдельный академический тег.
    if str(lead.get("responsible_user_id")) == "11513202":
        return ACADEMY_ALERT_TAG
    return mentions_for(lead.get("responsible_user_id"))


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
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if not contact:
        return "no_contact"
    lead = await _academy_lead(contact)
    if not lead:
        return "no_academy_lead"
    rows: list[tuple[int, str]] = []
    for field_id in sorted(_relevant.intersection(changed_field_ids)):
        value = str(amo_service.get_custom_field_value(contact, field_id) or "").strip()
        if value:
            rows.append((field_id, value))
    if not rows:
        logger.info(
            "Академия-действие: контакт %s, сделка %s, нечего слать",
            contact_id, lead.get("id"),
        )
        return "empty"

    lead_id = lead.get("id")
    fresh = [(fid, val) for fid, val in rows if _is_new(lead_id, fid, val)]
    if not fresh:
        logger.info(
            "Академия-действие: контакт %s, сделка %s, повтор в окне — молчим",
            contact_id, lead_id,
        )
        return "duplicate"

    ok = await telegram_bot.send_alert(
        _message(lead, contact, fresh),
        chat_id=NOTIFY_CHAT_ID,
        message_thread_id=NOTIFY_THREAD_ID,
        parse_mode="HTML",
    )
    if not ok:
        for fid, val in fresh:
            _unsee(lead_id, fid, val)
    logger.info(
        "Академия-действие: контакт %s, сделка %s, строк %s из %s, отправлено %s",
        contact_id, lead_id, len(fresh), len(rows), int(bool(ok)),
    )
    return "sent" if ok else "empty_or_failed"
