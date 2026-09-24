"""Уведомление ответственному о действии клиента Академии.

Срабатывает только если webhook amo прямо перечислил одно из двух целевых полей
контакта. Это защищает от рассылки по исторически заполненным карточкам при любом
постороннем изменении. В клиентские чаты модуль ничего не отправляет.
"""

import asyncio
import html
import logging

import amo_service
import telegram_bot
from alerts import lead_link
from tg_recipients import ACADEMY_ALERT_TAG, NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    ACADEMY_INTENT_ALERT_ENABLED,
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_MANAGER_ACTION,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")
_bg_tasks: set[asyncio.Task] = set()
_relevant = {FIELD_ACADEMY_EVENT_REGISTRATION, FIELD_ACADEMY_MANAGER_ACTION}


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
        if lead and str(lead.get("pipeline_id")) == str(PIPELINE_ACADEMY):
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


def _message(lead: dict, contact: dict, field_id: int, value: str) -> str:
    label = "Запись на мероприятие" if field_id == FIELD_ACADEMY_EVENT_REGISTRATION else "Действие клиента"
    return (
        "🎓 <b>Действие клиента в Академии</b>\n"
        f"{html.escape(_mentions(lead))}\n"
        f"👤 {html.escape(_client_name(contact))}\n"
        f"🔹 <b>{html.escape(label)}:</b> {html.escape(value)}\n"
        f'🔗 <a href="{lead_link(lead.get("id"))}">Открыть сделку</a>'
    )


async def process(contact_id, changed_field_ids: set[int]) -> str:
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if not contact:
        return "no_contact"
    lead = await _academy_lead(contact)
    if not lead:
        return "no_academy_lead"
    sent = 0
    for field_id in sorted(_relevant.intersection(changed_field_ids)):
        value = str(amo_service.get_custom_field_value(contact, field_id) or "").strip()
        if not value:
            continue
        ok = await telegram_bot.send_alert(
            _message(lead, contact, field_id, value),
            chat_id=NOTIFY_CHAT_ID,
            message_thread_id=NOTIFY_THREAD_ID,
            parse_mode="HTML",
        )
        sent += int(bool(ok))
    logger.info(
        "Академия-действие: контакт %s, сделка %s, уведомлений %s",
        contact_id, lead.get("id"), sent,
    )
    return "sent" if sent else "empty_or_failed"
