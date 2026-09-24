"""Одноразовая Telegram-ссылка в сделку Академии.

Поле «Регистрация на мероприятие» живёт в контакте, а ссылка — в сделке. Поэтому
модуль принимает и вебхуки сделки (обычный путь BotHelp), и вебхуки контакта
(страховка для чистого изменения поля контакта). Никаких сообщений в клиентский
чат он не отправляет: только createChatInviteLink с member_limit=1.
"""

import asyncio
import logging

import httpx

import amo_service
from waybill_config import (
    ACADEMY_CONFERENCE_CHAT_ID,
    ACADEMY_CUTOVER_TS,
    ACADEMY_INVITE_BOT_TOKEN,
    ACADEMY_INVITE_DELAY_S,
    ACADEMY_INVITE_LINK_ENABLED,
    ACADEMY_PRACTICUM_CHAT_ID,
    FIELD_ACADEMY_CONFERENCE_LINK,
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_PRACTICUM_LINK,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set[asyncio.Task] = set()
_locks: dict[str, asyncio.Lock] = {}


def configured() -> bool:
    """Фича включена и есть минимум токен + чат практикума."""
    return bool(
        ACADEMY_INVITE_LINK_ENABLED
        and ACADEMY_INVITE_BOT_TOKEN
        and ACADEMY_PRACTICUM_CHAT_ID
    )


def _target(registration) -> tuple[str, str, int] | None:
    value = str(registration or "").strip().casefold()
    if "практикум" in value and ACADEMY_PRACTICUM_CHAT_ID:
        return "practicum", ACADEMY_PRACTICUM_CHAT_ID, FIELD_ACADEMY_PRACTICUM_LINK
    if "конференц" in value and ACADEMY_CONFERENCE_CHAT_ID:
        return "conference", ACADEMY_CONFERENCE_CHAT_ID, FIELD_ACADEMY_CONFERENCE_LINK
    return None


def _main_contact_id(lead: dict):
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    for contact in contacts:
        if contact.get("is_main"):
            return contact.get("id")
    return contacts[0].get("id") if contacts else None


def _linked_lead_ids(contact: dict) -> list[int]:
    ids: list[int] = []
    for lead in ((contact.get("_embedded") or {}).get("leads")) or []:
        try:
            ids.append(int(lead["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return ids


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def on_lead_change(lead_id) -> None:
    if configured() and lead_id is not None:
        _spawn(process_lead(lead_id, delay=ACADEMY_INVITE_DELAY_S))


def on_contact_change(contact_id, changed_field_ids: set[int] | None = None) -> None:
    if (
        configured()
        and contact_id is not None
        and (changed_field_ids is None or FIELD_ACADEMY_EVENT_REGISTRATION in changed_field_ids)
    ):
        _spawn(process_contact(contact_id, delay=ACADEMY_INVITE_DELAY_S))


async def _telegram(method: str, payload: dict) -> dict | None:
    url = f"https://api.telegram.org/bot{ACADEMY_INVITE_BOT_TOKEN}/{method}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(url, json=payload)
        data = response.json()
    except Exception:
        logger.exception("Академия-ссылки: Telegram API недоступен (%s)", method)
        return None
    if response.status_code != 200 or not data.get("ok"):
        # Ответ Telegram может содержать техническую причину, но URL/токен не логируем.
        logger.warning(
            "Академия-ссылки: Telegram API %s вернул %s (%s)",
            method, response.status_code, data.get("description") or "без описания",
        )
        return None
    return data.get("result") or {}


async def _create_link(chat_id: str, lead_id) -> str | None:
    result = await _telegram(
        "createChatInviteLink",
        {
            "chat_id": chat_id,
            "name": f"academy lead {lead_id}",
            "member_limit": 1,
        },
    )
    return str((result or {}).get("invite_link") or "").strip() or None


async def _revoke_link(chat_id: str, link: str) -> None:
    await _telegram("revokeChatInviteLink", {"chat_id": chat_id, "invite_link": link})


async def process_contact(contact_id, *, delay: float = 0) -> str:
    """Обработать изменение контакта и все его сделки Академии."""
    if not configured():
        return "disabled"
    if delay:
        await asyncio.sleep(delay)
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if not contact:
        return "amo_silent"
    lead_ids = _linked_lead_ids(contact)
    if not lead_ids:
        return "no_leads"
    outcomes = [await process_lead(lead_id, contact=contact) for lead_id in lead_ids]
    return "written" if "written" in outcomes else outcomes[0]


async def process_lead(lead_id, *, delay: float = 0, contact: dict | None = None) -> str:
    """Создать ровно одну ссылку и записать её в пустое поле сделки.

    Возвраты стабильны для наблюдаемости/тестов: disabled, no_lead, other_pipeline,
    no_contact, no_registration, unsupported_event, already_filled, telegram_error,
    written, patch_error.
    """
    if not configured():
        return "disabled"
    if delay:
        await asyncio.sleep(delay)

    key = str(lead_id)
    lock = _locks.setdefault(key, asyncio.Lock())
    try:
        async with lock:
            lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
            if not lead:
                return "no_lead"
            if str(lead.get("pipeline_id")) != str(PIPELINE_ACADEMY):
                return "other_pipeline"
            if not ACADEMY_CUTOVER_TS or int(lead.get("created_at") or 0) < ACADEMY_CUTOVER_TS:
                return "before_cutover"

            contact_id = _main_contact_id(lead)
            if not contact_id:
                return "no_contact"
            if not contact or str(contact.get("id")) != str(contact_id):
                contact = await amo_service.get_contact_by_id(contact_id)
            if not contact:
                return "no_contact"

            registration = amo_service.get_custom_field_value(
                contact, FIELD_ACADEMY_EVENT_REGISTRATION,
            )
            if not str(registration or "").strip():
                return "no_registration"
            target = _target(registration)
            if not target:
                return "unsupported_event"
            event_kind, chat_id, link_field_id = target

            if str(amo_service.get_custom_field_value(lead, link_field_id) or "").strip():
                return "already_filled"

            link = await _create_link(chat_id, lead_id)
            if not link:
                return "telegram_error"
            result = await amo_service.patch_lead(lead_id, custom_fields={link_field_id: link})
            if result.get("ok"):
                logger.info(
                    "Академия-ссылки: одноразовая ссылка %s записана в сделку %s",
                    event_kind, lead_id,
                )
                return "written"

            # Не оставляем действующую бесхозную ссылку, если amo не приняла запись.
            await _revoke_link(chat_id, link)
            logger.warning("Академия-ссылки: amo не приняла ссылку в сделку %s", lead_id)
            return "patch_error"
    finally:
        if not lock.locked():
            _locks.pop(key, None)
