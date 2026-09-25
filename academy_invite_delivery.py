"""Fail-closed Academy invite delivery through the exact Wazzup channel."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path

import httpx

import academy_invite_link
import amo_service
from waybill_config import (
    ACADEMY_INVITE_MESSAGE_DELAY_S,
    ACADEMY_INVITE_SEND_ENABLED,
    ACADEMY_INVITE_SENT_PATH,
    ACADEMY_INVITE_WAZZUP_CHANNEL_ID,
    ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID,
    ACADEMY_MANAGER_FIRST_NAME,
    FIELD_ACADEMY_PRACTICUM_LINK,
    PIPELINE_ACADEMY,
    STATUS_ACADEMY_RECORDED_PRACTICUM,
    WAZZUP_API_KEY,
    WAZZUP_API_URL,
)

logger = logging.getLogger(__name__)
_tasks: set[asyncio.Task] = set()
_locks: dict[str, asyncio.Lock] = {}
_PRE_INVITE_STATUSES = {87654850, 88838378, 88838382, 88838386}
_EXPLICIT_PRACTICUM_ACTION = "записаться на практикум"


def configured() -> bool:
    return bool(ACADEMY_INVITE_SEND_ENABLED and WAZZUP_API_KEY
                and ACADEMY_INVITE_WAZZUP_CHANNEL_ID
                and ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID)


def _sent_path() -> Path:
    return Path(ACADEMY_INVITE_SENT_PATH)


def _read_sent() -> set[str]:
    try:
        data = json.loads(_sent_path().read_text(encoding="utf-8"))
        return {str(x) for x in data} if isinstance(data, list) else set()
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return set()


def _write_sent(values: set[str]) -> None:
    path = _sent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(sorted(values), ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _first_name(payload: dict) -> str:
    value = str(payload.get("first_name") or payload.get("name") or "").strip()
    return value.split()[0] if value else ""


def _message(payload: dict, link: str) -> str:
    greeting = f"Здравствуйте, {_first_name(payload)}!" if _first_name(payload) else "Здравствуйте!"
    return (
        f"{greeting}\n"
        f"Меня зовут {ACADEMY_MANAGER_FIRST_NAME}, менеджер академии Sunscrypt.\n\n"
        f"Добавляйтесь в чат практикума по ссылке: {link}\n\n"
        "Если у Вас остались какие-то вопросы или нужна будет помощь - обращайтесь, я на связи!"
    )


def _valid_link(value: object) -> str:
    link = str(value or "").strip()
    return link if re.fullmatch(r"https://t\.me/\S+", link) else ""


def _phone(payload: dict) -> str:
    digits = re.sub(r"\D", "", str(payload.get("phone") or ""))
    return digits if 10 <= len(digits) <= 15 else ""


def _is_explicit_request(payload: dict) -> bool:
    """Only the dedicated BotHelp CTA may start client delivery.

    Registration is deliberately insufficient: it is also present on historical
    profiles and on contacts who asked the manager to call instead.
    """
    value = payload.get("действие менеджера") or payload.get("manager_action")
    return str(value or "").strip().casefold() == _EXPLICIT_PRACTICUM_ACTION


async def _request(method: str, path: str, *, body: dict | None = None) -> httpx.Response | None:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            return await client.request(
                method, f"{WAZZUP_API_URL}{path}",
                headers={"Authorization": f"Bearer {WAZZUP_API_KEY}"}, json=body,
            )
    except Exception:
        logger.exception("Академия-приглашения: Wazzup API недоступен")
        return None


async def _channel_is_exact() -> bool:
    response = await _request("GET", "/channels")
    if response is None or response.status_code != 200:
        return False
    try:
        channels = response.json()
    except ValueError:
        return False
    if isinstance(channels, dict):
        channels = channels.get("channels") or channels.get("data") or []
    for channel in channels if isinstance(channels, list) else []:
        if str(channel.get("channelId") or channel.get("id") or "") != ACADEMY_INVITE_WAZZUP_CHANNEL_ID:
            continue
        plain = str(channel.get("plainId") or channel.get("plain_id") or "")
        state = str(channel.get("state") or channel.get("status") or "").lower()
        transport = str(channel.get("transport") or channel.get("type") or "").lower()
        return plain == ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID and state in ("active", "") and transport == "tgapi"
    return False


async def _send(payload: dict, lead_id: int, link: str) -> str:
    phone = _phone(payload)
    if not phone or not await _channel_is_exact():
        return "channel_or_recipient_invalid"
    response = await _request("POST", "/message", body={
        "channelId": ACADEMY_INVITE_WAZZUP_CHANNEL_ID,
        "chatType": "telegram", "phone": phone,
        "text": _message(payload, link),
        "crmMessageId": f"academy-practicum-{lead_id}",
    })
    if response is None:
        return "message_error"
    if response.status_code in (200, 201, 202):
        return "sent"
    if response.status_code == 400 and "repeatedCrmMessageId" in response.text:
        return "already_sent"
    logger.warning("Академия-приглашения: Wazzup API вернул %s", response.status_code)
    return "message_error"


async def process(payload: dict, lead_id: int, *, delay: float = 0) -> str:
    if not configured():
        return "disabled"
    if not _is_explicit_request(payload):
        return "not_explicit_request"
    if "практикум" not in str(payload.get("Регистрация на мероприятие") or "").casefold():
        return "not_practicum"
    if delay:
        await asyncio.sleep(delay)
    lock_key = str(lead_id)
    sent_key = f"wazzup:{lead_id}"
    lock = _locks.setdefault(lock_key, asyncio.Lock())
    try:
        async with lock:
            sent = _read_sent()
            if sent_key in sent:
                return "already_sent"
            lead = await amo_service.get_lead_full(lead_id, with_=())
            link = _valid_link(amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK))
            if not link:
                result = await academy_invite_link.process_lead(lead_id)
                if result not in ("written", "already_filled"):
                    return f"link_{result}"
                # Mandatory readback: never trust the amo PATCH response alone.
                lead = await amo_service.get_lead_full(lead_id, with_=())
                link = _valid_link(amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK))
            if not link:
                return "link_missing"

            current_status = int((lead or {}).get("status_id") or 0)
            if current_status != STATUS_ACADEMY_RECORDED_PRACTICUM:
                if current_status not in _PRE_INVITE_STATUSES:
                    return "stage_guard"
                patched = await amo_service.patch_lead(
                    lead_id,
                    status_id=STATUS_ACADEMY_RECORDED_PRACTICUM,
                    pipeline_id=PIPELINE_ACADEMY,
                )
                if not patched.get("ok"):
                    return "stage_error"

            # Final amo readback: stage and the exact link must both survive.
            lead = await amo_service.get_lead_full(lead_id, with_=())
            confirmed_link = _valid_link(
                amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK)
            )
            if (
                confirmed_link != link
                or int((lead or {}).get("status_id") or 0) != STATUS_ACADEMY_RECORDED_PRACTICUM
            ):
                return "stage_or_link_unconfirmed"
            result = await _send(payload, lead_id, link)
            if result == "sent":
                sent.add(sent_key)
                _write_sent(sent)
                logger.info("Академия-приглашения: Wazzup принял сообщение по сделке %s", lead_id)
            return result
    finally:
        if not lock.locked():
            _locks.pop(lock_key, None)


def schedule(payload: dict, lead_id: int) -> None:
    if not configured() or not _is_explicit_request(payload):
        return
    task = asyncio.create_task(process(dict(payload), int(lead_id), delay=ACADEMY_INVITE_MESSAGE_DELAY_S))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
