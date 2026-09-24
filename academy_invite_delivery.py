"""Доставка персонального приглашения через канал Академии в BotHelp.

Ссылка создаётся отдельным Telegram-ботом и хранится в сделке amoCRM. Клиенту
сообщение отправляет BotHelp по CUID, поэтому оно приходит из того же канала
Академии, в котором человек проходил сценарий. Локальный журнал по lead_id
защищает от повторной отправки при ретраях webhook.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

import academy_invite_link
import amo_service
from waybill_config import (
    ACADEMY_BOTHELP_CLIENT_ID,
    ACADEMY_BOTHELP_CLIENT_SECRET,
    ACADEMY_INVITE_MESSAGE_DELAY_S,
    ACADEMY_INVITE_SENT_PATH,
    ACADEMY_MANAGER_FIRST_NAME,
    FIELD_ACADEMY_PRACTICUM_LINK,
)

logger = logging.getLogger(__name__)
_tasks: set[asyncio.Task] = set()
_locks: dict[str, asyncio.Lock] = {}


def configured() -> bool:
    return bool(ACADEMY_BOTHELP_CLIENT_ID and ACADEMY_BOTHELP_CLIENT_SECRET)


def _sent_path() -> Path:
    return Path(ACADEMY_INVITE_SENT_PATH)


def _read_sent() -> set[str]:
    try:
        data = json.loads(_sent_path().read_text(encoding="utf-8"))
        return {str(item) for item in data if item is not None} if isinstance(data, list) else set()
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return set()


def _write_sent(values: set[str]) -> None:
    path = _sent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(sorted(values), ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


async def _token() -> str | None:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://oauth.bothelp.io/oauth2/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": ACADEMY_BOTHELP_CLIENT_ID,
                    "client_secret": ACADEMY_BOTHELP_CLIENT_SECRET,
                },
            )
        data = response.json()
    except Exception:
        logger.exception("Академия-приглашения: не удалось получить токен BotHelp")
        return None
    if response.status_code != 200:
        logger.warning("Академия-приглашения: BotHelp OAuth вернул %s", response.status_code)
        return None
    return str(data.get("access_token") or "").strip() or None


async def _bothelp_request(method: str, path: str, *, body: Any, content_type: str) -> bool:
    token = await _token()
    if not token:
        return False
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.request(
                method,
                f"https://api.bothelp.io{path}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": content_type},
                json=body,
            )
    except Exception:
        logger.exception("Академия-приглашения: BotHelp API недоступен")
        return False
    if response.status_code not in (200, 201, 202, 204):
        logger.warning("Академия-приглашения: BotHelp API вернул %s", response.status_code)
        return False
    return True


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


async def process(payload: dict, lead_id: int, *, delay: float = 0) -> str:
    if not configured():
        return "disabled"
    cuid = str(payload.get("cuid") or "").strip()
    registration = str(payload.get("Регистрация на мероприятие") or "").casefold()
    if not cuid or "практикум" not in registration:
        return "not_practicum"
    if delay:
        await asyncio.sleep(delay)

    key = str(lead_id)
    lock = _locks.setdefault(key, asyncio.Lock())
    try:
        async with lock:
            sent = _read_sent()
            if key in sent:
                return "already_sent"

            lead = await amo_service.get_lead_full(lead_id, with_=())
            link = str(amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK) or "").strip()
            if not link:
                result = await academy_invite_link.process_lead(lead_id)
                if result not in ("written", "already_filled"):
                    return f"link_{result}"
                lead = await amo_service.get_lead_full(lead_id, with_=())
                link = str(amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK) or "").strip()
            if not link:
                return "link_missing"

            field_ok = await _bothelp_request(
                "PATCH",
                f"/v1/subscribers/cuid/{cuid}/customFields",
                body=[{"op": "replace", "path": "/invite_link", "value": link}],
                content_type="application/json",
            )
            if not field_ok:
                return "field_error"
            message_ok = await _bothelp_request(
                "POST",
                f"/v1/subscribers/cuid/{cuid}/messages",
                body=[{"content": _message(payload, link)}],
                content_type="application/vnd.api+json",
            )
            if not message_ok:
                return "message_error"

            sent.add(key)
            _write_sent(sent)
            logger.info("Академия-приглашения: сообщение отправлено по сделке %s", lead_id)
            return "sent"
    finally:
        if not lock.locked():
            _locks.pop(key, None)


def schedule(payload: dict, lead_id: int) -> None:
    if not configured():
        return
    task = asyncio.create_task(process(dict(payload), int(lead_id), delay=ACADEMY_INVITE_MESSAGE_DELAY_S))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
