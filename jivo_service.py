"""Мост Jivo → amoCRM (замена связки через Albato).

Jivo (CRM Settings → CRM Webhooks) шлёт POST на /jivo/<secret>. На завершение
чата (event_name=chat_finished) или оффлайн-сообщение, ЕСЛИ у посетителя есть
телефон или email, повторяем сценарий Albato:
    1. контакт — дедуп по телефону, затем по email; создаём при отсутствии;
    2. сделка — новая, в CLEVER Основная → Неразобранное (дальше её разводит
       штатная автоматика воронки, как и прочие лиды из «Неразобранное»);
    3. примечание — полная история переписки, кладётся в ленту сделки.

Фиче-флаг JIVO_WEBHOOK_ENABLED (как WOO_STATUS_SYNC_ENABLED): пока выключен —
эндпоинт отвечает Jivo 200, но ничего не создаёт. Секрет в пути URL заменяет
отсутствующую у Jivo подпись вебхука.
"""

import logging
import os
import time

import api
from waybill_config import PIPELINE_CLEVER

logger = logging.getLogger("uvicorn")

JIVO_ENABLED = os.getenv("JIVO_WEBHOOK_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
JIVO_WEBHOOK_SECRET = os.getenv("JIVO_WEBHOOK_SECRET", "").strip()
JIVO_PIPELINE_ID = int(os.getenv("JIVO_PIPELINE_ID", str(PIPELINE_CLEVER)))
# Лид создаётся через Unsorted API → попадает в системный статус «Неразобранное»
# (type=1) этой воронки; отдельный status_id здесь не задаётся.

# Какие события Jivo обрабатываем. chat_finished — основной триггер Albato.
HANDLED_EVENTS = {"chat_finished", "offline_message"}

_TYPE_PREFIX = {
    "visitor": "Клиент",
    "client": "Клиент",
    "agent": "Оператор",
    "operator": "Оператор",
    "bot": "Бот",
    "system": "Система",
}


def is_enabled() -> bool:
    return JIVO_ENABLED and bool(JIVO_WEBHOOK_SECRET)


def secret_ok(token: str) -> bool:
    return bool(JIVO_WEBHOOK_SECRET) and token == JIVO_WEBHOOK_SECRET


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _format_transcript(messages) -> str:
    if not isinstance(messages, list):
        return ""
    lines = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        mtype = _clean(msg.get("type")).lower()
        who = _TYPE_PREFIX.get(mtype, mtype or "—")
        text = _clean(msg.get("message") or msg.get("body") or msg.get("text"))
        if text:
            lines.append(f"{who}: {text}")
    return "\n".join(lines)


def parse_event(event: dict) -> dict | None:
    """Достаёт из события Jivo нужные поля. Возвращает None, если событие не
    обрабатываем или у посетителя нет ни телефона, ни email («только когда
    есть контакт»)."""
    if not isinstance(event, dict):
        return None

    event_name = _clean(event.get("event_name"))
    if event_name and event_name not in HANDLED_EVENTS:
        return None

    visitor = event.get("visitor") or event.get("client") or {}
    if not isinstance(visitor, dict):
        visitor = {}

    phone = _clean(visitor.get("phone"))
    email = _clean(visitor.get("email"))
    if not phone and not email:
        return None

    name = _clean(visitor.get("name")) or phone or email

    messages = event.get("messages")
    if not isinstance(messages, list):
        chat = event.get("chat")
        messages = chat.get("messages") if isinstance(chat, dict) else None

    page = event.get("page")
    page_url = _clean(page.get("url")) if isinstance(page, dict) else ""

    return {
        "event_name": event_name or "chat",
        "name": name,
        "phone": phone,
        "email": email,
        "transcript": _format_transcript(messages),
        "page_url": page_url,
        "chat_id": _clean(event.get("chat_id")),
    }


def _build_contact(name: str, phone: str, email: str, contact_id: int | None) -> dict:
    """Встроенный контакт для Unsorted: ссылка на дубль либо новый с PHONE/EMAIL."""
    if contact_id:
        return {"id": int(contact_id)}
    custom_fields = []
    if phone:
        custom_fields.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]})
    if email:
        custom_fields.append({"field_code": "EMAIL", "values": [{"value": email, "enum_code": "WORK"}]})
    contact = {"name": name or phone or email or "Клиент Jivo"}
    if custom_fields:
        contact["custom_fields_values"] = custom_fields
    return contact


async def process_jivo_chat(payload: dict) -> None:
    """Дедуп контакта → заявка в «Неразобранное» → примечание с перепиской.
    Вызывается из очереди (фоном)."""
    phone = payload.get("phone") or ""
    email = payload.get("email") or ""
    name = payload.get("name") or phone or email or "Клиент Jivo"

    # 1) дедуп контакта (как в Albato — по телефону, затем по email)
    contact_id = None
    if phone:
        contact_id = await api.find_contact_id(phone)
    if not contact_id and email:
        contact_id = await api.find_contact_id(email)
    if contact_id:
        logger.info("Jivo: найден существующий контакт %s (по %s)", contact_id, phone or email)

    # 2) заявка в «Неразобранное» (Unsorted) с встроенным контактом
    contact = _build_contact(name, phone, email, contact_id)
    source_uid = f"jivo-{payload.get('chat_id') or phone or email}"
    lead_id, new_contact_id = await api.create_unsorted_lead(
        lead_name=f"Онлайн-чат Jivo — {name}",
        pipeline_id=JIVO_PIPELINE_ID,
        contact=contact,
        source_uid=source_uid,
        page_url=payload.get("page_url") or "",
        created_ts=int(time.time()),
    )
    if not lead_id:
        logger.error("Jivo: не удалось создать заявку в Неразобранное (контакт %s)", contact_id)
        return
    logger.info("Jivo: создана заявка %s (контакт %s)", lead_id, contact_id or new_contact_id)

    # 3) примечание с историей переписки в ленте сделки
    parts = ["💬 Онлайн-чат Jivo"]
    if payload.get("page_url"):
        parts.append(f"Страница: {payload['page_url']}")
    transcript = payload.get("transcript")
    parts.append("")
    parts.append(transcript if transcript else "(переписка не передана)")

    if not await api.add_note_to_lead(lead_id, "\n".join(parts)):
        logger.warning("Jivo: примечание не добавлено к сделке %s", lead_id)
