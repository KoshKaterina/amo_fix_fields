"""Мост Jivo → amoCRM (замена связки через Albato) + триаж чатов.

Jivo (CRM Settings → CRM Webhooks) шлёт POST на /jivo/<secret>. На завершение
чата (chat_finished) / оффлайн-сообщение, ЕСЛИ у посетителя есть телефон/email:

БЕЗ триажа (JIVO_TRIAGE_ENABLED off) — прежнее поведение: контакт (дедуп) →
заявка в «Неразобранное» CLEVER → примечание с перепиской.

С триажем (JIVO_TRIAGE_ENABLED on) — инверсия по тегу:
  • тег закрытия (JIVO_CLOSE_TAGS) стоит → создать сделку и СРАЗУ закрыть
    (статус 143, ответственный = сервисный юзер, причина отказа «Пропал»);
  • тега нет → «в работу»: сделать ответственным amo-юзера, сопоставленного
    оператору Jivo (JIVO_AGENT_MAP), + задача «Связаться». Оператор не
    сопоставлен → фолбэк в «Неразобранное» (распределение разведёт само).

Все целевые id/статусы/теги — через env (см. .env), чтобы крутить без правки кода.
JIVO_LOG_PAYLOAD=true пишет сырой payload Jivo в лог (для сверки полей tags/agents).
"""

import json
import logging
import os
import time

import api
from waybill_config import PIPELINE_CLEVER

logger = logging.getLogger("uvicorn")


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


JIVO_ENABLED = _flag("JIVO_WEBHOOK_ENABLED")
JIVO_WEBHOOK_SECRET = os.getenv("JIVO_WEBHOOK_SECRET", "").strip()
JIVO_PIPELINE_ID = int(os.getenv("JIVO_PIPELINE_ID", str(PIPELINE_CLEVER)))

# --- Триаж ---------------------------------------------------------------
JIVO_TRIAGE_ENABLED = _flag("JIVO_TRIAGE_ENABLED")
JIVO_LOG_PAYLOAD = _flag("JIVO_LOG_PAYLOAD")
# Теги-«закрыть» (по тегу сделка закрывается, без тега — в работу). Регистр игнор.
JIVO_CLOSE_TAGS = {t.strip().lower() for t in os.getenv("JIVO_CLOSE_TAGS", "закрыть,без ответа,не в работу").split(",") if t.strip()}
JIVO_CLOSE_STATUS_ID = int(os.getenv("JIVO_CLOSE_STATUS_ID", "143"))        # Закрыто и не реализовано
JIVO_WORK_STATUS_ID = int(os.getenv("JIVO_WORK_STATUS_ID", "83537718"))     # Взят в работу
JIVO_SERVICE_USER_ID = int(os.getenv("JIVO_SERVICE_USER_ID", "11513202"))   # Гладков — для закрытых
JIVO_CLOSE_REASON_FIELD = int(os.getenv("JIVO_CLOSE_REASON_FIELD", "577623"))   # Причина отказа
JIVO_CLOSE_REASON_ENUM = int(os.getenv("JIVO_CLOSE_REASON_ENUM", "1041147"))    # «Пропал»
JIVO_TASK_HOURS = float(os.getenv("JIVO_TASK_HOURS", "4"))


def _load_agent_map() -> dict:
    """JIVO_AGENT_MAP — JSON {"<email|id оператора Jivo>": <amo user_id>}.
    Ключи нормализуем к нижнему регистру-строке."""
    raw = os.getenv("JIVO_AGENT_MAP", "").strip()
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return {str(k).strip().lower(): int(v) for k, v in m.items()}
    except Exception:
        logger.error("JIVO_AGENT_MAP: невалидный JSON — игнорирую")
        return {}


JIVO_AGENT_MAP = _load_agent_map()

HANDLED_EVENTS = {"chat_finished", "offline_message"}

_TYPE_PREFIX = {
    "visitor": "Клиент", "client": "Клиент",
    "agent": "Оператор", "operator": "Оператор",
    "bot": "Бот", "system": "Система",
}


def is_enabled() -> bool:
    return JIVO_ENABLED and bool(JIVO_WEBHOOK_SECRET)


def secret_ok(token: str) -> bool:
    return bool(JIVO_WEBHOOK_SECRET) and token == JIVO_WEBHOOK_SECRET


def log_payload(event) -> None:
    """Пишет сырой payload Jivo в лог при JIVO_LOG_PAYLOAD — чтобы вживую
    свериться, где приходят tags/agents (доки про chat_finished неоднозначны)."""
    if not JIVO_LOG_PAYLOAD:
        return
    try:
        logger.info("Jivo RAW payload: %s", json.dumps(event, ensure_ascii=False)[:4000])
    except Exception:
        logger.info("Jivo RAW payload (repr): %s", repr(event)[:4000])


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _format_transcript(messages) -> str:
    if not isinstance(messages, list):
        return ""
    lines = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        who = _TYPE_PREFIX.get(_clean(msg.get("type")).lower(), _clean(msg.get("type")) or "—")
        text = _clean(msg.get("message") or msg.get("body") or msg.get("text"))
        if text:
            lines.append(f"{who}: {text}")
    return "\n".join(lines)


def _extract_tags(event: dict, visitor: dict) -> list:
    """Собирает теги из всех правдоподобных мест payload (доки расходятся:
    корневой tags[], visitor.tags, chat.tags). Возвращает список в нижнем регистре."""
    chat = event.get("chat") if isinstance(event.get("chat"), dict) else {}
    out = []
    for src in (event.get("tags"), visitor.get("tags"), chat.get("tags")):
        if not isinstance(src, list):
            continue
        for t in src:
            if isinstance(t, dict):
                val = _clean(t.get("title") or t.get("name") or t.get("value"))
            else:
                val = _clean(t)
            if val:
                out.append(val.lower())
    return out


def _extract_agents(event: dict) -> list:
    """Список операторов: chat_finished.agents[] (массив) или chat_accepted.agent."""
    agents = event.get("agents")
    if not isinstance(agents, list):
        single = event.get("agent")
        agents = [single] if isinstance(single, dict) else []
    out = []
    for a in agents:
        if isinstance(a, dict):
            out.append({
                "id": _clean(a.get("id")),
                "email": _clean(a.get("email")).lower(),
                "name": _clean(a.get("name")),
            })
    return out


def parse_event(event: dict) -> dict | None:
    """Достаёт нужные поля. None — если событие не наше или у посетителя нет
    ни телефона, ни email («только когда есть контакт»)."""
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
        "tags": _extract_tags(event, visitor),
        "agents": _extract_agents(event),
    }


# --- helpers --------------------------------------------------------------

def _lead_name(name: str) -> str:
    return f"Онлайн-чат Jivo — {name}"


def _transcript_note(payload: dict) -> str:
    parts = ["💬 Онлайн-чат Jivo"]
    if payload.get("page_url"):
        parts.append(f"Страница: {payload['page_url']}")
    parts.append("")
    parts.append(payload.get("transcript") or "(переписка не передана)")
    return "\n".join(parts)


def _build_contact(name: str, phone: str, email: str, contact_id: int | None) -> dict:
    if contact_id:
        return {"id": int(contact_id)}
    cf = []
    if phone:
        cf.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]})
    if email:
        cf.append({"field_code": "EMAIL", "values": [{"value": email, "enum_code": "WORK"}]})
    contact = {"name": name or phone or email or "Клиент Jivo"}
    if cf:
        contact["custom_fields_values"] = cf
    return contact


async def _dedup_contact(phone: str, email: str) -> int | None:
    contact_id = None
    if phone:
        contact_id = await api.find_contact_id(phone)
    if not contact_id and email:
        contact_id = await api.find_contact_id(email)
    return contact_id


async def _ensure_contact(contact_id: int | None, name: str, phone: str, email: str) -> int | None:
    if contact_id:
        return contact_id
    return await api.create_contact(name, phone, email)


def _map_agent(agents: list):
    """Возвращает (amo_user_id|None, agent_dict|None) — ищем оператора в
    JIVO_AGENT_MAP сперва по email, затем по id."""
    for a in agents:
        email = a.get("email")
        if email and email in JIVO_AGENT_MAP:
            return JIVO_AGENT_MAP[email], a
        aid = a.get("id")
        if aid and aid.lower() in JIVO_AGENT_MAP:
            return JIVO_AGENT_MAP[aid.lower()], a
    return None, (agents[0] if agents else None)


async def _create_unsorted(payload: dict, contact_id: int | None, name: str, phone: str, email: str) -> int | None:
    contact = _build_contact(name, phone, email, contact_id)
    source_uid = f"jivo-{payload.get('chat_id') or phone or email}"
    lead_id, _ = await api.create_unsorted_lead(
        lead_name=_lead_name(name),
        pipeline_id=JIVO_PIPELINE_ID,
        contact=contact,
        source_uid=source_uid,
        page_url=payload.get("page_url") or "",
        created_ts=int(time.time()),
    )
    return lead_id


# --- main ----------------------------------------------------------------

async def process_jivo_chat(payload: dict) -> None:
    """Дедуп контакта → (триаж) → сделка → примечание. Вызывается из очереди."""
    phone = payload.get("phone") or ""
    email = payload.get("email") or ""
    name = payload.get("name") or phone or email or "Клиент Jivo"
    note_text = _transcript_note(payload)

    contact_id = await _dedup_contact(phone, email)
    if contact_id:
        logger.info("Jivo: найден контакт %s (по %s)", contact_id, phone or email)

    # Триаж выключен → прежнее поведение: всё в «Неразобранное».
    if not JIVO_TRIAGE_ENABLED:
        lead_id = await _create_unsorted(payload, contact_id, name, phone, email)
        if not lead_id:
            logger.error("Jivo: заявка не создана (контакт %s)", contact_id)
            return
        logger.info("Jivo: заявка %s в Неразобранное", lead_id)
        await api.add_note_to_lead(lead_id, note_text)
        return

    tags = payload.get("tags") or []
    is_close = any(t in JIVO_CLOSE_TAGS for t in tags)

    # Бакет 1: тег закрытия → создать и сразу закрыть, без менеджера.
    if is_close:
        contact_id = await _ensure_contact(contact_id, name, phone, email)
        if not contact_id:
            logger.error("Jivo[close]: контакт не создан")
            return
        cf = None
        if JIVO_CLOSE_REASON_FIELD and JIVO_CLOSE_REASON_ENUM:
            cf = [{"field_id": JIVO_CLOSE_REASON_FIELD, "values": [{"enum_id": JIVO_CLOSE_REASON_ENUM}]}]
        lead_id = await api.create_lead_direct(
            name=_lead_name(name), pipeline_id=JIVO_PIPELINE_ID,
            status_id=JIVO_CLOSE_STATUS_ID, responsible_user_id=JIVO_SERVICE_USER_ID,
            custom_fields_values=cf, contact_id=contact_id,
        )
        if not lead_id:
            logger.error("Jivo[close]: сделка не создана")
            return
        logger.info("Jivo[close]: сделка %s закрыта по тегу %s", lead_id, tags)
        await api.add_note_to_lead(lead_id, note_text)
        return

    # Бакет 2: без тега → в работу. Сопоставляем оператора Jivo → ответственный.
    amo_user, agent = _map_agent(payload.get("agents") or [])
    if not amo_user:
        # Оператор не сопоставлен → фолбэк в «Неразобранное» (распределение разведёт).
        lead_id = await _create_unsorted(payload, contact_id, name, phone, email)
        if lead_id:
            await api.add_note_to_lead(lead_id, note_text)
        logger.warning("Jivo[work]: оператор не сопоставлен (%s) → Неразобранное, сделка %s", agent, lead_id)
        return

    contact_id = await _ensure_contact(contact_id, name, phone, email)
    if not contact_id:
        logger.error("Jivo[work]: контакт не создан")
        return
    lead_id = await api.create_lead_direct(
        name=_lead_name(name), pipeline_id=JIVO_PIPELINE_ID,
        status_id=JIVO_WORK_STATUS_ID, responsible_user_id=amo_user, contact_id=contact_id,
    )
    if not lead_id:
        logger.error("Jivo[work]: сделка не создана")
        return
    logger.info("Jivo[work]: сделка %s, ответственный %s (оператор %s)", lead_id, amo_user, agent)
    await api.add_note_to_lead(lead_id, note_text)
    complete_till = int(time.time() + JIVO_TASK_HOURS * 3600)
    await api.create_task(lead_id, "Связаться с клиентом (онлайн-чат Jivo)", amo_user, complete_till)
