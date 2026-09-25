"""Fail-closed, durable Academy practicum invite delivery.

The local Wazzup webhook database is intentionally not used as negative proof:
it is incomplete. A send is allowed only after either a completed official
Wazzup message export for the exact Academy channel, or a short-lived explicit
Wazzup-UI review permit for the exact lead and recipient.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import io
import json
import logging
import re
import sqlite3
import time
from pathlib import Path

import httpx

import academy_invite_link
import amo_service
from waybill_config import (
    ACADEMY_INVITE_HISTORY_REVIEW_PATH,
    ACADEMY_INVITE_HISTORY_START_AT,
    ACADEMY_INVITE_MESSAGE_DELAY_S,
    ACADEMY_INVITE_OUTBOX_PATH,
    ACADEMY_INVITE_RETRY_S,
    ACADEMY_INVITE_SENT_PATH,
    ACADEMY_INVITE_SEND_ENABLED,
    ACADEMY_INVITE_WAZZUP_CHANNEL_ID,
    ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID,
    ACADEMY_WAZZUP_HISTORY_API_URL,
    ACADEMY_WAZZUP_HISTORY_TOKEN,
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_PRACTICUM_LINK,
    FIELD_PHONE,
    PIPELINE_ACADEMY,
    STATUS_ACADEMY_RECORDED_PRACTICUM,
    MANAGER_NAMES,
    WAZZUP_API_KEY,
    WAZZUP_API_URL,
)

logger = logging.getLogger(__name__)
_tasks: set[asyncio.Task] = set()
_worker: asyncio.Task | None = None
_PRE_INVITE_STATUSES = {87654850, 88838378, 88838382, 88838386}
_EXPLICIT_PRACTICUM_ACTION = "записаться на практикум"
_LEGACY_PRACTICUM_ACTION = "связаться с клиентом"
_LEGACY_PRACTICUM_INTENT_REF = "179005890770899fb909ef"
_FIELD_BOTHELP_TELEGRAM_ID = 575851
_FIELD_WAZZUP_TELEGRAM_ID = 571839
_FIELD_WAZZUP_TELEGRAM_USERNAME = 577785
_SENT_TAG = "ссылка отправлена"
_TERMINAL = {
    "accepted", "sent", "delivered", "already_sent", "skipped_history",
    "send_uncertain", "delivery_error", "recipient_claimed", "blocked",
}


def configured() -> bool:
    return bool(
        ACADEMY_INVITE_SEND_ENABLED
        and WAZZUP_API_KEY
        and ACADEMY_INVITE_WAZZUP_CHANNEL_ID
        and ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID
    )


def _phone_value(value: object) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits if 10 <= len(digits) <= 15 else ""


def _first_name(value: object) -> str:
    text = str(value or "").strip()
    return text.split()[0] if text else ""


def _message(payload: dict, link: str, manager_first_name: str) -> str:
    client = _first_name(payload.get("first_name") or payload.get("name"))
    greeting = f"Здравствуйте, {client}!" if client else "Здравствуйте!"
    return (
        f"{greeting}\n"
        f"Меня зовут {manager_first_name}, менеджер академии Sunscrypt.\n\n"
        f"Добавляйтесь в чат практикума по ссылке: {link}\n\n"
        "Если у Вас остались какие-то вопросы или нужна будет помощь - обращайтесь, я на связи!"
    )


def _valid_link(value: object) -> str:
    link = str(value or "").strip()
    return link if re.fullmatch(r"https://t\.me/\S+", link) else ""


def _legacy_lead_sent(lead_id: int) -> bool:
    """Read the pre-SQLite ledger; never rewrite or truncate it."""
    try:
        data = json.loads(Path(ACADEMY_INVITE_SENT_PATH).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return False
    return isinstance(data, list) and f"wazzup:{lead_id}" in {str(item) for item in data}


def _is_explicit_request(payload: dict) -> bool:
    if payload.get("_intent_source") == "manual_stage":
        return True
    action = str(payload.get("действие менеджера") or payload.get("manager_action") or "").strip().casefold()
    if action == _EXPLICIT_PRACTICUM_ACTION:
        return True
    ref = str(payload.get("academy_intent_ref") or payload.get("bothelp_step_ref") or "").strip()
    return action == _LEGACY_PRACTICUM_ACTION and ref == _LEGACY_PRACTICUM_INTENT_REF


def _db() -> sqlite3.Connection:
    path = Path(ACADEMY_INVITE_OUTBOX_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invite_job (
            lead_id INTEGER PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            lease_until REAL NOT NULL DEFAULT 0, last_error TEXT,
            message_id TEXT, recipient_key TEXT, tagged_at REAL, updated_at REAL NOT NULL
        )
    """)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(invite_job)")}
    for name in ("message_id", "recipient_key", "tagged_at"):
        if name not in columns:
            conn.execute(f"ALTER TABLE invite_job ADD COLUMN {name} TEXT")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invite_recipient (
            channel_id TEXT NOT NULL, recipient_key TEXT NOT NULL,
            lead_id INTEGER NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(channel_id, recipient_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invite_orphan_status (
            message_id TEXT PRIMARY KEY, status TEXT NOT NULL,
            error TEXT, updated_at REAL NOT NULL
        )
    """)
    return conn


def _enqueue(payload: dict, lead_id: int, *, delay: float) -> bool:
    now = time.time()
    with _db() as conn:
        row = conn.execute("SELECT state FROM invite_job WHERE lead_id=?", (lead_id,)).fetchone()
        if row and row["state"] in _TERMINAL:
            return False
        conn.execute(
            """INSERT INTO invite_job(lead_id,payload,state,next_attempt,updated_at)
               VALUES(?,?,?,?,?) ON CONFLICT(lead_id) DO UPDATE SET
               payload=excluded.payload, next_attempt=MIN(invite_job.next_attempt, excluded.next_attempt),
               updated_at=excluded.updated_at WHERE invite_job.state NOT IN
               ('accepted','delivered','already_sent','skipped_history','send_uncertain',
                'delivery_error','recipient_claimed','blocked','sent')""",
            (lead_id, json.dumps(payload, ensure_ascii=False), "pending", now + delay, now),
        )
    return True


def _claim(lead_id: int, payload: dict | None = None) -> tuple[str, dict | None]:
    now = time.time()
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM invite_job WHERE lead_id=?", (lead_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO invite_job(lead_id,payload,state,updated_at) VALUES(?,?,?,?)",
                (lead_id, json.dumps(payload or {}, ensure_ascii=False), "pending", now),
            )
            row = conn.execute("SELECT * FROM invite_job WHERE lead_id=?", (lead_id,)).fetchone()
        if row["state"] in _TERMINAL:
            return row["state"], None
        if float(row["lease_until"] or 0) > now:
            return "busy", None
        conn.execute(
            "UPDATE invite_job SET state='processing', attempts=attempts+1, lease_until=?, updated_at=? WHERE lead_id=?",
            (now + 300, now, lead_id),
        )
        return "claimed", json.loads(row["payload"])


def _finish(lead_id: int, state: str, error: str = "") -> None:
    terminal = state in _TERMINAL
    with _db() as conn:
        conn.execute(
            "UPDATE invite_job SET state=?, lease_until=0, next_attempt=?, last_error=?, updated_at=? WHERE lead_id=?",
            (state, 0 if terminal else time.time() + ACADEMY_INVITE_RETRY_S, error or None, time.time(), lead_id),
        )


def _claim_recipient(lead_id: int, recipient_key: str) -> bool:
    """Cross-process/cross-lead dedupe for duplicate CRM cards."""
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT lead_id FROM invite_recipient WHERE channel_id=? AND recipient_key=?",
            (ACADEMY_INVITE_WAZZUP_CHANNEL_ID, recipient_key),
        ).fetchone()
        if row and int(row["lead_id"]) != lead_id:
            return False
        conn.execute(
            "INSERT OR IGNORE INTO invite_recipient(channel_id,recipient_key,lead_id,created_at) VALUES(?,?,?,?)",
            (ACADEMY_INVITE_WAZZUP_CHANNEL_ID, recipient_key, lead_id, time.time()),
        )
        conn.execute(
            "UPDATE invite_job SET recipient_key=?, updated_at=? WHERE lead_id=?",
            (recipient_key, time.time(), lead_id),
        )
    return True


def _mark_message(lead_id: int, message_id: str) -> None:
    with _db() as conn:
        orphan = conn.execute(
            "SELECT status,error FROM invite_orphan_status WHERE message_id=?", (message_id,),
        ).fetchone()
        state = "accepted"
        error = None
        if orphan and orphan["status"] in {"sent", "delivered", "read"}:
            state = "sent" if orphan["status"] == "sent" else "delivered"
        elif orphan and orphan["status"] == "error":
            state, error = "delivery_error", orphan["error"]
        conn.execute(
            "UPDATE invite_job SET state=?, message_id=?, last_error=?, updated_at=? WHERE lead_id=?",
            (state, message_id, error, time.time(), lead_id),
        )
        conn.execute("DELETE FROM invite_orphan_status WHERE message_id=?", (message_id,))
    if state in {"sent", "delivered"}:
        _schedule_tag(lead_id)


async def _tag_sent(lead_id: int) -> bool:
    """Idempotently add the business tag only after a positive Wazzup status."""
    result = await amo_service.add_tag(lead_id, _SENT_TAG)
    if not result.get("ok"):
        return False
    with _db() as conn:
        conn.execute(
            "UPDATE invite_job SET tagged_at=?, updated_at=? WHERE lead_id=? AND state IN ('sent','delivered')",
            (time.time(), time.time(), lead_id),
        )
    return True


def _schedule_tag(lead_id: int) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(_tag_sent(lead_id))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def record_webhook(payload: dict) -> None:
    """Resolve accepted deliveries from the authoritative Wazzup status hook."""
    if not isinstance(payload, dict):
        return
    rows = []
    for key in ("messages", "statuses"):
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(x for x in value if isinstance(x, dict))
    tag_leads: set[int] = set()
    with _db() as conn:
        for row in rows:
            message_id = str(row.get("messageId") or row.get("message_id") or "").strip()
            status = str(row.get("status") or "").strip().casefold()
            if not message_id or not status:
                continue
            if status in {"sent", "delivered", "read"}:
                state = "sent" if status == "sent" else "delivered"
                cursor = conn.execute(
                    "UPDATE invite_job SET state=?, last_error=NULL, updated_at=? WHERE message_id=?",
                    (state, time.time(), message_id),
                )
                match = conn.execute(
                    "SELECT lead_id FROM invite_job WHERE message_id=?", (message_id,),
                ).fetchone()
                if match:
                    tag_leads.add(int(match["lead_id"]))
            elif status == "error":
                error = row.get("error")
                cursor = conn.execute(
                    "UPDATE invite_job SET state='delivery_error', last_error=?, updated_at=? WHERE message_id=?",
                    (json.dumps(error, ensure_ascii=False)[:2000], time.time(), message_id),
                )
            else:
                continue
            if cursor.rowcount == 0:
                conn.execute(
                    "INSERT OR REPLACE INTO invite_orphan_status(message_id,status,error,updated_at) VALUES(?,?,?,?)",
                    (message_id, status, json.dumps(row.get("error"), ensure_ascii=False)[:2000], time.time()),
                )
    for lead_id in tag_leads:
        _schedule_tag(lead_id)


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


def _manual_history_review(lead_id: int, phone: str) -> bool:
    """Accept only a short-lived, exact, explicit Wazzup-UI no-history review."""
    try:
        raw = json.loads(Path(ACADEMY_INVITE_HISTORY_REVIEW_PATH).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return False
    rows = raw.get("reviews", []) if isinstance(raw, dict) else raw
    now = time.time()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if (
            int(row.get("lead_id") or 0) == lead_id
            and _phone_value(row.get("recipient")) == phone
            and row.get("channel_id") == ACADEMY_INVITE_WAZZUP_CHANNEL_ID
            and row.get("plain_id") == ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID
            and row.get("source") == "wazzup_ui"
            and row.get("history_absent") is True
            and now <= float(row.get("expires_at") or 0)
        ):
            return True
    return False


def _csv_history_for_recipient(content: str, recipient_keys: set[str]) -> bool | None:
    """True=history found, False=authoritative absence, None=unknown schema."""
    try:
        reader = csv.DictReader(io.StringIO(content.lstrip("\ufeff")))
        headers = [str(h or "").strip().casefold() for h in (reader.fieldnames or [])]
        recipient_headers = [h for h in headers if any(
            marker in h for marker in ("phone", "username", "recipient", "chat_id", "chatid")
        )]
        if not recipient_headers:
            return None
        for row in reader:
            normalized = {str(k or "").strip().casefold(): v for k, v in row.items()}
            for header in recipient_headers:
                value = str(normalized.get(header) or "").strip().lstrip("@").casefold()
                phone_keys = {x.removeprefix("phone:") for x in recipient_keys if x.startswith("phone:")}
                username_keys = {x.removeprefix("username:") for x in recipient_keys if x.startswith("username:")}
                telegram_keys = {x.removeprefix("telegram:") for x in recipient_keys if x.startswith("telegram:")}
                if phone_keys:
                    if _phone_value(value) in phone_keys:
                        return True
                if value in username_keys:
                    return True
                if value in telegram_keys:
                    return True
        return False
    except (csv.Error, TypeError):
        return None


async def _official_history(recipient_keys: set[str]) -> str:
    if not ACADEMY_WAZZUP_HISTORY_TOKEN:
        return "unverified"
    headers = {"Authorization": f"Bearer {ACADEMY_WAZZUP_HISTORY_TOKEN}"}
    end = dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            created = await client.post(
                f"{ACADEMY_WAZZUP_HISTORY_API_URL}/messages/messages_dump",
                headers=headers,
                json={"start_at": ACADEMY_INVITE_HISTORY_START_AT, "end_at": end,
                      "channel_id": ACADEMY_INVITE_WAZZUP_CHANNEL_ID},
            )
            if created.status_code not in (200, 201, 202):
                return "unverified"
            data = created.json().get("data") or {}
            export_id = str(data.get("export_id") or "")
            if not export_id:
                return "unverified"
            for _ in range(15):
                status = await client.get(
                    f"{ACADEMY_WAZZUP_HISTORY_API_URL}/messages/messages_dump/{export_id}",
                    headers=headers,
                )
                if status.status_code != 200:
                    return "unverified"
                info = status.json().get("data") or {}
                if info.get("status") in ("done", "webhook_failed"):
                    url = str(info.get("url") or "")
                    if not url:
                        return "unverified"
                    export = await client.get(url)
                    if export.status_code != 200:
                        return "unverified"
                    found = _csv_history_for_recipient(export.text, recipient_keys)
                    return "found" if found is True else "clear" if found is False else "unverified"
                await asyncio.sleep(2)
    except Exception:
        logger.exception("Академия-приглашения: Wazzup history export недоступен")
    return "unverified"


async def _history_gate(lead_id: int, recipient_keys: set[str], phone: str) -> str:
    result = await _official_history(recipient_keys)
    if result != "unverified":
        return result
    return "clear" if _manual_history_review(lead_id, phone) else "unverified"


def _main_contact_id(lead: dict) -> int | None:
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    item = next((x for x in contacts if x.get("is_main")), contacts[0] if contacts else None)
    try:
        return int(item["id"]) if item else None
    except (KeyError, TypeError, ValueError):
        return None


async def _hydrate(payload: dict, lead: dict) -> tuple[dict | None, str]:
    contact_id = _main_contact_id(lead)
    if not contact_id:
        return None, "contact_missing"
    contact = await amo_service.get_contact_by_id(contact_id, with_=())
    if not contact:
        return None, "contact_missing"
    merged = dict(payload)
    if not str(merged.get("name") or "").strip():
        merged["name"] = contact.get("name") or ""
    if not _phone_value(merged.get("phone")):
        merged["phone"] = amo_service.get_custom_field_value(contact, FIELD_PHONE) or ""
    if not str(merged.get("Регистрация на мероприятие") or "").strip():
        merged["Регистрация на мероприятие"] = amo_service.get_custom_field_value(
            contact, FIELD_ACADEMY_EVENT_REGISTRATION,
        ) or ""
    phone = _phone_value(merged.get("phone"))
    if not phone:
        return None, "recipient_invalid"
    identity = await _ensure_wazzup_identity(contact_id, contact, merged, phone)
    if identity != "verified":
        return None, identity
    merged["_verified_telegram_id"] = str(
        amo_service.get_custom_field_value(contact, _FIELD_WAZZUP_TELEGRAM_ID)
        or _telegram_id(merged)
    ).strip()
    payload_username = str(
        merged.get("messenger_username") or merged.get("telegram_username") or ""
    ).strip().lstrip("@").casefold()
    contact_username = str(
        amo_service.get_custom_field_value(contact, _FIELD_WAZZUP_TELEGRAM_USERNAME) or ""
    ).strip().lstrip("@").casefold()
    if payload_username and contact_username == payload_username:
        merged["_verified_username"] = payload_username
    return merged, phone


async def _manual_stage_event_confirmed(lead_id: int, observed_at: int) -> bool:
    """Reject webhook echoes: prove a real recent transition through amo events."""
    params = [
        ("filter[type]", "lead_status_changed"),
        ("filter[created_at][from]", str(max(0, observed_at - 30))),
        ("filter[created_at][to]", str(observed_at + 300)),
        ("filter[value_after][leads_statuses][0][pipeline_id]", str(PIPELINE_ACADEMY)),
        ("filter[value_after][leads_statuses][0][status_id]", str(STATUS_ACADEMY_RECORDED_PRACTICUM)),
        ("limit", "100"),
    ]
    data = await amo_service._do_get("/api/v4/events", params)
    events = ((data or {}).get("_embedded") or {}).get("events") or []
    return any(int(event.get("entity_id") or 0) == lead_id for event in events)


def _telegram_id(payload: dict) -> str:
    # `bothelp_id` is a BotHelp subscriber id, not necessarily Telegram id.
    # Only a deliberately supplied Telegram identity may bind the amo field.
    for key in ("telegram_id", "telegram_user_id"):
        value = str(payload.get(key) or "").strip()
        if value.isdigit():
            return value
    return ""


async def _ensure_wazzup_identity(contact_id: int, contact: dict, payload: dict, phone: str) -> str:
    """Bind Wazzup to the existing contact before opening a Telegram dialog.

    Never overwrite an existing Telegram id. For an empty Wazzup id, require a
    numeric BotHelp Telegram id equal to the observed Telegram ID field on the
    amo contact. CUser and phone alone are deliberately insufficient.
    """
    wanted = _telegram_id(payload)
    current = str(amo_service.get_custom_field_value(contact, _FIELD_WAZZUP_TELEGRAM_ID) or "").strip()
    if current:
        return "verified" if not wanted or current == wanted else "telegram_identity_conflict"
    if not wanted:
        return "telegram_identity_unverified"
    stored_bothelp_id = str(
        amo_service.get_custom_field_value(contact, _FIELD_BOTHELP_TELEGRAM_ID) or ""
    ).strip()
    if not stored_bothelp_id or stored_bothelp_id != wanted:
        return "telegram_identity_unverified"
    patched = await amo_service.patch_contact(
        contact_id, custom_fields={_FIELD_WAZZUP_TELEGRAM_ID: wanted},
    )
    if not patched.get("ok"):
        return "telegram_identity_patch_failed"
    fresh = await amo_service.get_contact_by_id(contact_id, with_=())
    confirmed = str(
        amo_service.get_custom_field_value(fresh or {}, _FIELD_WAZZUP_TELEGRAM_ID) or ""
    ).strip()
    return "verified" if confirmed == wanted else "telegram_identity_readback_failed"


def _recipient_keys(payload: dict, phone: str) -> set[str]:
    # Never use a raw BotHelp Telegram id as Wazzup chatId. A current username
    # can open a new Telegram dialog; phone remains the conservative fallback.
    username = str(payload.get("_verified_username") or "")
    username = username.strip().lstrip("@").casefold()
    keys = {f"phone:{phone}"}
    if username:
        keys.add(f"username:{username}")
    telegram_id = str(payload.get("_verified_telegram_id") or "").strip()
    if telegram_id.isdigit():
        keys.add(f"telegram:{telegram_id}")
    return keys


async def _send(payload: dict, lead_id: int, link: str, manager_first_name: str) -> tuple[str, str]:
    phone = _phone_value(payload.get("phone"))
    if not phone or not await _channel_is_exact():
        return "channel_or_recipient_invalid", ""
    body = {
        "channelId": ACADEMY_INVITE_WAZZUP_CHANNEL_ID,
        "chatType": "telegram",
        "text": _message(payload, link, manager_first_name),
        "crmMessageId": f"academy-practicum-{lead_id}",
    }
    username = str(payload.get("_verified_username") or "").strip().lstrip("@")
    body["username" if username else "phone"] = username or phone
    response = await _request("POST", "/message", body=body)
    if response is None:
        return "message_uncertain", ""
    if response.status_code in (200, 201, 202):
        try:
            data = response.json()
        except ValueError:
            data = {}
        message_id = str(data.get("messageId") or data.get("message_id") or (data.get("data") or {}).get("messageId") or "")
        # Accepted is not delivered. Without a Wazzup id we cannot correlate a
        # later status, so this is an uncertain terminal result.
        return ("accepted", message_id) if message_id else ("message_uncertain", "")
    if response.status_code == 400 and "repeatedCrmMessageId" in response.text:
        return "already_sent", ""
    logger.warning("Академия-приглашения: Wazzup API вернул %s", response.status_code)
    return "message_uncertain", ""


async def process(payload: dict, lead_id: int, *, delay: float = 0) -> str:
    if not configured():
        return "disabled"
    if not _is_explicit_request(payload):
        return "not_explicit_request"
    if delay:
        await asyncio.sleep(delay)
    claim, stored = _claim(int(lead_id), payload)
    if claim in _TERMINAL:
        return "already_sent" if claim in {"sent", "already_sent"} else claim
    if claim != "claimed":
        return "busy"
    payload = stored or payload
    if _legacy_lead_sent(int(lead_id)):
        _finish(int(lead_id), "already_sent", "legacy_ledger")
        return "already_sent"

    def fail(reason: str, *, terminal: bool = False) -> str:
        state = reason if reason in {"skipped_history", "recipient_claimed"} else "blocked"
        _finish(int(lead_id), state if terminal else "retry", reason)
        return reason

    lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    if not lead:
        return fail("lead_missing")
    if int(lead.get("pipeline_id") or 0) != PIPELINE_ACADEMY:
        return fail("other_pipeline", terminal=True)
    status = int(lead.get("status_id") or 0)
    if status not in _PRE_INVITE_STATUSES | {STATUS_ACADEMY_RECORDED_PRACTICUM}:
        return fail("stage_guard", terminal=True)
    if payload.get("_intent_source") == "manual_stage" and status != STATUS_ACADEMY_RECORDED_PRACTICUM:
        return fail("manual_stage_unconfirmed")
    if payload.get("_intent_source") == "manual_stage":
        observed_at = int(payload.get("_observed_at") or 0)
        if not observed_at or not await _manual_stage_event_confirmed(int(lead_id), observed_at):
            return fail("manual_stage_event_unconfirmed", terminal=True)
    hydrated, phone_or_error = await _hydrate(payload, lead)
    if hydrated is None:
        return fail(phone_or_error)
    payload = hydrated
    if "практикум" not in str(payload.get("Регистрация на мероприятие") or "").casefold():
        return fail("not_practicum", terminal=True)
    phone = phone_or_error
    recipient_keys = _recipient_keys(payload, phone)
    # Phone is the stable cross-card identity. Username may change and is used
    # only as an additional history/send address.
    if not _claim_recipient(int(lead_id), f"phone:{phone}"):
        return fail("recipient_claimed", terminal=True)

    history = await _history_gate(int(lead_id), recipient_keys, phone)
    if history == "found":
        return fail("skipped_history", terminal=True)
    if history != "clear":
        return fail("history_unverified")

    link = _valid_link(amo_service.get_custom_field_value(lead, FIELD_ACADEMY_PRACTICUM_LINK))
    if not link:
        result = await academy_invite_link.process_lead(lead_id)
        if result not in ("written", "already_filled"):
            return fail(f"link_{result}")
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        link = _valid_link(amo_service.get_custom_field_value(lead or {}, FIELD_ACADEMY_PRACTICUM_LINK))
    if not link:
        return fail("link_missing")
    if not await academy_invite_link.verify_practicum_link(int(lead_id), link):
        return fail("link_target_unverified")

    status = int((lead or {}).get("status_id") or 0)
    if status != STATUS_ACADEMY_RECORDED_PRACTICUM:
        if status not in _PRE_INVITE_STATUSES:
            return fail("stage_guard", terminal=True)
        patched = await amo_service.patch_lead(
            lead_id, status_id=STATUS_ACADEMY_RECORDED_PRACTICUM, pipeline_id=PIPELINE_ACADEMY,
        )
        if not patched.get("ok"):
            return fail("stage_error")

    confirmed = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    confirmed_link = _valid_link(amo_service.get_custom_field_value(confirmed or {}, FIELD_ACADEMY_PRACTICUM_LINK))
    if confirmed_link != link or int((confirmed or {}).get("status_id") or 0) != STATUS_ACADEMY_RECORDED_PRACTICUM:
        return fail("stage_or_link_unconfirmed")
    try:
        manager_id = int((confirmed or {}).get("responsible_user_id"))
    except (TypeError, ValueError):
        manager_id = 0
    # Explicit amo-id mapping avoids guessing whether an arbitrary amo display
    # name is written as "first surname" or "surname first".
    manager_first = _first_name(MANAGER_NAMES.get(manager_id))
    if not manager_first:
        return fail("manager_unresolved")

    # Durable uncertainty barrier: after this write no automatic retry can send
    # again unless a human/new authoritative history review resolves the result.
    _finish(int(lead_id), "send_uncertain", "send_started")
    result, message_id = await _send(payload, int(lead_id), link, manager_first)
    if result == "accepted":
        _mark_message(int(lead_id), message_id)
        logger.info("Академия-приглашения: Wazzup accepted lead=%s message=%s", lead_id, message_id)
        return result
    if result == "already_sent":
        _finish(int(lead_id), result)
        return result
    return "send_uncertain"


async def _run_due_once() -> None:
    now = time.time()
    with _db() as conn:
        rows = conn.execute(
            """SELECT lead_id,payload FROM invite_job
               WHERE (state IN ('pending','retry') AND next_attempt<=?)
                  OR (state='processing' AND lease_until<=?)
               ORDER BY next_attempt LIMIT 10""",
            (now, now),
        ).fetchall()
    for row in rows:
        await process(json.loads(row["payload"]), int(row["lead_id"]))
    with _db() as conn:
        tag_rows = conn.execute(
            "SELECT lead_id FROM invite_job WHERE state IN ('sent','delivered') AND tagged_at IS NULL LIMIT 10"
        ).fetchall()
    for row in tag_rows:
        await _tag_sent(int(row["lead_id"]))


async def _worker_loop() -> None:
    while True:
        try:
            await _run_due_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Академия-приглашения: outbox worker error")
        await asyncio.sleep(max(5.0, ACADEMY_INVITE_RETRY_S))


def start() -> None:
    global _worker
    if configured() and (_worker is None or _worker.done()):
        _worker = asyncio.create_task(_worker_loop())


async def stop() -> None:
    global _worker
    if _worker is not None:
        _worker.cancel()
        await asyncio.gather(_worker, return_exceptions=True)
        _worker = None
    pending = list(_tasks)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    _tasks.clear()


def schedule(payload: dict, lead_id: int) -> None:
    if not configured() or not _is_explicit_request(payload):
        return
    if not _enqueue(dict(payload), int(lead_id), delay=ACADEMY_INVITE_MESSAGE_DELAY_S):
        return
    task = asyncio.create_task(process(dict(payload), int(lead_id), delay=ACADEMY_INVITE_MESSAGE_DELAY_S))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def schedule_manual_stage(lead_id: int) -> None:
    schedule({"_intent_source": "manual_stage", "_observed_at": int(time.time())}, int(lead_id))
