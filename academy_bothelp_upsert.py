"""Надёжная доставка профиля BotHelp в одну карточку Академии.

Штатное действие BotHelp ищет сделку только по CUID. После автоматической
склейки NOVA CUID может исчезнуть с выжившего контакта, и следующие шаги бота
молча перестают обновлять amo. Этот обработчик принимает полный webhook
BotHelp, ищет человека по CUID/телефону/email и обновляет одну открытую сделку.
"""

from __future__ import annotations

import hmac
import logging
import re
from typing import Any

import amo_service
import api
from waybill_config import (
    ACADEMY_BOTHELP_UPSERT_ENABLED,
    ACADEMY_BOTHELP_WEBHOOK_SECRET,
    ACADEMY_CUTOVER_TS,
    ACADEMY_RESPONSIBLE_USER_ID,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger(__name__)

FIELD_CUID = 573753
FIELD_PD_CONSENT = 578239
FIELD_PD_VERSION = 578241
FIELD_MARKETING_CONSENT = 578245
FIELD_MARKETING_VERSION = 578247
FIELD_EVENT = 578259
FIELD_REQUEST = 578261
FIELD_EXPERIENCE = 578263
FIELD_CAPITAL = 578265
FIELD_PURPOSE = 578267
FIELD_ACTION = 578269

STATUS_INBOUND = 87654850
STATUS_BOT_STARTED = 88838378
STATUS_QUESTIONNAIRE = 88838382
STATUS_QUESTIONNAIRE_DONE = 88838386
STATUS_RECORDED_PRACTICUM = 88835666
_BOT_STATUSES = {STATUS_INBOUND, STATUS_BOT_STARTED, STATUS_QUESTIONNAIRE, STATUS_QUESTIONNAIRE_DONE}

_CUSTOM_MAP = {
    "pd_consent": FIELD_PD_CONSENT,
    "pd_version": FIELD_PD_VERSION,
    "marketing_consent": FIELD_MARKETING_CONSENT,
    "marketing_version": FIELD_MARKETING_VERSION,
    "Регистрация на мероприятие": FIELD_EVENT,
    "Запрос пользователя": FIELD_REQUEST,
    "опыт_в_инвестициях": FIELD_EXPERIENCE,
    "размер_капитала": FIELD_CAPITAL,
    "зачем_капитал": FIELD_PURPOSE,
    "действие менеджера": FIELD_ACTION,
}


def configured() -> bool:
    return bool(ACADEMY_BOTHELP_UPSERT_ENABLED and ACADEMY_BOTHELP_WEBHOOK_SECRET)


def authorized(secret: str) -> bool:
    return configured() and hmac.compare_digest(secret or "", ACADEMY_BOTHELP_WEBHOOK_SECRET)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _phone(value: Any) -> str:
    digits = re.sub(r"\D", "", _text(value))
    return digits[-10:] if len(digits) >= 10 else digits


def _field(contact: dict, field_id: int) -> str:
    for item in contact.get("custom_fields_values") or []:
        if int(item.get("field_id") or 0) == field_id:
            vals = item.get("values") or []
            return _text(vals[0].get("value")) if vals else ""
    return ""


def _multitext_values(contact: dict, code: str) -> list[dict]:
    for item in contact.get("custom_fields_values") or []:
        if item.get("field_code") == code:
            return list(item.get("values") or [])
    return []


def _merge_multitext(contact: dict, code: str, value: str) -> dict | None:
    if not value:
        return None
    existing = _multitext_values(contact, code)
    norm = _phone if code == "PHONE" else lambda x: _text(x).lower()
    if any(norm(row.get("value")) == norm(value) for row in existing):
        return None
    return {
        "field_code": code,
        "values": existing + [{"value": value, "enum_code": "WORK"}],
    }


def _payload_fields(payload: dict) -> dict[int, str]:
    out: dict[int, str] = {}
    cuid = _text(payload.get("cuid"))
    if cuid:
        out[FIELD_CUID] = cuid
    for key, field_id in _CUSTOM_MAP.items():
        value = _text(payload.get(key))
        if value:
            out[field_id] = value
    return out


async def _candidate_contacts(payload: dict) -> list[dict] | None:
    queries = []
    for value in (payload.get("cuid"), _phone(payload.get("phone")), payload.get("email")):
        value = _text(value)
        if value and value not in queries:
            queries.append(value)
    found: dict[int, dict] = {}
    for query in queries:
        rows = await amo_service.find_contacts_by_query(query, limit=25)
        if rows is None:
            return None  # fail closed: сетевой сбой не должен создать дубль
        for row in rows:
            cid = row.get("id")
            if cid:
                found[int(cid)] = row
    return list(found.values())


def _matches(contact: dict, payload: dict) -> bool:
    cuid = _text(payload.get("cuid"))
    if cuid and _field(contact, FIELD_CUID) == cuid:
        return True
    phone = _phone(payload.get("phone"))
    if phone and any(_phone(v.get("value")) == phone for v in _multitext_values(contact, "PHONE")):
        return True
    email = _text(payload.get("email")).lower()
    return bool(email and any(_text(v.get("value")).lower() == email for v in _multitext_values(contact, "EMAIL")))


async def _resolve(payload: dict) -> tuple[dict | None, dict | None, str]:
    rows = await _candidate_contacts(payload)
    if rows is None:
        return None, None, "search_failed"
    candidates = []
    for row in rows:
        full = await amo_service.get_contact_by_id(row["id"], with_=("leads",))
        if full and _matches(full, payload):
            candidates.append(full)

    best_contact = None
    best_lead = None
    for contact in candidates:
        lead_ids = [int(x["id"]) for x in (contact.get("_embedded") or {}).get("leads") or [] if x.get("id")]
        leads = await amo_service.get_leads_by_ids(lead_ids)
        open_academy = [
            lead for lead in leads
            if int(lead.get("pipeline_id") or 0) == PIPELINE_ACADEMY
            and int(lead.get("status_id") or 0) not in (142, 143)
        ]
        if open_academy:
            lead = max(open_academy, key=lambda x: (int(x.get("created_at") or 0), int(x.get("id") or 0)))
            if best_lead is None or int(lead.get("created_at") or 0) > int(best_lead.get("created_at") or 0):
                best_contact, best_lead = contact, lead

    if best_contact:
        return best_contact, best_lead, "existing"
    if candidates:
        return min(candidates, key=lambda x: int(x.get("created_at") or 0)), None, "existing_without_lead"
    return None, None, "new"


def _target_status(payload: dict, current_status: int | None) -> int | None:
    if current_status is not None and current_status not in _BOT_STATUSES:
        return None
    event = _text(payload.get("Регистрация на мероприятие")).lower()
    manager_action = _text(payload.get("действие менеджера")).lower()
    if "практикум" in event or "практикум" in manager_action:
        return STATUS_RECORDED_PRACTICUM
    answers = [_text(payload.get(k)) for k in ("опыт_в_инвестициях", "размер_капитала", "зачем_капитал")]
    if all(answers):
        return STATUS_QUESTIONNAIRE_DONE
    if any(answers):
        return STATUS_QUESTIONNAIRE
    return STATUS_BOT_STARTED if current_status in (None, STATUS_INBOUND) else None


async def process(payload: dict) -> dict[str, Any]:
    if not configured():
        return {"ok": False, "reason": "disabled"}
    contact, lead, resolution = await _resolve(payload)
    if resolution == "search_failed":
        return {"ok": False, "reason": resolution}

    name = _text(payload.get("name") or payload.get("first_name"))
    phone = _text(payload.get("phone"))
    email = _text(payload.get("email"))
    if contact is None:
        cid = await api.create_contact(name or "Подписчик BotHelp", phone, email)
        if not cid:
            return {"ok": False, "reason": "contact_create_failed"}
        contact = await amo_service.get_contact_by_id(cid, with_=("leads",)) or {"id": cid}

    fields = [
        {"field_id": fid, "values": [{"value": value}]}
        for fid, value in _payload_fields(payload).items()
    ]
    for code, value in (("PHONE", phone), ("EMAIL", email)):
        item = _merge_multitext(contact, code, value)
        if item:
            fields.append(item)
    updated = await api.update_contact(contact["id"], name=name or None, custom_fields_values=fields or None)
    if not updated:
        return {"ok": False, "reason": "contact_update_failed", "contact_id": contact["id"]}

    if lead is None:
        lead_id = await api.create_lead_direct(
            f"Лид Академии — {name or phone or email or payload.get('cuid')}",
            PIPELINE_ACADEMY,
            STATUS_BOT_STARTED,
            responsible_user_id=ACADEMY_RESPONSIBLE_USER_ID,
            contact_id=int(contact["id"]),
        )
        if not lead_id:
            return {"ok": False, "reason": "lead_create_failed", "contact_id": contact["id"]}
        lead = {"id": lead_id, "status_id": STATUS_BOT_STARTED, "created_at": ACADEMY_CUTOVER_TS}

    target = _target_status(payload, int(lead.get("status_id") or 0))
    if target:
        result = await amo_service.patch_lead(
            lead["id"], status_id=target, pipeline_id=PIPELINE_ACADEMY,
            responsible_user_id=ACADEMY_RESPONSIBLE_USER_ID,
        )
        if not result.get("ok"):
            return {"ok": False, "reason": "lead_update_failed", "contact_id": contact["id"], "lead_id": lead["id"]}

    logger.info(
        "ACADEMY_BOTHELP_UPSERT ok cuid=%s contact=%s lead=%s resolution=%s",
        _text(payload.get("cuid")), contact["id"], lead["id"], resolution,
    )
    return {"ok": True, "contact_id": contact["id"], "lead_id": lead["id"], "resolution": resolution}
