"""Ставит фактическую дату согласия Академии в текстовые поля amoCRM.

BotHelp некорректно сериализует действие «Текущая дата» для дублированного
date-поля (на живом тесте получилось 19/10/1791). Поэтому источником даты
служит серверное время MSK, а модуль реагирует только на явное изменение
одного из двух consent-полей и только для новых сделок Академии после cutover.
"""

import asyncio
import datetime
import logging

import amo_service
from waybill_config import (
    ACADEMY_CUTOVER_TS,
    FIELD_ACADEMY_MARKETING_CONSENT,
    FIELD_ACADEMY_MARKETING_DATE_TEXT,
    FIELD_ACADEMY_PD_CONSENT,
    FIELD_ACADEMY_PD_DATE_TEXT,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")
_bg_tasks: set[asyncio.Task] = set()
_MSK = datetime.timezone(datetime.timedelta(hours=3))
_TARGETS = {
    FIELD_ACADEMY_PD_CONSENT: FIELD_ACADEMY_PD_DATE_TEXT,
    FIELD_ACADEMY_MARKETING_CONSENT: FIELD_ACADEMY_MARKETING_DATE_TEXT,
}


def on_contact_change(contact_id, changed_field_ids: set[int]) -> None:
    if contact_id is None or not ACADEMY_CUTOVER_TS:
        return
    changed = set(changed_field_ids or set()).intersection(_TARGETS)
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


async def _has_new_academy_lead(contact: dict) -> bool:
    for lead_id in _lead_ids(contact):
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if (
            lead
            and str(lead.get("pipeline_id")) == str(PIPELINE_ACADEMY)
            and int(lead.get("created_at") or 0) >= ACADEMY_CUTOVER_TS
        ):
            return True
    return False


async def process(contact_id, changed_field_ids: set[int], *, now=None) -> str:
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if not contact:
        return "no_contact"
    if not await _has_new_academy_lead(contact):
        return "no_new_academy_lead"

    date_text = (now or datetime.datetime.now(_MSK)).astimezone(_MSK).strftime("%d/%m/%Y")
    patch = {}
    for consent_field_id in sorted(set(changed_field_ids or set()).intersection(_TARGETS)):
        value = str(amo_service.get_custom_field_value(contact, consent_field_id) or "").strip().lower()
        if value == "да":
            patch[_TARGETS[consent_field_id]] = date_text
    if not patch:
        return "consent_not_yes"
    result = await amo_service.patch_contact(contact_id, custom_fields=patch)
    if result.get("ok"):
        logger.info("Академия-согласия: контакт %s, дата %s, полей %s", contact_id, date_text, len(patch))
        return "stamped"
    logger.warning("Академия-согласия: не удалось записать дату контакту %s", contact_id)
    return "patch_error"
