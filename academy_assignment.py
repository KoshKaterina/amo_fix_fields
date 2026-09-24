"""Временное распределение новых лидов Академии на одного ответственного."""

import asyncio
import logging

import amo_service
from waybill_config import (
    ACADEMY_ASSIGNMENT_ENABLED,
    ACADEMY_ASSIGNMENT_DELAY_S,
    ACADEMY_CUTOVER_TS,
    ACADEMY_RESPONSIBLE_USER_ID,
    PIPELINE_ACADEMY,
)

logger = logging.getLogger("uvicorn")
_bg_tasks: set[asyncio.Task] = set()


def assign_bg(lead_id, pipeline_id, status_id) -> None:
    """На любом событии новой сделки Академии удерживать ответственного Артёма."""
    if not ACADEMY_ASSIGNMENT_ENABLED or lead_id is None:
        return
    if str(pipeline_id) != str(PIPELINE_ACADEMY):
        return
    task = asyncio.create_task(apply(lead_id, delay=ACADEMY_ASSIGNMENT_DELAY_S))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def apply(lead_id, *, delay: float = 0) -> str:
    if delay:
        await asyncio.sleep(delay)
    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        return "no_lead"
    if str(lead.get("pipeline_id")) != str(PIPELINE_ACADEMY):
        return "other_pipeline"
    if not ACADEMY_CUTOVER_TS or int(lead.get("created_at") or 0) < ACADEMY_CUTOVER_TS:
        return "before_cutover"
    if str(lead.get("responsible_user_id")) == str(ACADEMY_RESPONSIBLE_USER_ID):
        return "already_assigned"
    result = await amo_service.patch_lead(
        lead_id, responsible_user_id=ACADEMY_RESPONSIBLE_USER_ID,
    )
    if result.get("ok"):
        logger.info(
            "Академия-распределение: сделка %s назначена пользователю %s",
            lead_id, ACADEMY_RESPONSIBLE_USER_ID,
        )
        return "assigned"
    logger.warning("Академия-распределение: не назначилась сделка %s", lead_id)
    return "patch_error"
