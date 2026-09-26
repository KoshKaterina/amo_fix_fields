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


def assign_bg(
    lead_id,
    pipeline_id,
    status_id,
    *,
    is_new: bool = False,
    initial_responsible_user_id=None,
) -> None:
    """Назначить Артёма только на действительно новую сделку Академии.

    ``initial_responsible_user_id`` берётся из события ``leads.add``. Если за
    время задержки менеджер уже успел вручную изменить ответственного, apply
    увидит расхождение и не станет перезаписывать выбор человека.
    """
    if not ACADEMY_ASSIGNMENT_ENABLED or lead_id is None:
        return
    if not is_new:
        return
    if str(pipeline_id) != str(PIPELINE_ACADEMY):
        return
    task = asyncio.create_task(apply(
        lead_id,
        delay=ACADEMY_ASSIGNMENT_DELAY_S,
        expected_responsible_user_id=initial_responsible_user_id,
    ))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def apply(
    lead_id,
    *,
    delay: float = 0,
    expected_responsible_user_id=None,
) -> str:
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
    current_responsible = lead.get("responsible_user_id")
    if expected_responsible_user_id is None and current_responsible:
        return "initial_responsible_unknown"
    if (
        expected_responsible_user_id is not None
        and str(current_responsible) != str(expected_responsible_user_id)
    ):
        return "responsible_changed"
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
