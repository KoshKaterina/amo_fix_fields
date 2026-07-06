"""Автотег «Срочно».

Когда менеджер ставит поле сделки «Срочность» (578127) = «Срочно», вешаем на
сделку тег «Срочно» (504609). Вызов из вебхука /lead_change (должен быть быстрым)
→ реальная работа в фоне. Идемпотентно (has_tag) и НЕ трёт существующие теги
(patch_lead с _embedded.tags заменяет весь набор → сохраняем текущие + добавляем).
"""

import asyncio
import logging

import amo_service
from waybill_config import (
    FIELD_URGENCY,
    TAG_SROCHNO_ID,
    TAG_SROCHNO_NAME,
    URGENCY_SROCHNO_VALUE,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()


def maybe_apply_bg(updates: dict, lead_id) -> None:
    """updates = leads.update.0.custom_fields из вебхука. Если среди изменённых
    полей «Срочность» стала «Срочно» — в фоне вешаем тег. Быстрый, не блокирует ответ."""
    if lead_id is None or not updates:
        return
    triggered = False
    for field in updates.values():
        if str(field.get("id")) != str(FIELD_URGENCY):
            continue
        value = (field.get("values") or {}).get("0", {}).get("value")
        if str(value or "").strip().casefold() == URGENCY_SROCHNO_VALUE.casefold():
            triggered = True
        break
    if not triggered:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            return
        if amo_service.has_tag(lead, TAG_SROCHNO_NAME):
            return  # уже есть — идемпотентно (и гасит эхо от нашего же PATCH)
        tags = amo_service.get_tags(lead) + [{"id": TAG_SROCHNO_ID}]
        await amo_service.patch_lead(lead_id, tags=tags)
        logger.info("Автотег «Срочно»: повешен на сделку %s", lead_id)
    except Exception:
        logger.exception("Автотег «Срочно»: ошибка на сделке %s", lead_id)
