"""Авто-перенос в ЗИН по мусорной причине отказа.

Когда менеджер ставит поле сделки «Причина отказа» (577623) в одно из «мусорных»
значений (Дубль сделки / Тест / Обменник / Тех поддержка / Не ЦА — см.
DUP_REASON_ENUM_IDS), автоматически переводим сделку в «Закрыто и не реализовано»
(143) в ЕЁ ТЕКУЩЕЙ воронке (143 — системный статус, есть во всех воронках).
Работает во всех воронках. Вызов из вебхука /lead_change → работа в фоне.

Идемпотентно: уже закрытую сделку (142/143) не трогаем — это же гасит эхо от нашего
собственного PATCH (после перевода в 143 повторный вебхук увидит закрытый статус).
Причину сверяем ПО enum_id через дочитывание сделки (reconciliation), не доверяя
тексту из пейлоада вебхука — устойчиво к переименованию значения.

Боевое (двигает реальные сделки) → за флагом DUP_AUTOCLOSE_ENABLED.
"""

import asyncio
import logging

import amo_service
from waybill_config import (
    DUP_AUTOCLOSE_ENABLED,
    DUP_CLOSE_STATUS_ID,
    DUP_REASON_ENUM_IDS,
    DUP_REASON_FIELD_ID,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
_CLOSED_STATUSES = {142, 143}


def maybe_close_bg(updates: dict, lead_id) -> None:
    """updates = leads.update.0.custom_fields из вебхука. Если среди изменённых
    полей «Причина отказа» — в фоне сверяем и, если значение «Дубль сделки»,
    переводим сделку в 143. Быстрый, не блокирует ответ вебхуку."""
    if not DUP_AUTOCLOSE_ENABLED or lead_id is None or not updates:
        return
    reason_changed = any(
        str(field.get("id")) == str(DUP_REASON_FIELD_ID) for field in updates.values()
    )
    if not reason_changed:
        return
    task = asyncio.create_task(_maybe_close(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _reason_enum_id(lead: dict) -> int | None:
    for f in lead.get("custom_fields_values") or []:
        if f.get("field_id") == DUP_REASON_FIELD_ID:
            values = f.get("values") or []
            if values and values[0].get("enum_id") is not None:
                try:
                    return int(values[0]["enum_id"])
                except (TypeError, ValueError):
                    return None
    return None


async def _maybe_close(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            return
        status = int(lead.get("status_id") or 0)
        if status in _CLOSED_STATUSES:
            return  # уже закрыта — идемпотентно, гасит эхо от нашего PATCH
        if _reason_enum_id(lead) not in DUP_REASON_ENUM_IDS:
            return  # причина не из мусорного набора (или уже сменили) — не трогаем
        pipeline_id = lead.get("pipeline_id")
        await amo_service.patch_lead(
            lead_id,
            status_id=DUP_CLOSE_STATUS_ID,
            pipeline_id=pipeline_id,
        )
        logger.info(
            "Причина→ЗИН: сделка %s переведена в 143 (воронка %s)", lead_id, pipeline_id
        )
    except Exception:
        logger.exception("Причина→ЗИН: ошибка на сделке %s", lead_id)
