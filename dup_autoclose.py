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

Любая причина (18.11.2026, решение Кати): в воронках ZNR_AUTOCLOSE_ANY_REASON_PIPELINES
сделку закрывает ЛЮБОЕ значение причины, не только мусорное. Но причина остаётся в сделке
после переноса и после переоткрытия, а вебхук несёт поле целиком, поэтому немусорная
причина закрывает только если её выбрали только что: в журнале amo есть событие
custom_field_577623_value_changed по этой сделке не старше ZNR_AUTOCLOSE_FRESH_WINDOW_S,
и выбранное в нём значение совпадает с текущим. Иначе 124 сделки Листа ожидания с
причиной «Лист ожидания» закрылись бы при первой правке карточки.
"""

import asyncio
import logging
import time

import amo_service
from waybill_config import (
    DUP_AUTOCLOSE_ENABLED,
    DUP_CLOSE_STATUS_ID,
    DUP_REASON_ENUM_IDS,
    DUP_REASON_FIELD_ID,
    ZNR_AUTOCLOSE_ANY_REASON_PIPELINES,
    ZNR_AUTOCLOSE_FRESH_WINDOW_S,
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


async def _events(lead_id, event_type: str, since: int) -> list | None:
    params = [
        ("filter[entity]", "lead"),
        ("filter[entity_id]", str(lead_id)),
        ("filter[type]", event_type),
        ("filter[created_at][from]", str(since)),
        ("limit", "100"),
    ]
    d = await amo_service._do_get("/api/v4/events", params)
    if d is None:
        return None
    return ((d or {}).get("_embedded") or {}).get("events") or []


async def _reason_chosen_recently(lead_id, enum_id: int) -> bool:
    """Причину enum_id выбрали в этой сделке недавно И после этого сделка не меняла
    этап или воронку.

    Свежесть: событие custom_field_577623_value_changed с этим значением не старше
    ZNR_AUTOCLOSE_FRESH_WINDOW_S (событие приходит раньше вебхука: 18.11.2026 на
    36602317 смена поля 07:34:42, наш перевод 07:34:44).

    «После этого этап не менялся» (поймано на выкатке 18.11.2026): перенос из ЗНР
    розницы в Лист ожидания сам рождает вебхук с полем причины, а причина выбрана
    минуту назад — без этой проверки сделку закрыло бы уже в Листе ожидания.
    Смена этапа в ту же секунду тоже считается «после»: лишнее закрытие хуже
    пропущенного, менеджер закроет сам. Журнал не прочитался — не закрываем."""
    since = int(time.time()) - ZNR_AUTOCLOSE_FRESH_WINDOW_S
    reason_events = await _events(lead_id, f"custom_field_{DUP_REASON_FIELD_ID}_value_changed", since)
    if not reason_events:
        return False
    chosen_at = None
    for e in reason_events:
        for v in e.get("value_after") or []:
            cfv = (v or {}).get("custom_field_value") or {}
            try:
                if int(cfv.get("enum_id")) == enum_id:
                    ts = int(e.get("created_at") or 0)
                    chosen_at = ts if chosen_at is None else max(chosen_at, ts)
            except (TypeError, ValueError):
                continue
    if chosen_at is None:
        return False
    status_events = await _events(lead_id, "lead_status_changed", chosen_at)
    if status_events is None:
        return False
    return not any(int(e.get("created_at") or 0) >= chosen_at for e in status_events)


async def _maybe_close(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            return
        status = int(lead.get("status_id") or 0)
        if status in _CLOSED_STATUSES:
            return  # уже закрыта — идемпотентно, гасит эхо от нашего PATCH
        reason = _reason_enum_id(lead)
        pipeline_id = lead.get("pipeline_id")
        if reason is None:
            return  # причину сняли — не трогаем
        if reason not in DUP_REASON_ENUM_IDS:
            # Немусорная причина: только в воронках из списка и только свежий выбор.
            try:
                in_scope = int(pipeline_id) in ZNR_AUTOCLOSE_ANY_REASON_PIPELINES
            except (TypeError, ValueError):
                in_scope = False
            if not in_scope:
                return
            if not await _reason_chosen_recently(lead_id, reason):
                logger.info(
                    "Причина→ЗИН: сделка %s, причина %s выбрана давно — не закрываем",
                    lead_id, reason,
                )
                return
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
