"""Синхронизация статусов СДЭК → этапы воронки «офис».

Два канала:
  1. Вебхуки СДЭК ORDER_STATUS (подписка оформляется при старте, если задан
     CDEK_WEBHOOK_URL) — события кладутся в очередь с низшим приоритетом.
  2. Фоновый опрос-страховка: раз в CDEK_SYNC_POLL_INTERVAL_S проходит по
     сделкам воронки «офис» в отслеживаемых этапах и досинхронизирует
     пропущенные вебхуки.

Правила перемещения:
  — сделки вне воронки «офис» не трогаем, даже если трек-номер заполнен;
  — двигаем только вперёд по воронке (по sort этапа);
  — сделки в финальных этапах (Успешно/Закрыто) не трогаем.
"""

import asyncio
import logging

import amo_service
import cdek_client
from queue_manager import enqueue_cdek_sync
from waybill_config import (
    CDEK_STATUS_TO_STAGE,
    CDEK_SYNC_POLL_INTERVAL_S,
    CDEK_WEBHOOK_URL,
    FIELD_CDEK_ORDER_NUMBER,
    STAGE_DELIVERED,
    STAGE_NOT_DELIVERED,
    STATUS_CREATE_WAYBILL,
    SYNC_POLL_STAGES,
    looks_like_uuid,
)

logger = logging.getLogger("uvicorn")

# название этапа → status_id (заполняется в init)
_stage_ids: dict[str, int] = {}
_office_pipeline_id: int | None = None
_poll_task: asyncio.Task | None = None
_enabled = False


def is_enabled() -> bool:
    return _enabled


async def init() -> None:
    """Резолв id этапов по названиям + подписка на вебхуки + старт опроса.

    Требует прогретый amo_service.warm_pipeline_cache() и cdek_client.init().
    """
    global _office_pipeline_id, _poll_task, _enabled

    _office_pipeline_id = amo_service.get_pipeline_id_for_status(STATUS_CREATE_WAYBILL)
    if _office_pipeline_id is None:
        msg = "CDEK sync: не определена воронка «офис» (pipeline cache пуст?) — синхронизация ВЫКЛЮЧЕНА"
        logger.error(msg)
        await _alert(msg)
        return

    needed = set(CDEK_STATUS_TO_STAGE.values())
    missing: list[str] = []
    for stage_name in needed:
        sid = amo_service.resolve_status_id_by_name(_office_pipeline_id, stage_name)
        if sid is None:
            missing.append(stage_name)
        else:
            _stage_ids[stage_name] = sid
    if missing:
        msg = (
            f"CDEK sync: в воронке «офис» (id={_office_pipeline_id}) не найдены этапы: "
            f"{', '.join(missing)} — синхронизация ВЫКЛЮЧЕНА"
        )
        logger.error(msg)
        await _alert(msg)
        return

    _enabled = True
    logger.info("CDEK sync: этапы воронки %s: %s", _office_pipeline_id, _stage_ids)

    await _ensure_webhook_subscription()

    if CDEK_SYNC_POLL_INTERVAL_S > 0:
        _poll_task = asyncio.create_task(_poll_loop())
        logger.info("CDEK sync: фоновый опрос каждые %s сек", CDEK_SYNC_POLL_INTERVAL_S)
    else:
        logger.info("CDEK sync: фоновый опрос выключен (CDEK_SYNC_POLL_INTERVAL_S=0)")


async def shutdown() -> None:
    global _poll_task, _enabled
    _enabled = False
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
    logger.info("CDEK sync stopped")


async def _alert(text: str) -> None:
    try:
        from telegram_bot import send_alert
        await send_alert(text)
    except Exception:
        logger.exception("CDEK sync alert failed: %s", text)


# ---------------------------------------------------------------------------
# Подписка на вебхуки СДЭК
# ---------------------------------------------------------------------------

async def _ensure_webhook_subscription() -> None:
    if not CDEK_WEBHOOK_URL:
        logger.warning("CDEK sync: CDEK_WEBHOOK_URL не задан — вебхуки не подписаны, работает только опрос")
        return
    try:
        hooks = await cdek_client.get_webhooks()
        for h in hooks:
            if h.get("type") == "ORDER_STATUS" and (h.get("url") or "").rstrip("/") == CDEK_WEBHOOK_URL.rstrip("/"):
                logger.info("CDEK sync: подписка ORDER_STATUS уже существует (uuid=%s)", h.get("uuid"))
                return
        resp = await cdek_client.add_webhook(CDEK_WEBHOOK_URL, type_="ORDER_STATUS")
        logger.info("CDEK sync: подписка ORDER_STATUS оформлена: %s", resp.get("entity") or resp)
    except cdek_client.CdekError as exc:
        msg = f"CDEK sync: не удалось оформить подписку на вебхуки СДЭК: {exc}"
        logger.error(msg)
        await _alert(msg)
    except Exception:
        logger.exception("CDEK sync: неожиданная ошибка подписки на вебхуки")


# ---------------------------------------------------------------------------
# Входная точка вебхука (вызывается из webhooks.py, должна быть быстрой)
# ---------------------------------------------------------------------------

def handle_webhook_event(payload: dict) -> None:
    if not _enabled:
        logger.warning("CDEK webhook получен, но синхронизация выключена: %s", payload.get("type"))
        return
    if payload.get("type") != "ORDER_STATUS":
        logger.info("CDEK webhook type=%s — игнор", payload.get("type"))
        return
    attrs = payload.get("attributes") or {}
    if attrs.get("is_return") is True:
        logger.info("CDEK webhook: возвратный заказ %s — игнор", attrs.get("cdek_number"))
        return
    code = attrs.get("code")
    cdek_number = attrs.get("cdek_number") or attrs.get("number")
    order_uuid = attrs.get("uuid") or payload.get("uuid")
    if not code or not (cdek_number or order_uuid):
        logger.warning("CDEK webhook без code/идентификаторов: %s", payload)
        return
    if code not in CDEK_STATUS_TO_STAGE:
        logger.info("CDEK webhook: статус %s не маппится на этап — игнор (заказ %s)", code, cdek_number)
        return
    enqueue_cdek_sync({
        "cdek_number": str(cdek_number) if cdek_number else None,
        "uuid": str(order_uuid) if order_uuid else None,
        "code": code,
    })


# ---------------------------------------------------------------------------
# Обработка из очереди (низший приоритет)
# ---------------------------------------------------------------------------

async def process_sync(payload: dict) -> None:
    if not _enabled:
        return
    code = payload.get("code")
    target_stage = CDEK_STATUS_TO_STAGE.get(code or "")
    if not target_stage:
        return
    target_status_id = _stage_ids.get(target_stage)
    if target_status_id is None:
        return

    lead_id = payload.get("lead_id")
    if lead_id is not None:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        leads = [lead] if lead else []
    else:
        leads = await _find_leads(payload.get("cdek_number"), payload.get("uuid"))

    if not leads:
        logger.info(
            "CDEK sync: сделка не найдена (cdek_number=%s uuid=%s code=%s)",
            payload.get("cdek_number"), payload.get("uuid"), code,
        )
        return

    for lead in leads:
        await _move_lead(lead, target_status_id, target_stage, code)


async def _find_leads(cdek_number: str | None, order_uuid: str | None) -> list[dict]:
    """Ищет сделки, у которых поле «номер заказа СДЭК» равно номеру или UUID."""
    wanted = {v for v in (cdek_number, order_uuid) if v}
    found: dict[int, dict] = {}
    for query in wanted:
        for lead in await amo_service.find_leads_by_query(query):
            value = amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER)
            if value is not None and str(value).strip() in wanted:
                lid = lead.get("id")
                if lid is not None:
                    found[int(lid)] = lead
    if len(found) > 1:
        logger.warning(
            "CDEK sync: по заказу %s найдено %s сделок: %s",
            cdek_number or order_uuid, len(found), sorted(found),
        )
    return list(found.values())


async def _move_lead(lead: dict, target_status_id: int, target_stage: str, code: str | None) -> None:
    lead_id = lead.get("id")
    current_status = lead.get("status_id")
    current_pipeline = lead.get("pipeline_id")

    if current_pipeline != _office_pipeline_id:
        logger.info(
            "CDEK sync: сделка %s в другой воронке (%s) — не трогаем", lead_id, current_pipeline,
        )
        return
    if current_status == target_status_id:
        return

    # Финальные этапы не перезаписываем
    final_ids = {_stage_ids.get(STAGE_DELIVERED), _stage_ids.get(STAGE_NOT_DELIVERED)}
    if current_status in final_ids:
        logger.info("CDEK sync: сделка %s уже в финальном этапе — не трогаем", lead_id)
        return

    # Только вперёд по воронке
    current_sort = amo_service.get_status_sort(int(current_status)) if current_status is not None else None
    target_sort = amo_service.get_status_sort(target_status_id)
    if current_sort is None or target_sort is None or target_sort <= current_sort:
        logger.info(
            "CDEK sync: сделка %s — этап «%s» не дальше текущего (sort %s → %s), пропуск",
            lead_id, target_stage, current_sort, target_sort,
        )
        return

    res = await amo_service.patch_lead(lead_id, status_id=target_status_id)
    if res.get("ok"):
        logger.info(
            "CDEK sync: сделка %s → «%s» (статус СДЭК %s)", lead_id, target_stage, code,
        )
    else:
        logger.error(
            "CDEK sync: не удалось переместить сделку %s в «%s»: %s", lead_id, target_stage, res,
        )


# ---------------------------------------------------------------------------
# Фоновый опрос-страховка
# ---------------------------------------------------------------------------

async def _poll_loop() -> None:
    while True:
        await asyncio.sleep(CDEK_SYNC_POLL_INTERVAL_S)
        try:
            await _poll_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("CDEK sync: ошибка фонового опроса")


async def _poll_once() -> None:
    checked = 0
    enqueued = 0
    for stage_name in SYNC_POLL_STAGES:
        status_id = _stage_ids.get(stage_name)
        if status_id is None:
            continue
        leads = await amo_service.get_leads_by_status(status_id, with_=())
        for lead in leads:
            lead_id = lead.get("id")
            cdek_value = amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER)
            if not cdek_value:
                continue
            cdek_value = str(cdek_value).strip()
            checked += 1
            code = await _fetch_latest_status(cdek_value)
            if not code:
                continue
            target_stage = CDEK_STATUS_TO_STAGE.get(code)
            if not target_stage or _stage_ids.get(target_stage) == status_id:
                continue
            enqueue_cdek_sync({"lead_id": lead_id, "code": code, "cdek_number": cdek_value})
            enqueued += 1
    logger.info("CDEK sync poll: проверено %s сделок, в очередь %s", checked, enqueued)


async def _fetch_latest_status(cdek_value: str) -> str | None:
    try:
        if looks_like_uuid(cdek_value):
            entity = (await cdek_client.get_order(cdek_value)).get("entity") or {}
        else:
            entity = await cdek_client.get_order_by_cdek_number(cdek_value) or {}
    except cdek_client.CdekError as exc:
        logger.warning("CDEK sync poll: не удалось получить заказ %s: %s", cdek_value, exc)
        return None
    statuses = entity.get("statuses") or []
    if not statuses:
        return None
    latest = max(statuses, key=lambda s: (s or {}).get("date_time") or "")
    return (latest or {}).get("code")
