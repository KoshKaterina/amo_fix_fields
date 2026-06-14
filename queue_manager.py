import asyncio
import datetime
import itertools
import logging
import time
from dataclasses import dataclass, field

from api import add_info_from_ms, get_lead_by_id, is_circuit_open
from help_function import get_custom_field_value, normalize_text
from memory import MAX_RETRY_ATTEMPTS

logger = logging.getLogger("uvicorn")

# Новое изменение сделки — всегда первым; всё остальное потом.
PRIORITY_NEW = 0
PRIORITY_WAYBILL = 5
PRIORITY_RETRY = 10
PRIORITY_CDEK_SYNC = 20
PRIORITY_METRIKA_SYNC = 25

RATE_LIMIT_SECONDS = 3
ECHO_COOLDOWN_SECONDS = 10

lead_last_processed: dict[str, datetime.datetime] = {}

_counter = itertools.count()


@dataclass(order=True)
class WorkItem:
    priority: int
    payload: dict = field(compare=False)
    sequence: int = field(default_factory=lambda: next(_counter))
    enqueue_time: float = field(default_factory=time.time, compare=False)


_task_queue: asyncio.PriorityQueue[WorkItem] | None = None
_worker_task: asyncio.Task | None = None
_retry_tasks: set[asyncio.Task] = set()
_pending_leads: set[str] = set()
_pending_waybills: set[str] = set()
_pending_cdek_sync: set[str] = set()
_pending_metrika_sync: set[str] = set()
_items_processed: int = 0


def init_queue() -> None:
    global _task_queue, _worker_task
    _task_queue = asyncio.PriorityQueue()
    _worker_task = asyncio.create_task(_worker())
    logger.info("Task priority queue started")


async def shutdown_queue() -> None:
    global _worker_task
    if _worker_task is not None:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None

    for task in list(_retry_tasks):
        task.cancel()
    _retry_tasks.clear()
    _pending_leads.clear()
    _pending_waybills.clear()
    _pending_cdek_sync.clear()
    _pending_metrika_sync.clear()

    if _task_queue is not None:
        remaining = 0
        while not _task_queue.empty():
            try:
                _task_queue.get_nowait()
                remaining += 1
            except asyncio.QueueEmpty:
                break
        if remaining:
            logger.warning("Drained %s items from task queue on shutdown", remaining)

    logger.info("Task priority queue stopped")


def enqueue_new(payload: dict) -> None:
    if _task_queue is None:
        logger.error("Task queue not initialized, dropping webhook for lead %s", payload.get("lead_id"))
        return
    lead_id = str(payload.get("lead_id", ""))
    if lead_id in _pending_leads:
        logger.info("Lead %s already in queue, skipping duplicate", lead_id)
        return
    _pending_leads.add(lead_id)
    _task_queue.put_nowait(WorkItem(priority=PRIORITY_NEW, payload=payload))
    logger.info("ENQUEUE lead_id=%s queue_size=%d", lead_id, _task_queue.qsize())


def enqueue_retry(payload: dict) -> None:
    if _task_queue is None:
        logger.error("Task queue not initialized, dropping retry for lead %s", payload.get("lead_id"))
        return
    lead_id = str(payload.get("lead_id", ""))
    if lead_id in _pending_leads:
        logger.info("Lead %s already in queue, skipping retry duplicate", lead_id)
        return
    _pending_leads.add(lead_id)
    _task_queue.put_nowait(WorkItem(priority=PRIORITY_RETRY, payload=payload))


def enqueue_waybill(lead_id, source: str = "webhook") -> None:
    if _task_queue is None:
        logger.error("Task queue not initialized, dropping waybill for lead %s", lead_id)
        return
    key = str(lead_id)
    if key in _pending_waybills:
        logger.info("Lead %s waybill already in queue, skipping duplicate", key)
        return
    _pending_waybills.add(key)
    payload = {"_kind": "waybill", "lead_id": lead_id, "source": source}
    _task_queue.put_nowait(WorkItem(priority=PRIORITY_WAYBILL, payload=payload))
    logger.info("ENQUEUE waybill lead_id=%s source=%s queue_size=%d", key, source, _task_queue.qsize())


def enqueue_cdek_sync(payload: dict) -> None:
    """Низший приоритет: перемещение сделки по статусу СДЭК."""
    if _task_queue is None:
        logger.error("Task queue not initialized, dropping cdek_sync %s", payload)
        return
    key = str(payload.get("lead_id") or payload.get("cdek_number") or payload.get("uuid") or "")
    if not key:
        logger.warning("enqueue_cdek_sync: payload без идентификаторов, дроп: %s", payload)
        return
    if key in _pending_cdek_sync:
        logger.info("CDEK sync %s already in queue, skipping duplicate", key)
        return
    _pending_cdek_sync.add(key)
    _task_queue.put_nowait(WorkItem(priority=PRIORITY_CDEK_SYNC, payload={**payload, "_kind": "cdek_sync", "_key": key}))
    logger.info("ENQUEUE cdek_sync key=%s code=%s queue_size=%d", key, payload.get("code"), _task_queue.qsize())


def enqueue_metrika_sync(lead_id) -> None:
    """Низший приоритет: отправка статуса сделки в Яндекс.Метрику."""
    if _task_queue is None:
        logger.error("Task queue not initialized, dropping metrika_sync for lead %s", lead_id)
        return
    key = str(lead_id)
    if key in _pending_metrika_sync:
        logger.info("Lead %s metrika_sync already in queue, skipping duplicate", key)
        return
    _pending_metrika_sync.add(key)
    _task_queue.put_nowait(WorkItem(
        priority=PRIORITY_METRIKA_SYNC,
        payload={"_kind": "metrika_sync", "lead_id": lead_id},
    ))
    logger.info("ENQUEUE metrika_sync lead_id=%s queue_size=%d", key, _task_queue.qsize())


async def _worker() -> None:
    global _items_processed
    while True:
        item = await _task_queue.get()
        lead_id = str(item.payload.get("lead_id", ""))
        kind = item.payload.get("_kind") or "lead_update"
        if kind == "waybill":
            _pending_waybills.discard(lead_id)
        elif kind == "cdek_sync":
            _pending_cdek_sync.discard(str(item.payload.get("_key", "")))
        elif kind == "metrika_sync":
            _pending_metrika_sync.discard(lead_id)
        else:
            _pending_leads.discard(lead_id)
        waited = time.time() - item.enqueue_time
        logger.info(
            "DEQUEUE kind=%s lead_id=%s waited=%.1fs queue_remaining=%d",
            kind, lead_id, waited, _task_queue.qsize(),
        )
        try:
            if is_circuit_open():
                logger.info("Circuit breaker open — dropping %s for lead %s", kind, lead_id)
                continue

            if kind == "waybill":
                from waybill_service import create_waybill_for_lead
                await create_waybill_for_lead(
                    item.payload["lead_id"],
                    source=item.payload.get("source", "webhook"),
                )
            elif kind == "cdek_sync":
                from cdek_status_sync import process_sync
                await process_sync(item.payload)
            elif kind == "metrika_sync":
                from metrika_sync import process_sync as metrika_process
                await metrika_process(item.payload)
            else:
                await _process_lead_update(item.payload)

            _items_processed += 1
            if _items_processed % 100 == 0:
                _cleanup_lead_last_processed()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unhandled error processing lead %s", item.payload.get("lead_id"))
        finally:
            _task_queue.task_done()


def _cleanup_lead_last_processed() -> None:
    cutoff = datetime.datetime.now() - datetime.timedelta(seconds=60)
    stale = [k for k, v in lead_last_processed.items() if v < cutoff]
    for k in stale:
        del lead_last_processed[k]
    if stale:
        logger.info("Cleaned up %s stale lead_last_processed entries", len(stale))


async def _process_lead_update(payload: dict) -> None:
    lead_id = payload["lead_id"]
    goods = payload.get("goods")
    delivery_type = payload.get("delivery_type")
    delivery_address = payload.get("delivery_address")
    lead_name = payload.get("lead_name")
    promo_type = payload.get("promo_type")
    comment = payload.get("comment")
    attempt = payload.get("attempt", 0)

    if lead_id in lead_last_processed:
        elapsed = (datetime.datetime.now() - lead_last_processed[lead_id]).total_seconds()
        if elapsed < ECHO_COOLDOWN_SECONDS:
            logger.info("Lead %s was updated %.1fs ago, skipping echo webhook", lead_id, elapsed)
            return

    current_info = await get_lead_by_id(lead_id)
    if not isinstance(current_info, dict):
        logger.warning("Fetch failed for lead %s (attempt %s), scheduling retry", lead_id, attempt)
        _schedule_retry(payload, delay_seconds=3 * (2 ** attempt))
        return

    current_goods = await get_custom_field_value(current_info, 577313)
    current_delivery_type = await get_custom_field_value(current_info, 577315)
    current_delivery_address = await get_custom_field_value(current_info, 577311)
    current_promo_type = await get_custom_field_value(current_info, 570661)
    current_comment = await get_custom_field_value(current_info, 577753)

    if goods:
        is_goods_match = await normalize_text(current_goods) == await normalize_text(goods)
    else:
        is_goods_match = True

    if delivery_type:
        is_delivery_match = await normalize_text(current_delivery_type) == await normalize_text(delivery_type)
    else:
        is_delivery_match = True

    if delivery_address:
        is_address_match = await normalize_text(current_delivery_address) == await normalize_text(delivery_address)
    else:
        is_address_match = True

    if lead_name:
        normalized_name = await normalize_text(lead_name)
        current_name = current_info.get("name") if isinstance(current_info, dict) else None
        normalized_current_name = await normalize_text(current_name)
        is_name_match = normalized_name == normalized_current_name
        logger.info(f"normalized_name: {normalized_name}, normalized_current_name: {normalized_current_name}")
    else:
        is_name_match = True

    if promo_type:
        is_promo_match = await normalize_text(current_promo_type) == await normalize_text(promo_type)
    else:
        is_promo_match = True

    if comment:
        is_comment_match = await normalize_text(current_comment) == await normalize_text(comment)
    else:
        is_comment_match = True

    if is_goods_match and is_delivery_match and is_address_match and is_name_match and is_promo_match and is_comment_match:
        logger.info("MATCH: Data is identical (ignoring whitespace).")
        lead_last_processed[lead_id] = datetime.datetime.now()
        return

    current_time = datetime.datetime.now()
    if lead_id in lead_last_processed:
        elapsed_seconds = (current_time - lead_last_processed[lead_id]).total_seconds()
        if elapsed_seconds < RATE_LIMIT_SECONDS:
            delay = RATE_LIMIT_SECONDS - elapsed_seconds
            logger.info(f"Rate limit hit for lead {lead_id}, will retry in {delay:.1f}s")
            _schedule_retry(payload, delay_seconds=delay)
            return

    lead_last_processed[lead_id] = current_time
    logger.info("MISMATCH lead_id=%s: Updating info...", lead_id)
    result = await add_info_from_ms(
        goods=goods,
        delivery_type=delivery_type,
        delivery_address=delivery_address,
        lead_id=lead_id,
        name=lead_name,
        promo_type=promo_type,
        comment=comment,
    )
    logger.info(
        f"UPDATING:\n goods: {goods}\n delivery_type: {delivery_type}\n"
        f" delivery_address: {delivery_address}\n lead_name: {lead_name}\n"
        f" promo: {promo_type}\n comment: {comment}"
    )

    is_ok = bool(result.get("ok")) if isinstance(result, dict) else bool(result)
    retryable = bool(result.get("retryable", not is_ok)) if isinstance(result, dict) else (not is_ok)

    if not is_ok and retryable:
        delay = RATE_LIMIT_SECONDS * (2 ** attempt)
        logger.warning("PATCH failed for lead %s (retryable), scheduling retry in %.1fs", lead_id, delay)
        _schedule_retry(payload, delay_seconds=delay)


def _schedule_retry(payload: dict, delay_seconds: float) -> None:
    task = asyncio.create_task(_delayed_retry(payload, delay_seconds))
    _retry_tasks.add(task)
    task.add_done_callback(_retry_tasks.discard)


async def _delayed_retry(payload: dict, delay_seconds: float) -> None:
    attempt = payload.get("attempt", 0)
    if attempt >= MAX_RETRY_ATTEMPTS:
        logger.error("Giving up on lead %s after %s retry attempts", payload.get("lead_id"), attempt)
        return

    await asyncio.sleep(delay_seconds)
    enqueue_retry({**payload, "attempt": attempt + 1})
