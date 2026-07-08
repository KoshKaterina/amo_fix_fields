import asyncio
import datetime
import itertools
import logging
import os
import time
from dataclasses import dataclass, field

from api import add_info_from_ms, api_queue_size, get_lead_by_id, is_circuit_open, set_breaker_category
from help_function import get_custom_field_value, normalize_text
from memory import MAX_RETRY_ATTEMPTS

logger = logging.getLogger("uvicorn")

# Новое изменение сделки (заполнение полей) — всегда первым; остальное потом.
PRIORITY_NEW = 0
# Jivo — тот же (первый) приоритет, что и заполнение полей: МОПам нужен лид из
# чата мгновенно (07.07.2026, жалоба отдела продаж). Было PRIORITY_JIVO=7 (фон,
# ниже waybill/lead) после аудит-хардненинга 66ecf38 → сделки Jivo застревали
# за очередью lead_update и приходили с задержкой ~20–30 мин. Возврат к исходному
# поведению (в 9dd0e93 было PRIORITY_NEW). Тай-брейк с lead_update — FIFO по sequence.
PRIORITY_JIVO = PRIORITY_NEW
PRIORITY_WAYBILL = 5
PRIORITY_RETRY = 10
PRIORITY_CDEK_SYNC = 20
PRIORITY_METRIKA_SYNC = 25

# ---------------------------------------------------------------------------
# Дорожки (lanes) — редизайн 08.07.2026. У каждого класса задач своя очередь и
# свой воркер, чтобы медленные внешние вызовы (Woo/Метрика/СДЭК) не задерживали
# критичный клиентский путь (поля сделки, накладные, Jivo). Запросы к amo из
# всех дорожек по-прежнему идут через ОБЩИЙ последовательный API-пайплайн
# (api.py) — лимит amo один на аккаунт; дорожки убирают только взаимную
# блокировку на внешних API (раньше один воркер на всё → медленный ответ
# WordPress/СДЭК держал amo-задачи).
# ---------------------------------------------------------------------------
LANE_AMO = "amo"    # lead_update, waybill, jivo, retry — клиентский путь
LANE_SYNC = "sync"  # metrika_sync (метрика+woo) — аналитика
LANE_CDEK = "cdek"  # cdek_sync — движение по статусам СДЭК
LANES = (LANE_AMO, LANE_SYNC, LANE_CDEK)

# kind задачи → категория брейкера (изоляция 429 по типу интеграции)
_CATEGORY_BY_KIND = {
    "waybill": "waybill",
    "cdek_sync": "cdek",
    "metrika_sync": "sync",
    "jivo": "jivo",
    "lead_update": "lead",
}

RATE_LIMIT_SECONDS = 3
ECHO_COOLDOWN_SECONDS = 10

# Наблюдаемость/алерты (разбор 08.07.2026: «огромная очередь» не была измерима —
# рестарты делались по ощущениям; теперь глубина и ожидание видны и алертятся в TG).
QUEUE_ALERT_DEPTH = int(os.getenv("QUEUE_ALERT_DEPTH", "30"))
QUEUE_ALERT_WAIT_SECONDS = float(os.getenv("QUEUE_ALERT_WAIT_SECONDS", "60"))
QUEUE_ALERT_COOLDOWN_SECONDS = float(os.getenv("QUEUE_ALERT_COOLDOWN_SECONDS", "1800"))
_MONITOR_INTERVAL_SECONDS = 60

lead_last_processed: dict[str, datetime.datetime] = {}

_counter = itertools.count()


@dataclass(order=True)
class WorkItem:
    priority: int
    payload: dict = field(compare=False)
    sequence: int = field(default_factory=lambda: next(_counter))
    enqueue_time: float = field(default_factory=time.time, compare=False)


_queues: dict[str, asyncio.PriorityQueue] = {}
_workers: dict[str, asyncio.Task] = {}
_monitor_task: asyncio.Task | None = None
_retry_tasks: set[asyncio.Task] = set()
_alert_tasks: set[asyncio.Task] = set()
# Коалесинг lead_update: lead_id → payload, лежащий в очереди. Повторный вебхук
# по той же сделке ОБНОВЛЯЕТ этот payload (раньше дропался целиком → терялись
# более свежие значения полей).
_pending_leads: dict[str, dict] = {}
_pending_waybills: set[str] = set()
_pending_cdek_sync: set[str] = set()
_pending_metrika_sync: set[str] = set()
_pending_jivo: set[str] = set()
_items_processed: int = 0
# lane → {"last_waited": float, "processed": int} — для /health и QUEUE STATUS.
_lane_stats: dict[str, dict] = {}
_alert_last_sent: dict[str, float] = {}


def init_queue() -> None:
    global _monitor_task
    for lane in LANES:
        _queues[lane] = asyncio.PriorityQueue()
        _workers[lane] = asyncio.create_task(_worker(lane))
        _lane_stats[lane] = {"last_waited": 0.0, "processed": 0}
    _monitor_task = asyncio.create_task(_monitor())
    logger.info("Task queues started (lanes: %s)", ", ".join(LANES))


async def shutdown_queue() -> None:
    global _monitor_task
    if _monitor_task is not None:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
        _monitor_task = None

    for worker in list(_workers.values()):
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
    _workers.clear()

    for task in list(_retry_tasks):
        task.cancel()
    _retry_tasks.clear()
    for task in list(_alert_tasks):
        task.cancel()
    _alert_tasks.clear()
    _pending_leads.clear()
    _pending_waybills.clear()
    _pending_cdek_sync.clear()
    _pending_metrika_sync.clear()
    _pending_jivo.clear()

    remaining = 0
    for queue in _queues.values():
        while not queue.empty():
            try:
                queue.get_nowait()
                remaining += 1
            except asyncio.QueueEmpty:
                break
    _queues.clear()
    if remaining:
        logger.warning("Drained %s items from task queues on shutdown", remaining)

    logger.info("Task queues stopped")


def queue_stats() -> dict:
    """Срез очередей для /health, QUEUE STATUS и алертов: глубина каждой
    дорожки, глубина внутренней очереди API-пайплайна, последнее ожидание и
    счётчики обработанного по дорожкам."""
    return {
        "lanes": {lane: q.qsize() for lane, q in _queues.items()},
        "api_queue": api_queue_size(),
        "last_waited_s": {
            lane: round(stats["last_waited"], 1) for lane, stats in _lane_stats.items()
        },
        "processed": {lane: stats["processed"] for lane, stats in _lane_stats.items()},
    }


def _alert_bg(key: str, text: str) -> None:
    """TG-алерт с кулдауном по ключу (не чаще раза в QUEUE_ALERT_COOLDOWN_SECONDS),
    отправка в фоне — не блокирует воркер/монитор."""
    now = time.monotonic()
    last = _alert_last_sent.get(key)
    if last is not None and now - last < QUEUE_ALERT_COOLDOWN_SECONDS:
        return
    _alert_last_sent[key] = now
    logger.warning("QUEUE ALERT [%s]: %s", key, text)

    async def _send() -> None:
        try:
            from telegram_bot import send_alert
            await send_alert(text)
        except Exception:
            logger.exception("Queue alert send failed: %s", text)

    task = asyncio.create_task(_send())
    _alert_tasks.add(task)
    task.add_done_callback(_alert_tasks.discard)


async def _monitor() -> None:
    """Раз в минуту: если где-то есть хвост — лог QUEUE STATUS; глубина выше
    порога — алерт в TG. Закрывает слепую зону разбора 08.07.2026 (размер
    _api_queue не был виден нигде)."""
    while True:
        await asyncio.sleep(_MONITOR_INTERVAL_SECONDS)
        try:
            stats = queue_stats()
            lanes = stats["lanes"]
            api_q = stats["api_queue"]
            if any(lanes.values()) or api_q:
                logger.info("QUEUE STATUS lanes=%s api_queue=%d", lanes, api_q)
            for lane, depth in lanes.items():
                if depth >= QUEUE_ALERT_DEPTH:
                    _alert_bg(
                        f"depth-{lane}",
                        f"⚠️ amo_fix_fields: очередь «{lane}» = {depth} задач "
                        f"(порог {QUEUE_ALERT_DEPTH}). Все дорожки: {lanes}, api_queue={api_q}.",
                    )
            if api_q >= QUEUE_ALERT_DEPTH:
                _alert_bg(
                    "depth-api",
                    f"⚠️ amo_fix_fields: очередь API-пайплайна amo = {api_q} запросов "
                    f"(порог {QUEUE_ALERT_DEPTH}). Дорожки: {lanes}.",
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Queue monitor error")


def enqueue_new(payload: dict) -> None:
    if not _queues:
        logger.error("Task queue not initialized, dropping webhook for lead %s", payload.get("lead_id"))
        return
    lead_id = str(payload.get("lead_id", ""))
    queued = _pending_leads.get(lead_id)
    if queued is not None:
        # Коалесинг вместо дропа: сделка уже ждёт в очереди — обновляем её payload
        # на месте (элемент очереди ссылается на этот же dict). Непустые поля
        # нового вебхука перекрывают старые: применится самое свежее состояние.
        merged = [k for k, v in payload.items() if k != "lead_id" and v is not None]
        for key in merged:
            queued[key] = payload[key]
        logger.info("COALESCE lead_id=%s: обновлены поля %s (сделка уже в очереди)", lead_id, merged or "—")
        return
    _pending_leads[lead_id] = payload
    _queues[LANE_AMO].put_nowait(WorkItem(priority=PRIORITY_NEW, payload=payload))
    logger.info("ENQUEUE lead_id=%s lane=%s queue_size=%d", lead_id, LANE_AMO, _queues[LANE_AMO].qsize())


def enqueue_retry(payload: dict) -> None:
    if not _queues:
        logger.error("Task queue not initialized, dropping retry for lead %s", payload.get("lead_id"))
        return
    lead_id = str(payload.get("lead_id", ""))
    queued = _pending_leads.get(lead_id)
    if queued is not None:
        # Пока ретрай спал, по сделке пришёл свежий вебхук — он уже в очереди.
        # Данные ретрая СТАРШЕ: только заполняем пробелы (None), не перетираем.
        for key, value in payload.items():
            if key not in ("lead_id", "attempt") and value is not None and queued.get(key) is None:
                queued[key] = value
        logger.info("Lead %s retry merged into queued item (fresh webhook wins)", lead_id)
        return
    _pending_leads[lead_id] = payload
    _queues[LANE_AMO].put_nowait(WorkItem(priority=PRIORITY_RETRY, payload=payload))


def enqueue_waybill(lead_id, source: str = "webhook") -> None:
    if not _queues:
        logger.error("Task queue not initialized, dropping waybill for lead %s", lead_id)
        return
    key = str(lead_id)
    if key in _pending_waybills:
        logger.info("Lead %s waybill already in queue, skipping duplicate", key)
        return
    _pending_waybills.add(key)
    payload = {"_kind": "waybill", "lead_id": lead_id, "source": source}
    _queues[LANE_AMO].put_nowait(WorkItem(priority=PRIORITY_WAYBILL, payload=payload))
    logger.info(
        "ENQUEUE waybill lead_id=%s source=%s lane=%s queue_size=%d",
        key, source, LANE_AMO, _queues[LANE_AMO].qsize(),
    )


def enqueue_cdek_sync(payload: dict) -> None:
    """Отдельная дорожка cdek: перемещение сделки по статусу СДЭК не встаёт
    в общую очередь и не тормозит клиентский путь (и наоборот)."""
    if not _queues:
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
    _queues[LANE_CDEK].put_nowait(
        WorkItem(priority=PRIORITY_CDEK_SYNC, payload={**payload, "_kind": "cdek_sync", "_key": key})
    )
    logger.info(
        "ENQUEUE cdek_sync key=%s code=%s lane=%s queue_size=%d",
        key, payload.get("code"), LANE_CDEK, _queues[LANE_CDEK].qsize(),
    )


def enqueue_jivo(payload: dict) -> None:
    """Первый приоритет — тот же, что и заполнение полей (lead_update): создание
    контакта+сделки+примечания из завершённого чата Jivo идёт наравне с новыми
    изменениями сделок, без ожидания за waybill/sync. Дедуп в очереди по
    chat_id/контакту, чтобы повторная доставка вебхука Jivo не плодила дубли."""
    if not _queues:
        logger.error("Task queue not initialized, dropping jivo event")
        return
    key = str(payload.get("chat_id") or payload.get("phone") or payload.get("email") or "")
    if key and key in _pending_jivo:
        logger.info("Jivo event %s already in queue, skipping duplicate", key)
        return
    if key:
        _pending_jivo.add(key)
    _queues[LANE_AMO].put_nowait(WorkItem(
        priority=PRIORITY_JIVO,
        payload={**payload, "_kind": "jivo", "_jivo_key": key},
    ))
    logger.info("ENQUEUE jivo key=%s lane=%s queue_size=%d", key, LANE_AMO, _queues[LANE_AMO].qsize())


def enqueue_metrika_sync(lead_id, status_id=None) -> None:
    """Точечный пуш Метрика+Woo по одной сделке (дорожка sync).

    С 08.07.2026 вебхук /lead_change сюда БОЛЬШЕ НЕ шлёт: Метрика и Woo идут
    сверкой по расписанию (metrika_sync — интрадей + ночная), реальное время им
    не нужно. Функция сохранена для ручного/точечного запуска. Тонкий дедуп
    остаётся в metrika_sync.process_sync (по фактическому исходящему состоянию
    заказа)."""
    if not _queues:
        logger.error("Task queue not initialized, dropping metrika_sync for lead %s", lead_id)
        return
    key = str(lead_id)
    if key in _pending_metrika_sync:
        logger.info("Lead %s metrika_sync already in queue, skipping duplicate", key)
        return

    _pending_metrika_sync.add(key)
    _queues[LANE_SYNC].put_nowait(WorkItem(
        priority=PRIORITY_METRIKA_SYNC,
        payload={"_kind": "metrika_sync", "lead_id": lead_id},
    ))
    logger.info(
        "ENQUEUE metrika_sync lead_id=%s lane=%s queue_size=%d",
        key, LANE_SYNC, _queues[LANE_SYNC].qsize(),
    )


async def _worker(lane: str) -> None:
    global _items_processed
    queue = _queues[lane]
    while True:
        item = await queue.get()
        lead_id = str(item.payload.get("lead_id", ""))
        kind = item.payload.get("_kind") or "lead_update"
        category = _CATEGORY_BY_KIND.get(kind, "lead")
        set_breaker_category(category)
        if kind == "waybill":
            _pending_waybills.discard(lead_id)
        elif kind == "cdek_sync":
            _pending_cdek_sync.discard(str(item.payload.get("_key", "")))
        elif kind == "metrika_sync":
            _pending_metrika_sync.discard(lead_id)
        elif kind != "jivo":
            # jivo-пометку снимаем в finally (после обработки) — чтобы повторная
            # доставка того же chat_id во время обработки схлопывалась, а не
            # плодила дубль сделки.
            _pending_leads.pop(lead_id, None)
        waited = time.time() - item.enqueue_time
        stats = _lane_stats.get(lane)
        if stats is not None:
            stats["last_waited"] = waited
        logger.info(
            "DEQUEUE lane=%s kind=%s lead_id=%s waited=%.1fs queue_remaining=%d",
            lane, kind, lead_id, waited, queue.qsize(),
        )
        if waited >= QUEUE_ALERT_WAIT_SECONDS:
            _alert_bg(
                f"wait-{lane}",
                f"⚠️ amo_fix_fields: задача {kind} (сделка {lead_id or '—'}) ждала в очереди "
                f"«{lane}» {waited:.0f}с (порог {QUEUE_ALERT_WAIT_SECONDS:.0f}с).",
            )
        try:
            if is_circuit_open(category):
                logger.info("Circuit breaker open [%s] — dropping %s for lead %s", category, kind, lead_id)
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
                # Метрика и Woo идут ВМЕСТЕ, одним элементом очереди, строго
                # последовательно: сначала Метрика, затем Woo. Ошибка Метрики не
                # должна лишать Woo попытки — оборачиваем её отдельно.
                from metrika_sync import process_sync as metrika_process
                from woo_status_sync import process_sync as woo_process
                try:
                    await metrika_process(item.payload)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("metrika_sync error for lead %s", lead_id)
                await woo_process(item.payload)
            elif kind == "jivo":
                from jivo_service import process_jivo_chat
                await process_jivo_chat(item.payload)
            else:
                await _process_lead_update(item.payload)

            _items_processed += 1
            if stats is not None:
                stats["processed"] += 1
            if _items_processed % 100 == 0:
                _cleanup_lead_last_processed()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unhandled error processing lead %s", item.payload.get("lead_id"))
        finally:
            if kind == "jivo":
                _pending_jivo.discard(str(item.payload.get("_jivo_key", "")))
            queue.task_done()


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
