"""amoCRM → Яндекс.Метрика CDP (сквозная аналитика).

Логика (согласовано):
  - id заказа в Метрике = id ОРИГИНАЛЬНОЙ сделки из воронки CLEVER Основная.
    Дубликат (Офис/Фулфилмент) → оригинал резолвится по полю 576689 (UUID МойСклад).
  - Определение типа оплаты по полю 577373 «Способ оплаты»:
        наложка (COD) = содержит «при получении / наличны / эвотор / наложен».
    Пустой способ оплаты → сделку не передаём.
  - Маппинг статусов:
        CANCELLED  — этап 143 в ЛЮБОЙ воронке (даже поверх PAID).
        IN_PROGRESS — CLEVER, любой не-терминальный этап.
        PAID (предоплата) — CLEVER «Успешно реализовано» (142).
        PAID (наложка)   — Офис «Успешно реализовано» (142)
                           ИЛИ Фулфилмент «09. Доставлено» / «09.2 Платёж отправлен владельцу».
  - Сумма (revenue) пишется только при PAID.

Триггерится вебхуком смены статуса (через очередь, низкий приоритет).
"""

import asyncio
import datetime
import logging
import time

import amo_service
import metrika_client
from waybill_config import (
    FIELD_EMAIL,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
    FIELD_YM_CLIENT_ID,
    FULFILLMENT_DELIVERED,
    FULFILLMENT_PAYMENT_FORWARDED,
    METRIKA_COUNTER_ID,
    METRIKA_TOKEN,
    PIPELINE_CLEVER,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
    is_cod_payment,
)

logger = logging.getLogger("uvicorn")

# Метрика ждёт даты как LocalDateTime (НЕ unix). Счётчик в МСК.
_MSK = datetime.timezone(datetime.timedelta(hours=3))


def _fmt_dt(ts) -> str | None:
    try:
        return datetime.datetime.fromtimestamp(int(ts), tz=_MSK).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return None


_enabled = False
_counter_id: int | None = None


def is_enabled() -> bool:
    return _enabled


async def init() -> None:
    """Проверка токена + определение counter id. Требует amo-кэш не нужен."""
    global _enabled, _counter_id
    if not METRIKA_TOKEN:
        logger.warning("Metrika sync: METRIKA_TOKEN не задан — синхронизация ВЫКЛЮЧЕНА")
        return

    await metrika_client.init()

    counter = METRIKA_COUNTER_ID
    if counter is None:
        try:
            counters = await metrika_client.get_counters()
        except Exception as exc:
            await _alert(f"Metrika sync: не удалось получить список счётчиков: {exc} — ВЫКЛЮЧЕНА")
            await metrika_client.aclose()
            return
        if len(counters) == 1:
            counter = counters[0].get("id")
            logger.info("Metrika sync: счётчик определён автоматически: %s", counter)
        else:
            await _alert(
                f"Metrika sync: в аккаунте {len(counters)} счётчиков — задай METRIKA_COUNTER_ID. ВЫКЛЮЧЕНА"
            )
            await metrika_client.aclose()
            return

    _counter_id = counter
    _enabled = True
    logger.info("Metrika sync включён (counter=%s)", _counter_id)


async def shutdown() -> None:
    global _enabled
    _enabled = False
    await metrika_client.aclose()
    logger.info("Metrika sync stopped")


async def _alert(text: str) -> None:
    logger.error(text)
    try:
        from telegram_bot import send_alert
        await send_alert(text)
    except Exception:
        logger.exception("Metrika alert failed: %s", text)


def _cf(entity: dict, field_id: int):
    return amo_service.get_custom_field_value(entity, field_id)


def _classify(pipeline_id, status_id, cod: bool) -> tuple[str | None, bool]:
    """Возвращает (order_status, need_resolve_clever).

    need_resolve_clever=True → событие пришло из дубликата, id заказа надо взять
    из оригинала в CLEVER.
    """
    if status_id == STATUS_CLOSED_LOST:
        return "CANCELLED", pipeline_id != PIPELINE_CLEVER

    if pipeline_id == PIPELINE_CLEVER:
        if status_id == STATUS_SUCCESS and not cod:
            return "PAID", False
        if status_id not in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
            return "IN_PROGRESS", False
        return None, False

    if cod and pipeline_id == PIPELINE_OFFICE and status_id == STATUS_SUCCESS:
        return "PAID", True
    if cod and pipeline_id == PIPELINE_FULFILLMENT and status_id in (
        FULFILLMENT_DELIVERED,
        FULFILLMENT_PAYMENT_FORWARDED,
    ):
        return "PAID", True

    return None, False


async def process_sync(payload: dict) -> None:
    if not _enabled:
        return
    lead_id = payload["lead_id"]
    lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    if not lead:
        return

    pipeline_id = lead.get("pipeline_id")
    status_id = lead.get("status_id")

    # Работаем только со сквозным потоком заказа: CLEVER → Офис/Фулфилмент.
    # Сделки из прочих воронок (опт, отдел продаж и т.п.) игнорируем.
    if pipeline_id not in (PIPELINE_CLEVER, PIPELINE_OFFICE, PIPELINE_FULFILLMENT):
        return

    payment = _cf(lead, FIELD_PAYMENT_METHOD)
    if not str(payment or "").strip():
        logger.info("Metrika: сделка %s без способа оплаты — пропуск", lead_id)
        return
    cod = is_cod_payment(payment)

    order_status, need_resolve = _classify(pipeline_id, status_id, cod)
    if not order_status:
        return

    # Каноническая сделка — всегда оригинал в CLEVER (источник id, ym, суммы, дат).
    if need_resolve:
        canonical = await _resolve_clever(lead)
        if not canonical:
            # Для старых/архивных дублей оригинал в CLEVER может не находиться —
            # это ожидаемый пропуск, не ошибка. Без алерта, чтобы не спамить TG.
            logger.warning(
                "Metrika: не нашёл оригинал в CLEVER для сделки %s "
                "(UUID МойСклад=%r, статус %s) — пропуск",
                lead_id, _cf(lead, FIELD_MOYSKLAD_ORDER_UUID), order_status,
            )
            return
    else:
        canonical = lead

    metrika_order_id = canonical.get("id")
    ym = str(_cf(canonical, FIELD_YM_CLIENT_ID) or "").strip()
    contact_id, email, phone = await _contact_info(canonical)
    # Метрике нужен хотя бы один идентификатор клиента (client_uniq_id тоже годится).
    if not (ym or email or phone or contact_id):
        logger.warning("Metrika: заказ %s без идентификаторов клиента — пропуск", metrika_order_id)
        return

    now = int(time.time())
    # дата смены статуса — у триггерящей сделки (для COD это момент доставки в дубликате)
    event_dt = _fmt_dt(lead.get("updated_at") or now)

    row: dict = {
        "id": metrika_order_id,
        "create_date_time": _fmt_dt(canonical.get("created_at") or now),
        "update_date_time": event_dt,
        "order_status": order_status,
        "currency": "RUB",
    }
    if ym:
        row["client_ids"] = ym
    if email:
        row["emails"] = email
    if phone:
        row["phones"] = phone
    if contact_id:
        row["client_uniq_id"] = contact_id
    if order_status in ("PAID", "CANCELLED"):
        row["finish_date_time"] = event_dt
    if order_status == "PAID":
        revenue = canonical.get("price") or 0
        if revenue:
            row["revenue"] = revenue

    try:
        await metrika_client.upload_simple_order(_counter_id, row)
        logger.info(
            "Metrika: заказ %s → %s (cod=%s, trigger lead %s)",
            metrika_order_id, order_status, cod, lead_id,
        )
    except metrika_client.MetrikaError as exc:
        await _alert(f"Metrika: ошибка загрузки заказа {metrika_order_id}: {exc}")


async def _resolve_clever(dup_lead: dict) -> dict | None:
    """Находит оригинал в CLEVER Основной по UUID заказа МойСклад (576689)."""
    uuid = str(_cf(dup_lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not uuid:
        return None
    for cand in await amo_service.find_leads_by_query(uuid, with_=("contacts",)):
        if cand.get("pipeline_id") == PIPELINE_CLEVER and str(
            _cf(cand, FIELD_MOYSKLAD_ORDER_UUID) or ""
        ).strip() == uuid:
            return cand
    return None


async def _contact_info(lead: dict) -> tuple[int | None, str | None, str | None]:
    """Возвращает (contact_id, email, phone) основного контакта сделки."""
    embedded = (lead.get("_embedded") or {}).get("contacts") or []
    cid = None
    for c in embedded:
        if c.get("is_main"):
            cid = c.get("id")
            break
    if cid is None and embedded:
        cid = embedded[0].get("id")
    if cid is None:
        return None, None, None

    contact = await amo_service.get_contact_by_id(cid)
    if not contact:
        return cid, None, None

    email = amo_service.get_custom_field_value(contact, FIELD_EMAIL)
    phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    email = str(email).strip().lower() if email else None
    phone = "".join(ch for ch in str(phone) if ch.isdigit()) if phone else None
    return cid, email, phone
