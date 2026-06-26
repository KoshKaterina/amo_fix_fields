"""amoCRM → WooCommerce: статус 'completed' для рефералки.

Ставит заказу в WooCommerce статус 'completed' РОВНО тогда, когда сделка считается
оплаченной (PAID) — по той же логике, что и Яндекс.Метрика (metrika_sync._classify:
предоплата → CLEVER «Успешно реализовано»; наложка → Офис «Успешно реализовано» или
Фулфилмент «09. Доставлено»/«09.2 Платёж отправлен владельцу», с резолвом оригинала
CLEVER по UUID МойСклад).

Принципы (согласовано):
  - передаём ТОЛЬКО статус и ТОЛЬКО completed (промежуточные статусы, сумму, товары
    и всё остальное не трогаем);
  - ключ связки — поле сделки 577415 «Номер заказа на сайте» = WC order id
    (НЕ id клиента); пусто → заказ не с сайта, пропускаем;
  - МойСклад не задействован;
  - если заказ уже completed — ничего не делаем (идемпотентно, чтобы не задваивать
    комиссию рефералки); заказы в cancelled/refunded/failed НЕ трогаем (брошенные
    онлайн-чекауты, оплата по ним идёт другим каналом — см. woo_client.RISKY_STATUSES);
    из остальных статусов (pending/on-hold/processing) — ставим completed;
  - гард по дате создания: WOO_STATUS_SINCE (по умолчанию 2026-03-30).

Запускается ВМЕСТЕ с metrika_sync, строго ПОСЛЕ неё, тем же приоритетом очереди
(queue_manager) и тем же ночным проходом сверки (metrika_sync.reconcile_window).
"""

import logging

import amo_service
import metrika_sync
import woo_client
from waybill_config import (
    FIELD_PAYMENT_METHOD,
    FIELD_SITE_ORDER_NUMBER,
    PIPELINE_CLEVER,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    WOO_COMPLETED_STATUS,
    WOO_STATUS_SINCE_TS,
    WOO_STATUS_SYNC_ENABLED,
    is_cod_payment,
)

logger = logging.getLogger("uvicorn")

_enabled = False

# Дедуп по исходящему состоянию: WC order id (=577415) → статус, который мы уже
# успешно проставили. Гасит повторные PAID-события одного заказа (echo-вебхуки,
# ночная сверка). В памяти: после рестарта максимум один лишний GET+пропуск на
# заказ (он уже completed → 'already'). Идемпотентно.
_last_sent: dict[str, str] = {}


def is_enabled() -> bool:
    return _enabled


async def init() -> None:
    """Включаем синк только если заданы WC_* И поднят боевой флаг."""
    global _enabled
    if not woo_client.is_configured():
        logger.warning("Woo status sync: WC_URL/WC_CONSUMER_* не заданы — ВЫКЛЮЧЕН")
        return
    if not WOO_STATUS_SYNC_ENABLED:
        logger.warning(
            "Woo status sync: WC_* заданы, но WOO_STATUS_SYNC_ENABLED не включён — "
            "запись в WC ВЫКЛЮЧЕНА (режим dry-run)"
        )
        return
    await woo_client.init()
    _enabled = True
    logger.info(
        "Woo status sync включён (статус=%s, since=%s)",
        WOO_COMPLETED_STATUS,
        "выкл" if WOO_STATUS_SINCE_TS is None else f"unix {WOO_STATUS_SINCE_TS}",
    )


async def shutdown() -> None:
    global _enabled
    _enabled = False
    _last_sent.clear()
    await woo_client.aclose()
    logger.info("Woo status sync stopped")


async def _alert(text: str) -> None:
    logger.error(text)
    try:
        from telegram_bot import send_alert
        await send_alert(text)
    except Exception:
        logger.exception("Woo alert failed: %s", text)


def _cf(entity: dict, field_id: int):
    return amo_service.get_custom_field_value(entity, field_id)


async def resolve_target(payload: dict, lead: dict | None = None) -> dict | None:
    """Общая часть realtime и dry-run: возвращает {'site', 'canonical'} для заказа,
    который ДОЛЖЕН быть completed, либо None (не наш случай / не PAID / нет номера).

    Логика «оплачен» — единая с Метрикой (metrika_sync._classify/_resolve_clever).
    """
    lead_id = payload["lead_id"]
    if lead is None:
        # Контакты для woo не нужны (ключ — номер заказа на сайте, не клиент).
        lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        return None

    pipeline_id = lead.get("pipeline_id")
    status_id = lead.get("status_id")
    if pipeline_id not in (PIPELINE_CLEVER, PIPELINE_OFFICE, PIPELINE_FULFILLMENT):
        return None

    payment = _cf(lead, FIELD_PAYMENT_METHOD)
    if not str(payment or "").strip():
        return None
    cod = is_cod_payment(payment)

    order_status, need_resolve = metrika_sync._classify(pipeline_id, status_id, cod)
    if order_status != "PAID":
        return None  # Woo действует ТОЛЬКО на выполненные заказы

    canonical = await metrika_sync._resolve_clever(lead) if need_resolve else lead
    if not canonical:
        # Оригинал в CLEVER не нашёлся (старьё/архив) — ожидаемый пропуск, без алерта.
        logger.info(
            "Woo: не нашёл оригинал CLEVER для сделки %s — пропуск", lead_id
        )
        return None

    # Гард по дате создания заказа.
    if WOO_STATUS_SINCE_TS is not None:
        created = canonical.get("created_at")
        if created is not None and int(created) < WOO_STATUS_SINCE_TS:
            return None

    site = str(_cf(canonical, FIELD_SITE_ORDER_NUMBER) or "").strip()
    if not site:
        logger.info(
            "Woo: заказ %s без номера на сайте (577415) — не с сайта, пропуск",
            canonical.get("id"),
        )
        return None

    return {"site": site, "canonical": canonical}


async def process_sync(payload: dict, lead: dict | None = None) -> None:
    if not _enabled:
        return
    target = await resolve_target(payload, lead)
    if not target:
        return
    site = target["site"]
    canonical_id = target["canonical"].get("id")

    if _last_sent.get(site) == WOO_COMPLETED_STATUS:
        logger.info("Woo: заказ сайта %s уже отмечен completed (дедуп) — пропуск", site)
        return

    try:
        result = await woo_client.complete_order(site)
    except woo_client.WooError as exc:
        await _alert(f"Woo: ошибка по заказу сайта {site} (сделка {canonical_id}): {exc}")
        return

    if result in ("completed", "already"):
        _last_sent[site] = WOO_COMPLETED_STATUS
        if len(_last_sent) > 10000:
            _last_sent.clear()

    if result == "completed":
        logger.info("Woo: заказ сайта %s (сделка %s) → completed", site, canonical_id)
    elif result == "already":
        logger.info("Woo: заказ сайта %s уже completed — пропуск", site)
    elif result == "skipped":
        logger.info(
            "Woo: заказ сайта %s (сделка %s) в cancelled/refunded/failed — не трогаем",
            site, canonical_id,
        )
    elif result == "not_found":
        logger.warning(
            "Woo: заказ сайта %s (сделка %s) не найден в WC (удалён?) — пропуск",
            site, canonical_id,
        )
