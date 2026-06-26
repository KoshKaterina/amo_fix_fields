"""Async-клиент WooCommerce REST API v3 — ТОЛЬКО статус заказа.

Делает ровно две вещи: читает статус заказа и ставит 'completed'. Сумму, товары,
контрагента и прочее НЕ трогает. Авторизация — Basic (consumer key/secret) поверх
HTTPS, как ожидает WC REST. По образцу metrika_client (httpx, ретраи на 429/5xx).

Идентификатор заказа = WC order id, он же значение поля сделки amoCRM 577415
«Номер заказа на сайте» (FIELD_SITE_ORDER_NUMBER).
"""

import asyncio
import logging

import httpx

from waybill_config import (
    WC_CONSUMER_KEY,
    WC_CONSUMER_SECRET,
    WC_URL,
    WOO_COMPLETED_STATUS,
)

logger = logging.getLogger("uvicorn")

_TIMEOUT = 30.0
_MAX_RETRIES = 3

# Статусы, из которых НЕ переводим в completed. Это, как правило, брошенные
# онлайн-чекауты (WC сам отменяет неоплаченные по таймауту), а оплата по ним
# приходит другим каналом — «расотменять» их и начислять по ним реф-комиссию
# нельзя (у части ещё и сумма WC ≠ реальной). Решение согласовано 26.06.2026.
RISKY_STATUSES = ("cancelled", "refunded", "failed")


class WooError(Exception):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


def is_configured() -> bool:
    return bool(WC_URL and WC_CONSUMER_KEY and WC_CONSUMER_SECRET)


class WooClient:
    def __init__(self, base_url: str, consumer_key: str, consumer_secret: str):
        self._base_url = f"{base_url.rstrip('/')}/wp-json/wc/v3"
        self._auth = httpx.BasicAuth(consumer_key, consumer_secret)
        self._client: httpx.AsyncClient | None = None

    async def init(self) -> None:
        self._client = httpx.AsyncClient(timeout=_TIMEOUT, auth=self._auth)
        logger.info("WooClient initialized (base=%s)", self._base_url)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("WooClient closed")

    async def _request(self, method: str, path: str, json_body: dict | None = None) -> httpx.Response:
        """Запрос с ретраями на сетевые ошибки и 429/5xx. Возвращает Response
        (включая 4xx — их трактует вызывающий код). Исчерпание ретраев → WooError."""
        if self._client is None:
            raise WooError("WooClient not initialized")
        url = f"{self._base_url}{path}"
        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.request(method, url, json=json_body)
            except httpx.RequestError as exc:
                last_err = exc
                logger.warning("Woo %s %s attempt %s/%s — %s", method, path, attempt, _MAX_RETRIES, exc)
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(min(8.0, 2 ** attempt))
                    continue
                break

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < _MAX_RETRIES:
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue
            return resp

        raise WooError(f"Woo {method} {path} network error: {last_err}")

    async def get_order_status(self, order_id) -> str | None:
        """Текущий статус заказа. None → заказа нет в WC (удалён/неверный id)."""
        resp = await self._request("GET", f"/orders/{order_id}")
        if resp.status_code == 200:
            try:
                return resp.json().get("status")
            except ValueError:
                raise WooError(f"Woo GET orders/{order_id}: невалидный JSON", resp.status_code)
        if resp.status_code in (400, 404):
            # WC отдаёт 400/404 woocommerce_rest_shop_order_invalid_id для удалённых/чужих id
            return None
        raise WooError(
            f"Woo GET orders/{order_id}: {resp.status_code}", resp.status_code, resp.text[:300]
        )

    async def complete_order(self, order_id) -> str:
        """Ставит заказу статус 'completed'. Возвращает:
          'completed'  — статус успешно изменён;
          'already'    — заказ уже в 'completed' (ничего не делаем, идемпотентно);
          'skipped'    — заказ в cancelled/refunded/failed (НЕ трогаем, см. RISKY_STATUSES);
          'not_found'  — заказа нет в WC (удалён/неверный id).
        ТОЛЬКО статус: PUT с одним полем status. Сумму/товары не трогаем.
        """
        current = await self.get_order_status(order_id)
        if current is None:
            return "not_found"
        if current == WOO_COMPLETED_STATUS:
            return "already"
        if current in RISKY_STATUSES:
            return "skipped"
        resp = await self._request(
            "PUT", f"/orders/{order_id}", json_body={"status": WOO_COMPLETED_STATUS}
        )
        if resp.status_code in (200, 201):
            return "completed"
        if resp.status_code in (400, 404):
            return "not_found"
        raise WooError(
            f"Woo PUT orders/{order_id}: {resp.status_code}", resp.status_code, resp.text[:300]
        )


_client: WooClient | None = None


async def init() -> None:
    global _client
    _client = WooClient(WC_URL, WC_CONSUMER_KEY, WC_CONSUMER_SECRET)
    await _client.init()


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _ensure() -> WooClient:
    if _client is None:
        raise WooError("woo_client not initialized — call init() first")
    return _client


async def get_order_status(order_id) -> str | None:
    return await _ensure().get_order_status(order_id)


async def complete_order(order_id) -> str:
    return await _ensure().complete_order(order_id)
