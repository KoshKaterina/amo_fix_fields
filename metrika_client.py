"""Async-клиент Яндекс.Метрика CDP.

Используем упрощённый поток simple_orders: один POST грузит заказ вместе с
идентификаторами клиента (ClientID/email/phone), отдельная загрузка клиентов
не нужна.

ВНИМАНИЕ: точную обёртку загрузки (multipart `file` vs сырой CSV-боди) и формат
дат документация Яндекса отдаёт только в JS-рендере — проверить на первом живом
вызове по логам ответа. Здесь реализован multipart `file` (как в остальных
upload-ручках Метрики) + UNIX-таймстампы.
"""

import asyncio
import csv
import io
import logging

import httpx

from waybill_config import METRIKA_API_URL, METRIKA_TOKEN

logger = logging.getLogger("uvicorn")

_TIMEOUT = 30.0
_MAX_RETRIES = 3

# Порядок колонок CSV simple_orders. Пустые значения не отправляем (пустая ячейка).
SIMPLE_ORDER_COLUMNS = [
    "id",
    "create_date_time",
    "update_date_time",
    "finish_date_time",
    "client_uniq_id",
    "client_ids",
    "emails",
    "phones",
    "order_status",
    "revenue",
    "currency",
]


class MetrikaError(Exception):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


def build_simple_orders_csv(rows: list[dict]) -> bytes:
    """Собирает CSV (заголовок + строки) для simple_orders."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(SIMPLE_ORDER_COLUMNS)
    for row in rows:
        writer.writerow(["" if row.get(c) is None else row.get(c) for c in SIMPLE_ORDER_COLUMNS])
    return buf.getvalue().encode("utf-8")


class MetrikaClient:
    def __init__(self, base_url: str, token: str):
        self._base_url = base_url
        self._token = token
        self._client: httpx.AsyncClient | None = None

    async def init(self) -> None:
        self._client = httpx.AsyncClient(timeout=_TIMEOUT)
        logger.info("MetrikaClient initialized (base=%s)", self._base_url)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("MetrikaClient closed")

    def _headers(self) -> dict:
        return {"Authorization": f"OAuth {self._token}"}

    async def get_counters(self) -> list[dict]:
        if self._client is None:
            raise MetrikaError("MetrikaClient not initialized")
        resp = await self._client.get(
            f"{self._base_url}/management/v1/counters",
            headers=self._headers(),
            params={"per_page": 100},
        )
        if resp.status_code != 200:
            raise MetrikaError(
                f"Metrika counters: {resp.status_code}", resp.status_code, resp.text[:300]
            )
        return resp.json().get("counters") or []

    async def upload_simple_orders(self, counter_id: int, rows: list[dict]) -> dict:
        if self._client is None:
            raise MetrikaError("MetrikaClient not initialized")
        if not rows:
            return {}
        data = build_simple_orders_csv(rows)
        url = f"{self._base_url}/cdp/api/v1/counter/{counter_id}/data/simple_orders"
        params = {"merge_mode": "SAVE", "delimiter_type": "COMMA"}
        files = {"file": ("orders.csv", data, "text/csv")}

        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.post(url, params=params, headers=self._headers(), files=files)
            except httpx.RequestError as exc:
                last_err = exc
                logger.warning("Metrika upload attempt %s/%s — %s", attempt, _MAX_RETRIES, exc)
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue

            if resp.status_code in (200, 201, 202):
                try:
                    return resp.json() if resp.text else {}
                except ValueError:
                    return {}

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < _MAX_RETRIES:
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue

            raise MetrikaError(
                f"Metrika simple_orders: {resp.status_code}", resp.status_code, resp.text[:300]
            )

        raise MetrikaError(f"Metrika simple_orders network error: {last_err}")


_client: MetrikaClient | None = None


async def init() -> None:
    global _client
    _client = MetrikaClient(METRIKA_API_URL, METRIKA_TOKEN)
    await _client.init()


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _ensure() -> MetrikaClient:
    if _client is None:
        raise MetrikaError("metrika_client not initialized — call init() first")
    return _client


async def get_counters() -> list[dict]:
    return await _ensure().get_counters()


async def upload_simple_order(counter_id: int, row: dict) -> dict:
    return await _ensure().upload_simple_orders(counter_id, [row])
