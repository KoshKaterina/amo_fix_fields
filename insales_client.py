"""Async-клиент InSales (tangemshop.ru) — ТОЛЬКО поиск заказа и его статус.

Делает ровно две вещи: находит заказ по отображаемому номеру и ставит ему пользовательский
статус (custom_status_permalink). Оплату, состав, адрес и прочее НЕ трогает. Авторизация —
Basic (идентификатор ключа API и пароль) поверх HTTPS, как у woo-sklad. По образцу
woo_client (httpx, ретраи на 429/5xx).

Что проверено на живом магазине 02.11.2026 (ключом woo-sklad, только чтение):
  - фильтра по номеру у GET /admin/orders.json нет: `number=` молча игнорируется, а
    GET /admin/orders/<номер>.json отвечает 404 — путь принимает только внутренний id;
  - с `updated_since` список идёт по возрастанию updated_at, курсор — пара
    updated_since + from_id последнего заказа (так же листает woo-sklad);
  - окно «дата создания минус час» нашло заказ на первой странице из 100 за 3 секунды.
Запись по документации: PUT /admin/orders/<id>.json, тело
{"order": {"custom_status_permalink": "<код статуса>"}}.

⚠️ InSales отвечает только с российских адресов (иначе 403 «Доступ ограничен»): клиент
живёт на сервере интеграций, с машины за границей его не проверить.
⚠️ Ключ woo-sklad только читает (PUT → 403 «Недостаточно прав»): здесь нужен свой ключ
с правом записи заказов.
"""

import asyncio
import logging

import httpx

from waybill_config import INSALES_API_KEY, INSALES_API_PASSWORD, INSALES_SHOP_URL

logger = logging.getLogger("uvicorn")

_TIMEOUT = 30.0
_MAX_RETRIES = 3
_PER_PAGE = 100
# Потолок перебора: 20 страниц = 2000 заказов, около восьми месяцев магазина. Дальше не
# ищем — заказ с такой давностью скорее признак ошибки в номере, чем реальный случай.
_MAX_PAGES = 20


class InSalesError(Exception):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


def is_configured() -> bool:
    return bool(INSALES_SHOP_URL and INSALES_API_KEY and INSALES_API_PASSWORD)


class InSalesClient:
    def __init__(self, shop_url: str, api_key: str, password: str,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._base_url = f"https://{shop_url}/admin"
        self._auth = httpx.BasicAuth(api_key, password)
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    async def init(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=_TIMEOUT, auth=self._auth, transport=self._transport,
            # Дефолтный UA httpx режут WAF'ы — тот же приём, что в woo-sklad.
            headers={"User-Agent": "amo-fix-fields/insales-status"},
        )
        logger.info("InSalesClient initialized (base=%s)", self._base_url)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("InSalesClient closed")

    async def _request(self, method: str, path: str, params: dict | None = None,
                       json_body: dict | None = None) -> httpx.Response:
        """Запрос с ретраями на сетевые ошибки и 429/5xx. Возвращает Response
        (включая 4xx — их трактует вызывающий код). Исчерпание ретраев → InSalesError."""
        if self._client is None:
            raise InSalesError("InSalesClient not initialized")
        url = f"{self._base_url}{path}"
        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.request(method, url, params=params, json=json_body)
            except httpx.RequestError as exc:
                last_err = exc
                logger.warning("InSales %s %s attempt %s/%s — %s", method, path, attempt, _MAX_RETRIES, exc)
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(min(8.0, 2 ** attempt))
                    continue
                break

            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                # Лимит InSales — 500 запросов за 5 минут. Retry-After уважаем, но
                # ждём не дольше минуты: дальше ретраи кончатся и ошибка уйдёт в чат.
                try:
                    wait = float(resp.headers.get("Retry-After", "10"))
                except ValueError:
                    wait = 10.0
                await asyncio.sleep(min(60.0, max(1.0, wait)))
                continue
            if resp.status_code in (500, 502, 503, 504) and attempt < _MAX_RETRIES:
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue
            return resp

        raise InSalesError(f"InSales {method} {path} network error: {last_err}")

    async def find_order_by_number(self, number: int, updated_since: str) -> dict | None:
        """Заказ с отображаемым номером `number` среди изменённых с `updated_since`
        (ISO-строка с зоной). None — не нашёлся в пределах _MAX_PAGES страниц."""
        params = {"per_page": _PER_PAGE, "updated_since": updated_since}
        for _ in range(_MAX_PAGES):
            resp = await self._request("GET", "/orders.json", params=dict(params))
            if resp.status_code != 200:
                raise InSalesError(
                    f"InSales GET orders: {resp.status_code}", resp.status_code, resp.text[:300]
                )
            try:
                batch = resp.json()
            except ValueError:
                raise InSalesError("InSales GET orders: невалидный JSON", resp.status_code)
            if not batch:
                return None
            for order in batch:
                if order.get("number") == number:
                    return order
            if len(batch) < _PER_PAGE:
                return None
            last = batch[-1]
            params["updated_since"] = last.get("updated_at")
            params["from_id"] = last.get("id")
        logger.warning(
            "InSales: заказ №%s не нашёлся за %s страниц от %s", number, _MAX_PAGES, updated_since
        )
        return None

    async def set_custom_status(self, order_id, permalink: str) -> dict:
        """Ставит заказу пользовательский статус. Возвращает заказ из ответа.
        ТОЛЬКО статус: PUT с одним полем custom_status_permalink."""
        resp = await self._request(
            "PUT", f"/orders/{order_id}.json",
            json_body={"order": {"custom_status_permalink": permalink}},
        )
        if resp.status_code != 200:
            raise InSalesError(
                f"InSales PUT orders/{order_id}: {resp.status_code}", resp.status_code, resp.text[:300]
            )
        try:
            order = resp.json()
        except ValueError:
            raise InSalesError(f"InSales PUT orders/{order_id}: невалидный JSON", resp.status_code)
        got = ((order or {}).get("custom_status") or {}).get("permalink")
        if got is not None and got != permalink:
            # Ответ 200, а статус другой: запись не легла так, как просили. Считаем
            # ошибкой, чтобы она дошла до технического чата, а не осела в логе.
            raise InSalesError(
                f"InSales PUT orders/{order_id}: статус после записи «{got}», ждали «{permalink}»",
                resp.status_code,
            )
        return order


_client: InSalesClient | None = None


async def init() -> None:
    global _client
    _client = InSalesClient(INSALES_SHOP_URL, INSALES_API_KEY, INSALES_API_PASSWORD)
    await _client.init()


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _ensure() -> InSalesClient:
    if _client is None:
        raise InSalesError("insales_client not initialized — call init() first")
    return _client


async def find_order_by_number(number: int, updated_since: str) -> dict | None:
    return await _ensure().find_order_by_number(number, updated_since)


async def set_custom_status(order_id, permalink: str) -> dict:
    return await _ensure().set_custom_status(order_id, permalink)
