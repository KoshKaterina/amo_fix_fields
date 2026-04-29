"""Async-клиент СДЭК API. Отдельный от AMO httpx-клиент."""

import asyncio
import logging
import time
from typing import Any

import httpx

from waybill_config import CDEK_API_URL, CDEK_CLIENT_ID, CDEK_CLIENT_SECRET

logger = logging.getLogger("uvicorn")

_REQUEST_TIMEOUT = 30.0
_MAX_RETRIES = 3
_BARCODE_POLL_INTERVAL_S = 2
_BARCODE_POLL_MAX_ATTEMPTS = 30


class CdekError(Exception):
    """Любая невосстановимая ошибка вызова СДЭК API."""

    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


class CdekClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self._base_url = base_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._client: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._token_expires: float = 0.0
        self._token_lock = asyncio.Lock()

    async def init(self) -> None:
        self._client = httpx.AsyncClient(timeout=_REQUEST_TIMEOUT)
        logger.info("CdekClient initialized (base=%s)", self._base_url)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("CdekClient closed")

    async def _ensure_token(self) -> str:
        async with self._token_lock:
            if self._token and time.monotonic() < self._token_expires:
                return self._token
            if self._client is None:
                raise CdekError("CdekClient not initialized")
            resp = await self._client.post(
                f"{self._base_url}/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
            )
            if resp.status_code != 200:
                raise CdekError(
                    f"СДЭК OAuth failed: {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:300],
                )
            data = resp.json()
            self._token = data["access_token"]
            self._token_expires = time.monotonic() + max(60, int(data.get("expires_in", 3600)) - 60)
            return self._token

    async def _headers(self) -> dict:
        token = await self._ensure_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
    ) -> dict:
        if self._client is None:
            raise CdekError("CdekClient not initialized")
        url = f"{self._base_url}{path}"
        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                headers = await self._headers()
                resp = await self._client.request(method, url, json=json, params=params, headers=headers)
            except httpx.RequestError as exc:
                last_err = exc
                logger.warning("CDEK %s %s attempt %s/%s — %s", method, path, attempt, _MAX_RETRIES, exc)
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue

            if resp.status_code in (200, 202):
                try:
                    return resp.json()
                except ValueError:
                    raise CdekError(f"CDEK {path}: invalid JSON")

            if resp.status_code == 401:
                # token revoked — drop and retry once
                self._token = None
                self._token_expires = 0.0
                if attempt < _MAX_RETRIES:
                    continue

            if resp.status_code in (408, 429, 500, 502, 503, 504) and attempt < _MAX_RETRIES:
                await asyncio.sleep(min(8.0, 2 ** attempt))
                continue

            raise CdekError(
                f"CDEK {method} {path}: {resp.status_code}",
                status=resp.status_code,
                body=resp.text[:300],
            )

        raise CdekError(f"CDEK {method} {path}: network error", body=str(last_err))

    async def create_order(self, order_data: dict) -> dict:
        return await self._request("POST", "/orders", json=order_data)

    async def get_order(self, uuid: str) -> dict:
        return await self._request("GET", f"/orders/{uuid}")

    async def find_uuid_by_cdek_number(self, cdek_number: str) -> str | None:
        data = await self._request("GET", "/orders", params={"cdek_number": cdek_number})
        # СДЭК возвращает либо entity (одиночный объект), либо массив orders
        entity = data.get("entity")
        if isinstance(entity, dict):
            uuid = entity.get("uuid")
            if uuid:
                return uuid
        if isinstance(entity, list) and entity:
            uuid = entity[0].get("uuid")
            if uuid:
                return uuid
        return None

    async def get_barcodes_batch_pdf(self, order_uuids: list[str], format_: str = "A6") -> bytes:
        if not order_uuids:
            raise CdekError("get_barcodes_batch_pdf: empty uuid list")

        task = await self._request(
            "POST",
            "/print/barcodes",
            json={
                "orders": [{"order_uuid": u} for u in order_uuids],
                "format": format_,
            },
        )
        entity_uuid = (task.get("entity") or {}).get("uuid")
        if not entity_uuid:
            raise CdekError(f"CDEK /print/barcodes: no entity uuid in response: {task}")

        url: str | None = None
        for _ in range(_BARCODE_POLL_MAX_ATTEMPTS):
            await asyncio.sleep(_BARCODE_POLL_INTERVAL_S)
            result = await self._request("GET", f"/print/barcodes/{entity_uuid}")
            entity = result.get("entity") or {}
            statuses = entity.get("statuses") or []
            if not statuses:
                continue
            last_code = (statuses[-1] or {}).get("code")
            if last_code == "READY":
                url = entity.get("url")
                break
            if last_code == "INVALID":
                raise CdekError(f"CDEK /print/barcodes INVALID: {result}")

        if not url:
            raise CdekError("CDEK /print/barcodes: timeout waiting for READY")

        if self._client is None:
            raise CdekError("CdekClient not initialized")
        headers = await self._headers()
        resp = await self._client.get(url, headers=headers)
        if resp.status_code != 200:
            raise CdekError(
                f"CDEK barcode download: {resp.status_code}",
                status=resp.status_code,
                body=resp.text[:200],
            )
        return resp.content


_client: CdekClient | None = None


async def init() -> None:
    global _client
    _client = CdekClient(CDEK_API_URL, CDEK_CLIENT_ID, CDEK_CLIENT_SECRET)
    await _client.init()


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _ensure() -> CdekClient:
    if _client is None:
        raise CdekError("cdek_client not initialized — call init() first")
    return _client


async def create_order(order_data: dict) -> dict:
    return await _ensure().create_order(order_data)


async def get_order(uuid: str) -> dict:
    return await _ensure().get_order(uuid)


async def find_uuid_by_cdek_number(cdek_number: str) -> str | None:
    return await _ensure().find_uuid_by_cdek_number(cdek_number)


async def get_barcodes_batch_pdf(order_uuids: list[str], format_: str = "A6") -> bytes:
    return await _ensure().get_barcodes_batch_pdf(order_uuids, format_=format_)
