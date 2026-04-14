import asyncio
import logging
import os
import time
from dataclasses import dataclass
from pprint import pprint
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uvicorn")

integration_id = os.environ.get("INTEGRATION_ID")
secret_key = os.getenv("SECRET_KEY")

current_token = os.getenv("TOKEN")

headers = {"Authorization": f"Bearer {current_token}"}

REQUEST_TIMEOUT_SECONDS = float(os.getenv("AMO_REQUEST_TIMEOUT_SECONDS", "20"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("AMO_CONNECT_TIMEOUT_SECONDS", "30"))
POOL_TIMEOUT_SECONDS = float(os.getenv("AMO_POOL_TIMEOUT_SECONDS", "20"))
MAX_FETCH_RETRIES = int(os.getenv("AMO_FETCH_RETRIES", "4"))
MAX_PATCH_RETRIES = int(os.getenv("AMO_PATCH_RETRIES", "4"))
MAX_CUSTOM_FIELD_VALUE_LEN = 256
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("AMO_MIN_REQUEST_INTERVAL_SECONDS", "0.51"))
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

HTTP_TIMEOUT = httpx.Timeout(
    timeout=REQUEST_TIMEOUT_SECONDS,
    connect=CONNECT_TIMEOUT_SECONDS,
    pool=POOL_TIMEOUT_SECONDS,
)

# ---------------------------------------------------------------------------
# Global circuit breaker — pauses all outgoing requests when 429s pile up
# ---------------------------------------------------------------------------
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("AMO_CB_THRESHOLD", "3"))
CIRCUIT_BREAKER_COOLDOWN = float(os.getenv("AMO_CB_COOLDOWN", "60"))

_consecutive_429s = 0
_circuit_open_until = 0.0


def is_circuit_open() -> bool:
    return time.monotonic() < _circuit_open_until


def _record_429() -> None:
    global _consecutive_429s, _circuit_open_until
    _consecutive_429s += 1
    if _consecutive_429s >= CIRCUIT_BREAKER_THRESHOLD:
        _circuit_open_until = time.monotonic() + CIRCUIT_BREAKER_COOLDOWN
        logger.warning(
            "Circuit breaker OPEN after %s consecutive 429s — pausing all requests for %.0fs",
            _consecutive_429s,
            CIRCUIT_BREAKER_COOLDOWN,
        )


def _record_success() -> None:
    global _consecutive_429s
    _consecutive_429s = 0


# ---------------------------------------------------------------------------
# Sequential API pipeline — one request at a time, 0.51s after each response
# ---------------------------------------------------------------------------

@dataclass
class ApiRequest:
    method: str
    url: str
    req_headers: dict
    json_body: dict | None
    future: asyncio.Future


_api_queue: asyncio.Queue[ApiRequest] | None = None
_api_worker_task: asyncio.Task | None = None
_last_response_at: float = 0.0


def init_api_pipeline() -> None:
    global _api_queue, _api_worker_task
    _api_queue = asyncio.Queue()
    _api_worker_task = asyncio.create_task(_api_worker())
    logger.info("API sequential pipeline started (interval=%.2fs)", MIN_REQUEST_INTERVAL_SECONDS)


async def shutdown_api_pipeline() -> None:
    global _api_worker_task
    if _api_worker_task is not None:
        _api_worker_task.cancel()
        try:
            await _api_worker_task
        except asyncio.CancelledError:
            pass
        _api_worker_task = None

    if _api_queue is not None:
        while not _api_queue.empty():
            try:
                req = _api_queue.get_nowait()
                if not req.future.done():
                    req.future.set_exception(asyncio.CancelledError())
            except asyncio.QueueEmpty:
                break

    logger.info("API sequential pipeline stopped")


async def _api_worker() -> None:
    global _last_response_at
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        while True:
            req = await _api_queue.get()
            try:
                now = time.monotonic()
                wait_for = (_last_response_at + MIN_REQUEST_INTERVAL_SECONDS) - now
                if wait_for > 0:
                    await asyncio.sleep(wait_for)

                if req.method == "GET":
                    response = await client.get(req.url, headers=req.req_headers)
                elif req.method == "PATCH":
                    response = await client.patch(req.url, headers=req.req_headers, json=req.json_body)
                else:
                    response = await client.request(req.method, req.url, headers=req.req_headers, json=req.json_body)

                _last_response_at = time.monotonic()

                if not req.future.done():
                    req.future.set_result(response)
            except asyncio.CancelledError:
                if not req.future.done():
                    req.future.set_exception(asyncio.CancelledError())
                raise
            except Exception as exc:
                _last_response_at = time.monotonic()
                if not req.future.done():
                    req.future.set_exception(exc)
            finally:
                _api_queue.task_done()


async def submit_request(method: str, url: str, req_headers: dict, json_body: dict | None = None) -> httpx.Response:
    if _api_queue is None:
        raise RuntimeError("API pipeline not initialized — call init_api_pipeline() first")
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    await _api_queue.put(ApiRequest(
        method=method,
        url=url,
        req_headers=req_headers,
        json_body=json_body,
        future=future,
    ))
    return await future


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_retry_delay(attempt: int, retry_after_header: str | None = None) -> float:
    if retry_after_header:
        try:
            return max(1.0, float(retry_after_header))
        except ValueError:
            pass
    return min(30.0, float(2 ** (attempt - 1)))


def _trim_text(value: str, max_len: int = 700) -> str:
    if len(value) <= max_len:
        return value
    return f"{value[:max_len]}...<truncated>"


def _sanitize_custom_field_value(value: Any, field_id: int, lead_id: Any) -> str:
    value_str = str(value)
    if len(value_str) <= MAX_CUSTOM_FIELD_VALUE_LEN:
        return value_str

    logger.warning(
        "Truncating field %s for lead %s from %s to %s chars",
        field_id,
        lead_id,
        len(value_str),
        MAX_CUSTOM_FIELD_VALUE_LEN,
    )
    return value_str[:MAX_CUSTOM_FIELD_VALUE_LEN]


def _update_result(ok: bool, status_code: int | None = None, retryable: bool = False) -> dict[str, Any]:
    return {"ok": ok, "status_code": status_code, "retryable": retryable}


# ---------------------------------------------------------------------------
# AmoCRM API functions
# ---------------------------------------------------------------------------

BASE_URL = "https://new5a2e8ea7b16b4.amocrm.ru"


async def auth():
    body = {
        "client_id": integration_id,
        "client_secret": secret_key,
        "grant_type": "authorization_code",
        "code": "def5020093463e984c956d5b3258cfad73c1387473a85cd733b384576db0555198b3f674efacec89407f4a055d619eee71b693c80a3ae045e05418a0ef2a098ebbf43f9f0405c56ac3c419bd9e3479d0f6fca16146fdf7b0ca844a3563bed928d79dfcfb2e0445314bea6d470b5c36aaeb146bb58647078e7829cb190ef600f1072dd36ecd7230cd7e6ae4830bf0e251d5321f7f5d564d77f2cd597e2508423fb391f05760d10f4c88d1d4ba783c62852b489510dba58e0e2540ba54e93afcafda77a7b0a29d1b35c20d1c6da55fcb4733224d1b0e66e2f2caea774071d6efd717403e17906a0e48af31ca1e5e3a50246a64070cdea3b48417b719a060b8cc4a44cd6736cf82d207c4c1288c3d3ecf20b93e413fa138d243b542c6db85e154aa606a0a3066b675e6e0d882832e7ccbfbceea0e6d417438f08bfbdd79b198f144c59127b62164395a1bf152ed19415a6a3cb7bf0a354e7e84e16bbe7549cf0bbf3815403bcbb7ee56ac16f1efff5318ae529758ca8c15b91ccdef1f636f46f532aff17bc2573005a4a7997846b36ffb6988badaefa6b5c46606d6efc80d2ce796a5f6ade82be70998ecc9c082cd5af915cffaefa422db2ba94edda6cc5d98f6066a2dd446980c9449fc5c75299a60a889737aa15f7924fffd2fc1fe3846bdae55ceede77e0b8fdba81f5fa3ae6c9031d76a26ac",
        "redirect_uri": f"{BASE_URL}",
    }
    async with httpx.AsyncClient() as client:
        response = (
            await client.post(f"{BASE_URL}/oauth2/access_token", data=body)
        ).json()
    return response


async def get_lead_by_id(lead_id):
    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            response = await submit_request("GET", url, headers)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            if attempt < MAX_FETCH_RETRIES:
                delay = _compute_retry_delay(attempt)
                logger.warning(
                    "Request error fetching lead %s on attempt %s/%s (%s). Retrying in %.1fs",
                    lead_id,
                    attempt,
                    MAX_FETCH_RETRIES,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.exception(
                "Request error fetching lead %s after %s attempts",
                lead_id,
                MAX_FETCH_RETRIES,
            )
            return None
        except Exception as exc:
            logger.exception("Unexpected error fetching lead %s on attempt %s", lead_id, attempt)
            if attempt < MAX_FETCH_RETRIES:
                await asyncio.sleep(_compute_retry_delay(attempt))
                continue
            return None

        if response.status_code == 200:
            _record_success()
            try:
                return response.json()
            except ValueError:
                logger.exception("Invalid JSON payload while fetching lead %s", lead_id)
                return None

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("Circuit breaker open — aborting fetch for lead %s", lead_id)
                return None

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_FETCH_RETRIES:
            delay = _compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "AmoCRM status %s for lead %s on fetch attempt %s/%s. Retrying in %.1fs",
                response.status_code,
                lead_id,
                attempt,
                MAX_FETCH_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error(
            "AmoCRM error %s for lead %s: %s",
            response.status_code,
            lead_id,
            _trim_text(response.text),
        )
        return None

    return None


async def add_info_from_ms(goods, delivery_type, delivery_address, comment, promo_type, lead_id, name):
    custom_fields = []
    if goods:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(goods, 577313, lead_id), 577313))
    if delivery_type:
        custom_fields.append(
            create_custom_field(_sanitize_custom_field_value(delivery_type, 577315, lead_id), 577315)
        )
    if delivery_address:
        custom_fields.append(
            create_custom_field(_sanitize_custom_field_value(delivery_address, 577311, lead_id), 577311)
        )
    if comment:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(comment, 577753, lead_id), 577753))
    if promo_type:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(promo_type, 570661, lead_id), 570661))

    body = {
        "id": lead_id,
        "custom_fields_values": custom_fields,
    }
    if name:
        body["name"] = str(name)

    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    for attempt in range(1, MAX_PATCH_RETRIES + 1):
        try:
            response = await submit_request("PATCH", url, headers, json_body=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            if attempt < MAX_PATCH_RETRIES:
                delay = _compute_retry_delay(attempt)
                logger.warning(
                    "Request error patching lead %s on attempt %s/%s (%s). Retrying in %.1fs",
                    lead_id,
                    attempt,
                    MAX_PATCH_RETRIES,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.exception(
                "Request error patching lead %s after %s attempts",
                lead_id,
                MAX_PATCH_RETRIES,
            )
            return _update_result(ok=False, status_code=None, retryable=True)
        except Exception as exc:
            logger.exception("Unexpected error patching lead %s on attempt %s", lead_id, attempt)
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(_compute_retry_delay(attempt))
                continue
            return _update_result(ok=False, status_code=None, retryable=True)

        if response.status_code in [200, 204]:
            _record_success()
            logger.info("Successfully updated lead %s", lead_id)
            return _update_result(ok=True, status_code=response.status_code, retryable=False)

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("Circuit breaker open — aborting patch for lead %s", lead_id)
                return _update_result(ok=False, status_code=429, retryable=False)

        if response.status_code == 400:
            logger.error(
                "Failed to update lead %s: %s %s",
                lead_id,
                response.status_code,
                _trim_text(response.text),
            )
            return _update_result(ok=False, status_code=response.status_code, retryable=False)

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_PATCH_RETRIES:
            delay = _compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "AmoCRM status %s for lead %s on patch attempt %s/%s. Retrying in %.1fs",
                response.status_code,
                lead_id,
                attempt,
                MAX_PATCH_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error(
            "Failed to update lead %s: %s %s",
            lead_id,
            response.status_code,
            _trim_text(response.text),
        )
        return _update_result(
            ok=False,
            status_code=response.status_code,
            retryable=response.status_code in RETRYABLE_STATUS_CODES,
        )

    return _update_result(ok=False, status_code=None, retryable=True)


def create_custom_field(value, id):
    new_field = {
        "field_id": id,
        "values": [
            {
                "value": value,
            }
        ],
    }
    return new_field


if __name__ == "__main__":
    async def _main():
        init_api_pipeline()
        try:
            lead_info = await get_lead_by_id(36420147)
            pprint(lead_info)
        finally:
            await shutdown_api_pipeline()

    asyncio.run(_main())
