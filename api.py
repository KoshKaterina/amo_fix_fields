import asyncio
import logging
import os
import time
from pprint import pprint
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uvicorn")

integration_id = os.environ.get("INTEGRATION_ID")
secret_key = os.getenv("SECRET_KEY")

current_token = os.getenv("TOKEN")

headers = {f"Authorization": f"Bearer {current_token}"}

REQUEST_TIMEOUT_SECONDS = float(os.getenv("AMO_REQUEST_TIMEOUT_SECONDS", "20"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("AMO_CONNECT_TIMEOUT_SECONDS", "30"))
POOL_TIMEOUT_SECONDS = float(os.getenv("AMO_POOL_TIMEOUT_SECONDS", "20"))
MAX_FETCH_RETRIES = int(os.getenv("AMO_FETCH_RETRIES", "4"))
MAX_PATCH_RETRIES = int(os.getenv("AMO_PATCH_RETRIES", "4"))
MAX_CUSTOM_FIELD_VALUE_LEN = 256
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("AMO_MIN_REQUEST_INTERVAL_SECONDS", "0.35"))
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

_request_lock = asyncio.Lock()
_last_request_at = 0.0
HTTP_TIMEOUT = httpx.Timeout(
    timeout=REQUEST_TIMEOUT_SECONDS,
    connect=CONNECT_TIMEOUT_SECONDS,
    pool=POOL_TIMEOUT_SECONDS,
)


async def _throttle_outgoing_requests() -> None:
    if MIN_REQUEST_INTERVAL_SECONDS <= 0:
        return

    global _last_request_at
    async with _request_lock:
        now = time.monotonic()
        wait_for = (_last_request_at + MIN_REQUEST_INTERVAL_SECONDS) - now
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _last_request_at = time.monotonic()


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


async def auth():
    body = {
        "client_id": integration_id,
        "client_secret": secret_key,
        "grant_type": "authorization_code",
        "code": "def5020093463e984c956d5b3258cfad73c1387473a85cd733b384576db0555198b3f674efacec89407f4a055d619eee71b693c80a3ae045e05418a0ef2a098ebbf43f9f0405c56ac3c419bd9e3479d0f6fca16146fdf7b0ca844a3563bed928d79dfcfb2e0445314bea6d470b5c36aaeb146bb58647078e7829cb190ef600f1072dd36ecd7230cd7e6ae4830bf0e251d5321f7f5d564d77f2cd597e2508423fb391f05760d10f4c88d1d4ba783c62852b489510dba58e0e2540ba54e93afcafda77a7b0a29d1b35c20d1c6da55fcb4733224d1b0e66e2f2caea774071d6efd717403e17906a0e48af31ca1e5e3a50246a64070cdea3b48417b719a060b8cc4a44cd6736cf82d207c4c1288c3d3ecf20b93e413fa138d243b542c6db85e154aa606a0a3066b675e6e0d882832e7ccbfbceea0e6d417438f08bfbdd79b198f144c59127b62164395a1bf152ed19415a6a3cb7bf0a354e7e84e16bbe7549cf0bbf3815403bcbb7ee56ac16f1efff5318ae529758ca8c15b91ccdef1f636f46f532aff17bc2573005a4a7997846b36ffb6988badaefa6b5c46606d6efc80d2ce796a5f6ade82be70998ecc9c082cd5af915cffaefa422db2ba94edda6cc5d98f6066a2dd446980c9449fc5c75299a60a889737aa15f7924fffd2fc1fe3846bdae55ceede77e0b8fdba81f5fa3ae6c9031d76a26ac",
        "redirect_uri": "https://new5a2e8ea7b16b4.amocrm.ru",
    }
    async with httpx.AsyncClient() as client:
        response = (
            await client.post("https://new5a2e8ea7b16b4.amocrm.ru/oauth2/access_token", data=body)
        ).json()
    return response


async def get_lead_by_id(lead_id):
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for attempt in range(1, MAX_FETCH_RETRIES + 1):
            await _throttle_outgoing_requests()
            try:
                response = await client.get(
                    f"https://new5a2e8ea7b16b4.amocrm.ru/api/v4/leads/{lead_id}",
                    headers=headers,
                )
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

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    logger.exception("Invalid JSON payload while fetching lead %s", lead_id)
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

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for attempt in range(1, MAX_PATCH_RETRIES + 1):
            await _throttle_outgoing_requests()
            try:
                response = await client.patch(
                    f"https://new5a2e8ea7b16b4.amocrm.ru/api/v4/leads/{lead_id}",
                    headers=headers,
                    json=body,
                )
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

            if response.status_code in [200, 204]:
                logger.info("Successfully updated lead %s", lead_id)
                return _update_result(ok=True, status_code=response.status_code, retryable=False)

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
    lead_info = asyncio.run(get_lead_by_id(36420147))
    pprint(lead_info)
