import asyncio
import contextvars
import logging
import os
import time
from dataclasses import dataclass
from pprint import pprint
from typing import Any
from urllib.parse import quote

import httpx
from dotenv import load_dotenv

from api_helpers import (
    MAX_CUSTOM_FIELD_VALUE_LEN,
    RETRYABLE_STATUS_CODES,
    compute_retry_delay,
    trim_text,
)

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
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("AMO_MIN_REQUEST_INTERVAL_SECONDS", "0.17"))

HTTP_TIMEOUT = httpx.Timeout(
    timeout=REQUEST_TIMEOUT_SECONDS,
    connect=CONNECT_TIMEOUT_SECONDS,
    pool=POOL_TIMEOUT_SECONDS,
)

# ---------------------------------------------------------------------------
# Circuit breaker — ПО КАТЕГОРИЯМ (тип задачи). Всплеск 429 от одной интеграции
# (jivo / sync-метрика-woo / …) открывает ТОЛЬКО её брейкер; критичные категории
# (lead / waybill / cdek) продолжают идти. Категорию несёт contextvar, который
# ставит вызывающий (воркер очереди — по kind задачи). Непомеченные пути (старт,
# фоновые опросы) идут в бакет "default". Порог/кулдаун — общие.
# ---------------------------------------------------------------------------
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("AMO_CB_THRESHOLD", "3"))
CIRCUIT_BREAKER_COOLDOWN = float(os.getenv("AMO_CB_COOLDOWN", "60"))

_breaker_category: contextvars.ContextVar[str] = contextvars.ContextVar(
    "amo_breaker_category", default="default"
)
# category -> {"consecutive": int, "open_until": float}
_breakers: dict[str, dict] = {}


def set_breaker_category(category: str) -> None:
    """Пометить текущий async-контекст типом задачи, чтобы 429 и пауза брейкера
    относились только к этой категории (jivo/lead/waybill/cdek/sync)."""
    _breaker_category.set(category or "default")


def _bstate(category: str | None) -> dict:
    cat = category or _breaker_category.get()
    st = _breakers.get(cat)
    if st is None:
        st = {"consecutive": 0, "open_until": 0.0}
        _breakers[cat] = st
    return st


def is_circuit_open(category: str | None = None) -> bool:
    return time.monotonic() < _bstate(category)["open_until"]


def _record_429(category: str | None = None) -> None:
    cat = category or _breaker_category.get()
    st = _bstate(cat)
    st["consecutive"] += 1
    if st["consecutive"] >= CIRCUIT_BREAKER_THRESHOLD:
        st["open_until"] = time.monotonic() + CIRCUIT_BREAKER_COOLDOWN
        logger.warning(
            "Circuit breaker OPEN [%s] after %s consecutive 429s — pausing '%s' for %.0fs",
            cat, st["consecutive"], cat, CIRCUIT_BREAKER_COOLDOWN,
        )


def _record_success(category: str | None = None) -> None:
    _bstate(category)["consecutive"] = 0


# ---------------------------------------------------------------------------
# Sequential API pipeline — one request at a time, 0.17s after each response
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
                delay = compute_retry_delay(attempt)
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
                await asyncio.sleep(compute_retry_delay(attempt))
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
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
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
            trim_text(response.text),
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
                delay = compute_retry_delay(attempt)
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
                await asyncio.sleep(compute_retry_delay(attempt))
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
                trim_text(response.text),
            )
            return _update_result(ok=False, status_code=response.status_code, retryable=False)

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_PATCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
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
            trim_text(response.text),
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


# ---------------------------------------------------------------------------
# Generic create/find helpers (Jivo bridge: контакт + сделка + примечание)
# Идут через тот же последовательный pipeline (submit_request), что и остальные
# вызовы amo, с теми же ретраями и общим circuit breaker по 429.
# ---------------------------------------------------------------------------

async def _request_json(method: str, url: str, body: Any = None, what: str = "") -> Any:
    """Выполняет GET/POST с ретраями. Возвращает распарсенный JSON (dict/list),
    {} для пустого 204, либо None при неустранимой ошибке."""
    for attempt in range(1, MAX_PATCH_RETRIES + 1):
        try:
            response = await submit_request(method, url, headers, json_body=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError:
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            logger.exception("%s: request error after %s attempts", what, MAX_PATCH_RETRIES)
            return None
        except Exception:
            logger.exception("%s: unexpected error on attempt %s", what, attempt)
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return None

        if response.status_code in (200, 201, 204):
            _record_success()
            if response.status_code == 204 or not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                logger.exception("%s: invalid JSON in response", what)
                return None

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("%s: circuit breaker open — aborting", what)
                return None

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_PATCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "%s: amo status %s, retry %s/%s in %.1fs",
                what, response.status_code, attempt, MAX_PATCH_RETRIES, delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error("%s: failed %s %s", what, response.status_code, trim_text(response.text))
        return None

    return None


async def find_contact_id(query: str) -> int | None:
    """Ищет контакт по строке (телефон или email). Возвращает id первого
    совпадения или None. Используется для дедупликации перед созданием."""
    query = (query or "").strip()
    if not query:
        return None
    url = f"{BASE_URL}/api/v4/contacts?query={quote(query)}&limit=1"
    data = await _request_json("GET", url, what=f"find_contact[{query}]")
    if not isinstance(data, dict):
        return None
    contacts = (data.get("_embedded") or {}).get("contacts") or []
    return contacts[0].get("id") if contacts else None


async def add_note_to_lead(lead_id: Any, text: str) -> bool:
    """Добавляет обычное примечание (common) к сделке. True при успехе."""
    url = f"{BASE_URL}/api/v4/leads/{lead_id}/notes"
    body = [{"note_type": "common", "params": {"text": str(text)}}]
    data = await _request_json("POST", url, body=body, what=f"add_note[{lead_id}]")
    return data is not None


async def create_contact(name: Any, phone: Any, email: Any) -> int | None:
    """Создаёт контакт с телефоном/email. Возвращает id или None."""
    custom_fields = []
    if phone:
        custom_fields.append({"field_code": "PHONE", "values": [{"value": str(phone), "enum_code": "WORK"}]})
    if email:
        custom_fields.append({"field_code": "EMAIL", "values": [{"value": str(email), "enum_code": "WORK"}]})
    contact: dict[str, Any] = {"name": str(name) if name else (str(phone or email) or "Клиент Jivo")}
    if custom_fields:
        contact["custom_fields_values"] = custom_fields
    url = f"{BASE_URL}/api/v4/contacts"
    data = await _request_json("POST", url, body=[contact], what="create_contact")
    if not isinstance(data, dict):
        return None
    items = (data.get("_embedded") or {}).get("contacts") or []
    return items[0].get("id") if items else None


async def create_lead_direct(
    name: Any,
    pipeline_id: int,
    status_id: int,
    responsible_user_id: int | None = None,
    custom_fields_values: list | None = None,
    contact_id: int | None = None,
    tags: list | None = None,
) -> int | None:
    """Создаёт сделку напрямую в обычном (type=0) статусе воронки — в обход
    «Неразобранного» и его автораспределения. Для триажа Jivo: закрыть в 143
    или сразу назначить ответственного. tags — метки (напр. для исключения из
    распределения). Возвращает id сделки или None."""
    lead: dict[str, Any] = {
        "name": str(name),
        "pipeline_id": int(pipeline_id),
        "status_id": int(status_id),
    }
    if responsible_user_id:
        lead["responsible_user_id"] = int(responsible_user_id)
    if custom_fields_values:
        lead["custom_fields_values"] = custom_fields_values
    embedded: dict[str, Any] = {}
    if contact_id:
        embedded["contacts"] = [{"id": int(contact_id)}]
    if tags:
        embedded["tags"] = [{"name": str(t)} for t in tags if str(t).strip()]
    if embedded:
        lead["_embedded"] = embedded
    url = f"{BASE_URL}/api/v4/leads"
    data = await _request_json("POST", url, body=[lead], what="create_lead_direct")
    if not isinstance(data, dict):
        return None
    items = (data.get("_embedded") or {}).get("leads") or []
    return items[0].get("id") if items else None


async def create_task(
    entity_id: int,
    text: str,
    responsible_user_id: int | None,
    complete_till: int,
    task_type_id: int | None = None,
    entity_type: str = "leads",
) -> bool:
    """Создаёт задачу на сделке (entity_type=leads). complete_till — unix-срок."""
    task: dict[str, Any] = {
        "entity_id": int(entity_id),
        "entity_type": entity_type,
        "text": str(text),
        "complete_till": int(complete_till),
    }
    if responsible_user_id:
        task["responsible_user_id"] = int(responsible_user_id)
    if task_type_id:
        task["task_type_id"] = int(task_type_id)
    url = f"{BASE_URL}/api/v4/tasks"
    data = await _request_json("POST", url, body=[task], what=f"create_task[{entity_id}]")
    return data is not None


async def create_unsorted_lead(
    lead_name: Any,
    pipeline_id: int,
    contact: dict,
    source_uid: str,
    page_url: str,
    created_ts: int,
    source_name: str = "Jivo онлайн-чат",
) -> tuple[int | None, int | None]:
    """Создаёт заявку в «Неразобранное» воронки (system-статус type=1, куда
    обычный POST /leads нельзя). Контакт передаётся встроенно: либо {"id": ...}
    для найденного дубля, либо новый dict с PHONE/EMAIL. Возвращает
    (lead_id, contact_id)."""
    referer = page_url or f"{BASE_URL}"
    form = {
        "source_name": source_name,
        "source_uid": str(source_uid),
        "pipeline_id": int(pipeline_id),
        "created_at": int(created_ts),
        # metadata — на ВЕРХНЕМ уровне запроса (внутри _embedded amo даёт 400
        # FieldMissing), это обязательный блок формы.
        "metadata": {
            "form_id": "jivo_chat",
            "form_name": source_name,
            "form_page": referer,
            "form_sent_at": int(created_ts),
            "referer": referer,
            "ip": "0.0.0.0",
        },
        "_embedded": {
            "leads": [{"name": str(lead_name)}],
            "contacts": [contact],
        },
    }
    url = f"{BASE_URL}/api/v4/leads/unsorted/forms"
    data = await _request_json("POST", url, body=[form], what="create_unsorted")
    if not isinstance(data, dict):
        return None, None
    unsorted = (data.get("_embedded") or {}).get("unsorted") or []
    if not unsorted:
        return None, None
    emb = (unsorted[0].get("_embedded") or {})
    leads = emb.get("leads") or []
    contacts = emb.get("contacts") or []
    lead_id = leads[0].get("id") if leads else None
    contact_id = contacts[0].get("id") if contacts else None
    return lead_id, contact_id


if __name__ == "__main__":
    async def _main():
        init_api_pipeline()
        try:
            lead_info = await get_lead_by_id(36420147)
            pprint(lead_info)
        finally:
            await shutdown_api_pipeline()

    asyncio.run(_main())
