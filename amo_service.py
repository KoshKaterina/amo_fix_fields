"""High-level amoCRM операции поверх submit_request (api.py)."""

import asyncio
import logging
import urllib.parse
from typing import Any

import httpx

from api import (
    BASE_URL,
    MAX_FETCH_RETRIES,
    MAX_PATCH_RETRIES,
    _record_429,
    _record_success,
    headers,
    is_circuit_open,
    submit_request,
)
from api_helpers import (
    RETRYABLE_STATUS_CODES,
    compute_retry_delay,
    sanitize_custom_field_value,
    trim_text,
)

logger = logging.getLogger("uvicorn")

_pipeline_cache: dict[int, int] = {}
# (pipeline_id, status_id) → {"name": str, "sort": int}
# Ключ — пара, потому что системные статусы 142/143 имеют одинаковый id во ВСЕХ воронках.
_status_info: dict[tuple[int, int], dict] = {}


def _build_url(path: str, params: list[tuple[str, str]] | None = None) -> str:
    base = f"{BASE_URL}{path}"
    if not params:
        return base
    qs = urllib.parse.urlencode(params, doseq=True)
    return f"{base}?{qs}"


async def _do_get(path: str, params: list[tuple[str, str]] | None = None) -> dict | None:
    url = _build_url(path, params)
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        if is_circuit_open():
            logger.warning("Circuit breaker open — aborting GET %s", path)
            return None
        try:
            response = await submit_request("GET", url, headers)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            logger.warning(
                "Request error on GET %s attempt %s/%s (%s)",
                path, attempt, MAX_FETCH_RETRIES, exc.__class__.__name__,
            )
            if attempt < MAX_FETCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return None
        except Exception:
            logger.exception("Unexpected error on GET %s attempt %s", path, attempt)
            if attempt < MAX_FETCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return None

        if response.status_code == 200:
            _record_success()
            try:
                return response.json()
            except ValueError:
                logger.exception("Invalid JSON for GET %s", path)
                return None

        if response.status_code == 204:
            _record_success()
            return {}

        if response.status_code == 429:
            _record_429()

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_FETCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "AmoCRM %s on GET %s attempt %s/%s — retry in %.1fs",
                response.status_code, path, attempt, MAX_FETCH_RETRIES, delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error("AmoCRM error %s on GET %s: %s", response.status_code, path, trim_text(response.text))
        return None
    return None


async def _do_patch(path: str, body: dict) -> dict[str, Any]:
    url = _build_url(path)
    for attempt in range(1, MAX_PATCH_RETRIES + 1):
        if is_circuit_open():
            logger.warning("Circuit breaker open — aborting PATCH %s", path)
            return {"ok": False, "status_code": None, "retryable": True}
        try:
            response = await submit_request("PATCH", url, headers, json_body=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            logger.warning(
                "Request error on PATCH %s attempt %s/%s (%s)",
                path, attempt, MAX_PATCH_RETRIES, exc.__class__.__name__,
            )
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return {"ok": False, "status_code": None, "retryable": True}
        except Exception:
            logger.exception("Unexpected error on PATCH %s attempt %s", path, attempt)
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return {"ok": False, "status_code": None, "retryable": True}

        if response.status_code in (200, 204):
            _record_success()
            return {"ok": True, "status_code": response.status_code, "retryable": False}

        if response.status_code == 429:
            _record_429()

        if response.status_code == 400:
            logger.error("PATCH %s 400: %s body=%s", path, trim_text(response.text), trim_text(str(body)))
            return {"ok": False, "status_code": 400, "retryable": False}

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_PATCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            await asyncio.sleep(delay)
            continue

        logger.error("PATCH %s %s: %s", path, response.status_code, trim_text(response.text))
        return {
            "ok": False,
            "status_code": response.status_code,
            "retryable": response.status_code in RETRYABLE_STATUS_CODES,
        }

    return {"ok": False, "status_code": None, "retryable": True}


# ---------------------------------------------------------------------------
# High-level operations
# ---------------------------------------------------------------------------

async def warm_pipeline_cache() -> None:
    data = await _do_get("/api/v4/leads/pipelines")
    if not data:
        logger.warning("warm_pipeline_cache: empty response — pipeline cache not loaded")
        return
    pipelines = (data.get("_embedded") or {}).get("pipelines") or []
    cache: dict[int, int] = {}
    info: dict[tuple[int, int], dict] = {}
    for pipe in pipelines:
        pipe_id = pipe.get("id")
        for st in (pipe.get("_embedded") or {}).get("statuses") or []:
            sid = st.get("id")
            if sid is not None and pipe_id is not None:
                cache[sid] = pipe_id
                info[(pipe_id, sid)] = {
                    "name": st.get("name") or "",
                    "sort": st.get("sort") or 0,
                }
    _pipeline_cache.clear()
    _pipeline_cache.update(cache)
    _status_info.clear()
    _status_info.update(info)
    logger.info("Pipeline cache warmed: %s statuses", len(cache))


def get_pipeline_id_for_status(status_id: int) -> int | None:
    return _pipeline_cache.get(status_id)


def resolve_status_id_by_name(pipeline_id: int, name: str) -> int | None:
    name_norm = (name or "").strip().lower()
    for (pid, sid), info in _status_info.items():
        if pid == pipeline_id and info["name"].strip().lower() == name_norm:
            return sid
    return None


def get_status_sort(status_id: int, pipeline_id: int) -> int | None:
    info = _status_info.get((pipeline_id, status_id))
    return info["sort"] if info else None


async def get_lead_full(lead_id: int | str, with_: tuple[str, ...] = ("contacts", "companies")) -> dict | None:
    params: list[tuple[str, str]] = []
    if with_:
        params.append(("with", ",".join(with_)))
    return await _do_get(f"/api/v4/leads/{lead_id}", params)


async def get_contact_by_id(contact_id: int | str) -> dict | None:
    return await _do_get(f"/api/v4/contacts/{contact_id}")


async def get_contacts_by_ids(contact_ids: list[int]) -> dict[int, dict]:
    """Батч-загрузка контактов по id (до 250 за один GET)."""
    unique = list({int(i) for i in contact_ids if i})
    out: dict[int, dict] = {}
    for start in range(0, len(unique), 250):
        batch = unique[start:start + 250]
        params: list[tuple[str, str]] = [("filter[id][]", str(i)) for i in batch]
        params.append(("limit", "250"))
        data = await _do_get("/api/v4/contacts", params)
        if not data:
            continue
        for c in (data.get("_embedded") or {}).get("contacts") or []:
            cid = c.get("id")
            if cid is not None:
                out[int(cid)] = c
    return out


async def get_company_by_id(company_id: int | str) -> dict | None:
    return await _do_get(f"/api/v4/companies/{company_id}")


async def get_leads_by_status(status_id: int, with_: tuple[str, ...] = ("contacts",), page_limit: int = 50) -> list[dict]:
    pipeline_id = _pipeline_cache.get(status_id)
    if pipeline_id is None:
        await warm_pipeline_cache()
        pipeline_id = _pipeline_cache.get(status_id)
    if pipeline_id is None:
        logger.error("No pipeline_id for status %s — нельзя получить сделки", status_id)
        return []

    leads: list[dict] = []
    page = 1
    while True:
        params: list[tuple[str, str]] = [
            ("filter[statuses][0][pipeline_id]", str(pipeline_id)),
            ("filter[statuses][0][status_id]", str(status_id)),
            ("limit", str(page_limit)),
            ("page", str(page)),
        ]
        if with_:
            params.append(("with", ",".join(with_)))

        data = await _do_get("/api/v4/leads", params)
        if not data:
            break
        batch = (data.get("_embedded") or {}).get("leads") or []
        if not batch:
            break
        leads.extend(batch)
        if len(batch) < page_limit:
            break
        page += 1
    return leads


async def find_leads_by_query(query: str, with_: tuple[str, ...] = ()) -> list[dict]:
    """Полнотекстовый поиск сделок (query ищет и по значениям custom-полей)."""
    params: list[tuple[str, str]] = [("query", query), ("limit", "50")]
    if with_:
        params.append(("with", ",".join(with_)))
    data = await _do_get("/api/v4/leads", params)
    if not data:
        return []
    return (data.get("_embedded") or {}).get("leads") or []


def get_custom_field_value(entity: dict, field_id: int) -> Any:
    for f in entity.get("custom_fields_values") or []:
        if f.get("field_id") == field_id:
            values = f.get("values") or []
            if values:
                return values[0].get("value")
    return None


def get_tags(entity: dict) -> list[dict]:
    return ((entity.get("_embedded") or {}).get("tags")) or []


def has_tag(entity: dict, name: str) -> bool:
    name_norm = (name or "").strip().lower()
    return any((t.get("name") or "").strip().lower() == name_norm for t in get_tags(entity))


def _make_custom_field(field_id: int, value) -> dict:
    return {"field_id": field_id, "values": [{"value": sanitize_custom_field_value(value)}]}


def _tags_payload(tags: list[dict]) -> list[dict]:
    """Сводит список tag-объектов AMO к payload-форме (id или name)."""
    out: list[dict] = []
    for t in tags:
        if t.get("id") is not None:
            out.append({"id": t["id"]})
        elif t.get("name"):
            out.append({"name": t["name"]})
    return out


async def patch_lead(
    lead_id: int | str,
    *,
    custom_fields: dict[int, Any] | None = None,
    status_id: int | None = None,
    pipeline_id: int | None = None,
    tags: list[dict] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if custom_fields:
        body["custom_fields_values"] = [
            _make_custom_field(fid, val) for fid, val in custom_fields.items()
        ]
    if status_id is not None:
        body["status_id"] = status_id
        # Для системных статусов (142/143, общие для всех воронок) воронку
        # нужно передавать явно — кэш для них неоднозначен.
        pipe = pipeline_id if pipeline_id is not None else _pipeline_cache.get(status_id)
        if pipe is not None:
            body["pipeline_id"] = pipe
    if tags is not None:
        body.setdefault("_embedded", {})["tags"] = _tags_payload(tags)
    if not body:
        return {"ok": True, "status_code": 204, "retryable": False}
    return await _do_patch(f"/api/v4/leads/{lead_id}", body)


async def add_tag(lead_id: int | str, tag_name: str) -> dict[str, Any]:
    lead = await get_lead_full(lead_id, with_=())
    if not lead:
        return {"ok": False, "status_code": None, "retryable": True}
    tags = get_tags(lead)
    if has_tag(lead, tag_name):
        return {"ok": True, "status_code": 200, "retryable": False}
    tags = list(tags) + [{"name": tag_name}]
    return await patch_lead(lead_id, tags=tags)


async def remove_tag(lead_id: int | str, tag_name: str, *, lead: dict | None = None) -> dict[str, Any]:
    if lead is None:
        lead = await get_lead_full(lead_id, with_=())
    if not lead:
        return {"ok": False, "status_code": None, "retryable": True}
    if not has_tag(lead, tag_name):
        return {"ok": True, "status_code": 200, "retryable": False}
    name_norm = tag_name.strip().lower()
    new_tags = [t for t in get_tags(lead) if (t.get("name") or "").strip().lower() != name_norm]
    return await patch_lead(lead_id, tags=new_tags)


def filter_tags_excluding(tags: list[dict], excluded_name: str) -> list[dict]:
    name_norm = (excluded_name or "").strip().lower()
    return [t for t in tags if (t.get("name") or "").strip().lower() != name_norm]


async def commit_waybill(
    lead_id: int | str,
    cdek_value: str,
    current_tags: list[dict],
    *,
    error_tag: str,
    target_status: int,
) -> dict[str, Any]:
    """Записать номер накладной + двинуть в готова + снять тег ошибки. Один PATCH."""
    from waybill_config import FIELD_CDEK_ORDER_NUMBER
    new_tags = filter_tags_excluding(current_tags, error_tag)
    return await patch_lead(
        lead_id,
        custom_fields={FIELD_CDEK_ORDER_NUMBER: cdek_value},
        status_id=target_status,
        tags=new_tags,
    )


async def move_to_ready_and_clear_error(
    lead_id: int | str,
    current_tags: list[dict],
    *,
    error_tag: str,
    target_status: int,
) -> dict[str, Any]:
    new_tags = filter_tags_excluding(current_tags, error_tag)
    return await patch_lead(lead_id, status_id=target_status, tags=new_tags)
