"""Клиент API GetCourse: записать номер сделки amo в заказ и выгрузить заказы для сверки.

Справка: https://getcourse.ru/help/api. Особенности, о которые легко споткнуться:
- Import API (запись) - POST form-data: action, key и params = JSON в base64.
- Export API асинхронный: запрос возвращает export_id, данные забираются отдельным
  запросом, когда файл готов. Выгрузка заказов за сутки собирается МИНУТАМИ.
- Экспорт идёт в один поток на школу: пока строится один, второй отвечает «Уже запущен
  один экспорт». Лимит - 100 запросов за 2 часа, и проверки готовности тоже считаются.
"""

import base64
import json
import logging

import httpx

from waybill_config import GETCOURSE_ACCOUNT_URL, GETCOURSE_API_KEY

logger = logging.getLogger("uvicorn.error")

TIMEOUT_S = 30.0

# Подменяется в тестах (httpx.MockTransport).
_transport: httpx.AsyncBaseTransport | None = None


class GcUnavailable(Exception):
    """GetCourse не ответил или ответил ошибкой сервера - можно повторить позже."""


def configured() -> bool:
    return bool(GETCOURSE_API_KEY)


async def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=GETCOURSE_ACCOUNT_URL, timeout=TIMEOUT_S, transport=_transport)


async def _get(path: str, params: dict) -> dict:
    try:
        async with await _client() as client:
            resp = await client.get(path, params={**params, "key": GETCOURSE_API_KEY})
    except httpx.HTTPError as exc:
        raise GcUnavailable(f"GET {path}: {type(exc).__name__}") from exc
    if resp.status_code >= 400:
        raise GcUnavailable(f"GET {path}: {resp.status_code}")
    try:
        return resp.json()
    except ValueError as exc:
        raise GcUnavailable(f"GET {path}: не JSON") from exc


async def set_deal_field(deal_number: str, email: str, field_name: str, value: str) -> bool:
    """Записать доп. поле существующего заказа по его номеру. True - GetCourse подтвердил.

    refresh_if_exists=0: данные самого ученика не трогаем, меняется только поле заказа.
    """
    params = {
        "user": {"email": email},
        "system": {"refresh_if_exists": 0},
        "deal": {"deal_number": str(deal_number), "addfields": {field_name: str(value)}},
    }
    body = {
        "action": "add",
        "key": GETCOURSE_API_KEY,
        "params": base64.b64encode(json.dumps(params, ensure_ascii=False).encode("utf-8")).decode("ascii"),
    }
    try:
        async with await _client() as client:
            resp = await client.post("/pl/api/deals", data=body)
    except httpx.HTTPError as exc:
        raise GcUnavailable(f"deals add: {type(exc).__name__}") from exc
    if resp.status_code >= 500:
        raise GcUnavailable(f"deals add: {resp.status_code}")
    try:
        data = resp.json()
    except ValueError:
        return False
    result = data.get("result") or {}
    ok = bool(data.get("success")) and bool(result.get("success"))
    if not ok:
        logger.warning("getcourse: заказ %s - поле не записалось: %s", deal_number,
                       (result.get("error_message") or data.get("error_message") or "")[:200])
    return ok


async def start_deals_export(created_from: str) -> int | None:
    """Запустить выгрузку заказов, созданных начиная с даты (ГГГГ-ММ-ДД). None - сейчас нельзя
    (уже идёт другой экспорт или лимит); это не ошибка, сверка попробует в следующий раз."""
    data = await _get("/pl/api/account/deals", {"created_at[from]": created_from})
    export_id = (data.get("info") or {}).get("export_id")
    if not data.get("success") or not export_id:
        logger.info("getcourse: выгрузка заказов не запущена: %s", (data.get("error_message") or "")[:200])
        return None
    return int(export_id)


async def fetch_export(export_id: int) -> tuple[list[str], list[list]] | None:
    """Готовая выгрузка: (названия колонок, строки). None - файл ещё строится."""
    data = await _get(f"/pl/api/account/exports/{int(export_id)}", {})
    info = data.get("info") or {}
    if not data.get("success") or "items" not in info:
        return None
    return list(info.get("fields") or []), list(info.get("items") or [])
