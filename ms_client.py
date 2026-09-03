"""Минимальный async-клиент API МойСклад (только чтение) для счёта Ozon (ozon_invoice).

Нужен лишь GET: список изменённых заказов и метаданные статусов. Запись в МС
модуль НЕ делает (двигает только сделки amoCRM). Bearer-токен + rate limit + retry.
"""

import asyncio
import logging
import time

import httpx

from waybill_config import MS_API_URL, MS_TOKEN

logger = logging.getLogger("uvicorn")

_client: httpx.AsyncClient | None = None
_last_request = 0.0
_min_interval = 1.0 / 3  # ≤3 запроса/с (как в woo-клиенте МС)
_lock = asyncio.Lock()


def init() -> None:
    """Поднимает общий клиент. Зовётся из lifespan (webhooks.py) и НИКОГДА не
    прячется внутрь модуля, который можно выключить флагом: 05.08.2026 клиент
    жил внутри синка Фулфилмента, синк выключили — и счета Ozon перестали
    создаваться. Идемпотентно: повторный вызов не пересоздаёт соединения."""
    global _client
    if _client is not None:
        return
    if not MS_TOKEN:
        logger.warning("MS_TOKEN пуст — клиент МойСклада не поднят")
        return
    _client = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {MS_TOKEN}", "Accept-Encoding": "gzip"},
        timeout=httpx.Timeout(30.0, connect=30.0),
    )


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def get(path: str, params: dict | None = None, retries: int = 3) -> dict | None:
    global _last_request
    if _client is None:
        logger.error("MS client не инициализирован")
        return None
    url = f"{MS_API_URL}/{path.lstrip('/')}"
    for attempt in range(1, retries + 1):
        async with _lock:
            elapsed = time.monotonic() - _last_request
            if elapsed < _min_interval:
                await asyncio.sleep(_min_interval - elapsed)
            _last_request = time.monotonic()
        try:
            resp = await _client.get(url, params=params)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            logger.warning("MS GET %s ошибка соединения (%s/%s): %s", path, attempt, retries, exc.__class__.__name__)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        if resp.status_code == 429:
            await asyncio.sleep(int(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code >= 500 and attempt < retries:
            await asyncio.sleep(2 ** attempt)
            continue
        if resp.status_code >= 400:
            logger.error("MS GET %s → %s: %s", path, resp.status_code, resp.text[:300])
            return None
        try:
            return resp.json()
        except ValueError:
            logger.exception("MS GET %s: невалидный JSON", path)
            return None
    return None


async def put(path: str, body: dict, retries: int = 3) -> dict | None:
    """PUT к МС. Используется для простановки статуса заказа (идемпотентно —
    set state X безопасно ретраить)."""
    global _last_request
    if _client is None:
        logger.error("MS client не инициализирован")
        return None
    url = f"{MS_API_URL}/{path.lstrip('/')}"
    for attempt in range(1, retries + 1):
        async with _lock:
            elapsed = time.monotonic() - _last_request
            if elapsed < _min_interval:
                await asyncio.sleep(_min_interval - elapsed)
            _last_request = time.monotonic()
        try:
            resp = await _client.put(url, json=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            logger.warning("MS PUT %s ошибка (%s/%s): %s", path, attempt, retries, exc.__class__.__name__)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        if resp.status_code == 429:
            await asyncio.sleep(int(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code >= 500 and attempt < retries:
            await asyncio.sleep(2 ** attempt)
            continue
        if resp.status_code >= 400:
            logger.error("MS PUT %s → %s: %s", path, resp.status_code, resp.text[:300])
            return None
        try:
            return resp.json()
        except ValueError:
            return {}
    return None


async def post(path: str, body: dict, retries: int = 3) -> dict | None:
    """POST к МС - создание сущностей (заведён для протеза amgroup, сама
    сборка и вызов - в соседних срезах). Ретраи, rate-limit и заголовки -
    ровно как в put(), ничего не меняем в get()/put() рядом.

    ⚠️ Как и get()/put(), при ошибке соединения или ответе с кодом ошибки
    возвращает None - и это НЕ значит «склад ответил пусто». 03.09.2026 на
    этой путанице сгорел order_watchdog: get() вернул None из-за обрыва сети,
    вызывающий код прочитал None как пустой список и объявил живые заказы
    потерянными. Вызывающий код обязан отличать None (склад не ответил) от
    настоящего успешного ответа, а не смешивать их."""
    global _last_request
    if _client is None:
        logger.error("MS client не инициализирован")
        return None
    url = f"{MS_API_URL}/{path.lstrip('/')}"
    for attempt in range(1, retries + 1):
        async with _lock:
            elapsed = time.monotonic() - _last_request
            if elapsed < _min_interval:
                await asyncio.sleep(_min_interval - elapsed)
            _last_request = time.monotonic()
        try:
            resp = await _client.post(url, json=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            logger.warning("MS POST %s ошибка (%s/%s): %s", path, attempt, retries, exc.__class__.__name__)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        if resp.status_code == 429:
            await asyncio.sleep(int(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code >= 500 and attempt < retries:
            await asyncio.sleep(2 ** attempt)
            continue
        if resp.status_code >= 400:
            logger.error("MS POST %s → %s: %s", path, resp.status_code, resp.text[:300])
            return None
        try:
            return resp.json()
        except ValueError:
            return {}
    return None
