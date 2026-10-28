"""Движок -> ИИ-сервис: `POST /v1/turn`, `POST /v1/turn/{id}/cancel`, `GET /v1/health`.

Только транспорт. Очередь, повторы и решения «что делать с отказом» живут в `ai_engine.py`, чтобы их
можно было проверить без сети: тесты подставляют `transport` (httpx.MockTransport).

Заголовки на каждом запросе: `X-Contract: 2` и `X-Service-Token`. Версия не совпала - `426`, токен не
тот - `401`/`403`: оба случая настройка, а не сбой сети, повторять их бессмысленно.
"""
import logging
from dataclasses import dataclass
from typing import Any

import httpx

import ai_engine_config as cfg

logger = logging.getLogger("uvicorn")

# Подмена транспорта для тестов (None - настоящая сеть).
_transport: httpx.AsyncBaseTransport | None = None

TIMEOUT_S = 10.0

# Результаты отправки хода.
SENT = "sent"               # сервис принял (202) или уже видел этот turn_id (200 duplicate)
RETRY = "retry"             # сеть, 5xx, 429: можно повторить
REFUSED = "refused"         # 403/426/401/4xx: повтор не поможет, нужен человек


@dataclass
class SendResult:
    kind: str
    status: int = 0
    detail: str = ""


def _headers() -> dict[str, str]:
    return {"X-Service-Token": cfg.SERVICE_TOKEN, "X-Contract": cfg.CONTRACT_VERSION}


def configured() -> bool:
    return bool(cfg.SERVICE_URL and cfg.SERVICE_TOKEN)


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=cfg.SERVICE_URL, timeout=TIMEOUT_S, transport=_transport, headers=_headers())


async def post_turn(payload: dict[str, Any]) -> SendResult:
    if not configured():
        return SendResult(REFUSED, 0, "AI_SERVICE_URL/AI_SERVICE_TOKEN не заданы")
    try:
        async with _client() as client:
            resp = await client.post("/v1/turn", json=payload)
    except httpx.HTTPError as exc:
        return SendResult(RETRY, 0, f"{type(exc).__name__}: {exc}")
    if resp.status_code in (200, 202):
        return SendResult(SENT, resp.status_code)
    detail = resp.text[:200]
    if resp.status_code == 429 or resp.status_code >= 500:
        return SendResult(RETRY, resp.status_code, detail)
    return SendResult(REFUSED, resp.status_code, detail)


async def post_cancel(turn_id: int) -> bool:
    """Best effort: без отмены сервис всё равно ничего не отправит без `permit`."""
    if not configured():
        return False
    try:
        async with _client() as client:
            resp = await client.post(f"/v1/turn/{int(turn_id)}/cancel")
        return resp.status_code == 200
    except httpx.HTTPError:
        return False


async def get_health() -> dict[str, Any] | None:
    """`{"ok": true, "queue": 0}` или None, если сервис не ответил как положено."""
    if not configured():
        return None
    try:
        async with _client() as client:
            resp = await client.get("/v1/health")
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) and data.get("ok") else None
    except (httpx.HTTPError, ValueError):
        return None
