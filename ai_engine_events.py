"""Движок -> панель: события для журнала ИИ (`POST /api/ingest/ai/engine-event`).

Сегодня одно событие, `mode_release` (сделка тихо снята при смене режима, `ai_engine._release_for_mode`): оно показывается в журнале ИИ
на экране панели. Тело собирает `ai_engine_modes.release_event`, контракт зафиксирован с панелью и не меняется в одну сторону:

    {"kind": "mode_release", "deal_id": 36587533, "from_mode": "test", "to_mode": "full", "epoch": 5,
     "at": "2026-11-23T10:00:00Z", "note": "снята при смене режима"}

Адрес и заголовок те же, что у чтения настроек (`ai_engine_settings.fetch_once`): `AI_PANEL_URL` и `X-Ingest-Token: AI_INGEST_TOKEN`.
Личных данных клиента в событии нет, только номер сделки. По паре `deal_id` + `at` панель отсекает повторы.

⚠️ Источник правды о снятиях остаётся таблица `mode_release` в sqlite движка. Событие лишь витрина, поэтому потеря допустима и ничего
не копится: отправка идёт фоном и не задерживает снятие, повторы ограничены (паузы 1, 5, 25 с, как у `POST /v1/turn`, потом отказ),
очередь ограничена `QUEUE_MAX` (полная очередь отбрасывает новое событие), на диск ничего не пишется, рестарт теряет неотправленное.

⚠️ Движок выкатывается РАНЬШЕ панели: пока ручки в панели нет, она отвечает 404. Это штатно: 404, 401/403 и прочие 4xx (кроме 408/429)
не повторяются, событие отбрасывается, а в лог такой отказ попадает не чаще раза в `LOG_EVERY_S` на вид ошибки (без спама).

Ничего не отправляется, если: движок выключен (`AI_ENGINE_ENABLED`), нет адреса или токена панели, активный режим `off` (проверяется
перед каждой попыткой, не только при постановке). Тесты подставляют `_transport` (httpx.MockTransport); без него под pytest сети нет вовсе.
"""
import asyncio
import collections
import logging
import os
import time
from typing import Any, Callable

import httpx

import ai_engine_config as cfg
import ai_engine_settings as settings

logger = logging.getLogger("uvicorn")

PATH = "/api/ingest/ai/engine-event"
TIMEOUT_S = 10.0
RETRY_DELAYS_S = (1, 5, 25)             # паузы между попытками: всего 4 попытки, потом отказ
QUEUE_MAX = 100                         # событий в очереди (не считая отправляемого): больше не копим
LOG_EVERY_S = 600                       # один и тот же вид отказа в лог не чаще раза в 10 минут

# Подмена транспорта для тестов (None: настоящая сеть, под pytest её нет, см. `may_send`).
_transport: httpx.AsyncBaseTransport | None = None

# Результат одной попытки.
SENT = "sent"                           # панель приняла (2xx)
RETRY = "retry"                         # сеть, 5xx, 408, 429: можно повторить
REFUSED = "refused"                     # 404, 401/403, прочие 4xx: повтор не поможет
# Итог события.
GAVE_UP = "gave_up"                     # попытки кончились
SKIPPED = "skipped"                     # слать стало нельзя (режим off, нет токена)

_queue: collections.deque[dict[str, Any]] = collections.deque()
_worker: asyncio.Task | None = None
_log_seen: dict[str, float] = {}        # вид отказа -> когда последний раз писали в лог
_suppressed: dict[str, int] = {}        # вид отказа -> сколько таких проглочено с тех пор


def _now() -> float:
    return time.monotonic()


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def configured() -> bool:
    return bool(cfg.ENABLED and cfg.PANEL_URL and cfg.PANEL_TOKEN)


def may_send() -> bool:
    """Можно ли сейчас слать в панель. Под pytest без подставного транспорта нельзя: настоящей сети в тестах нет."""
    if not configured():
        return False
    if _transport is None and os.getenv("PYTEST_CURRENT_TEST"):
        return False
    return settings.active_mode_id() != "off"


def _log(kind: str, level: int, message: str, *args: Any, exc_info: bool = False) -> None:
    """Запись в лог не чаще раза в `LOG_EVERY_S` на `kind`; проглоченные между записями считаются и выводятся со следующей."""
    now = _now()
    last = _log_seen.get(kind)
    if last is not None and now - last < LOG_EVERY_S:
        _suppressed[kind] = _suppressed.get(kind, 0) + 1
        return
    _log_seen[kind] = now
    skipped = _suppressed.pop(kind, 0)
    if skipped:
        message += f" (ещё {skipped} таких за последние {LOG_EVERY_S // 60} мин не записано)"
    logger.log(level, message, *args, exc_info=exc_info)


def enqueue(event: dict[str, Any], spawn: Callable[[Any], asyncio.Task]) -> bool:
    """Поставить событие в очередь отправки: синхронно, мгновенно, без сети. True: поставлено. Фоновую задачу при необходимости
    запускает `spawn` (у движка это `_spawn`: задача попадает в `_bg`, её отменяет остановка движка). Задача одна и живёт, пока
    в очереди есть события; события уходят по порядку."""
    global _worker
    if not may_send():
        return False
    if len(_queue) >= QUEUE_MAX:
        _log("queue_full", logging.WARNING, "ai_engine_events: очередь событий в панель полна (%s), новое событие отброшено (сделка %s)",
             QUEUE_MAX, event.get("deal_id"))
        return False
    _queue.append(event)
    if _worker is None or _worker.done():
        _worker = spawn(_run())
    return True


async def _run() -> None:
    while _queue:
        event = _queue.popleft()
        try:
            await deliver(event)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - сбой одного события не должен остановить остальные
            _log("error", logging.ERROR, "ai_engine_events: сбой отправки события в панель (сделка %s)", event.get("deal_id"), exc_info=True)


async def deliver(event: dict[str, Any]) -> str:
    """Отправить одно событие с повторами. Возвращает SENT, REFUSED, GAVE_UP или SKIPPED. Пишет в лог отказы (с ограничением частоты)."""
    deal_id = event.get("deal_id")
    for delay in (0,) + RETRY_DELAYS_S:
        if delay:
            await _sleep(delay)
        if not may_send():
            return SKIPPED
        kind, status, detail = await _post(event)
        if kind == SENT:
            return SENT
        if kind == REFUSED:
            _log_refusal(status, detail, deal_id)
            return REFUSED
    _log("unavailable", logging.WARNING,
         "ai_engine_events: панель недоступна, событие отброшено после %s попыток (сделка %s, %s)", len(RETRY_DELAYS_S) + 1, deal_id, detail)
    return GAVE_UP


async def _post(event: dict[str, Any]) -> tuple[str, int, str]:
    url = f"{cfg.PANEL_URL}{PATH}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, transport=_transport) as http:
            resp = await http.post(url, json=event, headers={"X-Ingest-Token": cfg.PANEL_TOKEN})
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - сеть, таймаут, что угодно: попытка не удалась
        return RETRY, 0, f"{type(exc).__name__}: {str(exc)[:120]}"
    code = resp.status_code
    if 200 <= code < 300:
        return SENT, code, ""
    detail = f"HTTP {code}"
    if code in (408, 429) or code >= 500:
        return RETRY, code, detail
    return REFUSED, code, detail


def _log_refusal(status: int, detail: str, deal_id: Any) -> None:
    if status == 404:
        _log("http_404", logging.WARNING,
             "ai_engine_events: панель ответила 404 на %s (ручка ещё не выкачана?): события не доходят, повторов нет (сделка %s)", PATH, deal_id)
    elif status in (401, 403):
        _log("http_auth", logging.WARNING,
             "ai_engine_events: панель ответила %s: токен не принят, событие отброшено без повторов (сделка %s)", status, deal_id)
    else:
        _log(f"http_{status}", logging.WARNING, "ai_engine_events: панель отклонила событие (%s), повторов нет (сделка %s)", detail, deal_id)
