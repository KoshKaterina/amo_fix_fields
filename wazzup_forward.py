"""Пересылка вебхуков Wazzup в панель team.sunscrypt.ru (POST /api/ingest/wazzup).

Зачем: тексты WhatsApp/Telegram — сырьё целостного разбора сделки (го Кати
31.07.2026), а вебхук Wazzup — ЕДИНСТВЕННЫЙ их источник: ретро-выгрузки у Wazzup
нет (messages_dump — только техпартнёрам). Потерянный вебхук = сообщение потеряно
навсегда. Поэтому дисциплина такая:

  вебхук → очередь в памяти → POST в панель (3 попытки с паузами)
                                   └─ не ушло → бэклог var/wazzup_backlog.jsonl
  при старте: бэклог дозаливается первым (панель могла лежать на деплое)

Панель отвечает {"saved": N, ...} и апсертит по messageId — повторная отправка
безопасна, слать «лишний раз» лучше, чем потерять. statuses[] пересылаем тоже:
панель ими обновляет статус доставки.

Вебхук-обработчик это НЕ блокирует: enqueue() кладёт в очередь и сразу возвращает
управление (Wazzup ждёт 200 быстро, иначе ретраит и может отключить подписку).
"""

import asyncio
import json
import logging
import os
import pathlib

import httpx

from waybill_config import TEAM_INGEST_TOKEN, TEAM_INGEST_URL

logger = logging.getLogger("uvicorn")

# Файл-бэклог: переживает рестарт контейнера; чтобы пережил пересборку, каталог
# /app/var примонтирован с хоста (docker-compose.yml: ./amo_fix_fields/var:/app/var).
_BACKLOG_PATH = pathlib.Path(os.getenv("WAZZUP_BACKLOG_PATH", "var/wazzup_backlog.jsonl"))

_RETRY_DELAYS_S = (1, 5, 25)   # паузы между попытками POST
_QUEUE_MAX = 1000              # защита памяти; при переполнении — сразу в бэклог
_HTTP_TIMEOUT_S = 15.0

_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_enabled = False


def is_enabled() -> bool:
    return _enabled


def enqueue(payload: dict) -> None:
    """Кладёт тело вебхука в очередь на пересылку. Не блокирует, не бросает."""
    if not _enabled:
        return
    # Пересылаем только вебхуки с содержимым — тестовые пинги Wazzup панели не нужны.
    if not (isinstance(payload, dict) and (payload.get("messages") or payload.get("statuses"))):
        return
    try:
        _queue.put_nowait(payload)
    except asyncio.QueueFull:
        logger.warning("Wazzup→панель: очередь полна — пишу сразу в бэклог")
        _append_backlog(payload)
    except Exception:
        logger.exception("Wazzup→панель: enqueue не удался")


async def init() -> None:
    global _queue, _worker_task, _enabled
    if not (TEAM_INGEST_URL and TEAM_INGEST_TOKEN):
        logger.warning(
            "Wazzup→панель: ВЫКЛЮЧЕНО — нет TEAM_INGEST_URL/TEAM_INGEST_TOKEN, "
            "тексты сообщений НЕ сохраняются"
        )
        return
    _queue = asyncio.Queue(maxsize=_QUEUE_MAX)
    _enabled = True
    _worker_task = asyncio.create_task(_worker())
    logger.info("Wazzup→панель: включено (%s), бэклог %s", TEAM_INGEST_URL, _BACKLOG_PATH)


async def shutdown() -> None:
    global _worker_task, _enabled
    _enabled = False
    if _worker_task is not None:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None
    # Всё, что не успели отправить, — в бэклог: дозальётся при следующем старте.
    if _queue is not None:
        drained = 0
        while not _queue.empty():
            try:
                _append_backlog(_queue.get_nowait())
                drained += 1
            except Exception:
                break
        if drained:
            logger.info("Wazzup→панель: при остановке в бэклог ушло %s вебхуков", drained)
    logger.info("Wazzup→панель: остановлено")


async def _worker() -> None:
    # Сначала — бэклог с прошлых запусков (панель могла лежать на своём деплое).
    await _flush_backlog()
    while True:
        payload = await _queue.get()
        ok = await _send_with_retries(payload)
        if not ok:
            _append_backlog(payload)


async def _send_with_retries(payload: dict) -> bool:
    for i, delay in enumerate((0,) + _RETRY_DELAYS_S):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_S) as client:
                r = await client.post(
                    TEAM_INGEST_URL,
                    headers={"X-Ingest-Token": TEAM_INGEST_TOKEN},
                    json=payload,
                )
            if r.status_code == 200:
                return True
            # 4xx кроме 429 ретраить бессмысленно (токен/формат) — в бэклог и громко в лог.
            if 400 <= r.status_code < 500 and r.status_code != 429:
                logger.error(
                    "Wazzup→панель: HTTP %s (не ретраю): %s", r.status_code, r.text[:200]
                )
                return False
            logger.warning("Wazzup→панель: HTTP %s, попытка %s", r.status_code, i + 1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Wazzup→панель: попытка %s не удалась: %r", i + 1, e)
    return False


def _append_backlog(payload: dict) -> None:
    try:
        _BACKLOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _BACKLOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Хуже уже не будет: и сеть, и диск отказали — фиксируем в лог хотя бы факт.
        logger.exception("Wazzup→панель: НЕ записал в бэклог — вебхук потерян")


async def _flush_backlog() -> None:
    """Дозаливка бэклога при старте. Упсерты панели идемпотентны, поэтому частичная
    заливка с повтором безопасна: файл переписывается только оставшимися строками."""
    if not _BACKLOG_PATH.exists():
        return
    try:
        lines = _BACKLOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        logger.exception("Wazzup→панель: бэклог не прочитался")
        return
    if not lines:
        _BACKLOG_PATH.unlink(missing_ok=True)
        return
    logger.info("Wazzup→панель: дозаливаю бэклог, строк: %s", len(lines))
    remaining: list[str] = []
    sent = 0
    for idx, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            logger.warning("Wazzup→панель: битая строка бэклога пропущена")
            continue
        if await _send_with_retries(payload):
            sent += 1
        else:
            # Панель недоступна — оставшееся не мучаем, вернём в файл целиком.
            remaining.extend(l for l in lines[idx:] if l.strip())
            break
    try:
        if remaining:
            _BACKLOG_PATH.write_text("\n".join(remaining) + "\n", encoding="utf-8")
        else:
            _BACKLOG_PATH.unlink(missing_ok=True)
    except Exception:
        logger.exception("Wazzup→панель: бэклог не переписался")
    logger.info("Wazzup→панель: бэклог — отправлено %s, осталось %s", sent, len(remaining))
