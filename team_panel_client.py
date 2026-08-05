"""Клиент team-panel — источник правды графика сотрудников (05.08.2026).

team-panel хранит и синхронизирует график (двусторонне с Google-таблицей),
эта функция здесь только потребитель: батч-запрос «кто сейчас на месте» по
amoCRM user_id, нужен lead_distribution.eligible_pool()/_is_on_shift().

Фоновая задача (тот же приём, что reconcile-циклы office_transfer.py/
lead_distribution.py) раз в TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S тянет батчем
статус по объединению участников+дежурных всех ВКЛЮЧЁННЫХ профилей, кладёт в
кэш в памяти процесса. Кэш протух или team-panel недоступен →
lead_distribution._is_on_shift сам откатывается на прежний плейсхолдер
(LEAD_DISTRIBUTION_DEFAULT_WINDOW) — распределение лидов не должно
останавливаться из-за сбоя ДРУГОГО сервиса.
"""

import asyncio
import logging
import time

import httpx

from waybill_config import (
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
    TEAM_PANEL_SCHEDULE_ENABLED,
    TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S,
)

logger = logging.getLogger("uvicorn")

# Кэш считается свежим не дольше, чем во столько раз интервал опроса — если
# team-panel молчит дольше (сбой/деплой), get_cached() должен вернуть None и
# заставить вызывающего откатиться на плейсхолдер, а не доверять старым данным.
_STALENESS_MULTIPLIER = 2

_cache: dict[int, bool] = {}
_last_fetch_monotonic: float | None = None
_poll_task: asyncio.Task | None = None


def is_cache_fresh() -> bool:
    if _last_fetch_monotonic is None:
        return False
    return (time.monotonic() - _last_fetch_monotonic) < TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S * _STALENESS_MULTIPLIER


def get_cached(user_id: int) -> bool | None:
    """None — нет свежих данных (кэш пуст или протух), вызывающий сам решает
    фолбэк. True/False — известный статус (в пределах TTL, возможно немного
    устаревший — на то и опрос раз в интервал, не на каждый чих)."""
    if not is_cache_fresh():
        return None
    return _cache.get(int(user_id))


async def fetch_once(user_ids: set[int]) -> bool:
    """Один батч-запрос. True — успех (кэш обновлён). False — сбой: кэш
    НЕ трогаем — старые (но ещё не протухшие по TTL) данные лучше, чем
    немедленный откат на плейсхолдер из-за единичного сетевого сбоя."""
    if not user_ids:
        return True
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return False

    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/schedule/on-shift"
    params = {"amo_user_ids": ",".join(str(uid) for uid in sorted(user_ids))}
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            logger.warning("team_panel_client: HTTP %s на /schedule/on-shift", resp.status_code)
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("team_panel_client: сбой запроса /schedule/on-shift")
        return False

    global _cache, _last_fetch_monotonic
    _cache = {int(k): bool(v) for k, v in data.items()}
    _last_fetch_monotonic = time.monotonic()
    return True


def _collect_tracked_user_ids() -> set[int]:
    """Объединение participant_ids + duty_user_id всех ВКЛЮЧЁННЫХ профилей
    lead_distribution. Ленивый импорт — team_panel_client не должен требовать
    lead_distribution на верхнем уровне (симметрично тому, как office_transfer.py
    лениво импортирует metrika_sync/woo_status_sync)."""
    import lead_distribution as ld

    ids: set[int] = set()
    for profile in ld.list_profiles():
        if not profile.enabled:
            continue
        ids.update(profile.participant_ids)
        if profile.duty_user_id is not None:
            ids.add(profile.duty_user_id)
    return ids


async def _poll_loop() -> None:
    while True:
        try:
            ids = _collect_tracked_user_ids()
            ok = await fetch_once(ids)
            if not ok:
                logger.warning("team_panel_client: опрос графика не удался, кэш не обновлён")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("team_panel_client: ошибка цикла опроса")
        await asyncio.sleep(TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if not TEAM_PANEL_SCHEDULE_ENABLED:
        logger.info("team_panel_client: ВЫКЛЮЧЕН (TEAM_PANEL_SCHEDULE_ENABLED)")
        return
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.error(
            "team_panel_client: TEAM_PANEL_SCHEDULE_ENABLED=1, но не заданы "
            "TEAM_PANEL_BASE_URL/TEAM_PANEL_INGEST_TOKEN — опрос не запущен, "
            "_is_on_shift будет работать на плейсхолдере."
        )
        return
    _poll_task = asyncio.create_task(_poll_loop())
    logger.info("team_panel_client: опрос графика каждые %s сек", TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S)


async def stop() -> None:
    global _poll_task
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
