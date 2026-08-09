"""Клиент чтения профилей распределения лидов из team-panel — write-through кэш (09.08.2026).

09.08.2026: хранилище профилей переехало в team-panel (Postgres, app/lead_distribution/
service.py) — team-panel стал единственным владельцем данных редактирования, amo_fix_fields
больше не пишет профили на диск сам (create/update/delete_profile отсюда убраны), только
читает и кэширует. Раньше var/lead_distribution_profiles.json был мастер-файлом — под риском
затирания пустым при деплое, если бы его не защитили gitignore/dockerignore/volume разом.
Теперь это именно КЭШ: каждый успешный опрос team-panel атомарно переписывает файл, а при
недоступности team-panel (сбой сети, деплой team-panel, обрыв) читаем последнюю сохранённую
копию с диска — так лиды продолжают распределяться по последним известным правилам, а не
встают, пока team-panel не оживёт. Файл по-прежнему может свободно затираться редеплоем —
это уже не потеря данных, а просто прогрев кэша заново на первом опросе после старта.

Опрос — тот же приём, что team_panel_client.py (график смен): фоновая задача раз в
LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S, кэш в памяти процесса поверх файла на диске.
Ротация (var/lead_distribution_rotation.json) и счётчики нагрузки
(var/lead_distribution_counters.json) сюда не относятся — это runtime-состояние диспетчера
amo_fix_fields, не правила, они остаются локальными файлами в lead_distribution.py как были.
"""
import asyncio
import json
import logging
import os
import pathlib
from typing import Any

import httpx

from waybill_config import (
    LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

CACHE_PATH = pathlib.Path(os.getenv("LEAD_DISTRIBUTION_PROFILES_PATH", "var/lead_distribution_profiles.json"))

_cache: dict[str, dict[str, Any]] | None = None  # None до первой загрузки (диск или сеть)
_poll_task: asyncio.Task | None = None


def _atomic_write_json(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _load_from_disk() -> dict[str, dict[str, Any]]:
    if not CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.exception("lead_distribution_profiles_client: не удалось прочитать кэш %s", CACHE_PATH)
        return {}
    return raw.get("profiles") or {}


def get_profiles() -> dict[str, dict[str, Any]]:
    """{profile_id: profile_dict}. Пока фоновый опрос ни разу не отработал —
    читаем то, что успело накопиться на диске с прошлого запуска (переживает
    рестарт amo_fix_fields при недоступном на этот момент team-panel)."""
    global _cache
    if _cache is None:
        _cache = _load_from_disk()
    return _cache


def invalidate_cache() -> None:
    """Только для тестов — сбрасывает кэш в памяти, следующий get_profiles()
    перечитает диск."""
    global _cache
    _cache = None


async def fetch_once() -> bool:
    """Один опрос team-panel. True — успех (кэш в памяти и на диске обновлён).
    False — сбой: кэш НЕ трогаем, работаем на последних известных правилах —
    единичный сетевой сбой не должен обнулять действующие профили."""
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return False

    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/lead-distribution/profiles"
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            logger.warning(
                "lead_distribution_profiles_client: HTTP %s на /lead-distribution/profiles", resp.status_code,
            )
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("lead_distribution_profiles_client: сбой запроса профилей")
        return False

    profiles = data.get("profiles") or {}
    global _cache
    _cache = profiles
    _atomic_write_json(CACHE_PATH, {"profiles": profiles})
    return True


async def _poll_loop() -> None:
    while True:
        try:
            ok = await fetch_once()
            if not ok:
                logger.warning("lead_distribution_profiles_client: опрос профилей не удался, работаем на кэше")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("lead_distribution_profiles_client: ошибка цикла опроса")
        await asyncio.sleep(LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.error(
            "lead_distribution_profiles_client: TEAM_PANEL_BASE_URL/TEAM_PANEL_INGEST_TOKEN не заданы — "
            "профили распределения лидов будут читаться только из последнего кэша на диске (%s), "
            "если он есть, без обновлений.", CACHE_PATH,
        )
        return
    _poll_task = asyncio.create_task(_poll_loop())
    logger.info(
        "lead_distribution_profiles_client: опрос профилей каждые %s сек",
        LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S,
    )


async def stop() -> None:
    global _poll_task
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
