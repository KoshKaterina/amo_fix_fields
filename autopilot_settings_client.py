"""Клиент настроек авто-режима из team-panel - write-through кэш.

Списан с `lead_distribution_profiles_client.py` дословно, и это осознанно: контур тот же -
панель владеет правилами, интеграция их только читает. Второй формы у одинаковой задачи быть
не должно, иначе одна из двух однажды разойдётся с другой в мелочи вроде обработки сбоя.

Устройство: фоновая задача раз в `AUTOPILOT_SETTINGS_POLL_INTERVAL_S` спрашивает панель,
успешный ответ атомарно переписывает файл-кэш, при недоступности панели читаем последнюю
сохранённую копию с диска.

⚠️ **Сбой опроса НЕ обнуляет настройки.** Панель может лечь, может уехать на деплой, может
просто моргнуть сетью - робот при этом продолжает работать по последним известным правилам.
Обратное поведение («не ответила - значит выключено») выглядит безопасным, но им же и опасно:
робот бросил бы сделки на полпути молча, и никто не понял бы почему.

⚠️ **Пустой ответ - не то же самое, что «ничего не настроено».** Панель отдаёт документ с
режимом и маршрутом; если пришёл не документ, а мусор, кэш не трогаем.
"""
import asyncio
import json
import logging
import os
import pathlib
from typing import Any

import httpx

from waybill_config import (
    AUTOPILOT_SETTINGS_POLL_INTERVAL_S,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

CACHE_PATH = pathlib.Path(os.getenv("AUTOPILOT_SETTINGS_PATH", "var/autopilot_settings.json"))

_cache: dict[str, Any] | None = None  # None до первой загрузки (диск или сеть)
_poll_task: asyncio.Task | None = None
_last_ok_at: float | None = None


def _atomic_write_json(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _load_from_disk() -> dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8")) or {}
    except (ValueError, OSError):
        logger.exception("autopilot_settings_client: не удалось прочитать кэш %s", CACHE_PATH)
        return {}


def get_settings() -> dict[str, Any]:
    """Весь документ: режим, воронка режима, маршрут, белый список тест-контактов."""
    global _cache
    if _cache is None:
        _cache = _load_from_disk()
    return _cache


def invalidate_cache() -> None:
    """Только для тестов - следующий `get_settings()` перечитает диск."""
    global _cache
    _cache = None


def get_mode() -> str:
    return str((get_settings().get("settings") or {}).get("mode") or "off")


def get_pipeline_id() -> int | None:
    value = get_settings().get("pipeline_id")
    return int(value) if value else None


def get_route() -> list[dict[str, Any]]:
    """Этапы маршрута ТЕКУЩЕЙ воронки, по порядку. Какая воронка текущая, решила панель."""
    return list(get_settings().get("route") or [])


def get_stage(status_id: int) -> dict[str, Any] | None:
    for stage in get_route():
        if int(stage.get("status_id") or 0) == int(status_id):
            return stage
    return None


def get_test_contact_ids() -> set[int]:
    """Белый список для режима «Тест».

    ⚠️ Пустой список означает «никому», а не «всем». В воронке «Тест» ничто не мешает завести
    сделку с реальным человеком, и слово «тест» без этого списка защищено только дисциплиной.
    """
    return {int(x) for x in (get_settings().get("test_contact_ids") or [])}


def is_fresh(max_age_s: float) -> bool:
    """Отвечала ли панель за последние N секунд. Нужно, чтобы честно сказать на экране
    «робот не забирал настройки с 14:32», а не делать вид, что всё в порядке."""
    if _last_ok_at is None:
        return False
    return (asyncio.get_event_loop().time() - _last_ok_at) <= max_age_s


async def fetch_once() -> bool:
    """Один опрос панели. True - успех. False - сбой, кэш НЕ трогаем."""
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return False

    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/autopilot/settings"
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            logger.warning("autopilot_settings_client: HTTP %s на /autopilot/settings", resp.status_code)
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot_settings_client: сбой запроса настроек")
        return False

    if not isinstance(data, dict) or "settings" not in data:
        logger.warning("autopilot_settings_client: ответ не похож на настройки, кэш не трогаю")
        return False

    global _cache, _last_ok_at
    _cache = data
    _last_ok_at = asyncio.get_event_loop().time()
    _atomic_write_json(CACHE_PATH, data)
    return True


async def _poll_loop() -> None:
    while True:
        try:
            if not await fetch_once():
                logger.warning("autopilot_settings_client: опрос не удался, работаем на кэше")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("autopilot_settings_client: ошибка цикла опроса")
        await asyncio.sleep(AUTOPILOT_SETTINGS_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.error(
            "autopilot_settings_client: TEAM_PANEL_BASE_URL/TEAM_PANEL_INGEST_TOKEN не заданы - "
            "настройки авто-режима будут читаться только из последнего кэша на диске (%s), "
            "если он есть, без обновлений.", CACHE_PATH,
        )
        return
    _poll_task = asyncio.create_task(_poll_loop())
    logger.info(
        "autopilot_settings_client: опрос настроек каждые %s сек",
        AUTOPILOT_SETTINGS_POLL_INTERVAL_S,
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
