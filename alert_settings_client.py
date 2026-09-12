"""Клиент настроек уведомлений из team-panel - write-through кэш.

Третий потребитель одного и того же протокола после профилей распределения и авто-режима:
панель владеет настройками, интеграция их только читает. Списан с
`autopilot_settings_client.py` дословно - второй формы у одинаковой задачи быть не должно,
иначе одна из двух однажды разойдётся с другой в мелочи вроде обработки сбоя.

Устройство: фоновая задача раз в `ALERT_SETTINGS_POLL_INTERVAL_S` спрашивает панель,
успешный ответ атомарно переписывает файл-кэш, при недоступности панели читаем последнюю
сохранённую копию с диска.

Документ панели (`GET /api/ingest/alerts/settings`): `events[ключ] = {enabled, channel,
template, recipients_mode, recipients:[{name, handle, amo_user_id}]}` плюс `people` -
сотрудники с телеграмом. Номеров чатов в документе нет и быть не должно: панель отдаёт
КЛЮЧ канала («op_notify»), номер резолвит `alerts.py` из своего конфига.

Сбой опроса НЕ обнуляет настройки. Панель легла или уехала на деплой - уведомления идут по
последним известным настройкам. «Не ответила - значит выключено» выглядит безопасно и этим
опасно: уведомления тихо перестали бы ходить, и никто не узнал бы, пока не потеряли клиента.

Режим работы - `ALERT_SETTINGS_FROM_PANEL` (подробно в `alerts.py`): `off` - панель не
опрашиваем вовсе, всё как до этой правки; `shadow` - опрашиваем и пишем в лог, что панель
велела бы, но шлём как раньше; `on` - настройки панели действуют. Выключено по умолчанию:
свежая выкатка не должна поменять ни одного боевого уведомления сама.
"""
import asyncio
import json
import logging
import os
import pathlib
from typing import Any

import httpx

from waybill_config import (
    ALERT_SETTINGS_FROM_PANEL,
    ALERT_SETTINGS_POLL_INTERVAL_S,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

CACHE_PATH = pathlib.Path(os.getenv("ALERT_SETTINGS_PATH", "var/alert_settings.json"))

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
        logger.exception("alert_settings_client: не удалось прочитать кэш %s", CACHE_PATH)
        return {}


def mode() -> str:
    """off | shadow | on. Значения `0`/`1` тоже понимаются - привычка от других флагов."""
    raw = (ALERT_SETTINGS_FROM_PANEL or "off").strip().lower()
    if raw in ("1", "on", "true", "yes"):
        return "on"
    if raw == "shadow":
        return "shadow"
    return "off"


def get_settings() -> dict[str, Any]:
    """Весь документ: события и люди. Пусто - панель ещё не отвечала и кэша на диске нет."""
    global _cache
    if _cache is None:
        _cache = _load_from_disk()
    return _cache


def set_settings_for_tests(data: dict[str, Any] | None) -> None:
    """Только для тестов: подложить документ или (None) заставить перечитать диск."""
    global _cache
    _cache = data


def get_event(key: str) -> dict[str, Any] | None:
    """Настройка одного события или None - панель про такое не знает (нет в каталоге или
    документ ещё не забирали). None = «работаем как раньше», не «выключено»."""
    events = get_settings().get("events")
    if not isinstance(events, dict):
        return None
    ev = events.get(key)
    return ev if isinstance(ev, dict) else None


def get_people() -> list[dict[str, Any]]:
    people = get_settings().get("people")
    return [p for p in people if isinstance(p, dict)] if isinstance(people, list) else []


def is_fresh(max_age_s: float) -> bool:
    """Отвечала ли панель за последние N секунд - чтобы честно сказать «настройки не
    забирались с 14:32», а не делать вид, что всё в порядке."""
    if _last_ok_at is None:
        return False
    return (asyncio.get_event_loop().time() - _last_ok_at) <= max_age_s


async def fetch_once() -> bool:
    """Один опрос панели. True - успех. False - сбой, кэш НЕ трогаем."""
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return False

    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/alerts/settings"
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            logger.warning("alert_settings_client: HTTP %s на /alerts/settings", resp.status_code)
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("alert_settings_client: сбой запроса настроек")
        return False

    # Пустой ответ - не то же самое, что «ничего не настроено»: панель отдаёт документ с
    # событиями; пришёл не документ, а мусор - кэш не трогаем.
    if not isinstance(data, dict) or not isinstance(data.get("events"), dict):
        logger.warning("alert_settings_client: ответ не похож на настройки, кэш не трогаю")
        return False

    global _cache, _last_ok_at
    changed = data != _cache
    _cache = data
    _last_ok_at = asyncio.get_event_loop().time()
    _atomic_write_json(CACHE_PATH, data)
    if changed:
        logger.info(
            "alert_settings_client: настройки обновились (%s событий, %s людей), режим %s",
            len(data["events"]), len(data.get("people") or []), mode(),
        )
    return True


async def _poll_loop() -> None:
    while True:
        try:
            if not await fetch_once():
                logger.warning("alert_settings_client: опрос не удался, работаем на кэше")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("alert_settings_client: ошибка цикла опроса")
        await asyncio.sleep(ALERT_SETTINGS_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if mode() == "off":
        logger.info(
            "alert_settings_client: ALERT_SETTINGS_FROM_PANEL=off - панель не опрашиваем, "
            "уведомления идут как раньше",
        )
        return
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.error(
            "alert_settings_client: TEAM_PANEL_BASE_URL/TEAM_PANEL_INGEST_TOKEN не заданы - "
            "настройки уведомлений будут читаться только из последнего кэша на диске (%s), "
            "если он есть, без обновлений.", CACHE_PATH,
        )
        return
    _poll_task = asyncio.create_task(_poll_loop())
    logger.info(
        "alert_settings_client: режим %s, опрос настроек каждые %s сек",
        mode(), ALERT_SETTINGS_POLL_INTERVAL_S,
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
