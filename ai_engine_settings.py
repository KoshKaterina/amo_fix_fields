"""Настройки ИИ-менеджера из панели: режим, воронки, списки контактов, таймеры, часы работы.

Устройство то же, что у `autopilot_settings_client`: фоновая задача раз в `AI_SETTINGS_POLL_INTERVAL_S`
спрашивает `GET /api/ingest/ai/settings`, успешный ответ ложится в файл-кэш, при недоступности панели
берётся последняя копия. Разница одна, и она намеренная: у авто-режима «панель молчит» значит «работаем по
последним правилам» без срока, а у ИИ срок есть (`AI_SETTINGS_STALE_S`). ИИ пишет живым людям, и вчерашний
`prod`, которого никто уже не может выключить, не должен жить вечно.

⚠️ Три случая, в которых `effective_mode()` возвращает `off`, хотя в панели написано иное:
  • флаг `AI_ENGINE_ENABLED` на сервере не стоит;
  • настроек нет вовсе (первый запуск без панели) или они старше `AI_SETTINGS_STALE_S`;
  • воронки в документе не совпали с зашитыми в движке для этого режима. Панель и движок держат одни и те
    же числа в двух местах, и расхождение означает опечатку в одном из них: лучше выключиться, чем увести
    ИИ в чужую воронку.

⚠️ Пустой список контактов при `restricted` означает «никого», а не «всех».
"""
import asyncio
import datetime
import logging
import time
from typing import Any

import httpx

import ai_engine_config as cfg
from shared.files import read_json, write_json_atomic
from shared.phones import digits_only, ru_8_to_7
from shared.timez import MSK

logger = logging.getLogger("uvicorn")

DEFAULT_TIMERS = {
    "first_reply_wait_s": 60,
    "debounce_s": 10,
    "debounce_cap_s": 60,
    "deal_resolve_timeout_s": 600,
    "ai_down_release_s": 120,
}

_doc: dict[str, Any] | None = None      # последний принятый документ
_fetched_at: float | None = None        # когда его получили (стенные часы; переживает рестарт через кэш)
_poll_task: asyncio.Task | None = None
_mismatch_logged = ""


def _now() -> float:
    return time.time()


# ---------------------------------------------------------------------------------------------
# документ
# ---------------------------------------------------------------------------------------------

def set_document(doc: dict[str, Any] | None, fetched_at: float | None = None) -> None:
    """Подменить документ. Нужно опросу после успешного ответа и тестам."""
    global _doc, _fetched_at
    _doc = doc
    _fetched_at = fetched_at if fetched_at is not None else (_now() if doc is not None else None)


def _load_cache() -> None:
    global _doc, _fetched_at
    data = read_json(cfg.SETTINGS_CACHE_PATH, None)
    if isinstance(data, dict) and isinstance(data.get("doc"), dict):
        _doc = data["doc"]
        _fetched_at = float(data.get("fetched_at") or 0) or None


def document() -> dict[str, Any] | None:
    return _doc


def age_s() -> float | None:
    return None if _fetched_at is None else max(0.0, _now() - _fetched_at)


def is_fresh() -> bool:
    age = age_s()
    return age is not None and age <= cfg.SETTINGS_STALE_S


# ---------------------------------------------------------------------------------------------
# режим
# ---------------------------------------------------------------------------------------------

def effective_mode() -> str:
    """`off` | `test` | `prod` - то, чему движок верит ПРЯМО СЕЙЧАС."""
    global _mismatch_logged
    if not cfg.ENABLED or _doc is None or not is_fresh():
        return "off"
    mode = str(_doc.get("mode") or "off")
    if mode not in cfg.MODE_PIPELINES:
        return "off"
    try:
        got = {int(p) for p in (_doc.get("pipelines") or [])}
    except (TypeError, ValueError):
        got = set()
    if not got or not got <= cfg.MODE_PIPELINES[mode]:
        sig = f"{mode}:{sorted(got)}"
        if _mismatch_logged != sig:
            _mismatch_logged = sig
            logger.error(
                "ai_engine: воронки панели %s не согласуются с режимом %s (ожидалось не больше %s) - считаю off",
                sorted(got), mode, sorted(cfg.MODE_PIPELINES[mode]),
            )
        return "off"
    return mode


def allowed_pipelines() -> frozenset[int]:
    mode = effective_mode()
    return cfg.MODE_PIPELINES.get(mode, frozenset())


def timers() -> dict[str, int]:
    raw = (_doc or {}).get("timers") or {}
    out = dict(DEFAULT_TIMERS)
    for key in out:
        try:
            value = int(raw.get(key, out[key]))
        except (TypeError, ValueError):
            continue
        if value >= 0:
            out[key] = value
    return out


def post_sale_window_days() -> int:
    try:
        return int((_doc or {}).get("post_sale_window_days", 7))
    except (TypeError, ValueError):
        return 7


# ---------------------------------------------------------------------------------------------
# контакты
# ---------------------------------------------------------------------------------------------

def contacts_restricted() -> bool:
    """Ограничен ли режим списком. Нет документа - ограничен (пустой список = никого)."""
    contacts = (_doc or {}).get("contacts")
    if not isinstance(contacts, dict):
        return True
    return bool(contacts.get("restricted", True))


def contact_items() -> list[dict[str, Any]]:
    contacts = (_doc or {}).get("contacts")
    if not isinstance(contacts, dict):
        return []
    return [i for i in (contacts.get("items") or []) if isinstance(i, dict)]


def phone_key(raw: Any) -> str:
    """Телефон для сравнения: цифры, российская 8 -> 7. Короче десяти цифр - ключа нет."""
    digits = ru_8_to_7(digits_only(str(raw or "")))
    return digits if len(digits) >= 10 else ""


def _same_phone(a: str, b: str) -> bool:
    return bool(a) and bool(b) and a[-10:] == b[-10:]


def phone_may_match(*phones: Any) -> bool:
    """Дешёвая проверка ДО похода в amo: может ли чат принадлежать списку.

    Вебхук Wazzup приходит на каждое сообщение аккаунта, а ходить в amo на каждый чужой чат
    нельзя. Если у всех записей списка есть телефон и ни один не совпал с телефонами чата - чат
    точно не наш. Если у какой-то записи телефона нет, проверить без amo нельзя - пропускаем дальше.
    """
    if not contacts_restricted():
        return True
    items = contact_items()
    keys = [phone_key(p) for p in phones]
    keys = [k for k in keys if k]
    unknown = False
    for item in items:
        pk = phone_key(item.get("phone_norm"))
        if not pk:
            unknown = True
            continue
        if any(_same_phone(pk, k) for k in keys):
            return True
    return unknown


def contact_allowed(contact_ids: list[int], phones: list[Any]) -> bool:
    """Решение по контакту сделки: он в списке (по id в amo или по телефону) либо список не нужен."""
    if not contacts_restricted():
        return True
    ids = {int(c) for c in contact_ids}
    keys = [k for k in (phone_key(p) for p in phones) if k]
    for item in contact_items():
        try:
            if int(item.get("contact_id") or 0) in ids:
                return True
        except (TypeError, ValueError):
            pass
        pk = phone_key(item.get("phone_norm"))
        if pk and any(_same_phone(pk, k) for k in keys):
            return True
    return False


# ---------------------------------------------------------------------------------------------
# рабочие часы
# ---------------------------------------------------------------------------------------------

def work_hours() -> list[dict[str, str]]:
    return [h for h in ((_doc or {}).get("work_hours") or []) if isinstance(h, dict) and h.get("start") and h.get("end")]


def send_window(now: datetime.datetime | None = None) -> dict[str, Any]:
    """Окно отправки на сейчас по часам работы (МСК). Пустые часы = никогда, а не круглосуточно:
    так читает их и авто-режим, пустые часы способ приостановить отправки, не теряя настроек."""
    hours = work_hours()
    moment = (now or datetime.datetime.now(MSK)).astimezone(MSK)
    if not hours:
        return {"open_now": False, "next_open_at": None}
    hhmm = moment.strftime("%H:%M")
    if any(h["start"] <= hhmm < h["end"] for h in hours):
        return {"open_now": True, "next_open_at": None}
    starts = []
    for h in hours:
        hh, mm = (int(x) for x in str(h["start"]).split(":"))
        starts.append(datetime.datetime.combine(moment.date(), datetime.time(hh, mm), tzinfo=MSK))
    upcoming = [t for t in starts if t > moment]
    if upcoming:
        nxt = min(upcoming)
    else:
        hh, mm = (int(x) for x in min(h["start"] for h in hours).split(":"))
        nxt = datetime.datetime.combine(
            moment.date() + datetime.timedelta(days=1), datetime.time(hh, mm), tzinfo=MSK,
        )
    return {"open_now": False, "next_open_at": nxt.isoformat()}


# ---------------------------------------------------------------------------------------------
# опрос панели
# ---------------------------------------------------------------------------------------------

async def fetch_once() -> bool:
    """Один опрос. True - успех. Сбой кэш НЕ трогает (но срок годности продолжает идти)."""
    if not cfg.PANEL_URL or not cfg.PANEL_TOKEN:
        return False
    url = f"{cfg.PANEL_URL}/api/ingest/ai/settings"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"X-Ingest-Token": cfg.PANEL_TOKEN})
        if resp.status_code != 200:
            logger.warning("ai_engine_settings: HTTP %s на /ai/settings", resp.status_code)
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ai_engine_settings: сбой запроса настроек")
        return False
    if not isinstance(data, dict) or "mode" not in data:
        logger.warning("ai_engine_settings: ответ не похож на настройки ИИ, кэш не трогаю")
        return False
    set_document(data)
    try:
        write_json_atomic(cfg.SETTINGS_CACHE_PATH, {"fetched_at": _fetched_at, "doc": data}, ensure_ascii=False)
    except OSError:
        logger.exception("ai_engine_settings: не записался кэш %s", cfg.SETTINGS_CACHE_PATH)
    return True


async def _poll_loop() -> None:
    while True:
        try:
            if not await fetch_once():
                logger.warning("ai_engine_settings: опрос не удался, работаем на кэше (возраст %s с)", age_s())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine_settings: ошибка цикла опроса")
        await asyncio.sleep(cfg.SETTINGS_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if not cfg.ENABLED:
        return
    _load_cache()
    if not cfg.PANEL_URL or not cfg.PANEL_TOKEN:
        logger.error("ai_engine_settings: нет AI_INGEST_TOKEN/адреса панели - режим останется off")
        return
    _poll_task = asyncio.create_task(_poll_loop())


async def stop() -> None:
    global _poll_task
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
