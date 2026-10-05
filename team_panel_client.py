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
import datetime
import logging
import os
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

# ════════════════ повторы и наблюдаемость (05.10.2026) ════════════════
#
# Форму этого кода задал замер по логам nginx за 14.7 суток — 4671 запрос к
# /schedule/on-shift:
#   • горячий путь (fetch_for_datetime, 404 запроса; каждый сбой = лид уехал
#     не тому менеджеру) — 0 сбоев;
#   • фоновый опрос (4267 запросов) — 34 раза HTTP 403 и 2 раза 499;
#   • все 34 403 — ОДИН инцидент 25.09 с 07:10 до 09:46 UTC: 2.5 часа, каждый
#     опрос подряд. Это доступ/токен, а не сеть;
#   • пропусков каденции опроса (сбоев, не доехавших до nginx) — 0.
#
# Отсюда ровно два вывода, и оба закодированы ниже.
#
# 1. Слепой повтор не нужен, а местами вреден. На 25.09 он сделал бы 102
#    запроса вместо 34 к сервису, который нам и так отказывал, и ничего бы не
#    спас: 4xx повтором не лечится. Повтор по ТАЙМАУТУ на горячем пути вдвое
#    удлиняет ожидание лида ради того же сервиса, который только что молчал
#    10 секунд. Поэтому повторяем ТОЛЬКО быстрые транзиентные сбои — порванное
#    соединение, 5xx, 429: они стоят миллисекунды, и ожидание лида не растёт.
# 2. Главная дыра была не в повторах. Про инцидент 25.09 просто никто не узнал:
#    2.5 часа распределение лидов шло по плейсхолдеру вместо реального графика
#    (кэш протухает после 2 неудачных опросов), а единственным следом были
#    WARNING в docker logs, которые умирают вместе с контейнером. Поэтому —
#    счётчики в /health и алерт в Телеграм раз на инцидент.

# Доп. попыток сверх первой. 0 — полностью прежнее поведение (откат без деплоя).
TEAM_PANEL_RETRIES = int(os.getenv("TEAM_PANEL_RETRIES", "2"))
TEAM_PANEL_RETRY_BACKOFF_S = float(os.getenv("TEAM_PANEL_RETRY_BACKOFF_S", "0.25"))
# Порог алерта совпадает с _STALENESS_MULTIPLIER не случайно: до него
# распределение ещё идёт по реальному графику (кэш жив), после — по
# плейсхолдеру. Алерт должен прозвучать ровно в этот момент.
TEAM_PANEL_FAIL_ALERT_AFTER = int(os.getenv("TEAM_PANEL_FAIL_ALERT_AFTER", str(_STALENESS_MULTIPLIER)))

# Накопительные счётчики за жизнь процесса: /health отдаёт их наружу, пассивный
# сборщик метрик считает по ним дельты. Именно `retry_recovered` — честная мера
# пользы повторов: сколько запросов повтор реально спас. Ноль в нём через неделю
# означает, что повторы тут не нужны, и это будет видно, а не додумано.
_stats: dict[str, int | str | None] = {
    "poll_ok": 0,
    "poll_fail": 0,
    "hot_ok": 0,
    "hot_fail": 0,
    "retry_attempts": 0,
    "retry_recovered": 0,
    "no_retry_permanent": 0,   # 4xx — повтор был бы чистым вредом (25.09: 34 таких)
    "no_retry_timeout": 0,     # таймаут — повтор удвоил бы ожидание лида
    "consecutive_poll_failures": 0,
    "last_status": None,
    "last_error": None,
}

# Алерт — раз на инцидент, не раз на сбой: 25.09 сбоев было 34 подряд.
_alert_active = False


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


def _retryable(exc: Exception | None, status: int | None) -> bool:
    """Стоит ли повторять. Да — только то, что упало БЫСТРО и с шансом, что
    вторая попытка пройдёт. Нет — всё, что либо не лечится повтором (4xx:
    токен, права, путь), либо уже съело весь таймаут (повтор тут просто
    удваивает ожидание). См. разбор замера выше."""
    if status is not None:
        return status == 429 or 500 <= status <= 599
    if isinstance(exc, httpx.TimeoutException):
        return False
    # TimeoutException проверен выше, так что здесь — порванное/неподнявшееся
    # соединение и протокольные сбои: это миллисекунды, а не ожидание.
    return isinstance(exc, httpx.HTTPError)


async def _request(params: dict, *, log_suffix: str = "") -> dict | None:
    """Один логический запрос к /schedule/on-shift с повторами только быстрых
    транзиентных сбоев. None — не получилось (причина уже в логе и в _stats).

    Тексты логов намеренно те же, что до 05.10.2026: по ним уже делались
    замеры, и ломать их шаблоны нельзя. Повторные попытки в лог не пишем —
    объём лога в обычной жизни не меняется; пишем только спасённый повтором
    запрос (INFO) и окончательный сбой (прежние WARNING/ERROR)."""
    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/schedule/on-shift"
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    attempts = max(1, TEAM_PANEL_RETRIES + 1)

    for attempt in range(1, attempts + 1):
        exc: Exception | None = None
        status: int | None = None
        data: dict | None = None
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json() or {}
            else:
                status = resp.status_code
        except asyncio.CancelledError:
            raise
        except Exception as e:  # сеть, разбор json — классифицирует _retryable
            exc = e

        if exc is None and status is None:
            _stats["last_status"] = 200
            _stats["last_error"] = None
            if attempt > 1:
                _stats["retry_recovered"] = int(_stats["retry_recovered"]) + 1
                logger.info(
                    "team_panel_client: запрос /schedule/on-shift%s прошёл с попытки %d — повтор спас",
                    log_suffix, attempt,
                )
            return data

        _stats["last_status"] = status
        _stats["last_error"] = None if exc is None else type(exc).__name__

        if _retryable(exc, status):
            if attempt < attempts:
                _stats["retry_attempts"] = int(_stats["retry_attempts"]) + 1
                await asyncio.sleep(TEAM_PANEL_RETRY_BACKOFF_S * attempt)
                continue
        elif isinstance(exc, httpx.TimeoutException):
            _stats["no_retry_timeout"] = int(_stats["no_retry_timeout"]) + 1
        elif status is not None and 400 <= status < 500:
            _stats["no_retry_permanent"] = int(_stats["no_retry_permanent"]) + 1

        if status is not None:
            logger.warning("team_panel_client: HTTP %s на /schedule/on-shift%s", status, log_suffix)
        else:
            logger.error(
                "team_panel_client: сбой запроса /schedule/on-shift%s", log_suffix, exc_info=exc,
            )
        return None
    return None


async def fetch_once(user_ids: set[int]) -> bool:
    """Один батч-запрос. True — успех (кэш обновлён). False — сбой: кэш
    НЕ трогаем — старые (но ещё не протухшие по TTL) данные лучше, чем
    немедленный откат на плейсхолдер из-за единичного сетевого сбоя."""
    if not user_ids:
        return True
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return False

    params = {"amo_user_ids": ",".join(str(uid) for uid in sorted(user_ids))}
    data = await _request(params)
    if data is None:
        _stats["poll_fail"] = int(_stats["poll_fail"]) + 1
        _stats["consecutive_poll_failures"] = int(_stats["consecutive_poll_failures"]) + 1
        return False

    global _cache, _last_fetch_monotonic
    _cache = {int(k): bool(v) for k, v in data.items()}
    _last_fetch_monotonic = time.monotonic()
    _stats["poll_ok"] = int(_stats["poll_ok"]) + 1
    _stats["consecutive_poll_failures"] = 0
    return True


async def fetch_for_datetime(user_ids: set[int], at: datetime.datetime) -> dict[int, bool]:
    """Прямой (не кэшируемый) запрос «кто на месте» на явно заданный момент -
    для _tomorrow_pool в lead_distribution.py: когда рабочий день профиля
    закончился, нужен статус на ЗАВТРА, а не «сейчас» (который держит get_cached).
    Срабатывает раз в профиль на переходе через конец дня, не на каждый лид -
    отдельный кэш под это не заводим, {} на любой сбой (конфиг/сеть/статус).

    Это ГОРЯЧИЙ путь: решение по лиду ждёт ответа, а пустой результат уводит
    сделку к дежурному вместо того, кто реально на смене. Поэтому повтор здесь
    и ценен — но только быстрый (см. _retryable): затягивать ожидание лида
    повтором таймаута нельзя."""
    if not user_ids:
        return {}
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return {}

    params = {"amo_user_ids": ",".join(str(uid) for uid in sorted(user_ids)), "at": at.isoformat()}
    data = await _request(params, log_suffix=f" (at={at})")
    if data is None:
        _stats["hot_fail"] = int(_stats["hot_fail"]) + 1
        return {}
    _stats["hot_ok"] = int(_stats["hot_ok"]) + 1
    return {int(k): bool(v) for k, v in data.items()}


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


async def _alert(text: str, event: str | None = None, values: dict | None = None) -> None:
    """Технический рапорт в Телеграм (тот же приём, что cdek_status_sync.py и
    lead_distribution.py). `event` — ключ события в каталоге панели: панель
    может выключить его или перенаправить; ключа нет в каталоге — уйдёт прежним
    текстом в технический чат.

    ⚠️ Именно в Телеграм, а НЕ в ленту панели (alerts.panel_notify_bg): сломана
    здесь как раз панель, через неё же сообщение о её недоступности не доедет."""
    try:
        from telegram_bot import send_alert

        import alerts
        body, kw = text, {}
        if event:
            d = alerts.decide(event, legacy_text=text, values=values or {})
            if d is None:
                logger.info("%s: уведомление выключено в панели", event)
                return
            body, kw = d.text, d.send_kwargs()
        await send_alert(body, **kw)
    except Exception:
        logger.exception("team_panel_client alert failed: %s", text)


async def _alert_down() -> None:
    """Алерт РАЗ НА ИНЦИДЕНТ. 25.09.2026 опрос падал 34 раза подряд 2.5 часа —
    34 сообщения никто бы не читал, а важен момент перехода: именно с этого
    опроса кэш считается протухшим и распределение лидов идёт по плейсхолдеру."""
    global _alert_active
    fails = int(_stats["consecutive_poll_failures"])
    if _alert_active or fails < TEAM_PANEL_FAIL_ALERT_AFTER:
        return
    _alert_active = True
    status = _stats["last_status"]
    why = f"HTTP {status}" if status is not None else f"сбой связи ({_stats['last_error']})"
    await _alert(
        f"⚠️ amo_fix_fields: график смен из team-panel не читается {fails} опроса подряд ({why}). "
        f"Кэш протух — распределение лидов идёт по плейсхолдеру (единое окно на всех), "
        f"а не по реальному графику. Проверить: панель жива? TEAM_PANEL_INGEST_TOKEN действителен?",
        "team_panel_schedule_down",
        {"сбоев подряд": fails, "причина": why,
         "подробности": "Распределение лидов работает по плейсхолдеру, а не по графику смен."},
    )


async def _alert_up() -> None:
    """Флаг снимаем ДО отправки: если это сообщение не доставится, следующий
    инцидент всё равно должен прозвучать."""
    global _alert_active
    _alert_active = False
    await _alert(
        "✅ amo_fix_fields: график смен из team-panel снова читается, "
        "распределение лидов вернулось на реальный график.",
        "team_panel_schedule_up",
        {"подробности": "Опрос графика восстановился."},
    )


async def _poll_loop() -> None:
    while True:
        try:
            ids = _collect_tracked_user_ids()
            ok = await fetch_once(ids)
            if not ok:
                logger.warning("team_panel_client: опрос графика не удался, кэш не обновлён")
                await _alert_down()
            elif _alert_active:
                await _alert_up()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("team_panel_client: ошибка цикла опроса")
        await asyncio.sleep(TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S)


def stats() -> dict:
    """Срез для /health и пассивного сборщика метрик. Счётчики накопительные
    за жизнь процесса — сборщик считает дельты между срезами.

    До 05.10.2026 единственным следом сбоя графика были WARNING в docker logs,
    а их пересоздание контейнера стирает (несколько выкаток в день). Из-за этого
    инцидент 25.09 нашёлся только в логах nginx, задним числом и случайно."""
    age = None if _last_fetch_monotonic is None else round(time.monotonic() - _last_fetch_monotonic, 1)
    out: dict = dict(_stats)
    out["enabled"] = bool(TEAM_PANEL_SCHEDULE_ENABLED)
    out["cache_fresh"] = is_cache_fresh()
    out["cache_age_s"] = age
    out["cache_size"] = len(_cache)
    out["alert_active"] = _alert_active
    out["retries"] = TEAM_PANEL_RETRIES
    return out


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
