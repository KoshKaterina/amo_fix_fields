import asyncio
import contextvars
import logging
import os
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from pprint import pprint
from typing import Any
from urllib.parse import quote

import httpx
from dotenv import load_dotenv

from api_helpers import (
    MAX_CUSTOM_FIELD_VALUE_LEN,
    RETRYABLE_STATUS_CODES,
    compute_retry_delay,
    trim_text,
)

load_dotenv()

logger = logging.getLogger("uvicorn")

integration_id = os.environ.get("INTEGRATION_ID")
secret_key = os.getenv("SECRET_KEY")

current_token = os.getenv("TOKEN")

headers = {"Authorization": f"Bearer {current_token}"}

REQUEST_TIMEOUT_SECONDS = float(os.getenv("AMO_REQUEST_TIMEOUT_SECONDS", "20"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("AMO_CONNECT_TIMEOUT_SECONDS", "30"))
POOL_TIMEOUT_SECONDS = float(os.getenv("AMO_POOL_TIMEOUT_SECONDS", "20"))
MAX_FETCH_RETRIES = int(os.getenv("AMO_FETCH_RETRIES", "4"))
MAX_PATCH_RETRIES = int(os.getenv("AMO_PATCH_RETRIES", "4"))
# Минимальный зазор между ОТПРАВКАМИ запросов к amo — оставлен для совместимости
# и как нижняя страховка; темп теперь держит общий token bucket (см. ниже).
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("AMO_MIN_REQUEST_INTERVAL_SECONDS", "0.15"))

# ---------------------------------------------------------------------------
# ПАРАЛЛЕЛЬНЫЕ ОТПРАВИТЕЛИ под общим token bucket (05.10.2026, решение Тианы).
#
# ⚠️ ЭТО РАЗВОРОТ РЕШЕНИЯ КАТИ ОТ 08.07.2026 («параллельная отправка была
# хуже», `tech-stack-map.md`). Разворачиваем осознанно, потому что цена
# последовательной схемы там же названа прямо: «если один запрос завис (таймаут
# до 20 секунд), вся остальная работа с amoCRM тоже ждёт своей очереди». Один
# подвисший GET держал весь контур — ровно это и лечим.
#
# Почему прошлый раз было хуже: без общего ограничителя N отправителей просто
# превышали темп. А лимитов у amo ДВА, и второй неочевиден (замеры 03–04.08.2026,
# `integrations.md`): 7 rps на интеграцию И **~10 rps на IP-адрес**. Два ключа по
# 6 rps с одной машины дают 429 обоим — то есть разгонять отправку с одного
# сервера можно только до общего потолка, и держать его должен ОДИН счётчик на
# весь процесс. Поэтому здесь не «несколько клиентов», а bucket, который все
# отправители дёргают сообща.
#
# Темп по умолчанию 6 rps, а не 7: в заметке прямо написано «безопасный рабочий
# темп на клиент — 5-6 rps». Запас отдаём добровольно, разница в пропускной
# против 7 мизерная, а 429 от второго сита прилетает html-ом от nginx и бьёт по
# всем категориям сразу.
#
# ОТКАТ В ОДНУ ПЕРЕМЕННУЮ: AMO_API_SENDERS=1 возвращает последовательную схему
# (один отправитель, следующий запрос только после ответа на предыдущий).
# ---------------------------------------------------------------------------
API_SENDERS = max(1, int(os.getenv("AMO_API_SENDERS", "3")))
API_RATE_RPS = float(os.getenv("AMO_API_RATE_RPS", "6"))
# Размер «ведра»: сколько запросов можно выпустить разом после простоя. Держим
# маленьким — всплеск в десяток запросов за секунду упёрся бы в сито по IP.
API_RATE_BURST = float(os.getenv("AMO_API_RATE_BURST", "2"))
# На сколько глохнем ВСЕМИ отправителями, получив 429. Брейкер по категориям
# остаётся, но он про «эту категорию больше не трогаем», а тут нужно сбить темп
# целиком: 429 от сита по IP не относится ни к какой категории.
API_RATE_PENALTY_S = float(os.getenv("AMO_API_RATE_PENALTY_S", "1.0"))

HTTP_TIMEOUT = httpx.Timeout(
    timeout=REQUEST_TIMEOUT_SECONDS,
    connect=CONNECT_TIMEOUT_SECONDS,
    pool=POOL_TIMEOUT_SECONDS,
)

# ---------------------------------------------------------------------------
# Circuit breaker — ПО КАТЕГОРИЯМ (тип задачи). Всплеск 429 от одной интеграции
# (jivo / sync-метрика-woo / …) открывает ТОЛЬКО её брейкер; критичные категории
# (lead / waybill / cdek) продолжают идти. Категорию несёт contextvar, который
# ставит вызывающий (воркер очереди — по kind задачи). Непомеченные пути (старт,
# фоновые опросы) идут в бакет "default". Порог/кулдаун — общие.
# ---------------------------------------------------------------------------
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("AMO_CB_THRESHOLD", "3"))
CIRCUIT_BREAKER_COOLDOWN = float(os.getenv("AMO_CB_COOLDOWN", "60"))

_breaker_category: contextvars.ContextVar[str] = contextvars.ContextVar(
    "amo_breaker_category", default="default"
)
# category -> {"consecutive": int, "open_until": float}
_breakers: dict[str, dict] = {}


def set_breaker_category(category: str) -> None:
    """Пометить текущий async-контекст типом задачи, чтобы 429 и пауза брейкера
    относились только к этой категории (jivo/lead/waybill/cdek/sync)."""
    _breaker_category.set(category or "default")


def _bstate(category: str | None) -> dict:
    cat = category or _breaker_category.get()
    st = _breakers.get(cat)
    if st is None:
        st = {"consecutive": 0, "open_until": 0.0}
        _breakers[cat] = st
    return st


def is_circuit_open(category: str | None = None) -> bool:
    return time.monotonic() < _bstate(category)["open_until"]


def _record_429(category: str | None = None) -> None:
    cat = category or _breaker_category.get()
    # Сбить общий темп ВСЕМ отправителям: 429 от сита по IP (~10 rps на адрес)
    # не относится ни к какой категории, и брейкер его не поймает.
    _apply_rate_penalty()
    st = _bstate(cat)
    st["consecutive"] += 1
    if st["consecutive"] >= CIRCUIT_BREAKER_THRESHOLD:
        st["open_until"] = time.monotonic() + CIRCUIT_BREAKER_COOLDOWN
        logger.warning(
            "Circuit breaker OPEN [%s] after %s consecutive 429s — pausing '%s' for %.0fs",
            cat, st["consecutive"], cat, CIRCUIT_BREAKER_COOLDOWN,
        )


def _record_success(category: str | None = None) -> None:
    _bstate(category)["consecutive"] = 0


# ---------------------------------------------------------------------------
# Sequential API pipeline — строго по одному запросу за раз (ждём ответ amo
# перед следующим); зазор MIN_REQUEST_INTERVAL_SECONDS считается от момента
# ОТПРАВКИ предыдущего запроса, а не от ответа. Раньше пауза 0.17с добавлялась
# ПОСЛЕ ответа — при задержке amo 0.5–1.5с это резало пропускную до ~1 req/s
# из разрешённых 7 (разбор 08.07.2026).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ПРИОРИТЕТЫ API-пайплайна (05.10.2026, разбор затора очереди).
#
# Зачем. Пайплайн был плоской FIFO, и это обнуляло всю приоритизацию уровнем
# выше: queue_manager снимает клиентскую задачу с дорожки первой (у него
# PriorityQueue), но её первый же вызов amo встаёт в хвост за тысячами фоновых
# запросов. 05.10 так и вышло: в пайплайне стояло 4810 запросов, платёжные
# ссылки СБП по сделкам 36565047 и 36572215 ждали больше часа, при том что сами
# задачи были сняты с дорожки вовремя.
#
# Откуда берётся приоритет. НЕ новый механизм: пользуемся тем же contextvar
# категории, что и брейкер (_breaker_category), — его уже ставит воркер дорожки
# по kind задачи, и contextvars наследуются в create_task, то есть фоновые
# обработчики, рождённые из вебхука, несут категорию запроса сами.
# Непомеченные пути («default»: старт, админские ручки панели, сторожа из
# вебхука) идут НОРМАЛЬНЫМ приоритетом — не хуже, чем было до правки.
#
# ⚠️ Чего правка НЕ делает: не меняет баланс «office_transfer против счёта»
# внутри дорожки. Там office_transfer стоит с PRIORITY_NEW по прямому
# требованию Кати, и здесь он тоже отнесён к клиентскому классу. Развязка этих
# двух — отдельное бизнес-решение, пункт 4 плана улучшений.
# ---------------------------------------------------------------------------
# ⚠️ Платёжная ссылка — ВЫШЕ всего остального (решение Тианы 05.10.2026). Клиент
# в этот момент смотрит в экран оплаты: он уже согласился платить и ждёт QR. Всё
# прочее, включая перенос в Офис, может подождать секунды, а он — нет. До этого
# счёт стоял в одном классе с остальным клиентским путём, а в дорожке очереди и
# вовсе НИЖЕ office_transfer (PRIORITY_INVOICE был равен PRIORITY_WAYBILL = 5
# против PRIORITY_NEW = 0) — то есть ровно наоборот.
API_PRIORITY_URGENT = -5      # платёжная ссылка СБП
API_PRIORITY_CLIENT = 0       # человек ждёт прямо сейчас
API_PRIORITY_NORMAL = 5       # всё непомеченное
API_PRIORITY_BACKGROUND = 9   # сверки, аналитика, догоняющие опросы

_PRIORITY_BY_CATEGORY = {
    "invoice": API_PRIORITY_URGENT,            # клиент ждёт ссылку на оплату — вперёд всех
    "lead_distribution": API_PRIORITY_CLIENT,  # лид ждёт живого менеджера
    "jivo": API_PRIORITY_CLIENT,               # чат идёт в реальном времени
    "waybill": API_PRIORITY_CLIENT,            # клиент ждёт отправку
    "lead": API_PRIORITY_CLIENT,               # заполнение полей по вебхуку
    "office_transfer": API_PRIORITY_CLIENT,    # см. оговорку выше
    "cdek": API_PRIORITY_NORMAL,               # движение статусов, операционное
    "sync": API_PRIORITY_BACKGROUND,           # метрика+woo, реальное время не нужно
}

# Сколько запрос любого класса может прождать, прежде чем его пропустят вперёд
# несмотря на приоритет. Без этого непрерывный клиентский поток мог бы держать
# сверки в очереди бесконечно — а сверки у нас страховка, именно они догоняют
# потерянное. 30 с: заметно меньше интервала самих сверок (120-180 с).
API_STARVATION_SECONDS = float(os.getenv("AMO_API_STARVATION_SECONDS", "30"))

# ---------------------------------------------------------------------------
# BACKPRESSURE для фона (05.10.2026).
#
# Приоритеты решают, КОГО отправить следующим, но не решают главного: фоновые
# проходы продолжают СЫПАТЬ в пайплайн, пока он и так забит. 05.10 всплеск
# вебхуков развернулся в ~4800 запросов именно так — обработчики и сверки
# стартовали как asyncio.create_task без всякого «а можно ли сейчас».
#
# Теперь фоновый проход сам спрашивает разрешения: при глубокой очереди он
# ПРОПУСКАЕТ тик и повторит через свой интервал. Все такие проходы идемпотентны
# и периодичны, поэтому пропуск — это отсрочка, а не потеря данных.
#
# Гистерезис обязателен: без него состояние дрожало бы у порога и половина
# проходов отваливалась бы на ровном месте. Входим на DEPTH, выходим на CLEAR.
# ---------------------------------------------------------------------------
BACKPRESSURE_DEPTH = int(os.getenv("AMO_BACKPRESSURE_DEPTH", "200"))
BACKPRESSURE_CLEAR_DEPTH = int(os.getenv("AMO_BACKPRESSURE_CLEAR_DEPTH", "50"))

_congested = False
_backpressure_skips: dict[str, int] = {}


def is_congested() -> bool:
    """Перегружен ли пайплайн прямо сейчас (с гистерезисом).

    ⚠️ Не чистый геттер: здесь же переключается состояние. Зовут это фоновые
    проходы раз в свой интервал, поэтому отдельного тикера машине состояний не
    нужно. Срез для health читает _congested напрямую, не двигая состояние."""
    global _congested
    if BACKPRESSURE_DEPTH <= 0:
        return False
    depth = api_queue_size()
    if _congested:
        if depth <= BACKPRESSURE_CLEAR_DEPTH:
            _congested = False
            logger.info("Backpressure СНЯТ: очередь пайплайна %s (порог снятия %s)",
                        depth, BACKPRESSURE_CLEAR_DEPTH)
    elif depth >= BACKPRESSURE_DEPTH:
        _congested = True
        logger.warning(
            "Backpressure ВКЛЮЧЁН: очередь пайплайна %s (порог %s) — фоновые проходы "
            "пропускаем, пока не разгрузится до %s",
            depth, BACKPRESSURE_DEPTH, BACKPRESSURE_CLEAR_DEPTH,
        )
    return _congested


def skip_if_congested(name: str) -> bool:
    """`True` — фоновому проходу «%name%» сейчас ходить не надо.

    Ставится В НАЧАЛЕ итерации фонового цикла, сразу после sleep:

        while True:
            await asyncio.sleep(INTERVAL)
            if api.skip_if_congested("office_transfer reconcile"):
                continue
            ...

    Клиентский путь этим не пользуется никогда: его задача — наоборот,
    пролезть вперёд."""
    if not is_congested():
        return False
    _backpressure_skips[name] = _backpressure_skips.get(name, 0) + 1
    logger.info(
        "Backpressure: проход «%s» пропущен (очередь %s), повторим на следующем тике",
        name, api_queue_size(),
    )
    return True

_api_priority: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "amo_api_priority", default=None
)

_CLASS_NAMES = {
    API_PRIORITY_URGENT: "urgent",
    API_PRIORITY_CLIENT: "client",
    API_PRIORITY_NORMAL: "normal",
    API_PRIORITY_BACKGROUND: "background",
}


def set_api_priority(priority: int | None) -> None:
    """Явно задать приоритет для текущего async-контекста (и всех задач,
    созданных из него). Нужен там, где категория брейкера ничего не говорит о
    срочности: фоновые циклы сверок помечают себя BACKGROUND сами."""
    _api_priority.set(priority)


def current_api_priority() -> int:
    explicit = _api_priority.get()
    if explicit is not None:
        return explicit
    return _PRIORITY_BY_CATEGORY.get(_breaker_category.get(), API_PRIORITY_NORMAL)


@contextmanager
def api_priority(priority: int):
    """Поднять (или опустить) приоритет на время блока и вернуть как было.

    Нужно внутри фоновых циклов: сам обход идёт фоном, но если он нашёл работу,
    которой ждёт живой человек (сверка счетов нашла сделку без платёжной
    ссылки), то саму работу делаем клиентским приоритетом."""
    token = _api_priority.set(priority)
    try:
        yield
    finally:
        _api_priority.reset(token)


@dataclass
class ApiRequest:
    method: str
    url: str
    req_headers: dict
    json_body: dict | None
    future: asyncio.Future
    priority: int = API_PRIORITY_NORMAL
    enqueued_at: float = 0.0


# Дорожка на класс приоритета. Намеренно deque, а не PriorityQueue: потребитель
# ОДИН (воркер), а заглядывать в голову дорожки нужно — без этого не сделать
# защиту от голодания. Будильник — Event, producers и consumer в одном лупе,
# поэтому гонок на append/popleft нет.
_lanes: dict[int, Any] = {}
_wakeup: asyncio.Event | None = None
_api_worker_tasks: list = []
_last_sent_at: float = 0.0
_served: dict[int, int] = {}
_promoted_by_starvation = 0

# ── общий token bucket на все отправители ──────────────────────────────────
_tokens: float = 0.0
_bucket_refilled_at: float = 0.0
_rate_lock: asyncio.Lock | None = None
_rate_penalty_until: float = 0.0
_in_flight: int = 0
_max_in_flight: int = 0
_rate_waits: int = 0
_penalty_hits: int = 0


def _refill_locked(now: float) -> None:
    """Долить ведро по прошедшему времени. Зовётся под _rate_lock."""
    global _tokens, _bucket_refilled_at
    elapsed = now - _bucket_refilled_at
    if elapsed > 0:
        _tokens = min(API_RATE_BURST, _tokens + elapsed * API_RATE_RPS)
        _bucket_refilled_at = now


async def _acquire_slot() -> None:
    """Дождаться права отправить ОДИН запрос.

    Держит общий темп для всех отправителей и уважает штраф после 429.
    Сон делаем ВНЕ замка, иначе один ждущий заблокировал бы остальных.
    """
    global _tokens, _rate_waits
    if _rate_lock is None:          # пайплайн не поднят — не ограничиваем
        return
    while True:
        async with _rate_lock:
            now = time.monotonic()
            if now < _rate_penalty_until:
                sleep_for = _rate_penalty_until - now
            else:
                _refill_locked(now)
                if _tokens >= 1.0:
                    _tokens -= 1.0
                    return
                sleep_for = (1.0 - _tokens) / API_RATE_RPS
        _rate_waits += 1
        await asyncio.sleep(max(sleep_for, 0.005))


def _apply_rate_penalty() -> None:
    """Сбить темп ВСЕМ отправителям после 429.

    Брейкер по категориям остаётся как был: он решает «эту категорию больше не
    трогаем». А 429 от сита по IP ни к какой категории не относится, и на него
    правильная реакция — короткая пауза всего пайплайна."""
    global _rate_penalty_until, _penalty_hits
    if API_RATE_PENALTY_S <= 0:
        return
    until = time.monotonic() + API_RATE_PENALTY_S
    if until > _rate_penalty_until:
        _rate_penalty_until = until
        _penalty_hits += 1
        logger.warning(
            "429 от amo — глушим ВСЕ отправители на %.1fs (общий темп %.1f rps, отправителей %s)",
            API_RATE_PENALTY_S, API_RATE_RPS, API_SENDERS,
        )


def api_queue_size() -> int:
    """Суммарный размер внутренней очереди API-пайплайна — для наблюдаемости.

    Это ВТОРАЯ очередь сервиса (первая — task-очереди в queue_manager): сюда
    сваливаются запросы и от воркеров дорожек, и от фоновых задач (unmiss/
    urgency/showroom/сверки). До 08.07.2026 её размер не логировался нигде —
    затор здесь был невидим."""
    return sum(len(d) for d in _lanes.values())


def api_queue_stats() -> dict:
    """Срез пайплайна по классам приоритета: глубина и сколько ждёт голова.

    Закрывает слепую зону разбора 05.10.2026: по одной суммарной глубине не
    видно, стоит ли клиентский путь или это фон копится."""
    now = time.monotonic()
    depth, oldest = {}, {}
    for prio, lane in _lanes.items():
        key = _CLASS_NAMES.get(prio, str(prio))
        depth[key] = len(lane)
        oldest[key] = round(now - lane[0].enqueued_at, 1) if lane else 0.0
    return {
        "api_queue": sum(depth.values()),
        "api_depth": depth,
        "api_oldest_wait_s": oldest,
        "api_served": {_CLASS_NAMES.get(p, str(p)): n for p, n in _served.items()},
        "api_promoted_by_starvation": _promoted_by_starvation,
        # Отправители и темп: видно, реально ли параллелится и упираемся ли в
        # ограничитель. max_in_flight > 1 — параллельная отправка работает.
        "senders": {
            "count": API_SENDERS,
            "rate_rps": API_RATE_RPS,
            "burst": API_RATE_BURST,
            "in_flight": _in_flight,
            "max_in_flight": _max_in_flight,
            "rate_waits": _rate_waits,
            "penalty_hits": _penalty_hits,
            "penalty_active": time.monotonic() < _rate_penalty_until,
        },
        # Срез читает состояние, НЕ двигая машину гистерезиса — иначе опрос
        # health сам влиял бы на то, пропускать ли фоновые проходы.
        "backpressure": {
            "congested": _congested,
            "depth_on": BACKPRESSURE_DEPTH,
            "depth_off": BACKPRESSURE_CLEAR_DEPTH,
            "skipped_passes": dict(_backpressure_skips),
        },
    }


def _take_next() -> ApiRequest | None:
    """Кого отправляем следующим. Сперва проверяем, не переждал ли кто порог
    (иначе клиентский поток заморозил бы сверки), и только потом — приоритет."""
    global _promoted_by_starvation
    now = time.monotonic()
    starved = [p for p, lane in _lanes.items()
               if lane and now - lane[0].enqueued_at >= API_STARVATION_SECONDS]
    if starved:
        worst = max(starved, key=lambda p: now - _lanes[p][0].enqueued_at)
        # поднимаем только если впереди него реально кто-то есть
        if any(_lanes[p] for p in _lanes if p < worst):
            _promoted_by_starvation += 1
        _served[worst] = _served.get(worst, 0) + 1
        return _lanes[worst].popleft()
    for prio in sorted(_lanes):
        if _lanes[prio]:
            _served[prio] = _served.get(prio, 0) + 1
            return _lanes[prio].popleft()
    return None


def init_api_pipeline() -> None:
    global _lanes, _wakeup, _api_worker_tasks, _served, _promoted_by_starvation
    global _congested, _backpressure_skips
    global _tokens, _bucket_refilled_at, _rate_lock, _rate_penalty_until
    global _in_flight, _max_in_flight, _rate_waits, _penalty_hits
    _lanes = {API_PRIORITY_URGENT: deque(), API_PRIORITY_CLIENT: deque(),
              API_PRIORITY_NORMAL: deque(), API_PRIORITY_BACKGROUND: deque()}
    _served = {}
    _promoted_by_starvation = 0
    _congested = False
    _backpressure_skips = {}
    _wakeup = asyncio.Event()
    # Ведро стартует полным: после простоя первые запросы уходят без паузы.
    _rate_lock = asyncio.Lock()
    _tokens = API_RATE_BURST
    _bucket_refilled_at = time.monotonic()
    _rate_penalty_until = 0.0
    _in_flight = 0
    _max_in_flight = 0
    _rate_waits = 0
    _penalty_hits = 0
    _api_worker_tasks = [
        asyncio.create_task(_api_worker(i)) for i in range(API_SENDERS)
    ]
    logger.info(
        "API pipeline started: отправителей %s, общий темп %.1f rps (burst %.0f), "
        "штраф на 429 %.1fs, приоритеты: счёт/клиент/норма/фон, порог голодания %.0fs, "
        "backpressure %s→%s%s",
        API_SENDERS, API_RATE_RPS, API_RATE_BURST, API_RATE_PENALTY_S,
        API_STARVATION_SECONDS, BACKPRESSURE_DEPTH, BACKPRESSURE_CLEAR_DEPTH,
        " [ПОСЛЕДОВАТЕЛЬНЫЙ РЕЖИМ]" if API_SENDERS == 1 else "",
    )


async def shutdown_api_pipeline() -> None:
    global _api_worker_tasks
    for task in _api_worker_tasks:
        task.cancel()
    for task in _api_worker_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    _api_worker_tasks = []

    for lane in _lanes.values():
        while lane:
            req = lane.popleft()
            if not req.future.done():
                req.future.set_exception(asyncio.CancelledError())

    logger.info("API sequential pipeline stopped")


async def _next_request() -> ApiRequest:
    while True:
        req = _take_next()
        if req is not None:
            return req
        _wakeup.clear()
        await _wakeup.wait()


async def _api_worker(worker_no: int = 0) -> None:
    global _last_sent_at, _in_flight, _max_in_flight
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        while True:
            req = await _next_request()
            try:
                # Право на отправку выдаёт общий token bucket — он один на все
                # отправители и держит темп ниже обоих лимитов amo (7 rps на
                # интеграцию и ~10 rps на IP).
                await _acquire_slot()
                # Нижняя страховка от аномально быстрых ответов осталась: при
                # одном отправителе поведение ровно как до правки.
                now = time.monotonic()
                wait_for = (_last_sent_at + MIN_REQUEST_INTERVAL_SECONDS) - now
                if API_SENDERS == 1 and wait_for > 0:
                    await asyncio.sleep(wait_for)
                _last_sent_at = time.monotonic()
                _in_flight += 1
                _max_in_flight = max(_max_in_flight, _in_flight)

                if req.method == "GET":
                    response = await client.get(req.url, headers=req.req_headers)
                elif req.method == "PATCH":
                    response = await client.patch(req.url, headers=req.req_headers, json=req.json_body)
                else:
                    response = await client.request(req.method, req.url, headers=req.req_headers, json=req.json_body)

                if not req.future.done():
                    req.future.set_result(response)
            except asyncio.CancelledError:
                if not req.future.done():
                    req.future.set_exception(asyncio.CancelledError())
                raise
            except Exception as exc:
                if not req.future.done():
                    req.future.set_exception(exc)
            finally:
                _in_flight = max(0, _in_flight - 1)


async def submit_request(
    method: str,
    url: str,
    req_headers: dict,
    json_body: dict | None = None,
    priority: int | None = None,
) -> httpx.Response:
    """Поставить запрос к amo в пайплайн и дождаться ответа.

    `priority` обычно не передают: он выводится из категории текущего контекста
    (см. current_api_priority). Явный аргумент — для редких случаев, когда
    вызывающий знает лучше.
    """
    if not _lanes or _wakeup is None:
        raise RuntimeError("API pipeline not initialized — call init_api_pipeline() first")
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    prio = current_api_priority() if priority is None else priority
    if prio not in _lanes:
        prio = API_PRIORITY_NORMAL
    _lanes[prio].append(ApiRequest(
        method=method,
        url=url,
        req_headers=req_headers,
        json_body=json_body,
        future=future,
        priority=prio,
        enqueued_at=time.monotonic(),
    ))
    _wakeup.set()
    return await future


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_custom_field_value(value: Any, field_id: int, lead_id: Any) -> str:
    value_str = str(value)
    if len(value_str) <= MAX_CUSTOM_FIELD_VALUE_LEN:
        return value_str

    logger.warning(
        "Truncating field %s for lead %s from %s to %s chars",
        field_id,
        lead_id,
        len(value_str),
        MAX_CUSTOM_FIELD_VALUE_LEN,
    )
    return value_str[:MAX_CUSTOM_FIELD_VALUE_LEN]


def _update_result(ok: bool, status_code: int | None = None, retryable: bool = False) -> dict[str, Any]:
    return {"ok": ok, "status_code": status_code, "retryable": retryable}


# ---------------------------------------------------------------------------
# AmoCRM API functions
# ---------------------------------------------------------------------------

BASE_URL = "https://new5a2e8ea7b16b4.amocrm.ru"


async def auth():
    body = {
        "client_id": integration_id,
        "client_secret": secret_key,
        "grant_type": "authorization_code",
        "code": "def5020093463e984c956d5b3258cfad73c1387473a85cd733b384576db0555198b3f674efacec89407f4a055d619eee71b693c80a3ae045e05418a0ef2a098ebbf43f9f0405c56ac3c419bd9e3479d0f6fca16146fdf7b0ca844a3563bed928d79dfcfb2e0445314bea6d470b5c36aaeb146bb58647078e7829cb190ef600f1072dd36ecd7230cd7e6ae4830bf0e251d5321f7f5d564d77f2cd597e2508423fb391f05760d10f4c88d1d4ba783c62852b489510dba58e0e2540ba54e93afcafda77a7b0a29d1b35c20d1c6da55fcb4733224d1b0e66e2f2caea774071d6efd717403e17906a0e48af31ca1e5e3a50246a64070cdea3b48417b719a060b8cc4a44cd6736cf82d207c4c1288c3d3ecf20b93e413fa138d243b542c6db85e154aa606a0a3066b675e6e0d882832e7ccbfbceea0e6d417438f08bfbdd79b198f144c59127b62164395a1bf152ed19415a6a3cb7bf0a354e7e84e16bbe7549cf0bbf3815403bcbb7ee56ac16f1efff5318ae529758ca8c15b91ccdef1f636f46f532aff17bc2573005a4a7997846b36ffb6988badaefa6b5c46606d6efc80d2ce796a5f6ade82be70998ecc9c082cd5af915cffaefa422db2ba94edda6cc5d98f6066a2dd446980c9449fc5c75299a60a889737aa15f7924fffd2fc1fe3846bdae55ceede77e0b8fdba81f5fa3ae6c9031d76a26ac",
        "redirect_uri": f"{BASE_URL}",
    }
    async with httpx.AsyncClient() as client:
        response = (
            await client.post(f"{BASE_URL}/oauth2/access_token", data=body)
        ).json()
    return response


async def get_lead_by_id(lead_id):
    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            response = await submit_request("GET", url, headers)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            if attempt < MAX_FETCH_RETRIES:
                delay = compute_retry_delay(attempt)
                logger.warning(
                    "Request error fetching lead %s on attempt %s/%s (%s). Retrying in %.1fs",
                    lead_id,
                    attempt,
                    MAX_FETCH_RETRIES,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.exception(
                "Request error fetching lead %s after %s attempts",
                lead_id,
                MAX_FETCH_RETRIES,
            )
            return None
        except Exception as exc:
            logger.exception("Unexpected error fetching lead %s on attempt %s", lead_id, attempt)
            if attempt < MAX_FETCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return None

        if response.status_code == 200:
            _record_success()
            try:
                return response.json()
            except ValueError:
                logger.exception("Invalid JSON payload while fetching lead %s", lead_id)
                return None

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("Circuit breaker open — aborting fetch for lead %s", lead_id)
                return None

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_FETCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "AmoCRM status %s for lead %s on fetch attempt %s/%s. Retrying in %.1fs",
                response.status_code,
                lead_id,
                attempt,
                MAX_FETCH_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error(
            "AmoCRM error %s for lead %s: %s",
            response.status_code,
            lead_id,
            trim_text(response.text),
        )
        return None

    return None


async def add_info_from_ms(goods, delivery_type, delivery_address, comment, promo_type, lead_id, name):
    custom_fields = []
    if goods:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(goods, 577313, lead_id), 577313))
    if delivery_type:
        custom_fields.append(
            create_custom_field(_sanitize_custom_field_value(delivery_type, 577315, lead_id), 577315)
        )
    if delivery_address:
        custom_fields.append(
            create_custom_field(_sanitize_custom_field_value(delivery_address, 576719, lead_id), 576719)
        )
    if comment:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(comment, 577753, lead_id), 577753))
    if promo_type:
        custom_fields.append(create_custom_field(_sanitize_custom_field_value(promo_type, 570661, lead_id), 570661))

    body = {
        "id": lead_id,
        "custom_fields_values": custom_fields,
    }
    if name:
        body["name"] = str(name)

    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    for attempt in range(1, MAX_PATCH_RETRIES + 1):
        try:
            response = await submit_request("PATCH", url, headers, json_body=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            if attempt < MAX_PATCH_RETRIES:
                delay = compute_retry_delay(attempt)
                logger.warning(
                    "Request error patching lead %s on attempt %s/%s (%s). Retrying in %.1fs",
                    lead_id,
                    attempt,
                    MAX_PATCH_RETRIES,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.exception(
                "Request error patching lead %s after %s attempts",
                lead_id,
                MAX_PATCH_RETRIES,
            )
            return _update_result(ok=False, status_code=None, retryable=True)
        except Exception as exc:
            logger.exception("Unexpected error patching lead %s on attempt %s", lead_id, attempt)
            if attempt < MAX_PATCH_RETRIES:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return _update_result(ok=False, status_code=None, retryable=True)

        if response.status_code in [200, 204]:
            _record_success()
            logger.info("Successfully updated lead %s", lead_id)
            return _update_result(ok=True, status_code=response.status_code, retryable=False)

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("Circuit breaker open — aborting patch for lead %s", lead_id)
                return _update_result(ok=False, status_code=429, retryable=False)

        if response.status_code == 400:
            logger.error(
                "Failed to update lead %s: %s %s",
                lead_id,
                response.status_code,
                trim_text(response.text),
            )
            return _update_result(ok=False, status_code=response.status_code, retryable=False)

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_PATCH_RETRIES:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "AmoCRM status %s for lead %s on patch attempt %s/%s. Retrying in %.1fs",
                response.status_code,
                lead_id,
                attempt,
                MAX_PATCH_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error(
            "Failed to update lead %s: %s %s",
            lead_id,
            response.status_code,
            trim_text(response.text),
        )
        return _update_result(
            ok=False,
            status_code=response.status_code,
            retryable=response.status_code in RETRYABLE_STATUS_CODES,
        )

    return _update_result(ok=False, status_code=None, retryable=True)


def create_custom_field(value, id):
    new_field = {
        "field_id": id,
        "values": [
            {
                "value": value,
            }
        ],
    }
    return new_field


# ---------------------------------------------------------------------------
# Generic create/find helpers (Jivo bridge: контакт + сделка + примечание)
# Идут через тот же последовательный pipeline (submit_request), что и остальные
# вызовы amo, с теми же ретраями и общим circuit breaker по 429.
# ---------------------------------------------------------------------------

async def _request_json(
    method: str, url: str, body: Any = None, what: str = "", *, max_attempts: int | None = None,
) -> Any:
    """Выполняет GET/POST с ретраями. Возвращает распарсенный JSON (dict/list),
    {} для пустого 204, либо None при неустранимой ошибке."""
    attempts_limit = MAX_PATCH_RETRIES if max_attempts is None else max_attempts
    if attempts_limit < 1:
        raise ValueError("max_attempts must be positive")
    for attempt in range(1, attempts_limit + 1):
        try:
            response = await submit_request(method, url, headers, json_body=body)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError:
            if attempt < attempts_limit:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            logger.exception("%s: request error after %s attempts", what, attempts_limit)
            return None
        except Exception:
            logger.exception("%s: unexpected error on attempt %s", what, attempt)
            if attempt < attempts_limit:
                await asyncio.sleep(compute_retry_delay(attempt))
                continue
            return None

        if response.status_code in (200, 201, 204):
            _record_success()
            if response.status_code == 204 or not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                logger.exception("%s: invalid JSON in response", what)
                return None

        if response.status_code == 429:
            _record_429()
            if is_circuit_open():
                logger.warning("%s: circuit breaker open — aborting", what)
                return None

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts_limit:
            delay = compute_retry_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "%s: amo status %s, retry %s/%s in %.1fs",
                what, response.status_code, attempt, attempts_limit, delay,
            )
            await asyncio.sleep(delay)
            continue

        logger.error("%s: failed %s %s", what, response.status_code, trim_text(response.text))
        return None

    return None


async def find_contact_id(query: str) -> int | None:
    """Ищет контакт по строке (телефон или email). Возвращает id первого
    совпадения или None. Используется для дедупликации перед созданием."""
    query = (query or "").strip()
    if not query:
        return None
    url = f"{BASE_URL}/api/v4/contacts?query={quote(query)}&limit=1"
    data = await _request_json("GET", url, what=f"find_contact[{query}]")
    if not isinstance(data, dict):
        return None
    contacts = (data.get("_embedded") or {}).get("contacts") or []
    return contacts[0].get("id") if contacts else None


async def add_note_to_lead(lead_id: Any, text: str, *, max_attempts: int | None = None) -> bool:
    """Добавляет обычное примечание (common) к сделке. True при успехе."""
    url = f"{BASE_URL}/api/v4/leads/{lead_id}/notes"
    body = [{"note_type": "common", "params": {"text": str(text)}}]
    request_options = {"max_attempts": max_attempts} if max_attempts is not None else {}
    data = await _request_json("POST", url, body=body, what=f"add_note[{lead_id}]", **request_options)
    return data is not None


async def set_lead_tags(lead_id: Any, tags: list) -> bool:
    """Ставит сделке теги по именам (ЗАМЕНЯЕТ весь набор). Нужен потому, что
    unsorted/forms существующий тег по имени не линкует — создаёт и цепляет
    только новые (боем 13.09.2026: «Тест» не лёг, новый тег формы лёг)."""
    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    body = {"_embedded": {"tags": [{"name": str(t)} for t in tags if str(t).strip()]}}
    data = await _request_json("PATCH", url, body=body, what=f"set_lead_tags[{lead_id}]")
    return data is not None


async def set_lead_utm(lead_id: Any, utm: dict) -> bool:
    """Пишет UTM-метки в стандартные поля отслеживания сделки (utm_source → field_code
    UTM_SOURCE и т.д.). Пустые значения пропускаются. False - amo не приняла: формы сайта
    держат те же метки в примечании, так что это не повод терять сделку."""
    fields = [
        {"field_code": str(key).upper(), "values": [{"value": str(value)}]}
        for key, value in (utm or {}).items()
        if str(value or "").strip()
    ]
    if not fields:
        return True
    url = f"{BASE_URL}/api/v4/leads/{lead_id}"
    data = await _request_json("PATCH", url, body={"custom_fields_values": fields}, what=f"set_lead_utm[{lead_id}]")
    return data is not None


async def create_contact(name: Any, phone: Any, email: Any) -> int | None:
    """Создаёт контакт с телефоном/email. Возвращает id или None."""
    custom_fields = []
    if phone:
        custom_fields.append({"field_code": "PHONE", "values": [{"value": str(phone), "enum_code": "WORK"}]})
    if email:
        custom_fields.append({"field_code": "EMAIL", "values": [{"value": str(email), "enum_code": "WORK"}]})
    contact: dict[str, Any] = {"name": str(name) if name else (str(phone or email) or "Клиент Jivo")}
    if custom_fields:
        contact["custom_fields_values"] = custom_fields
    url = f"{BASE_URL}/api/v4/contacts"
    data = await _request_json("POST", url, body=[contact], what="create_contact")
    if not isinstance(data, dict):
        return None
    items = (data.get("_embedded") or {}).get("contacts") or []
    return items[0].get("id") if items else None


async def get_contact(contact_id: Any, with_leads: bool = False) -> dict | None:
    """Возвращает контакт по id (с custom_fields_values: PHONE/EMAIL и т.п.),
    либо None. with_leads=True добавляет _embedded.leads[] (id связанных сделок) —
    для проверки, есть ли у клиента уже открытая сделка."""
    url = f"{BASE_URL}/api/v4/contacts/{contact_id}"
    if with_leads:
        url += "?with=leads"
    data = await _request_json("GET", url, what=f"get_contact[{contact_id}]")
    return data if isinstance(data, dict) else None


async def get_leads_by_ids(lead_ids) -> list:
    """Возвращает сделки по списку id (одним запросом, filter[id][]). Нужны
    status_id/updated_at, чтобы выбрать открытую и самую свежую по работе."""
    ids = []
    for i in lead_ids or []:
        try:
            ids.append(int(i))
        except (TypeError, ValueError):
            continue
    if not ids:
        return []
    params = "&".join(f"filter[id][]={i}" for i in ids)
    url = f"{BASE_URL}/api/v4/leads?{params}&limit=250"
    data = await _request_json("GET", url, what="get_leads_by_ids")
    if not isinstance(data, dict):
        return []
    return (data.get("_embedded") or {}).get("leads") or []


async def update_contact(
    contact_id: Any,
    name: Any = None,
    custom_fields_values: list | None = None,
) -> bool:
    """PATCH контакта: меняет только переданные поля (имя и/или custom fields).
    Значения multitext (PHONE/EMAIL) заменяются ЦЕЛИКОМ — вызывающий обязан
    передать полный список values (старые + новые). True при успехе."""
    body: dict[str, Any] = {}
    if name:
        body["name"] = str(name)
    if custom_fields_values:
        body["custom_fields_values"] = custom_fields_values
    if not body:
        return False
    url = f"{BASE_URL}/api/v4/contacts/{contact_id}"
    data = await _request_json("PATCH", url, body=body, what=f"update_contact[{contact_id}]")
    return data is not None


async def create_lead_direct(
    name: Any,
    pipeline_id: int,
    status_id: int,
    responsible_user_id: int | None = None,
    custom_fields_values: list | None = None,
    contact_id: int | None = None,
    tags: list | None = None,
) -> int | None:
    """Создаёт сделку напрямую в обычном (type=0) статусе воронки — в обход
    «Неразобранного» и его автораспределения. Для триажа Jivo: закрыть в 143
    или сразу назначить ответственного. tags — метки (напр. для исключения из
    распределения). Возвращает id сделки или None."""
    lead: dict[str, Any] = {
        "name": str(name),
        "pipeline_id": int(pipeline_id),
        "status_id": int(status_id),
    }
    if responsible_user_id:
        lead["responsible_user_id"] = int(responsible_user_id)
    if custom_fields_values:
        lead["custom_fields_values"] = custom_fields_values
    embedded: dict[str, Any] = {}
    if contact_id:
        embedded["contacts"] = [{"id": int(contact_id)}]
    if tags:
        embedded["tags"] = [{"name": str(t)} for t in tags if str(t).strip()]
    if embedded:
        lead["_embedded"] = embedded
    url = f"{BASE_URL}/api/v4/leads"
    data = await _request_json("POST", url, body=[lead], what="create_lead_direct")
    if not isinstance(data, dict):
        return None
    items = (data.get("_embedded") or {}).get("leads") or []
    return items[0].get("id") if items else None


async def create_task(
    entity_id: int,
    text: str,
    responsible_user_id: int | None,
    complete_till: int,
    task_type_id: int | None = None,
    entity_type: str = "leads",
) -> bool:
    """Создаёт задачу на сделке (entity_type=leads). complete_till — unix-срок."""
    task: dict[str, Any] = {
        "entity_id": int(entity_id),
        "entity_type": entity_type,
        "text": str(text),
        "complete_till": int(complete_till),
    }
    if responsible_user_id:
        task["responsible_user_id"] = int(responsible_user_id)
    if task_type_id:
        task["task_type_id"] = int(task_type_id)
    url = f"{BASE_URL}/api/v4/tasks"
    data = await _request_json("POST", url, body=[task], what=f"create_task[{entity_id}]")
    return data is not None


async def get_open_tasks_by_responsible(responsible_user_id: int) -> list[dict] | None:
    """Все незакрытые задачи менеджера (любые сущности). None - amo НЕ ОТВЕТИЛ.

    Разница между None и пустым списком принципиальна для сторожей: молчание amo нельзя
    читать как «задач нет», иначе на каждом сбое связи мы ставим задачу поверх живой.

    Отдаём ВСЕ задачи менеджера, а пересечение с клиентом считает вызывающий: фильтра
    «по клиенту» в amo нет, а у менеджера задач десятки, не тысячи (замер 29.09.2026:
    44 у самого загруженного), это одна-две страницы.
    """
    tasks: list[dict] = []
    page = 1
    while page <= 20:
        url = (
            f"{BASE_URL}/api/v4/tasks?filter[responsible_user_id]={int(responsible_user_id)}"
            f"&filter[is_completed]=0&limit=250&page={page}"
        )
        data = await _request_json("GET", url, what=f"open_tasks_by_resp[{responsible_user_id}]")
        if data is None:
            return None
        batch = ((data.get("_embedded") or {}).get("tasks")) or []
        tasks.extend(batch)
        if len(batch) < 250 or "next" not in ((data.get("_links") or {})):
            break
        page += 1
    return tasks


async def create_unsorted_lead(
    lead_name: Any,
    pipeline_id: int,
    contact: dict,
    source_uid: str,
    page_url: str,
    created_ts: int,
    source_name: str = "Jivo онлайн-чат",
) -> tuple[int | None, int | None]:
    """Создаёт заявку в «Неразобранное» воронки. Обёртка над
    create_unsorted_lead_ex с прежней сигнатурой (Jivo-мост)."""
    res = await create_unsorted_lead_ex(
        lead_name=lead_name,
        pipeline_id=pipeline_id,
        contact=contact,
        source_uid=source_uid,
        page_url=page_url,
        created_ts=created_ts,
        source_name=source_name,
    )
    return res.get("lead_id"), res.get("contact_id")


async def create_unsorted_lead_ex(
    lead_name: Any,
    pipeline_id: int,
    contact: dict,
    source_uid: str,
    page_url: str,
    created_ts: int,
    source_name: str = "Jivo онлайн-чат",
    form_id: str = "jivo_chat",
    lead_tags: list | None = None,
    ip: str = "0.0.0.0",
    *,
    max_attempts: int | None = None,
) -> dict:
    """Создаёт заявку в «Неразобранное» воронки (system-статус type=1, куда
    обычный POST /leads нельзя). Контакт передаётся встроенно: либо {"id": ...}
    для найденного дубля, либо новый dict с PHONE/EMAIL. Возвращает
    {"lead_id", "contact_id", "uid"} — uid нужен для accept_unsorted."""
    referer = page_url or f"{BASE_URL}"
    lead_entry: dict[str, Any] = {"name": str(lead_name)}
    if lead_tags:
        lead_entry["_embedded"] = {"tags": [{"name": str(t)} for t in lead_tags if str(t).strip()]}
    form = {
        "source_name": source_name,
        "source_uid": str(source_uid),
        "pipeline_id": int(pipeline_id),
        "created_at": int(created_ts),
        # metadata — на ВЕРХНЕМ уровне запроса (внутри _embedded amo даёт 400
        # FieldMissing), это обязательный блок формы.
        "metadata": {
            "form_id": str(form_id),
            "form_name": source_name,
            "form_page": referer,
            "form_sent_at": int(created_ts),
            "referer": referer,
            "ip": ip or "0.0.0.0",
        },
        "_embedded": {
            "leads": [lead_entry],
            "contacts": [contact],
        },
    }
    url = f"{BASE_URL}/api/v4/leads/unsorted/forms"
    request_options = {"max_attempts": max_attempts} if max_attempts is not None else {}
    data = await _request_json("POST", url, body=[form], what="create_unsorted", **request_options)
    if not isinstance(data, dict):
        return {}
    unsorted = (data.get("_embedded") or {}).get("unsorted") or []
    if not unsorted:
        return {}
    emb = (unsorted[0].get("_embedded") or {})
    leads = emb.get("leads") or []
    contacts = emb.get("contacts") or []
    return {
        "lead_id": leads[0].get("id") if leads else None,
        "contact_id": contacts[0].get("id") if contacts else None,
        "uid": unsorted[0].get("uid"),
    }


async def accept_unsorted(uid: str, status_id: int, user_id: int | None = None) -> int | None:
    """Принимает заявку из «Неразобранного» в обычный этап (источник сделки при
    этом сохраняется — ради него и ходим через unsorted). Возвращает id сделки."""
    body: dict[str, Any] = {"status_id": int(status_id)}
    if user_id:
        body["user_id"] = int(user_id)
    url = f"{BASE_URL}/api/v4/leads/unsorted/{uid}/accept"
    data = await _request_json("POST", url, body=body, what=f"accept_unsorted[{uid}]")
    if not isinstance(data, dict):
        return None
    leads = (data.get("_embedded") or {}).get("leads") or []
    return leads[0].get("id") if leads else None


async def list_sources() -> list:
    """Источники сделок, зарегистрированные НАШЕЙ интеграцией (чужие не видны)."""
    url = f"{BASE_URL}/api/v4/sources"
    data = await _request_json("GET", url, what="list_sources")
    if not isinstance(data, dict):
        return []
    return (data.get("_embedded") or {}).get("sources") or []


async def create_sources(items: list) -> list:
    """Регистрирует источники интеграции: [{"name": ..., "external_id": ...}]."""
    url = f"{BASE_URL}/api/v4/sources"
    data = await _request_json("POST", url, body=items, what="create_sources")
    if not isinstance(data, dict):
        return []
    return (data.get("_embedded") or {}).get("sources") or []


if __name__ == "__main__":
    async def _main():
        init_api_pipeline()
        try:
            lead_info = await get_lead_by_id(36420147)
            pprint(lead_info)
        finally:
            await shutdown_api_pipeline()

    asyncio.run(_main())
