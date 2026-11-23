"""Подбор существующих сделок для режима «Полный» (просьба и решение Кати 23.11.2026): ИИ берёт и те сделки, что лежали до его включения.

Раньше движок брал сделку только в момент нового сообщения клиента (`ai_engine._on_inbound` -> `_process_collecting`): прошлые сообщения он не перечитывает,
старые «забытые» сделки сам не находит. Подбор раз в `INTERVAL_S` спрашивает у панели кандидатов, берёт одну-две сделки за прогон, и дальше ИИ работает по ним
как по сделке, отданной кнопкой «Дать сделку ИИ» (событие `deal_given`, новых событий нет: ИИ сам читает историю чата, клиент ждёт ответа: отвечает; клиент молчит:
пробует возобновить контакт по своим правилам промпта, а не по правилам подбора).

Два пула (`existing.sweep.retail`, `existing.sweep.other` включают каждый отдельно):
  • «розница» (`retail`): ровно «ОП розница» и TangemShop (`ai_engine_config.SWEEP_RETAIL_PIPELINES`, переопределяется ключом `existing.sweep.retail_pipelines`), у которых есть промпт и
    которые режим пускает (`settings.pipeline_allowed`). ⚠️ Это НЕ `initiative_pipelines`: «Работа с базой» (11166334) получает инициативу как розница, но её старые сделки в этот пул не
    попадают, она в пуле «остальные». Берём ВСЕ открытые сделки, КРОМЕ тех, у которых есть открытая задача amo на любого сотрудника, кроме пользователя ИИ (`ai_user`).
    Задачи смотрит и панель (по снимку, он отстаёт до часа), и сам движок: ПЕРЕД взятием ОДИН живой запрос открытых задач именно этой сделки (`ai_engine_tasks._get_open`).
    Открытая задача не у ИИ: исход `blocked_by_task`, повтор не раньше чем через `RETRY_TASK_S` (6 часов); amo не ответила: `amo_unavailable`, повтор через `RETRY_ERROR_S`.
    Клиент ждёт ответа (последнее сообщение чата клиентское): берём. Клиент молчит (последнее сообщение наше): инициатива по СТАНДАРТНОМУ правилу молчания, то есть
    не раньше, чем пройдёт `functions.reminder.after_h` часов (умолчание 3, напоминание «жду подтверждения заказа», настройка панели, тот же порог движок отдаёт ИИ-сервису
    в ходе, `settings.reminder_config`) с последнего нашего сообщения. Раньше срока: исход `too_early`, `retry_at` = момент, когда порог пройдёт. Напоминание выключено в
    настройках: молчащих сделок не берём (инициативы по ним нет), исход `refused`. ⚠️ Решает ли ИИ потом писать, определяет промпт сервиса, не движок: на 23.11.2026 по
    событию `deal_given` при молчащем клиенте он пишет только после шаблона автоматики «Вы оформили заказ на сайте…» (не меньше трёх часов), иначе заканчивает `no_reply`.
  • «остальные» (`other`): прочие воронки, где у ИИ есть промпт, в том числе «Работа с базой» (`settings.pipeline_allowed`: правило «нет промпта: не пишем» не снято, а `pipelines_any` по-прежнему
    включается оператором `AI_PIPELINES_ANY_ALLOWED=1`; пока его нет, этот пул пуст). Берём сделку, если за последние `other_days` суток (1-7, умолчание 3) последнее
    сообщение чата клиентское и без ответа. При ВЗЯТИИ только ответ: молчащих не берём, задачи сотрудников НЕ смотрим. Уже взятая сделка живёт по правилам воронки: где инициатива разрешена
    (`initiative_pipelines`, «Работа с базой» в том числе), лестница тишины, окна УР и касания работают как у розницы, в остальных ход идёт с `initiative: false`.

Исключения, общие для обоих пулов: любая сделка, про которую у движка есть запись владельца (ведёт ИИ, снята с ИИ, у человека, закрыта), повторно не берётся (снятая с ИИ заново
не берётся, пинг-понга нет); отметки людей (`ai_engine_marks`: `removed` навсегда, `no_reply` пока клиент не написал позже отметки); записи `sweep_log` за 24 часа (любой исход)
и сделки, отложенные до `retry_at`; сделки с предложением смены ответственного за 7 дней.

Когда подбор работает, ВСЁ сразу (`blocked_by`): движок включён, настройки панели свежие, активный режим `full`, `existing.sweep.enabled` истинно и блок целый, включён хотя бы
один пул, сейчас рабочие часы режима (`settings.send_window()["open_now"]`, тот же признак, что у окна отправки в ходе), нет аварийного `AI_SWEEP_DISABLED=1` в `.env`
(`ai_engine_config.SWEEP_DISABLED`, читается при старте), а перед самим прогоном ещё и ИИ-сервис отвечает (`_ai_is_up`). Нет блока или мусор в нём: подбор выключен (как до
этого файла), мусорное поле в лог один раз (`ai_engine_filters._warn`). Подбор не работает и без целой политики `existing`: иначе он брал бы старые сделки как новые.

Как встроен. НЕ отдельный цикл: `tick_once` зовёт `tick(eng, now)`, который раз в `INTERVAL_S` запускает прогон фоновой задачей (`eng._spawn`, она не считается в
`loops_alive`, их по-прежнему пять), потому что прогон ходит в панель, amo и МойСклад и не должен задерживать таймеры ответов клиентам. Сам `tick` дешёвый: без включённого
подбора это одно чтение настроек. Прогон не перекрывается сам с собой.

Бюджет. `existing.sweep.per_hour`: не больше стольких ВЗЯТЫХ сделок за последние 60 минут, скользящее окно по таблице `sweep_log` (в той же sqlite, что у `ai_engine_store`,
создаётся при первом обращении). Взятой считается и сделка с открытым предложением смены ответственного при пилоте (`reassign_pending`): иначе при пилоте лимит ничего бы не
ограничивал, а очередь подтверждений заливало бы. Дальше жёсткие потолки: не больше `MAX_PER_RUN` взятых за прогон, не больше `MAX_EXAMINE` просмотренных кандидатов, дошедших
до сети (поиск в amo, МойСклад), не больше `MAX_OPEN_PROPOSALS` открытых предложений в очереди пилота. Всё состояние в sqlite: рестарт контейнера ничего не теряет и не повторяет,
память хранит только время следующего прогона и последнюю сводку для среза.

Кандидаты. `POST <AI_PANEL_URL>/api/ingest/ai/sweep-candidates` (`X-Ingest-Token`, как у `engine-event` и `reassign-proposal`), тело (контракт с панелью, не менять)
`{"retail_pipelines": [...], "other_pipelines": [...], "ai_user_id", "other_days", "exclude": [...до 3000], "limit": 30}`, ответ `{"candidates": [{deal_id, pool, pipeline_id, status_id,
responsible_user_id, contact_id, contact_name, phone, chat_id, chat_type, channel_id, last_msg_at, last_client_at, last_out_at, waiting}], "total_open", "generated_at"}`: сначала
ждущие ответа (дольше всех ждущие первыми), потом молчащие. Панель не выкачена (404), недоступна или ответила не тем: прогон ничего не делает, предупреждение в лог не чаще раза
в час. Пул кандидата движок определяет сам по воронке (`pool` панели не решает).

Взятие сделки то же, что при сообщении клиента: `dedup.resolve(..., eligible=filters.make_eligibility(...))` (воронка разрешена режимом И есть промпт, контакт в списке, фильтры и бюджет,
политика `existing`, правила дублей A и D), затем `ai_engine._gate_take` (товары, тишина менеджера `manager_quiet_min` с отсрочкой `retry_at`, пилот и смена ответственного), затем под общим
замком: при пилоте предложение (`ai_engine_approval.propose`, источник `sweep`), без пилота смена ответственного (`filters.reassign_responsible`) и общая с `give_deal` часть
`ai_engine._hand_over` (владелец ИИ, эпоха, `give.mark`, охрана инициативы, сессия чата, событие `deal_given`, примечание в сделку). Задачи amo подбор НЕ создаёт и НЕ закрывает
(`release_tasks=False`). Если у клиента несколько подходящих сделок (A, D), подбор НЕ зовёт людей: ни алерта, ни задачи, исход `needs_human` и дальше. Исход каждой попытки идёт в `sweep_log`.

Исходы: `given` (взята), `reassign_pending` (создано предложение, ждёт человека), `needs_human` (правила дублей), `refused` (причина в `note`), `closed` (подходящих открытых сделок у клиента нет),
`marked` (отметка человека), `too_early` (молчащий клиент, порог ещё не прошёл), `blocked_by_task` (открытая задача не у ИИ), `amo_unavailable` (amo не ответила), `error` (ИИ-сервис или запись
ответственного не ответили, исключение). Две неудачи подряд (`error`, `amo_unavailable`) обрывают прогон. Любая ошибка прогона не роняет цикл таймеров и не теряет сделки.
"""
import asyncio
import datetime
import logging
import os
import time
from typing import Any

import httpx

import ai_engine_approval as approval
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_filters as filters
import ai_engine_marks as marks
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store

logger = logging.getLogger("uvicorn")

PATH = "/api/ingest/ai/sweep-candidates"
TIMEOUT_S = 15.0
INTERVAL_S = 300                        # прогон раз в 5 минут (в тестах меняется)
MAX_PER_RUN = 2                         # взятых за прогон: очередь пилота не заливает пачкой
MAX_EXAMINE = 6                         # просмотренных кандидатов за прогон, дошедших до сети (поиск в amo, МойСклад)
CANDIDATES_LIMIT = 30                   # сколько кандидатов просим у панели
MAX_OPEN_PROPOSALS = 10                 # открытых предложений смены ответственного: больше не создаём, пока люди не разберут очередь
EXCLUDE_KEEP_S = 24 * 3600              # любой исход прошлой попытки держит сделку вне подбора столько
OWNER_KEEP_S = 120 * 24 * 3600          # записи владельца за этот срок идут в `exclude` панели (старее: движок отказывает сам, `get_owner`)
PROPOSAL_KEEP_S = 7 * 24 * 3600         # сделки с предложением смены ответственного за этот срок не предлагаем снова
LOG_KEEP_S = 7 * 24 * 3600              # `sweep_log` старше этого убирается
RETRY_BUSY_S = 600                      # чат занят движком или автопилотом: посмотреть позже
RETRY_ERROR_S = 900                     # amo, ИИ-сервис или запись ответственного не ответили: посмотреть позже (15 минут)
RETRY_TASK_S = 6 * 3600                 # у сделки открытая задача не у ИИ: посмотреть через 6 часов
WARN_EVERY_S = 3600                     # один вид сбоя панели в лог не чаще раза в час
EXCLUDE_MAX = 3000                      # потолок размера `exclude` в запросе
DEFAULT_PER_HOUR, DEFAULT_OTHER_DAYS = 10, 3

GIVEN, REASSIGN_PENDING, NEEDS_HUMAN, REFUSED, CLOSED, ERROR = "given", "reassign_pending", "needs_human", "refused", "closed", "error"
MARKED, TOO_EARLY, BLOCKED_BY_TASK, AMO_UNAVAILABLE = "marked", "too_early", "blocked_by_task", "amo_unavailable"
TAKEN = (GIVEN, REASSIGN_PENDING)        # что считается «взятой» сделкой для лимита в час
FAILURES = (ERROR, AMO_UNAVAILABLE)      # две подряд обрывают прогон
POOL_RETAIL, POOL_OTHER = "retail", "other"
POOL_TITLES = {POOL_RETAIL: "розница", POOL_OTHER: "остальные"}
SWEEP_REASON = "sweep"                  # `deal_owner.reason` у подобранных сделок (для журнала); новых событий хода нет, ИИ получает обычный `deal_given`
SWEEP_NOTE = "сделка подобрана ИИ-менеджером (режим Полный, подбор существующих сделок), дальше переписку ведёт он"

# Подмена транспорта для тестов (None: настоящая сеть, под pytest её нет, см. `may_send`).
_transport: httpx.AsyncBaseTransport | None = None

_task: asyncio.Task | None = None
_next_run_at = 0.0                       # не раньше этого момента следующий прогон (память: после рестарта первый прогон сразу, бюджет в sqlite)
_state: dict[str, Any] = {"last_run_at": None, "last_candidates": None, "total_open": None, "last_result": ""}
_warn_seen: dict[str, float] = {}


# =============================================================================================
# хранилище: журнал попыток
# =============================================================================================

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS sweep_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, "
    "outcome TEXT NOT NULL, note TEXT NOT NULL DEFAULT '', retry_at REAL);"
    "CREATE INDEX IF NOT EXISTS ix_sweep_log_at ON sweep_log (at);"
    "CREATE INDEX IF NOT EXISTS ix_sweep_log_deal ON sweep_log (deal_id, at)"
)


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(deal_id: int, outcome: str, note: str = "", now: float | None = None, retry_at: float | None = None) -> None:
    """Исход попытки. `retry_at`: до этого момента (а не 24 часа) сделку подбор не трогает (молчание клиента, задача сотрудника, активный менеджер, занятый чат, сбой сети)."""
    at = float(now if now is not None else time.time())
    with _Db() as conn:
        conn.execute("INSERT INTO sweep_log (deal_id, at, outcome, note, retry_at) VALUES (?,?,?,?,?)",
                     (int(deal_id), at, outcome, " ".join(str(note).split())[:300], float(retry_at) if retry_at else None))


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние попытки, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM sweep_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


def count_taken(now: float) -> int:
    """Взятых за последние 60 минут (скользящее окно): `given` и `reassign_pending`."""
    marks_sql = ",".join("?" * len(TAKEN))
    with _Db() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM sweep_log WHERE outcome IN ({marks_sql}) AND at>?", (*TAKEN, float(now) - 3600)).fetchone()[0]


def outcomes_24h(now: float) -> dict[str, int]:
    with _Db() as conn:
        return {r[0]: r[1] for r in conn.execute("SELECT outcome, COUNT(*) FROM sweep_log WHERE at>? GROUP BY outcome", (float(now) - EXCLUDE_KEEP_S,))}


def prune(now: float) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM sweep_log WHERE at<?", (float(now) - LOG_KEEP_S,))


def excluded_ids(now: float) -> list[int]:
    """Сделки, которые подбор не просит у панели, по важности (при потолке `EXCLUDE_MAX` отрезается хвост): отметки людей (`removed` все, `no_reply` за последние 24 часа),
    попытки за последние 24 часа (с `retry_at`: до него, не дольше суток), предложения смены ответственного за 7 дней, записи владельца за 120 суток (любой владелец:
    ИИ, человек, снята с ИИ, закрыта; свежие первыми)."""
    now = float(now)
    ordered: list[int] = []
    seen: set[int] = set()

    def add(ids) -> None:
        for deal_id in ids:
            deal_id = int(deal_id)
            if deal_id not in seen:
                seen.add(deal_id)
                ordered.append(deal_id)

    removed, recent_no_reply = marks.exclude_ids(now, EXCLUDE_KEEP_S)
    add(removed)
    add(recent_no_reply)
    with _Db() as conn:
        active = []
        for deal_id, at, retry_at in conn.execute("SELECT deal_id, at, retry_at FROM sweep_log WHERE at>? ORDER BY at DESC", (now - EXCLUDE_KEEP_S,)):
            until = at + EXCLUDE_KEEP_S if retry_at is None else min(float(retry_at), at + EXCLUDE_KEEP_S)
            if until > now:
                active.append(int(deal_id))
        add(active)
        add(approval.recent_deal_ids(now - PROPOSAL_KEEP_S))
        try:
            add(r[0] for r in conn.execute("SELECT deal_id FROM deal_owner WHERE since>=? ORDER BY since DESC", (_iso(now - OWNER_KEEP_S),)))
        except Exception:  # noqa: BLE001 - таблицы владельцев ещё нет (отдельный запуск без `store.init`): знать нечего
            pass
    return ordered[:EXCLUDE_MAX]


# =============================================================================================
# настройки
# =============================================================================================

def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def config() -> dict[str, Any] | None:
    """`existing.sweep` документа версии 2: `{"enabled": True, "per_hour": 1..60, "retail": bool, "other": bool, "other_days": 1..7, "retail_pipelines": [id, ...]}` (нет ключа: 10, True, True, 3,
    `cfg.SWEEP_RETAIL_PIPELINES`; в результате `retail_pipelines` всегда множество). Нет блока `existing`
    или `sweep`, `enabled` не истина, мусор в любом поле (не тот тип, вне границ), нет корректной политики `existing`: None, то есть подбор выключен. Мусорное поле идёт в лог один раз."""
    if not settings.is_v2():
        return None
    block = (settings.document() or {}).get("existing")
    if block is None:
        return None
    if not isinstance(block, dict):
        filters._warn("existing.sweep", "блок existing не словарь")
        return None
    raw = block.get("sweep")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        filters._warn("existing.sweep", "не словарь")
        return None
    enabled = raw.get("enabled")
    if enabled is not True:
        if enabled is not None and enabled is not False:
            filters._warn("existing.sweep.enabled", "не bool")
        return None
    per_hour = raw.get("per_hour", DEFAULT_PER_HOUR)
    if not _is_int(per_hour) or not 1 <= per_hour <= 60:
        filters._warn("existing.sweep.per_hour", "не целое 1-60")
        return None
    on: dict[str, bool] = {}
    for name in (POOL_RETAIL, POOL_OTHER):
        value = raw.get(name, True)
        if not isinstance(value, bool):
            filters._warn(f"existing.sweep.{name}", "не bool")
            return None
        on[name] = value
    other_days = raw.get("other_days", DEFAULT_OTHER_DAYS)
    if not _is_int(other_days) or not 1 <= other_days <= 7:
        filters._warn("existing.sweep.other_days", "не целое 1-7")
        return None
    retail_list = raw.get("retail_pipelines")                  # необязательный список воронок пула «розница»; нет ключа: `cfg.SWEEP_RETAIL_PIPELINES`
    if retail_list is None:
        retail_set = frozenset(cfg.SWEEP_RETAIL_PIPELINES)
    elif isinstance(retail_list, list) and retail_list and all(_is_int(p) and p > 0 for p in retail_list):
        retail_set = frozenset(retail_list)
    else:
        filters._warn("existing.sweep.retail_pipelines", "не непустой список положительных целых")
        return None
    if filters.existing_config() is None:
        filters._warn("existing.sweep", "нет корректной политики existing: подбор без неё брал бы старые сделки как новые")
        return None
    return {"enabled": True, "per_hour": per_hour, POOL_RETAIL: on[POOL_RETAIL], POOL_OTHER: on[POOL_OTHER], "other_days": other_days,
            "retail_pipelines": retail_set}


def blocked_by() -> str | None:
    """Почему подбор сейчас не работает (одной строкой) или None: можно. Только то, что решается без сети; «ИИ-сервис отвечает» проверяет прогон."""
    if not cfg.ENABLED:
        return "движок выключен"
    if cfg.SWEEP_DISABLED:
        return "аварийный рубильник AI_SWEEP_DISABLED=1"
    if settings.effective_mode() == "off":
        return "режим «Выключено» или настройки панели устарели"
    if settings.active_mode_id() != "full":
        return "активный режим не «Полный»"
    conf = config()
    if conf is None:
        return "подбор выключен в настройках"
    if not conf[POOL_RETAIL] and not conf[POOL_OTHER]:
        return "оба пула выключены в настройках"
    if not settings.send_window()["open_now"]:
        return "сейчас вне рабочих часов"
    return None


def pools() -> tuple[list[int], list[int]]:
    """Воронки, где ИИ вправе вести сделки СЕЙЧАС (те же решения, что при взятии по сообщению клиента: `settings.pipeline_allowed`, режим И промпт), по пулам: «розница» (список
    пула: `cfg.SWEEP_RETAIL_PIPELINES`, «ОП розница» и TangemShop, или `existing.sweep.retail_pipelines`; НЕ `initiative_pipelines`) и «остальные» (всё прочее с промптом, в том
    числе «Работа с базой»)."""
    conf = config()
    retail_set = conf["retail_pipelines"] if conf else frozenset(cfg.SWEEP_RETAIL_PIPELINES)
    allowed = sorted(p for p in prompts.pipelines_for(settings.effective_mode()) if settings.pipeline_allowed(p))
    retail = [p for p in allowed if p in retail_set]
    return retail, [p for p in allowed if p not in retail_set]


def silence_threshold_s() -> tuple[float | None, str]:
    """Стандартный порог молчания клиента, после которого ИИ вправе писать первым: `functions.reminder.after_h` часов (`settings.reminder_config`, умолчание 3), напоминание «жду
    подтверждения заказа». Выключено в настройках: (None, причина), молчащих не берём. Тот же порог движок передаёт ИИ-сервису в ходе (`mode_settings.functions.reminder`)."""
    rem = settings.reminder_config()
    if not rem["enabled"]:
        return None, "напоминание клиенту выключено в настройках: молчащие сделки не берём (инициативы по ним нет)"
    return float(rem["after_h"]) * 3600, ""


# =============================================================================================
# панель
# =============================================================================================

def may_send() -> bool:
    """Можно ли сейчас слать в панель. Под pytest без подставного транспорта нельзя: настоящей сети в тестах нет."""
    if not (cfg.ENABLED and cfg.PANEL_URL and cfg.PANEL_TOKEN):
        return False
    if _transport is None and os.getenv("PYTEST_CURRENT_TEST"):
        return False
    return settings.active_mode_id() != "off"


def _warn(kind: str, message: str, *args: Any) -> None:
    """Предупреждение о панели не чаще раза в час на вид сбоя: подбор повторяется каждые 5 минут, лог не должен превращаться в ленту одного и того же."""
    now = time.monotonic()
    last = _warn_seen.get(kind)
    if last is not None and now - last < WARN_EVERY_S:
        return
    _warn_seen[kind] = now
    logger.warning(message, *args)


async def fetch_candidates(body: dict[str, Any]) -> dict[str, Any] | None:
    """Один запрос кандидатов (без повторов: следующая попытка через `INTERVAL_S`). Ответ целиком (`candidates` список, `total_open`) или None: панель не
    выкачена (404), недоступна, отказала или ответила не JSON-объектом с `candidates`."""
    if not may_send():
        return None
    url = f"{cfg.PANEL_URL}{PATH}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, transport=_transport) as http:
            resp = await http.post(url, json=body, headers={"X-Ingest-Token": cfg.PANEL_TOKEN})
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - сеть, таймаут, что угодно: прогон пропускается
        _warn("unavailable", "ai_engine_sweep: панель недоступна (%s: %s), подбор в этот раз пропущен", type(exc).__name__, str(exc)[:120])
        return None
    if resp.status_code == 404:
        _warn("http_404", "ai_engine_sweep: панель ответила 404 на %s (ручка ещё не выкачана?), подбор ничего не делает", PATH)
        return None
    if resp.status_code != 200:
        _warn(f"http_{resp.status_code}", "ai_engine_sweep: панель ответила HTTP %s на %s, подбор в этот раз пропущен", resp.status_code, PATH)
        return None
    try:
        data = resp.json()
    except ValueError:
        data = None
    if not isinstance(data, dict) or not isinstance(data.get("candidates"), list):
        _warn("bad_body", "ai_engine_sweep: ответ панели на %s не похож на список кандидатов (не JSON-объект с candidates), подбор пропущен", PATH)
        return None
    return data


def _pos_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _parse_ts(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    try:
        moment = datetime.datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.timestamp()


def _clean(raw: Any) -> dict[str, Any] | None:
    """Кандидат панели -> то, что нужно подбору. Непонятная запись (нет сделки, воронки, чата или канала) пропускается. Личные данные (имя, телефон) в лог не идут."""
    if not isinstance(raw, dict):
        return None
    deal_id, pipeline_id = _pos_int(raw.get("deal_id")), _pos_int(raw.get("pipeline_id"))
    chat_id, channel_id = str(raw.get("chat_id") or "").strip(), str(raw.get("channel_id") or "").strip()
    if deal_id is None or pipeline_id is None or not chat_id or not channel_id:
        return None
    return {
        "deal_id": deal_id, "pipeline_id": pipeline_id, "status_id": _pos_int(raw.get("status_id")), "contact_id": _pos_int(raw.get("contact_id")),
        "responsible_user_id": _pos_int(raw.get("responsible_user_id")), "contact_name": str(raw.get("contact_name") or ""), "phone": str(raw.get("phone") or ""),
        "chat_id": chat_id, "chat_type": str(raw.get("chat_type") or ""), "channel_id": channel_id, "waiting": raw.get("waiting") is True,
        "last_msg_ts": _parse_ts(raw.get("last_msg_at")), "last_client_ts": _parse_ts(raw.get("last_client_at")), "last_out_ts": _parse_ts(raw.get("last_out_at")),
    }


# =============================================================================================
# взятие одной сделки
# =============================================================================================

Result = tuple[str, str, "float | None"]          # исход, причина одной строкой, `retry_at`


async def _chat_busy(eng, chat_key: str, now: float) -> Result | None:
    """Чат сейчас в работе движка (ждём тишину клиента, идёт ход, ждём решения человека) или помечен человеческим: подбор в него не лезёт, посмотрит позже."""
    s = await eng._db(store.get_session, chat_key)
    if s is None:
        return None
    if s["state"] in (store.S_COLLECTING, store.S_IN_TURN, approval.AWAITING):
        return REFUSED, "чат сейчас в работе движка", now + RETRY_BUSY_S
    if s["state"] == store.S_HUMAN:
        until = float(s["skip_until"]) if s.get("skip_until") and s["skip_until"] > now else now + RETRY_BUSY_S
        return REFUSED, "чат помечен как человеческий (менеджер ведёт диалог)", until
    return None


def _pool_of(cand: dict[str, Any], retail: set[int], other: set[int], conf: dict[str, Any]) -> tuple[str | None, Result | None]:
    """Пул кандидата по воронке (решает движок, не панель). Воронка вне обоих пулов или пул выключен в настройках: (None, исход)."""
    pid = cand["pipeline_id"]
    pool = POOL_RETAIL if pid in retail else POOL_OTHER if pid in other else None
    if pool is None:
        return None, (REFUSED, "воронка сделки не ведётся ИИ (режим не пускает или нет промпта)", None)
    if not conf[pool]:
        return None, (REFUSED, f"пул «{POOL_TITLES[pool]}» выключен в настройках", None)
    return pool, None


def _mark_verdict(mark: dict[str, Any] | None, cand: dict[str, Any]) -> Result | None:
    """Отметка человека (`ai_engine_marks`) не пускает сделку в подбор: `removed` всегда, `no_reply` пока клиент не написал позже отметки (по `last_client_at` кандидата)."""
    if mark is None:
        return None
    if mark["kind"] == marks.KIND_REMOVED:
        return MARKED, "отметка человека «удалить»: ИИ эту сделку не ведёт", None
    last = cand["last_client_ts"]
    if last is None or last <= float(mark["at"]):
        return MARKED, "отметка человека «не требует ответа» стоит позже последнего сообщения клиента", None
    return None


async def _local(eng, cand: dict[str, Any], pool: str, conf: dict[str, Any], now: float) -> Result | None:
    """Проверки без сети (только база): канал, отметки людей, правила пула (молчание клиента в «рознице», только ответ в «остальных»), владелец, занятый чат. None: можно идти дальше."""
    if cand["channel_id"] not in cfg.CHANNELS:
        return REFUSED, "канал чата не слушает движок: ответы клиента до него не дойдут", None
    verdict = _mark_verdict(await eng._db(marks.get, cand["deal_id"]), cand)
    if verdict is not None:
        return verdict
    if pool == POOL_OTHER:
        # «Остальные» воронки: только ответ на сообщение клиента за последние `other_days` суток, инициативы нет.
        if not cand["waiting"]:
            return REFUSED, "в «остальных» воронках только ответ: последнее сообщение чата не клиентское", None
        last = cand["last_client_ts"]
        if last is None or now - last > conf["other_days"] * 86400:
            return REFUSED, f"последнее сообщение клиента старше {conf['other_days']} сут. (или время неизвестно)", None
    elif not cand["waiting"]:
        # «Розница», клиент молчит: инициатива по стандартному правилу молчания, не раньше порога (`silence_threshold_s`). Инициатива в воронке выключена панелью: писать первым нельзя.
        if not settings.initiative_allowed(cand["pipeline_id"]):
            return REFUSED, "в воронке инициатива выключена настройками: молчащих не берём", None
        secs, why = silence_threshold_s()
        if secs is None:
            return REFUSED, why, None
        start = cand["last_out_ts"] or cand["last_msg_ts"]
        if start is None:
            return REFUSED, "время последнего сообщения чата неизвестно", None
        if now < start + secs:
            return TOO_EARLY, f"клиент молчит {max(0.0, now - start) / 3600:.1f} ч из {secs / 3600:g}: писать первым рано", start + secs
    if await eng._db(store.get_owner, cand["deal_id"]) is not None:
        return REFUSED, "по сделке уже есть запись владельца в движке (повторно не берём)", None
    busy = await _chat_busy(eng, eng.chat_key_of(cand["channel_id"], cand["chat_id"]), now)
    if busy is not None:
        return busy
    if eng._autopilot_busy(cand["chat_id"], cand["phone"], cand["deal_id"]):
        return REFUSED, "сделку ведёт авто-режим", now + RETRY_BUSY_S
    return None


def _tasks_verdict(tasks: list[dict] | None, ai_user_id: int, now: float) -> Result | None:
    """Живой запрос открытых задач сделки («розница»): открытая задача на сотрудника, кроме пользователя ИИ, не пускает. None: можно."""
    if tasks is None:
        return AMO_UNAVAILABLE, "amoCRM не ответила по задачам сделки", now + RETRY_ERROR_S
    foreign = [t for t in tasks if isinstance(t, dict) and _pos_int(t.get("responsible_user_id")) != int(ai_user_id)]
    if foreign:
        return BLOCKED_BY_TASK, f"открытых задач на сотрудников: {len(foreign)}", now + RETRY_TASK_S
    return None


def _classify(res: dedup.Resolution, deal_id: int, now: float) -> Result | None:
    """Выбор сделки не дал `ai` на ЭТУ сделку: исход подбора. None: сделка выбрана, идём дальше."""
    if res.kind == dedup.K_ERROR:
        return AMO_UNAVAILABLE, res.note or "amoCRM не ответила", now + RETRY_ERROR_S
    if res.kind in (dedup.K_NONE, dedup.K_GONE):
        return CLOSED, "открытых подходящих сделок у клиента нет (закрыта или в другой воронке)", None
    if res.kind == dedup.K_SKIP:
        return REFUSED, "контакта нет в списке режима", None
    if res.kind == dedup.K_HUMAN:
        ids = ", ".join(str(int(ld["id"])) for ld in (res.others or [])[:5] if ld.get("id") is not None)
        return NEEDS_HUMAN, f"правило {res.rule}: у клиента несколько подходящих сделок или другая в работе" + (f" ({ids})" if ids else ""), None
    if res.kind == dedup.K_REFUSED:
        return REFUSED, res.note, (res.retry_at if res.retry_at and res.retry_at > now else None)
    if res.kind == dedup.K_AI and res.deal is not None and int(res.deal["id"]) != int(deal_id):
        return REFUSED, f"у клиента выбрана другая сделка {int(res.deal['id'])}", None
    return None


async def _take(eng, cand: dict[str, Any], pool: str, ai_user_id: int) -> Result:
    now = eng._now()
    deal_id = cand["deal_id"]
    mode_id = settings.active_mode_id()
    if pool == POOL_RETAIL:
        # Снимок задач в панели отстаёт до часа: перед взятием ОДИН живой запрос задач именно этой сделки (задачи amo не создаём и не закрываем).
        verdict = _tasks_verdict(await eng.ai_engine_tasks._get_open(deal_id), ai_user_id, now)
        if verdict is not None:
            return verdict
    chat_key = eng.chat_key_of(cand["channel_id"], cand["chat_id"])

    # Выбор сделки и проверки, как при сообщении клиента: сеть здесь, без общего замка (`ai_engine._process_collecting`).
    eligible = filters.make_eligibility(mode_id, [cand["phone"], cand["chat_id"]])
    res = await dedup.resolve(cand["chat_id"], cand["phone"], eligible=eligible)
    verdict = _classify(res, deal_id, now)
    if verdict is not None:
        return verdict
    s = {"chat_key": chat_key, "chat_id": cand["chat_id"], "chat_type": cand["chat_type"], "channel_id": cand["channel_id"],
         "phone": cand["phone"], "contact_name": cand["contact_name"]}
    res = await eng._gate_take(s, res, mode_id)                  # товары (МойСклад), тишина менеджера, смена ответственного, открытое предложение
    verdict = _classify(res, deal_id, eng._now())
    if verdict is not None:
        return verdict
    deal = res.deal
    gate = res.gate

    async with eng._get_lock():
        now = eng._now()
        # Пока ходили в amo и МойСклад, могло измениться всё: режим, воронки, владелец (клиент написал сам), отметки, чат.
        if settings.effective_mode() == "off" or settings.active_mode_id() != mode_id:
            return REFUSED, "режим сменили, пока шла проверка", None
        if not settings.pipeline_allowed(deal.get("pipeline_id")):
            return REFUSED, "воронка сделки больше не ведётся ИИ", None
        if await eng._db(store.get_owner, deal_id) is not None:
            return REFUSED, "пока шла проверка, по сделке появился владелец (клиент написал сам или её взяли руками)", None
        verdict = _mark_verdict(await eng._db(marks.get, deal_id), cand)
        if verdict is not None:
            return verdict
        busy = await _chat_busy(eng, chat_key, now)
        if busy is not None:
            return busy
        if eng._autopilot_busy(cand["chat_id"], cand["phone"], deal_id):
            return REFUSED, "сделку ведёт авто-режим", now + RETRY_BUSY_S
        if not await eng._ai_is_up():
            return ERROR, "ИИ-сервис не отвечает", now + RETRY_ERROR_S
        if gate is not None and gate.propose:
            # Пилот: смену ответственного подтверждает человек, тот же путь предложений, что при сообщении клиента (`ai_engine_approval`).
            await eng._db(store.put_session, chat_key, channel_id=cand["channel_id"], chat_id=cand["chat_id"], chat_type=cand["chat_type"],
                          phone=cand["phone"], contact_name=cand["contact_name"], state=approval.AWAITING, deal_id=deal_id, next_at=None)
            if await approval.propose(eng, s, deal, gate, mode_id, source=approval.SOURCE_SWEEP):
                return REASSIGN_PENDING, "создано предложение смены ответственного, ждёт решения человека", None
            return REFUSED, "по сделке уже есть открытое предложение смены ответственного", None
        if gate is not None and gate.reassign and not await filters.reassign_responsible(deal_id, gate.reassign):
            return ERROR, "смена ответственного не удалась", now + RETRY_ERROR_S
        chat = {"chat_id": cand["chat_id"], "chat_type": cand["chat_type"], "channel_id": cand["channel_id"], "chat_key": chat_key}
        owner = await eng._hand_over(deal, chat=chat, phone=cand["phone"], contact_name=cand["contact_name"], mode_id=mode_id,
                                     forced=False, reason=SWEEP_REASON, note=SWEEP_NOTE, release_tasks=False)
        return GIVEN, f"взята, эпоха {owner['epoch']}", None


async def _attempt(eng, cand: dict[str, Any], pool: str, ai_user_id: int) -> Result:
    try:
        return await _take(eng, cand, pool, ai_user_id)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - сбой одной сделки не должен остановить остальные и уронить цикл
        logger.exception("ai_engine_sweep: сбой подбора сделки %s", cand.get("deal_id"))
        return ERROR, "исключение при подборе (см. лог движка)", eng._now() + RETRY_ERROR_S


# =============================================================================================
# прогон
# =============================================================================================

async def _run(eng, now: float, out: dict[str, Any]) -> None:
    why = blocked_by()
    if why:
        out["skipped"] = why
        return
    conf = config()
    if conf is None:
        out["skipped"] = "подбор выключен в настройках"
        return
    await eng._db(prune, now)
    if not await eng._ai_is_up():
        out["skipped"] = "ИИ-сервис недоступен"
        return
    taken = await eng._db(count_taken, now)
    budget = conf["per_hour"] - taken
    if budget <= 0:
        out["skipped"] = f"лимит за час исчерпан ({taken} из {conf['per_hour']})"
        return
    open_proposals = await eng._db(approval.count_open)
    if open_proposals >= MAX_OPEN_PROPOSALS:
        out["skipped"] = f"в очереди пилота уже {open_proposals} предложений, новые не создаю"
        return
    retail_all, other_all = pools()
    retail_req = retail_all if conf[POOL_RETAIL] else []
    other_req = other_all if conf[POOL_OTHER] else []
    if not retail_req and not other_req:
        out["skipped"] = "нет воронок, где ИИ вправе вести сделки"
        return
    user = filters.ai_user()
    if user is None:
        out["skipped"] = "пользователь ИИ в настройках не задан"
        return
    body = {"retail_pipelines": retail_req, "other_pipelines": other_req, "ai_user_id": int(user["id"]), "other_days": conf["other_days"],
            "exclude": await eng._db(excluded_ids, now), "limit": CANDIDATES_LIMIT}
    data = await fetch_candidates(body)
    if data is None:
        out["skipped"] = "панель не отдала кандидатов"
        return
    seen: set[int] = set()
    cands = []
    for raw in data["candidates"]:
        cand = _clean(raw)
        if cand is not None and cand["deal_id"] not in seen:
            seen.add(cand["deal_id"])
            cands.append(cand)
    out["candidates"] = len(cands)
    _state["last_candidates"] = len(cands)
    _state["total_open"] = _pos_int(data.get("total_open"))
    want = min(budget, MAX_PER_RUN)
    given_now = failures = 0
    retail_set, other_set = set(retail_all), set(other_all)
    for cand in cands:
        if given_now >= want:
            break
        why = blocked_by()                                       # окно рабочих часов закрылось или настройки сменились между сделками
        if why:
            out["skipped"] = f"прогон прерван: {why}"
            break
        pool, verdict = _pool_of(cand, retail_set, other_set, conf)
        if verdict is None:
            verdict = await _local(eng, cand, pool, conf, eng._now())
        if verdict is None:
            if out["examined"] >= MAX_EXAMINE:
                break                                            # дальше только сеть, а лимит просмотров на прогон исчерпан: остальные в следующий раз
            out["examined"] += 1
            verdict = await _attempt(eng, cand, pool, int(user["id"]))
        outcome, note, retry_at = verdict
        try:
            await eng._db(log, cand["deal_id"], outcome, note, eng._now(), retry_at)
        except Exception:  # noqa: BLE001 - журнал не должен ломать прогон; сделка уже взята или отказана
            logger.exception("ai_engine_sweep: не записал исход по сделке %s в журнал", cand["deal_id"])
        out["outcomes"][outcome] = out["outcomes"].get(outcome, 0) + 1
        logger.info("ai_engine_sweep: сделка %s: %s%s", cand["deal_id"], outcome, f" ({note})" if note else "")
        if outcome in TAKEN:
            given_now += 1
        failures = failures + 1 if outcome in FAILURES else 0
        if failures >= 2:
            out["skipped"] = "две неудачи подряд (amo или ИИ-сервис), прогон прерван"
            break


async def run_once(eng) -> dict[str, Any]:
    """Один прогон подбора. Не бросает (кроме отмены). Возвращает сводку: `skipped` (почему ничего не делал или оборвался), `candidates`, `examined`, `outcomes`."""
    now = eng._now()
    out: dict[str, Any] = {"skipped": None, "candidates": None, "examined": 0, "outcomes": {}}
    _state["last_run_at"] = now
    try:
        await _run(eng, now, out)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - любая ошибка прогона не роняет цикл и не теряет сделки (всё состояние в sqlite)
        logger.exception("ai_engine_sweep: прогон упал")
        out["skipped"] = "прогон упал (см. лог движка)"
    summary = f"кандидатов {out['candidates'] if out['candidates'] is not None else '-'}, просмотрено {out['examined']}, исходы {out['outcomes'] or '-'}"
    if out["skipped"]:
        summary += f", {out['skipped']}"
    _state["last_result"] = summary
    logger.info("ai_engine_sweep: прогон: %s", summary)
    return out


async def tick(eng, now: float) -> None:
    """Из `tick_once` каждый проход таймеров: дёшево. Раз в `INTERVAL_S`, если подбор вообще разрешён (`blocked_by`), запускает прогон фоновой задачей.
    Не бросает: ошибка здесь не должна остановить остальные таймеры."""
    global _task, _next_run_at
    try:
        if (_task is not None and not _task.done()) or now < _next_run_at:
            return
        if blocked_by() is not None:
            return
        _next_run_at = now + INTERVAL_S
        _task = eng._spawn(run_once(eng))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_sweep: не запустил прогон")


# =============================================================================================
# срез состояния
# =============================================================================================

def status_block(now: float) -> dict[str, Any]:
    """Блок `sweep` среза `GET /`: `enabled`, `per_hour`, `pools` (`retail`, `other`, `other_days`), `taken_last_hour`, `budget_left`, `last_run_at`, `last_candidates`, `outcomes_24h`,
    `marks` (число отметок людей по видам и всего); сверх того `blocked_by` (почему подбор сейчас не работает), `total_open` (открытых сделок у панели), `last_result` (сводка последнего прогона)."""
    conf = config()
    last = _state["last_run_at"]
    taken = count_taken(now)
    marks_now = marks.counts()
    return {
        "enabled": conf is not None,
        "per_hour": conf["per_hour"] if conf else None,
        "pools": {POOL_RETAIL: conf[POOL_RETAIL], POOL_OTHER: conf[POOL_OTHER], "other_days": conf["other_days"]} if conf else None,
        "taken_last_hour": taken,
        "budget_left": max(0, conf["per_hour"] - taken) if conf else None,
        "last_run_at": None if last is None else _iso(last),
        "last_candidates": _state["last_candidates"], "total_open": _state["total_open"], "last_result": _state["last_result"],
        "outcomes_24h": outcomes_24h(now),
        "marks": {**marks_now, "total": sum(marks_now.values())},
        "blocked_by": blocked_by(),
    }
