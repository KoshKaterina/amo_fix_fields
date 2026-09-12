"""Распределение лидов — замена нативного виджета «Генезис»/F5 (05.08.2026).

Конструктор профилей: администратор создаёт/редактирует/удаляет профили в
team-panel (владелец данных с 09.08.2026, app/lead_distribution/service.py) —
в этом файле нет захардкоженных правил, они данные, не код (в отличие от
office_transfer.py). Здесь профили только читаются — через
lead_distribution_profiles_client (write-through кэш, работает при
недоступности team-panel на последних известных данных).

Профиль: точка входа (pipeline_id/status_id) + фильтр источников (source_ids,
[] = любой) + участники + режим учёта повторных клиентов (always/load/random)
+ опциональные рабочие часы профиля. Один source_id не может быть в двух
профилях одновременно (валидация при создании/редактировании — на стороне
team-panel) — это же свойство гарантирует, что счётчики нагрузки, которые
ведутся ПО ИСТОЧНИКУ (не по профилю), однозначны: у каждого источника один
профиль-владелец.

Защита от гонки с amgroup (создаёт сделку и асинхронно привязывает контакт):
если у свежепрочитанной сделки ещё нет контакта, диспетчер не принимает
решение вообще, а запускает фоновую задачу активного ожидания (короткие
проверки, не блокирующие воркер очереди LANE_AMO) — см. _contact_wait_loop.

Надёжность — тот же приём, что в office_transfer.py: вебхук (быстрый путь) +
периодическая reconciliation по окну времени (без ретроактивности) как
страховка от сбоев API/пропущенных вебхуков. В отличие от office_transfer,
где точки входа всегда достигаются ПЕРЕХОДОМ из другого этапа, точки входа
lead_distribution (напр. «Неразобранное») часто — это статус сделки СРАЗУ
при создании, для которого amoCRM не даёт события lead_status_changed
(см. _created_in_status_leads) — поэтому окно проверяется и через
/api/v4/events (lead_status_changed), и напрямую через /api/v4/leads
(created_at в окне + сделка всё ещё в нужном статусе).
"""

import asyncio
import dataclasses
import datetime
import json
import logging
import os
import pathlib
import random
import re
import time
from fractions import Fraction
from typing import Any

import amo_service
import alerts
import lead_distribution_log_client
import lead_distribution_profiles_client
import team_panel_client
import telegram_bot
import tg_recipients
from waybill_config import (
    FIELD_DELIVERY_TYPE,
    FIELD_PHONE,
    LEAD_DISTRIBUTION_CONTACT_POLL_S,
    LEAD_DISTRIBUTION_CONTACT_WAIT_S,
    LEAD_DISTRIBUTION_DEFAULT_WINDOW,
    LEAD_DISTRIBUTION_ENABLED,
    LEAD_DISTRIBUTION_FAIRNESS_GAP,
    LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S,
    LEAD_DISTRIBUTION_SINCE_TS,
    LEAD_DISTRIBUTION_STALE_ALERT_MIN,
    TAG_LEAD_DISTRIBUTION_ERROR,
    TAG_LEAD_DISTRIBUTION_ROUTED,
)

logger = logging.getLogger("uvicorn")

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"
_MSK = datetime.timezone(datetime.timedelta(hours=3))

_VALID_MODES = ("always", "load", "random")

_HHMM_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _normalize_entry_points(d: dict) -> list[dict[str, Any]]:
    """Профиль до 08.08.2026 хранил ровно одну точку входа (pipeline_id/status_id
    плоскими полями); UI конструктора теперь даёт добавлять несколько воронок,
    каждая — с несколькими этапами. Старые записи читаем как список из одной
    точки входа — чисто защитный код на чтение кэша, писать умеет только team-panel."""
    raw = d.get("entry_points")
    if raw:
        return [
            {"pipeline_id": int(ep["pipeline_id"]), "status_ids": sorted({int(s) for s in ep["status_ids"]})}
            for ep in raw
        ]
    if d.get("pipeline_id") and d.get("status_id"):
        return [{"pipeline_id": int(d["pipeline_id"]), "status_ids": [int(d["status_id"])]}]
    return []


def _normalize_participant_weights(raw: Any) -> dict[int, int]:
    """{user_id: вес} — соотношение распределения между участниками профиля,
    задаётся отдельно в каждом профиле (team-panel app/lead_distribution/
    validation.py normalize_participant_weights - там же настоящая валидация).
    Здесь - защитный код на чтение кэша (см. докстринг модуля): некорректные
    записи молча пропускаются, а не роняют весь профиль, битые данные не
    должны останавливать распределение (та же дисциплина, что у остальных
    _normalize_* в этом файле)."""
    if not isinstance(raw, dict):
        return {}
    out: dict[int, int] = {}
    for k, v in raw.items():
        try:
            uid, weight = int(k), int(v)
        except (TypeError, ValueError):
            continue
        if weight >= 1:
            out[uid] = weight
    return out


def _normalize_work_hours(raw: Any) -> list[dict[str, str]] | None:
    """Профиль до 08.08.2026 хранил один интервал часами ({"from":10,"to":19});
    формат сменён на список интервалов ("HH:MM"), т.к. UI конструктора теперь
    даёт добавлять несколько окон и указывать минуты (по образцу графика
    сотрудников в team-panel). Старые записи в кэше читаем как раньше —
    конвертируем на лету, чисто защитный код на чтение, писать умеет только team-panel."""
    if not raw:
        return None
    if isinstance(raw, dict) and "from" in raw and "to" in raw:
        f, t = int(raw["from"]), int(raw["to"])
        return [{"start": f"{f:02d}:00", "end": f"{t:02d}:00"}]
    if isinstance(raw, list):
        return [{"start": str(iv["start"]), "end": str(iv["end"])} for iv in raw] or None
    return None


# ════════════════ модель профиля ════════════════

@dataclasses.dataclass
class Profile:
    id: str
    name: str
    enabled: bool = False
    priority: int = 100
    entry_points: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    source_ids: list[int] = dataclasses.field(default_factory=list)
    participant_ids: list[int] = dataclasses.field(default_factory=list)
    participant_weights: dict[int, int] = dataclasses.field(default_factory=dict)
    duty_user_id: int | None = None
    repeat_contact_mode: str = "load"
    work_hours: list[dict[str, str]] | None = None
    # 12.08.2026, задание Тианы: полностью опционально (дефолт False = поведение
    # не меняется). Вместо дневных счётчиков «сколько мы раздали сегодня»
    # (_load_counters_state, локальный JSON) - "load"-режим считает счётчик как
    # «сколько у сотрудника СЕЙЧАС открытых (вне 142/143) сделок по источнику»,
    # живым запросом к amoCRM (см. _open_deals_state). Работает только с
    # repeat_contact_mode="load" - на always/random не влияет.
    use_open_deals_counter: bool = False
    created_at: int = 0
    updated_at: int = 0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Profile":
        return cls(
            id=d["id"],
            name=d.get("name", ""),
            enabled=bool(d.get("enabled", False)),
            priority=int(d.get("priority", 100)),
            entry_points=_normalize_entry_points(d),
            source_ids=[int(x) for x in d.get("source_ids") or []],
            participant_ids=[int(x) for x in d.get("participant_ids") or []],
            participant_weights=_normalize_participant_weights(d.get("participant_weights")),
            duty_user_id=int(d["duty_user_id"]) if d.get("duty_user_id") is not None else None,
            repeat_contact_mode=d.get("repeat_contact_mode", "load"),
            work_hours=_normalize_work_hours(d.get("work_hours")),
            use_open_deals_counter=bool(d.get("use_open_deals_counter", False)),
            created_at=int(d.get("created_at", 0)),
            updated_at=int(d.get("updated_at", 0)),
        )


# ════════════════ хранилище профилей (владелец — team-panel, здесь read-only кэш) ════════════════
#
# 09.08.2026: CRUD и валидация профилей переехали в team-panel (app/lead_distribution/
# service.py + validation.py) — team-panel единственный, кто пишет правила. Здесь только
# читаем через lead_distribution_profiles_client (write-through кэш в var/, переживает
# недоступность team-panel на последних известных данных). См. docstring клиента.


def _profiles() -> dict[str, Profile]:
    out: dict[str, Profile] = {}
    for pid, pdata in lead_distribution_profiles_client.get_profiles().items():
        try:
            out[pid] = Profile.from_dict(pdata)
        except Exception:
            logger.exception("lead_distribution: битая запись профиля %s в кэше — пропущена", pid)
    return out


def list_profiles() -> list[Profile]:
    return list(_profiles().values())


def get_profile(profile_id: str) -> Profile | None:
    return _profiles().get(profile_id)


# ════════════════ утилита атомарной записи — используется ротацией/счётчиками ниже ════════════════
# (runtime-состояние диспетчера, не правила — остаётся локальным var/*.json как было)


def _atomic_write_json(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


# ════════════════ матчинг профиля по свежей сделке ════════════════

def _lead_source_id(lead: dict) -> int | None:
    src = (lead.get("_embedded") or {}).get("source")
    if not src or src.get("id") is None:
        return None
    return int(src["id"])


def _is_office_delivery(lead: dict) -> bool:
    """Тип доставки (FIELD_DELIVERY_TYPE, 577315, text) содержит «офис» -
    самовывоз из офиса Sunscrypt (та же подстрока, что DELIVERY_SHOWROOM_MARKER
    в office_transfer.py, регистронезависимо - .casefold(), тот же приём).
    Решение Тианы 19.08.2026: такие сделки не распределяются вообще, ими
    занимается офис-менеджер напрямую, не пул участников профиля."""
    text = str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").casefold()
    return "офис" in text


def _matches_entry(profile: Profile, pipeline_id: int, status_id: int) -> bool:
    return any(ep["pipeline_id"] == pipeline_id and status_id in ep["status_ids"] for ep in profile.entry_points)


def _entry_pairs(profile: Profile) -> set[tuple[int, int]]:
    return {(ep["pipeline_id"], sid) for ep in profile.entry_points for sid in ep["status_ids"]}


def match_profile(pipeline_id: int, status_id: int, source_id: int | None) -> Profile | None:
    pipeline_id, status_id = int(pipeline_id), int(status_id)
    candidates = [
        p for p in _profiles().values()
        if p.enabled and _matches_entry(p, pipeline_id, status_id)
        and (not p.source_ids or (source_id is not None and source_id in p.source_ids))
    ]
    if not candidates:
        return None
    if len(candidates) > 1:
        logger.error(
            "lead_distribution: НЕСКОЛЬКО профилей совпали (pipeline=%s status=%s source=%s): %s — "
            "берём с наименьшим priority, это аномалия (валидация уникальности источника не должна была это пропустить)",
            pipeline_id, status_id, source_id, [p.id for p in candidates],
        )
    return min(candidates, key=lambda p: p.priority)


def has_matching_enabled_profile(pipeline_id: int, status_id: int) -> bool:
    """Дешёвая проверка для вебхука (без учёта source_id — тот матчится в
    диспетчере на свежих данных), чтобы не ставить в очередь заведомо лишнее."""
    pipeline_id, status_id = int(pipeline_id), int(status_id)
    return any(p.enabled and _matches_entry(p, pipeline_id, status_id) for p in _profiles().values())


def has_matching_status(status_id: int) -> bool:
    """Как has_matching_enabled_profile, но без воронки — часть событий amo
    приходит без pipeline_id в теле вебхука (нормальное поведение)."""
    status_id = int(status_id)
    return any(
        p.enabled and any(status_id in ep["status_ids"] for ep in p.entry_points)
        for p in _profiles().values()
    )


# ════════════════ ротация (random-режим и фолбэк load без source_id) ════════════════

ROTATION_PATH = pathlib.Path(os.getenv("LEAD_DISTRIBUTION_ROTATION_PATH", "var/lead_distribution_rotation.json"))
_rotation_lock = asyncio.Lock()


def _load_rotation() -> dict:
    if not ROTATION_PATH.exists():
        return {}
    try:
        return json.loads(ROTATION_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.exception("lead_distribution: не удалось прочитать %s", ROTATION_PATH)
        return {}


def _save_rotation(state: dict) -> None:
    _atomic_write_json(ROTATION_PATH, state)


async def _next_in_rotation(profile_id: str, pool: list[int]) -> int:
    """Указатель — ПОСЛЕДНИЙ выданный user_id (не индекс): переживает изменение
    состава пула между вызовами (сотрудник добавлен/убран/ушёл с рабочих часов)
    без сбоя — если последнего получателя больше нет в пуле, начинаем сначала."""
    async with _rotation_lock:
        state = _load_rotation()
        last = (state.get(profile_id) or {}).get("last_user_id")
        idx = (pool.index(last) + 1) % len(pool) if last in pool else 0
        candidate = pool[idx]
        state[profile_id] = {"last_user_id": candidate}
        _save_rotation(state)
        return candidate


# ════════════════ счётчики нагрузки — ПО ИСТОЧНИКУ, сброс по дате МСК ════════════════

COUNTERS_PATH = pathlib.Path(os.getenv("LEAD_DISTRIBUTION_COUNTERS_PATH", "var/lead_distribution_counters.json"))
_counters_lock = asyncio.Lock()


def _today_msk() -> str:
    return datetime.datetime.now(_MSK).strftime("%Y-%m-%d")


def _load_counters_state() -> dict:
    """{"date": "YYYY-MM-DD", "counts": {user_id: {source_id: n}}, "assignments": {lead_id: {...}},
    "routed_ids": [lead_id, ...]}. Дата в файле отличается от сегодняшней → пустое
    состояние (вчерашние счётчики просто перестают использоваться, явного сброса
    не нужно) - routed_ids (см. _is_locally_routed/_mark_locally_routed) едет тем
    же рейсом, суточного окна с огромным запасом хватает на саму задачу (пережить
    read-after-write задержку amoCRM, которая разрешается за секунды)."""
    today = _today_msk()
    empty = {"date": today, "counts": {}, "assignments": {}, "routed_ids": []}
    if not COUNTERS_PATH.exists():
        return empty
    try:
        state = json.loads(COUNTERS_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.exception("lead_distribution: не удалось прочитать %s", COUNTERS_PATH)
        return empty
    if state.get("date") != today:
        return empty
    return state


def _save_counters_state(state: dict) -> None:
    _atomic_write_json(COUNTERS_PATH, state)


def _source_count(state: dict, user_id: int, source_id: int) -> int:
    return int(((state.get("counts") or {}).get(str(user_id)) or {}).get(str(source_id), 0))


def _total_count(state: dict, user_id: int) -> int:
    return sum(((state.get("counts") or {}).get(str(user_id)) or {}).values())


def _apply_assignment(state: dict, *, lead_id: int, source_id: int, user_id: int) -> None:
    counts = state.setdefault("counts", {})
    user_counts = counts.setdefault(str(user_id), {})
    user_counts[str(source_id)] = user_counts.get(str(source_id), 0) + 1
    state.setdefault("assignments", {})[str(lead_id)] = {
        "source_id": source_id, "user_id": user_id, "date": state["date"],
    }


async def _open_deals_state(profile: Profile) -> dict:
    """Альтернатива _load_counters_state() для profile.use_open_deals_counter:
    вместо «сколько мы раздали сегодня» (локальный JSON, сбрасывается по дате) —
    «сколько у участника СЕЙЧАС открытых (вне 142/143) сделок по источнику»,
    живым запросом к amoCRM. Считаем по всем воронкам профиля (entry_points) и
    суммируем — сама сделка попадает в диспетчер по этапу входа, но счётчик не
    ограничен этим этапом (задание Тианы 12.08.2026: «этап мы указываем для
    того, чтобы распределение выполнялось тогда, когда сделка в нём создаётся/
    переходит», не для ограничения подсчёта).

    Форма результата — {"counts": {user_id: {source_id: n}}} — та же, что у
    _load_counters_state(), без "date"/"assignments" (незачем: _apply_assignment
    сюда не пишет, см. decide_and_record — источник правды amoCRM, не наш файл).
    Совместима 1:1 с _source_count/_total_count/_decide_load_balanced без
    единой правки в них."""
    pipeline_ids = sorted({ep["pipeline_id"] for ep in profile.entry_points})
    merged: dict[int, dict[int, int]] = {}
    for pipeline_id in pipeline_ids:
        per_source = await amo_service.get_open_deal_counts_by_source(profile.participant_ids, pipeline_id)
        for uid, sources in per_source.items():
            bucket = merged.setdefault(uid, {})
            for sid, n in sources.items():
                bucket[sid] = bucket.get(sid, 0) + n
    return {"counts": {str(uid): {str(sid): n for sid, n in sources.items()} for uid, sources in merged.items()}}


def debug_state() -> dict:
    return _load_counters_state()


async def correct_reassignment(lead_id: int, new_user_id: int) -> None:
    """Ручная смена ответственного на сделке, распределённой этим модулем
    СЕГОДНЯ: снять счётчик у прежнего получателя, добавить новому. Сделки не
    из сегодняшнего журнала назначений игнорируются (не наша сегодняшняя
    выдача — вчерашние счётчики уже неактуальны)."""
    async with _counters_lock:
        state = _load_counters_state()
        record = (state.get("assignments") or {}).get(str(lead_id))
        if not record or record.get("date") != state["date"]:
            return
        old_user_id = record.get("user_id")
        source_id = record.get("source_id")
        if old_user_id == new_user_id:
            return
        counts = state.setdefault("counts", {})
        if source_id is not None:
            old_counts = counts.get(str(old_user_id)) or {}
            if str(source_id) in old_counts:
                old_counts[str(source_id)] = max(0, old_counts[str(source_id)] - 1)
            new_counts = counts.setdefault(str(new_user_id), {})
            new_counts[str(source_id)] = new_counts.get(str(source_id), 0) + 1
        state["assignments"][str(lead_id)] = {**record, "user_id": new_user_id}
        _save_counters_state(state)
        logger.info(
            "lead_distribution: коррекция счётчиков сделка=%s источник=%s %s→%s",
            lead_id, source_id, old_user_id, new_user_id,
        )


def correct_reassignment_bg(lead_id, new_user_id) -> None:
    if lead_id is None or new_user_id is None:
        return
    try:
        lead_id_i, new_user_id_i = int(lead_id), int(new_user_id)
    except (TypeError, ValueError):
        return
    _spawn(correct_reassignment(lead_id_i, new_user_id_i))


# ════════════════ рабочие часы ════════════════

def _in_hour_window(now_hour: int, window: tuple[int, int]) -> bool:
    start, end = window
    return start <= now_hour < end


def _is_on_shift(user_id: int) -> bool:
    """team-panel — источник правды графика сотрудника (team_panel_client.py,
    батч-опрос раз в TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S, свежий кэш в
    памяти). Кэш пуст/протух/team-panel выключен фичей-флагом — откат на
    прежний плейсхолдер: единое окно на всех, чтобы сбой ДРУГОГО сервиса не
    останавливал распределение лидов."""
    cached = team_panel_client.get_cached(user_id)
    if cached is not None:
        return cached
    now_hour = datetime.datetime.now(_MSK).hour
    return _in_hour_window(now_hour, LEAD_DISTRIBUTION_DEFAULT_WINDOW)


def _work_day_ended(profile: Profile) -> bool:
    """Рабочий день профиля ЗАКОНЧИЛСЯ на сегодня (сейчас позже конца последнего
    интервала work_hours). Один из двух триггеров ухода на _upcoming_pool в
    decide_and_record - см. его комментарий там: сам по себе этот флаг больше
    НЕ единственное условие (до 17.08.2026 было им, из-за чего пустой пул ДО
    начала первого интервала/в перерыве молча простаивал до конца дня)."""
    if not profile.work_hours:
        return False
    now_hhmm = datetime.datetime.now(_MSK).strftime("%H:%M")
    return now_hhmm > max(iv["end"] for iv in profile.work_hours)


def eligible_pool(profile: Profile) -> list[int]:
    return [uid for uid in profile.participant_ids if _is_on_shift(uid)]


def _next_work_moment(profile: Profile, now: datetime.datetime) -> datetime.datetime:
    """Ближайший момент старта интервала work_hours СТРОГО в будущем: сперва
    среди сегодняшних интервалов (следующий старт после now - покрывает и
    «до первого интервала», и «перерыв между интервалами»), а если сегодня
    стартов больше нет (все уже прошли/день закончился) - старт САМОГО РАННЕГО
    интервала завтра. Одна функция вместо старого "всегда завтра" - именно это
    убирает искусственное ожидание до 10:00, когда прямо сейчас никого нет на
    месте, а следующий интервал начинается позже сегодня же."""
    today = now.date()
    starts_today = []
    for iv in profile.work_hours:
        h, m = (int(x) for x in iv["start"].split(":"))
        starts_today.append(datetime.datetime.combine(today, datetime.time(h, m), tzinfo=_MSK))
    upcoming_today = [t for t in starts_today if t > now]
    if upcoming_today:
        return min(upcoming_today)
    tomorrow = today + datetime.timedelta(days=1)
    start_hhmm = min(iv["start"] for iv in profile.work_hours)
    start_h, start_m = (int(x) for x in start_hhmm.split(":"))
    return datetime.datetime.combine(tomorrow, datetime.time(start_h, start_m), tzinfo=_MSK)


async def _upcoming_pool(profile: Profile) -> list[int]:
    """Участники профиля, которые будут на месте в БЛИЖАЙШИЙ будущий момент
    начала интервала work_hours (_next_work_moment) - используется вместо
    eligible_pool, когда прямо сейчас пул пуст или рабочий день на сегодня уже
    закончился (см. decide_and_record). team-panel проверяет присутствие НА
    ЭТОТ МОМЕНТ, а не «весь интервал», поэтому сотрудник с более поздним
    началом смены внутри того же интервала теоретически может быть пропущен -
    известное ограничение, не критично для типичного графика."""
    at = _next_work_moment(profile, datetime.datetime.now(_MSK))
    statuses = await team_panel_client.fetch_for_datetime(set(profile.participant_ids), at)
    return [uid for uid in profile.participant_ids if statuses.get(uid)]


# ════════════════ алгоритм выбора ответственного ════════════════

def _tie_break(candidates: list[int]) -> int:
    return random.choice(candidates)


async def _find_repeat_responsible(lead: dict, *, meta: dict | None = None) -> int | None:
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    lead_id = int(lead["id"])
    captured_contact = False
    for c in contacts:
        cid = c.get("id")
        if cid is None:
            continue
        full = await amo_service.get_contact_by_id(cid, with_=("leads",))
        if not full:
            continue
        if meta is not None and not captured_contact:
            # Только у ПЕРВОГО прочитанного контакта - тот же contact_id, что
            # process_lead_distribution кладёt в лог (contacts[0].get("id")).
            # Дальше по циклу может пойти поиск по остальным контактам сделки
            # (редкий случай нескольких контактов) - их имя/телефон в лог не путаем.
            # Объект уже получен для поиска repeat-ответственного - второй
            # API-вызов за именем/телефоном не нужен.
            meta["contact_name"] = (full.get("name") or "").strip() or None
            meta["contact_phone"] = amo_service.get_custom_field_value(full, FIELD_PHONE)
            captured_contact = True
        found = await amo_service.find_other_deal_responsible(full, exclude_lead_id=lead_id)
        if found is not None:
            return found
    return None


def _weight(profile: Profile, uid: int) -> int:
    return max(1, profile.participant_weights.get(uid, 1))


def _decide_load_balanced(
    state: dict, profile: Profile, pool: list[int],
    repeat_responsible: int | None, source_id: int | None,
    is_available=None,
) -> int | None:
    """Сравнения ведутся не по сырым счётчикам, а по отношению count/вес
    (Fraction, для точных сравнений без ошибок округления) — при весе 1 у всех
    (профиль без единого заданного веса, дефолт) ratio == сырой count, решения
    побайтово те же, что раньше (см. normalize_participant_weights: профиль
    без весов сериализуется в {} именно ради этой гарантии). gap остаётся
    порогом в этом же ratio-пространстве — при весе 1 у всех это тот же самый
    порог "разница не больше N сделок", что и был; при разных весах — порог
    "разница не больше N сделок НА ЕДИНИЦУ веса".

    `is_available` — проверка "прежний ответственный вообще доступен". По
    умолчанию _is_on_shift (на смене СЕЙЧАС), но вызывающий обязан передать
    свою, когда пул построен не на "сейчас", а на ближайшую будущую смену:
    _is_on_shift за пределами рабочих часов всегда False, и приоритет
    повторного клиента молча пропадал (18.08.2026 — тот же баг, что уже чинили
    для режима always через _repeat_on_shift, ветку load тогда пропустили)."""
    gap = LEAD_DISTRIBUTION_FAIRNESS_GAP
    if is_available is None:
        is_available = _is_on_shift

    def source_ratio(uid: int) -> Fraction:
        return Fraction(_source_count(state, uid, source_id), _weight(profile, uid))

    def total_ratio(uid: int) -> Fraction:
        return Fraction(_total_count(state, uid), _weight(profile, uid))

    if repeat_responsible is not None and is_available(repeat_responsible) and source_id is not None:
        r_ratio = source_ratio(repeat_responsible)
        others = [uid for uid in pool if uid != repeat_responsible]
        if not others or all(abs(r_ratio - source_ratio(uid)) <= gap for uid in others):
            return repeat_responsible
        # иначе R остаётся рядовым кандидатом пула — просто без приоритета,
        # продолжаем обычный подбор ниже.

    if not pool:
        return profile.duty_user_id
    if source_id is None:
        # На сделке не определился источник (нетипичный случай) — алгоритм
        # «по нагрузке» неприменим без source_id, решает вызывающий (round-robin).
        return None

    min_source = min(source_ratio(uid) for uid in pool)
    candidates_x = [uid for uid in pool if source_ratio(uid) == min_source]

    totals = {uid: total_ratio(uid) for uid in pool}
    min_total = min(totals.values())
    x_at_min_total = [uid for uid in candidates_x if totals[uid] == min_total]
    if x_at_min_total:
        return _tie_break(x_at_min_total)

    best_x_total = min(totals[uid] for uid in candidates_x)
    if best_x_total - min_total <= gap:
        tied = [uid for uid in candidates_x if totals[uid] == best_x_total]
        return _tie_break(tied)
    tied = [uid for uid in pool if totals[uid] == min_total]
    return _tie_break(tied)


async def decide_and_record(lead: dict, profile: Profile, *, meta: dict | None = None) -> int | None:
    """Выбирает ответственного и (если выбран) сразу инкрементирует счётчики —
    под одной блокировкой на всё решение+запись, чтобы конкурентные вызовы
    (вебхук и reconciliation могут пересечься) не выдали одному человеку два
    решения на основе одного и того же устаревшего снимка счётчиков.

    `meta` — опциональный out-параметр (см. process_lead_distribution): если
    передан dict, туда кладётся meta["rule"] — каким путём выбран target
    ("load"/"random"/"always"/"duty_fallback"/"tomorrow_shift_fallback"),
    а также meta["repeat_responsible_user_id"] (последний ответственный по
    ДРУГИМ сделкам контакта, см. _find_repeat_responsible) и
    meta["contact_name"]/meta["contact_phone"] - для лога распределений.

    Пул на "сейчас" (eligible_pool) используется, только если он НЕ пуст И
    рабочий день профиля ещё не закончился формально (_work_day_ended) - иначе
    (пул пуст ПРЯМО СЕЙЧАС - до первого интервала, в перерыве, или день уже
    закончился) уходим на _upcoming_pool: ближайший будущий старт интервала,
    сегодня или завтра. До 17.08.2026 форвардинг срабатывал только по второму
    условию - пустой пул до начала первого интервала/в перерыве молча ждал
    наступления часов, вместо того чтобы сразу посмотреть на ближайшую смену."""
    if profile.work_hours:
        today_pool = eligible_pool(profile)
        if today_pool and not _work_day_ended(profile):
            pool, pool_is_future = today_pool, False
        else:
            pool = await _upcoming_pool(profile)
            pool_is_future = True
    else:
        pool = eligible_pool(profile)
        pool_is_future = False
    repeat_responsible = await _find_repeat_responsible(lead, meta=meta)
    source_id = _lead_source_id(lead)

    # Приоритет повторного клиента применим ТОЛЬКО если прежний ответственный —
    # участник ЭТОГО профиля (найдено вживую 20.08.2026: _find_repeat_responsible
    # берёт ответственного последней сделки контакта откуда угодно - в т.ч. уже
    # закрытой, из другой воронки, от офис-менеджера после office_transfer. Без
    # этой проверки такой человек, если просто "на месте" по графику, уводил
    # НОВУЮ сделку мимо пула продавцов профиля целиком). meta ниже всё равно
    # хранит настоящего repeat_responsible - для лога/аналитики он информативен
    # независимо от того, даёт ли приоритет.
    repeat_priority_candidate = (
        repeat_responsible if repeat_responsible in profile.participant_ids else None
    )

    def _repeat_on_shift(uid: int) -> bool:
        # Под forward-пулом "на месте" значит "участвует в пуле на ближайшую
        # будущую смену" - _is_on_shift (сейчас) для него не годится.
        return uid in pool if pool_is_future else _is_on_shift(uid)

    # use_open_deals_counter - живой запрос к amoCRM ДО блокировки: _counters_lock
    # общий на ВСЕ профили (не только этот), держать его на время сетевого похода
    # сериализовало бы решения по другим, никак не связанным профилям.
    open_deals_state = (
        await _open_deals_state(profile)
        if profile.use_open_deals_counter and profile.repeat_contact_mode == "load"
        else None
    )

    async with _counters_lock:
        state = open_deals_state if open_deals_state is not None else _load_counters_state()
        rule = ""

        if profile.repeat_contact_mode == "always":
            if repeat_priority_candidate is not None:
                target = repeat_priority_candidate if _repeat_on_shift(repeat_priority_candidate) else None
                rule = "always"
            elif pool:
                target = await _next_in_rotation(profile.id, pool)
                rule = "random"
            else:
                target = profile.duty_user_id
                rule = "duty_fallback"
        elif profile.repeat_contact_mode == "random":
            if pool:
                target = await _next_in_rotation(profile.id, pool)
                rule = "random"
            else:
                target = profile.duty_user_id
                rule = "duty_fallback"
        else:  # "load"
            target = _decide_load_balanced(
                state, profile, pool, repeat_priority_candidate, source_id, is_available=_repeat_on_shift,
            )
            # _decide_load_balanced сам возвращает profile.duty_user_id, когда pool
            # пуст (единственный путь, где это отличимо от «настоящего» load-подбора).
            rule = "duty_fallback" if (not pool and target == profile.duty_user_id) else "load"
            if target is None and source_id is None and profile.repeat_contact_mode == "load":
                if pool:
                    target = await _next_in_rotation(profile.id, pool)
                    rule = "random"
                else:
                    target = profile.duty_user_id
                    rule = "duty_fallback"

        if pool_is_future and target is not None and rule != "duty_fallback":
            rule = "tomorrow_shift_fallback"

        if target is not None and source_id is not None and open_deals_state is None:
            # open_deals_state: писать некуда и незачем - источник правды amoCRM,
            # следующее решение просто перезапросит его живьём (уже с учётом
            # PATCH, который process_lead_distribution сделает следом).
            _apply_assignment(state, lead_id=int(lead["id"]), source_id=source_id, user_id=target)
            _save_counters_state(state)
        if meta is not None:
            meta["rule"] = rule
            meta["repeat_responsible_user_id"] = repeat_responsible
        return target


# ════════════════ диспетчер ════════════════

_bg_tasks: set = set()
_contact_wait_pending: set[int] = set()
_pending_fail: dict[int, dict] = {}

# Защита от двойного распределения одного лида (найдено вживую 19-20.08.2026):
# amgroup дозаполняет поля сразу после создания сделки несколькими вебхуками
# подряд - каждый пере-ставит её в очередь lead_distribution.
#
# 25.08.2026, решение Тианы: новые сделки визуальный тег «распределено
# автоматически» больше не получают (старые, уже помеченные, тег сохраняют -
# см. проверку ниже). Раньше именно ЭТОТ тег был единственной идемпотентностью,
# и она была ненадёжной: между PATCH одного прогона и свежим GET следующего
# amoCRM могла ещё не отдавать только что записанный тег (read-after-write
# задержка на их стороне) - второй прогон читал сделку "как будто ещё не
# распределена" и решал заново, задваивая счётчик нагрузки (реальный кейс:
# сделка 36538301, 20.08.2026 - засчиталась дважды за 1-9 секунд). Без тега
# вообще эта проблема была бы постоянной, а не редкой гонкой.
#
# Замена - routed_ids внутри lead_distribution_counters.json (не отдельный
# файл): проверяется РАНЬШЕ тега и не зависит от amoCRM на чтение вообще -
# свой же процесс видит свою же запись мгновенно и без сети, сама причина
# гонки исчезает, а не просто сужается до окна. Тег для СТАРЫХ сделок
# по-прежнему проверяется отдельно (см. process_lead_distribution) -
# идемпотентность для них не теряется при переходе на новый механизм.
#
# 25.08.2026, по вопросу Тианы: отдельный вечно растущий файл был бы плохой
# идеей (каждая запись перезаписывала бы ВЕСЬ список целиком - линейный рост
# стоимости записи без потолка). Настоящая задача - пережить гонку
# read-after-write, которая разрешается за секунды; суточного окна с огромным
# запасом достаточно. lead_distribution_counters.json и так сбрасывается
# каждую полночь МСК (_load_counters_state) - routed_ids просто едет тем же
# рейсом, тот же _counters_lock, тот же атомарный write, никакого нового
# механизма не потребовалось.
#
# _in_flight - вторая, отдельная защита: блокирует ПЕРЕСЕКАЮЩИЕСЯ по времени
# прогоны одного lead_id (держится на весь цикл решение→PATCH→запись, не
# только на decide_and_record - см. коммит 20.08.2026, сделка 36538563).
_in_flight: set[int] = set()


async def _is_locally_routed(lead_id: int) -> bool:
    async with _counters_lock:
        state = _load_counters_state()
        return lead_id in (state.get("routed_ids") or [])


async def _mark_locally_routed(lead_id: int) -> None:
    async with _counters_lock:
        state = _load_counters_state()
        routed = state.setdefault("routed_ids", [])
        if lead_id not in routed:
            routed.append(lead_id)
        _save_counters_state(state)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _clear_fail(lead_id: int) -> None:
    _pending_fail.pop(int(lead_id), None)


async def _stale_alert(lead: dict, state: dict) -> None:
    if LEAD_DISTRIBUTION_STALE_ALERT_MIN <= 0 or state["alerted"]:
        return
    age_min = (time.time() - state["since"]) / 60
    if age_min < LEAD_DISTRIBUTION_STALE_ALERT_MIN:
        return
    state["alerted"] = True
    lead_id = lead.get("id")
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    d = alerts.decide(
        "lead_not_distributed",
        legacy_text=(
            f"🚨 Сделка {lead_id} не распределяется дольше {int(age_min)} мин — нужна ручная проверка.\n"
            f"{lead.get('name') or ''}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}"
        ),
        chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID, lead=lead,
        values={
            "сколько_ждали": f"{int(age_min)} мин",
            "сделка": lead.get("name") or "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())


async def _fail(lead: dict, reason: str) -> None:
    """Распределение не удалось: тег + примечание + (по истечении порога) один
    Telegram-алерт. НЕ блокирует повторные попытки reconciliation."""
    lead_id = int(lead.get("id"))
    logger.warning("lead_distribution %s: %s", lead_id, reason)
    state = _pending_fail.setdefault(lead_id, {"since": time.time(), "alerted": False})
    await amo_service.add_tag(lead_id, TAG_LEAD_DISTRIBUTION_ERROR)
    await amo_service.add_note(
        lead_id,
        f"⚠️ Автораспределение не выполнено: {reason}. Повторные попытки продолжаются автоматически.",
    )
    await _stale_alert(lead, state)


async def process_lead_distribution(lead_id, source: str = "webhook") -> str:
    """Обработчик очереди (LANE_AMO) / reconciliation / хвоста активного
    ожидания контакта. Всегда дочитывает сделку заново."""
    if not LEAD_DISTRIBUTION_ENABLED:
        return "disabled"

    lead = await amo_service.get_lead_full(lead_id, with_=("source", "tags", "contacts"))
    if not lead:
        logger.warning("lead_distribution %s: сделка не прочиталась", lead_id)
        return "failed-lead-read"

    # Нормализуем сразу - lead_id приходит то int (reconcile), то str (очередь,
    # из payload вебхука), а _in_flight/_routed_ids ниже ключуются им же:
    # разнотипица тихо сломала бы дедуп (int(1) и "1" - разные ключи/элементы).
    lid = int(lead["id"])

    # Ответственный ДО этого решения - PATCH ниже меняет его только на стороне
    # amoCRM, локальный lead-dict не мутирует, так что читать можно и позже,
    # но фиксируем сразу для ясности (для лога распределений).
    prev_responsible_user_id = lead.get("responsible_user_id")

    pipeline_id = int(lead.get("pipeline_id") or 0)
    status_id = int(lead.get("status_id") or 0)
    source_id = _lead_source_id(lead)
    profile = match_profile(pipeline_id, status_id, source_id)
    if profile is None:
        return "no-profile"

    if _is_office_delivery(lead):
        # Самовывоз из офиса — ведёт офис-менеджер напрямую, не пул профиля
        # (решение Тианы 19.08.2026). Без тега и без записи в лог распределений:
        # сделка вообще не считается вошедшей в диспетчер.
        return "skipped-office-delivery"

    if amo_service.has_tag(lead, TAG_LEAD_DISTRIBUTION_ROUTED) or await _is_locally_routed(lid):
        # Идемпотентно: решение уже зафиксировано - тег (старые сделки, им
        # его ещё ставили) или локальная запись (новые - тег больше не
        # ставим, см. _routed_ids выше). Повторный вебхук (в т.ч. эхо от
        # нашего же PATCH) — no-op.
        _clear_fail(lid)
        return "skipped-already-routed"

    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    if not contacts:
        # Гонка с amgroup: контакт ещё не привязан — решение не принимается
        # вообще, ротация/счётчики не расходуются. См. _contact_wait_loop.
        _spawn_contact_wait(lid, source)
        return "waiting-for-contact"

    if lid in _in_flight:
        # Пересекающийся по времени повторный заход (другой вебхук того же
        # шквала, или фоновое _contact_wait_loop, уже внутри этого блока
        # прямо сейчас) - не решаем параллельно один и тот же лид дважды.
        return "skipped-in-flight"
    _in_flight.add(lid)
    try:
        meta: dict = {}
        target = await decide_and_record(lead, profile, meta=meta)
        if target is None:
            # Пул пуст, дежурного нет — ждём reconciliation (не ошибка).
            return "no-candidate-waiting"

        # PATCH остаётся ПОД _in_flight (найдено вживую 20.08.2026, сделка
        # 36538563: снятие _in_flight сразу после decide_and_record и ДО этого
        # await оставляло незащищённую щель - параллельный вызов (обычно из
        # _contact_wait_loop, независимая задача, не через очередь) успевал
        # пройти обе проверки и тоже посчитать решение, пока этот PATCH ещё
        # летит по сети).
        #
        # tags не передаём вообще (25.08.2026) - новым сделкам тег «распределено
        # автоматически» больше не ставим, только назначаем ответственного;
        # amo_service.patch_lead без tags вообще не трогает _embedded.tags в
        # запросе, существующие теги сделки (напр. «тест», «Горячий») не задеты.
        result = await amo_service.patch_lead(lid, responsible_user_id=target)
        if not result.get("ok"):
            await _fail(lead, f"PATCH не прошёл (status_code={result.get('status_code')})")
            return "failed-patch"

        await _mark_locally_routed(lid)
    finally:
        _in_flight.discard(lid)

    _clear_fail(lid)
    logger.info(
        "lead_distribution %s: профиль=%s → пользователь=%s (source=%s, правило=%s)",
        lead_id, profile.id, target, source, meta.get("rule"),
    )
    contact_id = contacts[0].get("id") if contacts else None
    detail_blob = {k: v for k, v in {
        "repeat_responsible_user_id": meta.get("repeat_responsible_user_id"),
        "prev_responsible_user_id": prev_responsible_user_id,
    }.items() if v is not None}
    _spawn(lead_distribution_log_client.send({
        "lead_id": lead_id,
        "contact_id": contact_id,
        "assigned_user_id": target,
        "profile_id": profile.id,
        "profile_name": profile.name,
        "source_id": source_id,
        "rule": meta.get("rule", ""),
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "contact_name": meta.get("contact_name"),
        "contact_phone": meta.get("contact_phone"),
        "detail": detail_blob or None,
    }))
    return "routed"


# ════════════════ защита от гонки amgroup: активное ожидание контакта ════════════════

def _spawn_contact_wait(lead_id: int, source: str) -> None:
    if lead_id in _contact_wait_pending:
        return
    _contact_wait_pending.add(lead_id)
    _spawn(_contact_wait_loop(lead_id, source))


async def _contact_wait_loop(lead_id: int, source: str) -> None:
    """Гейт «контакт привязан» сработал: НЕ блокирующее ожидание (эта задача
    независима от воркера очереди — process_lead_distribution уже вернулся).
    Короткие проверки с интервалом LEAD_DISTRIBUTION_CONTACT_POLL_S до бюджета
    LEAD_DISTRIBUTION_CONTACT_WAIT_S. Не появился — нештатная гонка с amgroup,
    не тихое ожидание: тег+примечание+алерт (_fail), reconciliation остаётся
    финальной страховкой поверх этого."""
    try:
        deadline = time.monotonic() + LEAD_DISTRIBUTION_CONTACT_WAIT_S
        while time.monotonic() < deadline:
            await asyncio.sleep(LEAD_DISTRIBUTION_CONTACT_POLL_S)
            lead = await amo_service.get_lead_full(lead_id, with_=("source", "tags", "contacts"))
            if lead and (lead.get("_embedded") or {}).get("contacts"):
                await process_lead_distribution(lead_id, source=source)
                return
        lead = await amo_service.get_lead_full(lead_id, with_=("source", "tags", "contacts"))
        if lead:
            await _fail(
                lead,
                f"контакт не привязан за {LEAD_DISTRIBUTION_CONTACT_WAIT_S}с ожидания (гонка amgroup?)",
            )
        else:
            logger.warning(
                "lead_distribution %s: контакт не появился, и сделка не прочиталась на таймауте", lead_id,
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("lead_distribution %s: ошибка активного ожидания контакта", lead_id)
    finally:
        _contact_wait_pending.discard(lead_id)


# ════════════════ reconciliation (окно по времени, НЕ «текущий статус») ════════════════

async def _entered_status_leads(pipeline_id: int, status_id: int, ts_from: int, ts_to: int) -> set[int]:
    """ID сделок, ПЕРЕШЕДШИХ в pipeline_id/status_id за окно [ts_from, ts_to) —
    по событию lead_status_changed. Порт office_transfer._entered_status_leads."""
    leads: set[int] = set()
    page = 1
    while True:
        params = [
            ("filter[type]", "lead_status_changed"),
            ("filter[created_at][from]", str(ts_from)),
            ("filter[created_at][to]", str(ts_to)),
            ("filter[value_after][leads_statuses][0][pipeline_id]", str(pipeline_id)),
            ("filter[value_after][leads_statuses][0][status_id]", str(status_id)),
            ("limit", "100"), ("page", str(page)),
        ]
        d = await amo_service._do_get("/api/v4/events", params)
        evs = ((d or {}).get("_embedded") or {}).get("events") or []
        for e in evs:
            va = e.get("value_after") or []
            ls = (va[0].get("lead_status") if va else None) or {}
            if ls.get("id") == status_id and ls.get("pipeline_id") == pipeline_id:
                lid = e.get("entity_id")
                if lid is not None:
                    leads.add(int(lid))
        if len(evs) < 100:
            break
        page += 1
    return leads


async def _created_in_status_leads(pipeline_id: int, status_id: int, ts_from: int, ts_to: int) -> set[int]:
    """ID сделок, СОЗДАННЫХ и ВСЁ ЕЩЁ находящихся в pipeline_id/status_id, чей created_at
    попадает в окно [ts_from, ts_to) — дополняет _entered_status_leads.

    Обнаружено вживую 09.08.2026: сделка, созданная СРАЗУ в точке входа (а не
    перешедшая туда из другого этапа — типичный случай для входных этапов вроде
    «Неразобранное»/«Первичный контакт»), не даёт события lead_status_changed —
    amoCRM шлёт lead_added, а его value_after всегда пуст (проверено на живом
    аккаунте), так что по событиям её вообще нечем поймать. Вебхук (быстрый
    путь) эту сделку ловит нормально (webhooks.py читает leads[add][0][status_id]
    точно так же, как leads[update][0][status_id]) — а вот reconciliation как
    страховка от ПРОПУЩЕННОГО вебхука эту сделку раньше не подстраховывал.
    Идемпотентность (тег для старых сделок / _routed_ids для новых, см.
    process_lead_distribution) делает пересечение с _entered_status_leads
    безопасным — задвоенная обработка просто no-op."""
    leads: set[int] = set()
    page = 1
    while True:
        params = [
            ("filter[statuses][0][pipeline_id]", str(pipeline_id)),
            ("filter[statuses][0][status_id]", str(status_id)),
            ("filter[created_at][from]", str(ts_from)),
            ("filter[created_at][to]", str(ts_to)),
            ("limit", "100"), ("page", str(page)),
        ]
        d = await amo_service._do_get("/api/v4/leads", params)
        batch = ((d or {}).get("_embedded") or {}).get("leads") or []
        for lead in batch:
            lid = lead.get("id")
            if lid is not None:
                leads.add(int(lid))
        if len(batch) < 100:
            break
        page += 1
    return leads


_last_reconcile_ts: int = 0
_reconcile_task: asyncio.Task | None = None


async def _reconcile_once() -> str:
    global _last_reconcile_ts
    now = int(time.time())
    window_from = max(_last_reconcile_ts, LEAD_DISTRIBUTION_SINCE_TS)
    if window_from <= 0:
        logger.warning("lead_distribution reconcile: LEAD_DISTRIBUTION_SINCE_TS не задан — проход пропущен")
        return "skipped-no-cutover"

    entry_points = {pair for p in _profiles().values() if p.enabled for pair in _entry_pairs(p)}
    leads: set[int] = set()
    for pipeline_id, status_id in entry_points:
        leads |= await _entered_status_leads(pipeline_id, status_id, window_from, now)
        leads |= await _created_in_status_leads(pipeline_id, status_id, window_from, now)

    processed = 0
    for lead_id in leads:
        await process_lead_distribution(lead_id, source="reconcile")
        processed += 1
    _last_reconcile_ts = now
    logger.info(
        "lead_distribution reconcile: окно [%s, %s), точек входа %s, сделок %s",
        window_from, now, len(entry_points), processed,
    )
    return f"processed={processed}"


async def _reconcile_loop() -> None:
    while True:
        await asyncio.sleep(LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S)
        try:
            await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("lead_distribution reconcile: ошибка прохода")


# ════════════════ жизненный цикл ════════════════

async def _alert(text: str) -> None:
    try:
        await telegram_bot.send_alert(text)
    except Exception:
        logger.exception("lead_distribution alert failed: %s", text)


async def init() -> None:
    """Вызывается из lifespan. Запускает reconciliation, если фича включена и
    cutover задан."""
    global _last_reconcile_ts
    if not LEAD_DISTRIBUTION_ENABLED:
        logger.info("lead_distribution: ВЫКЛЮЧЕН (LEAD_DISTRIBUTION_ENABLED)")
        return

    if LEAD_DISTRIBUTION_SINCE_TS <= 0:
        msg = (
            "lead_distribution: LEAD_DISTRIBUTION_ENABLED=1, но LEAD_DISTRIBUTION_SINCE_TS не задан — "
            "reconciliation НЕ запущена (иначе задело бы сделки, висевшие в точках входа до включения). "
            "Вебхук-путь при этом работает."
        )
        logger.error(msg)
        await _alert(msg)
        return

    _last_reconcile_ts = LEAD_DISTRIBUTION_SINCE_TS
    start_reconcile()


def start_reconcile() -> None:
    global _reconcile_task
    if LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S <= 0:
        logger.info("lead_distribution: reconciliation выключена (LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S=0)")
        return
    _reconcile_task = asyncio.create_task(_reconcile_loop())
    logger.info(
        "lead_distribution: reconciliation каждые %s сек, cutover=%s",
        LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S, LEAD_DISTRIBUTION_SINCE_TS,
    )


async def stop_reconcile() -> None:
    global _reconcile_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        try:
            await _reconcile_task
        except asyncio.CancelledError:
            pass
        _reconcile_task = None
    # Досверить фоновые ожидания контакта, пока API-пайплайн ещё жив (тот же
    # принцип, что unmiss_tag.shutdown) — иначе на деплое во время ожидания
    # сделка осталась бы без решения и без алерта до следующего reconciliation.
    pending = [t for t in _bg_tasks if not t.done()]
    if pending:
        done, still_pending = await asyncio.wait(pending, timeout=15)
        if still_pending:
            logger.warning("lead_distribution: %d фоновых задач не успели на shutdown", len(still_pending))
            for t in still_pending:
                t.cancel()
