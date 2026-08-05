"""Распределение лидов — замена нативного виджета «Генезис»/F5 (05.08.2026).

Конструктор профилей: администратор создаёт/редактирует/удаляет профили через
API (lead_distribution_api.py), в этом файле нет захардкоженных правил — они
данные, не код (в отличие от office_transfer.py).

Профиль: точка входа (pipeline_id/status_id) + фильтр источников (source_ids,
[] = любой) + участники + режим учёта повторных клиентов (always/load/random)
+ опциональные рабочие часы профиля. Один source_id не может быть в двух
профилях одновременно (валидация в create_profile/update_profile) — это же
свойство гарантирует, что счётчики нагрузки, которые ведутся ПО ИСТОЧНИКУ (не
по профилю), однозначны: у каждого источника один профиль-владелец.

Защита от гонки с amgroup (создаёт сделку и асинхронно привязывает контакт):
если у свежепрочитанной сделки ещё нет контакта, диспетчер не принимает
решение вообще, а запускает фоновую задачу активного ожидания (короткие
проверки, не блокирующие воркер очереди LANE_AMO) — см. _contact_wait_loop.

Надёжность — тот же приём, что в office_transfer.py: вебхук (быстрый путь) +
периодическая reconciliation по окну времени через /api/v4/events (без
ретроактивности) как страховка от сбоев API.
"""

import asyncio
import dataclasses
import datetime
import json
import logging
import os
import pathlib
import random
import time
import uuid
from typing import Any

import amo_service
import team_panel_client
import telegram_bot
import tg_recipients
from waybill_config import (
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


# ════════════════ модель профиля ════════════════

@dataclasses.dataclass
class Profile:
    id: str
    name: str
    enabled: bool = False
    priority: int = 100
    pipeline_id: int = 0
    status_id: int = 0
    source_ids: list[int] = dataclasses.field(default_factory=list)
    participant_ids: list[int] = dataclasses.field(default_factory=list)
    duty_user_id: int | None = None
    repeat_contact_mode: str = "load"
    work_hours: dict[str, int] | None = None
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
            pipeline_id=int(d.get("pipeline_id", 0)),
            status_id=int(d.get("status_id", 0)),
            source_ids=[int(x) for x in d.get("source_ids") or []],
            participant_ids=[int(x) for x in d.get("participant_ids") or []],
            duty_user_id=int(d["duty_user_id"]) if d.get("duty_user_id") is not None else None,
            repeat_contact_mode=d.get("repeat_contact_mode", "load"),
            work_hours=d.get("work_hours"),
            created_at=int(d.get("created_at", 0)),
            updated_at=int(d.get("updated_at", 0)),
        )


class ProfileValidationError(Exception):
    pass


class ProfileConflictError(Exception):
    def __init__(self, message: str, conflicting_profile_id: str, conflicting_source_ids: set[int]):
        super().__init__(message)
        self.conflicting_profile_id = conflicting_profile_id
        self.conflicting_source_ids = conflicting_source_ids


# ════════════════ хранилище профилей (var/, атомарная запись) ════════════════

PROFILES_PATH = pathlib.Path(os.getenv("LEAD_DISTRIBUTION_PROFILES_PATH", "var/lead_distribution_profiles.json"))

_profiles_cache: dict[str, Profile] | None = None


def _atomic_write_json(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _load_profiles_from_disk() -> dict[str, Profile]:
    if not PROFILES_PATH.exists():
        return {}
    try:
        raw = json.loads(PROFILES_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.exception("lead_distribution: не удалось прочитать %s — считаем пустым", PROFILES_PATH)
        return {}
    out: dict[str, Profile] = {}
    for pid, pdata in (raw.get("profiles") or {}).items():
        try:
            out[pid] = Profile.from_dict(pdata)
        except Exception:
            logger.exception("lead_distribution: битая запись профиля %s — пропущена", pid)
    return out


def _save_profiles_to_disk(profiles: dict[str, Profile]) -> None:
    _atomic_write_json(PROFILES_PATH, {"profiles": {pid: p.to_dict() for pid, p in profiles.items()}})


def _profiles() -> dict[str, Profile]:
    global _profiles_cache
    if _profiles_cache is None:
        _profiles_cache = _load_profiles_from_disk()
    return _profiles_cache


def invalidate_cache() -> None:
    global _profiles_cache
    _profiles_cache = None


def list_profiles() -> list[Profile]:
    return list(_profiles().values())


def get_profile(profile_id: str) -> Profile | None:
    return _profiles().get(profile_id)


def _validate_fields(data: dict) -> None:
    if not str(data.get("name") or "").strip():
        raise ProfileValidationError("name обязателен")
    if not data.get("pipeline_id"):
        raise ProfileValidationError("pipeline_id обязателен")
    if not data.get("status_id"):
        raise ProfileValidationError("status_id обязателен")
    if not data.get("participant_ids"):
        raise ProfileValidationError("participant_ids не может быть пустым")
    mode = data.get("repeat_contact_mode", "load")
    if mode not in _VALID_MODES:
        raise ProfileValidationError(f"repeat_contact_mode должен быть одним из {_VALID_MODES}")
    wh = data.get("work_hours")
    if wh is not None:
        if not isinstance(wh, dict) or "from" not in wh or "to" not in wh:
            raise ProfileValidationError("work_hours должен быть {'from': int, 'to': int} или null")
        try:
            f, t = int(wh["from"]), int(wh["to"])
        except (TypeError, ValueError):
            raise ProfileValidationError("work_hours.from/to должны быть целыми часами")
        if not (0 <= f <= 23 and 0 <= t <= 23):
            raise ProfileValidationError("work_hours.from/to должны быть в диапазоне 0..23")


def _find_source_conflict(
    candidate: Profile, existing: dict[str, Profile], *, exclude_id: str | None = None
) -> tuple[str, set[int]] | None:
    """Источник не может быть в двух профилях одновременно. Пустой source_ids
    (любой источник) конфликтует с ЛЮБЫМ другим профилем на ТОЙ ЖЕ точке входа
    (pipeline_id/status_id); конкретные source_id конфликтуют глобально —
    между любыми профилями, независимо от точки входа."""
    cand_set = set(candidate.source_ids)
    for pid, other in existing.items():
        if pid == exclude_id:
            continue
        other_set = set(other.source_ids)
        if cand_set and other_set:
            overlap = cand_set & other_set
            if overlap:
                return pid, overlap
        else:
            same_entry = (candidate.pipeline_id == other.pipeline_id
                          and candidate.status_id == other.status_id)
            if same_entry:
                return pid, (other_set or cand_set)
    return None


def create_profile(data: dict) -> Profile:
    _validate_fields(data)
    profiles = dict(_profiles())
    now = int(time.time())
    candidate = Profile(
        id=uuid.uuid4().hex[:12],
        name=str(data["name"]).strip(),
        enabled=bool(data.get("enabled", False)),
        priority=int(data.get("priority", 100)),
        pipeline_id=int(data["pipeline_id"]),
        status_id=int(data["status_id"]),
        source_ids=sorted({int(x) for x in data.get("source_ids") or []}),
        participant_ids=[int(x) for x in data["participant_ids"]],
        duty_user_id=int(data["duty_user_id"]) if data.get("duty_user_id") is not None else None,
        repeat_contact_mode=data.get("repeat_contact_mode", "load"),
        work_hours=data.get("work_hours"),
        created_at=now,
        updated_at=now,
    )
    conflict = _find_source_conflict(candidate, profiles)
    if conflict is not None:
        other_id, overlap = conflict
        raise ProfileConflictError(
            f"источник(и) {sorted(overlap) if overlap else 'любой'} уже заняты профилем {other_id}",
            other_id, overlap,
        )
    profiles[candidate.id] = candidate
    _save_profiles_to_disk(profiles)
    invalidate_cache()
    return candidate


def update_profile(profile_id: str, patch: dict) -> Profile:
    profiles = dict(_profiles())
    current = profiles.get(profile_id)
    if current is None:
        raise KeyError(profile_id)
    merged = current.to_dict()
    merged.update({k: v for k, v in patch.items() if k not in ("id", "created_at")})
    _validate_fields(merged)
    updated = Profile.from_dict(merged)
    updated.id = profile_id
    updated.created_at = current.created_at
    updated.updated_at = int(time.time())
    updated.source_ids = sorted(set(updated.source_ids))
    conflict = _find_source_conflict(updated, profiles, exclude_id=profile_id)
    if conflict is not None:
        other_id, overlap = conflict
        raise ProfileConflictError(
            f"источник(и) {sorted(overlap) if overlap else 'любой'} уже заняты профилем {other_id}",
            other_id, overlap,
        )
    profiles[profile_id] = updated
    _save_profiles_to_disk(profiles)
    invalidate_cache()
    return updated


def delete_profile(profile_id: str) -> bool:
    profiles = dict(_profiles())
    if profile_id not in profiles:
        return False
    del profiles[profile_id]
    _save_profiles_to_disk(profiles)
    invalidate_cache()
    return True


# ════════════════ матчинг профиля по свежей сделке ════════════════

def _lead_source_id(lead: dict) -> int | None:
    src = (lead.get("_embedded") or {}).get("source")
    if not src or src.get("id") is None:
        return None
    return int(src["id"])


def match_profile(pipeline_id: int, status_id: int, source_id: int | None) -> Profile | None:
    candidates = [
        p for p in _profiles().values()
        if p.enabled and p.pipeline_id == int(pipeline_id) and p.status_id == int(status_id)
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
    return any(
        p.enabled and p.pipeline_id == int(pipeline_id) and p.status_id == int(status_id)
        for p in _profiles().values()
    )


def has_matching_status(status_id: int) -> bool:
    """Как has_matching_enabled_profile, но без воронки — часть событий amo
    приходит без pipeline_id в теле вебхука (нормальное поведение)."""
    return any(p.enabled and p.status_id == int(status_id) for p in _profiles().values())


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
    """{"date": "YYYY-MM-DD", "counts": {user_id: {source_id: n}}, "assignments": {lead_id: {...}}}.
    Дата в файле отличается от сегодняшней → пустое состояние (вчерашние
    счётчики просто перестают использоваться, явного сброса не нужно)."""
    today = _today_msk()
    if not COUNTERS_PATH.exists():
        return {"date": today, "counts": {}, "assignments": {}}
    try:
        state = json.loads(COUNTERS_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.exception("lead_distribution: не удалось прочитать %s", COUNTERS_PATH)
        return {"date": today, "counts": {}, "assignments": {}}
    if state.get("date") != today:
        return {"date": today, "counts": {}, "assignments": {}}
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


def _profile_in_work_hours(profile: Profile) -> bool:
    if not profile.work_hours:
        return True
    now_hour = datetime.datetime.now(_MSK).hour
    return _in_hour_window(now_hour, (int(profile.work_hours["from"]), int(profile.work_hours["to"])))


def eligible_pool(profile: Profile) -> list[int]:
    return [uid for uid in profile.participant_ids if _is_on_shift(uid)]


# ════════════════ алгоритм выбора ответственного ════════════════

def _tie_break(candidates: list[int]) -> int:
    return random.choice(candidates)


async def _find_repeat_responsible(lead: dict) -> int | None:
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    lead_id = int(lead["id"])
    for c in contacts:
        cid = c.get("id")
        if cid is None:
            continue
        full = await amo_service.get_contact_by_id(cid, with_=("leads",))
        if not full:
            continue
        found = amo_service.find_other_deal_responsible(full, exclude_lead_id=lead_id)
        if found is not None:
            return found
    return None


def _decide_load_balanced(
    state: dict, profile: Profile, pool: list[int],
    repeat_responsible: int | None, source_id: int | None,
) -> int | None:
    gap = LEAD_DISTRIBUTION_FAIRNESS_GAP

    if repeat_responsible is not None and _is_on_shift(repeat_responsible) and source_id is not None:
        r_count = _source_count(state, repeat_responsible, source_id)
        others = [uid for uid in pool if uid != repeat_responsible]
        if not others or all(abs(r_count - _source_count(state, uid, source_id)) <= gap for uid in others):
            return repeat_responsible
        # иначе R остаётся рядовым кандидатом пула — просто без приоритета,
        # продолжаем обычный подбор ниже.

    if not pool:
        return profile.duty_user_id
    if source_id is None:
        # На сделке не определился источник (нетипичный случай) — алгоритм
        # «по нагрузке» неприменим без source_id, решает вызывающий (round-robin).
        return None

    min_source = min(_source_count(state, uid, source_id) for uid in pool)
    candidates_x = [uid for uid in pool if _source_count(state, uid, source_id) == min_source]

    totals = {uid: _total_count(state, uid) for uid in pool}
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


async def decide_and_record(lead: dict, profile: Profile) -> int | None:
    """Выбирает ответственного и (если выбран) сразу инкрементирует счётчики —
    под одной блокировкой на всё решение+запись, чтобы конкурентные вызовы
    (вебхук и reconciliation могут пересечься) не выдали одному человеку два
    решения на основе одного и того же устаревшего снимка счётчиков."""
    pool = eligible_pool(profile)
    repeat_responsible = await _find_repeat_responsible(lead)
    source_id = _lead_source_id(lead)

    async with _counters_lock:
        state = _load_counters_state()

        if profile.repeat_contact_mode == "always":
            if repeat_responsible is not None:
                target = repeat_responsible if _is_on_shift(repeat_responsible) else None
            else:
                target = await _next_in_rotation(profile.id, pool) if pool else profile.duty_user_id
        elif profile.repeat_contact_mode == "random":
            target = await _next_in_rotation(profile.id, pool) if pool else profile.duty_user_id
        else:  # "load"
            target = _decide_load_balanced(state, profile, pool, repeat_responsible, source_id)
            if target is None and source_id is None and profile.repeat_contact_mode == "load":
                target = await _next_in_rotation(profile.id, pool) if pool else profile.duty_user_id

        if target is not None and source_id is not None:
            _apply_assignment(state, lead_id=int(lead["id"]), source_id=source_id, user_id=target)
            _save_counters_state(state)
        return target


# ════════════════ диспетчер ════════════════

_bg_tasks: set = set()
_contact_wait_pending: set[int] = set()
_pending_fail: dict[int, dict] = {}


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
    await telegram_bot.send_alert(
        f"🚨 Сделка {lead_id} не распределяется дольше {int(age_min)} мин — нужна ручная проверка.\n"
        f"{lead.get('name') or ''}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID,
        message_thread_id=tg_recipients.NOTIFY_THREAD_ID,
    )


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

    pipeline_id = int(lead.get("pipeline_id") or 0)
    status_id = int(lead.get("status_id") or 0)
    source_id = _lead_source_id(lead)
    profile = match_profile(pipeline_id, status_id, source_id)
    if profile is None:
        return "no-profile"

    if amo_service.has_tag(lead, TAG_LEAD_DISTRIBUTION_ROUTED):
        # Идемпотентно: решение уже зафиксировано в amoCRM, повторный вебхук
        # (в т.ч. эхо от нашего же PATCH) — no-op.
        _clear_fail(int(lead["id"]))
        return "skipped-already-routed"

    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    if not contacts:
        # Гонка с amgroup: контакт ещё не привязан — решение не принимается
        # вообще, ротация/счётчики не расходуются. См. _contact_wait_loop.
        _spawn_contact_wait(int(lead["id"]), source)
        return "waiting-for-contact"

    if not _profile_in_work_hours(profile):
        return "skipped-outside-work-hours"

    target = await decide_and_record(lead, profile)
    if target is None:
        # Пул пуст, дежурного нет — ждём reconciliation (не ошибка).
        return "no-candidate-waiting"

    tags = list(amo_service.get_tags(lead)) + [{"name": TAG_LEAD_DISTRIBUTION_ROUTED}]
    result = await amo_service.patch_lead(lead_id, responsible_user_id=target, tags=tags)
    if not result.get("ok"):
        await _fail(lead, f"PATCH не прошёл (status_code={result.get('status_code')})")
        return "failed-patch"

    _clear_fail(int(lead["id"]))
    logger.info(
        "lead_distribution %s: профиль=%s → пользователь=%s (source=%s)",
        lead_id, profile.id, target, source,
    )
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


_last_reconcile_ts: int = 0
_reconcile_task: asyncio.Task | None = None


async def _reconcile_once() -> str:
    global _last_reconcile_ts
    now = int(time.time())
    window_from = max(_last_reconcile_ts, LEAD_DISTRIBUTION_SINCE_TS)
    if window_from <= 0:
        logger.warning("lead_distribution reconcile: LEAD_DISTRIBUTION_SINCE_TS не задан — проход пропущен")
        return "skipped-no-cutover"

    entry_points = {(p.pipeline_id, p.status_id) for p in _profiles().values() if p.enabled}
    leads: set[int] = set()
    for pipeline_id, status_id in entry_points:
        leads |= await _entered_status_leads(pipeline_id, status_id, window_from, now)

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
