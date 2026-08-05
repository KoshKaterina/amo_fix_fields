"""Юнит-тесты lead_distribution (без сети/прода — amo_service/telegram_bot фейки,
файлы состояния — во временных путях, изолированных per-test).

Запуск: python test_lead_distribution.py   (свой мини-раннер, как test_wazzup_sla.py)
        или python -m pytest test_lead_distribution.py -q
"""
import asyncio
import datetime
import pathlib
import tempfile
import time

import amo_service
import lead_distribution as ld
import telegram_bot


def run(coro):
    return asyncio.run(coro)


_TMP = pathlib.Path(tempfile.mkdtemp(prefix="ld_test_"))


def setup_function(_=None):
    # Изолированные файлы состояния на каждый тест.
    ld.PROFILES_PATH = _TMP / f"profiles_{time.monotonic_ns()}.json"
    ld.COUNTERS_PATH = _TMP / f"counters_{time.monotonic_ns()}.json"
    ld.ROTATION_PATH = _TMP / f"rotation_{time.monotonic_ns()}.json"
    ld.invalidate_cache()
    ld._pending_fail.clear()
    ld._contact_wait_pending.clear()
    ld._bg_tasks.clear()
    ld.LEAD_DISTRIBUTION_ENABLED = True
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (0, 24)  # все всегда «на месте» по умолчанию
    ld.LEAD_DISTRIBUTION_FAIRNESS_GAP = 2
    ld.LEAD_DISTRIBUTION_CONTACT_WAIT_S = 10
    ld.LEAD_DISTRIBUTION_CONTACT_POLL_S = 2
    ld.LEAD_DISTRIBUTION_STALE_ALERT_MIN = 30
    ld.LEAD_DISTRIBUTION_SINCE_TS = 0
    ld.LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S = 120
    ld._last_reconcile_ts = 0


# ── билдеры ──────────────────────────────────────────────────────────────

def _profile_data(**over):
    data = {
        "name": "Тест",
        "pipeline_id": 10593102,
        "status_id": 83537714,
        "source_ids": [],
        "participant_ids": [1, 2, 3],
        "duty_user_id": None,
        "repeat_contact_mode": "load",
        "enabled": True,
    }
    data.update(over)
    return data


def _lead(*, lead_id=100, pipeline_id=10593102, status_id=83537714, source_id=None,
          tags=None, contacts=None, responsible_user_id=None):
    embedded = {}
    if source_id is not None:
        embedded["source"] = {"id": source_id, "name": f"source-{source_id}"}
    embedded["tags"] = tags or []
    embedded["contacts"] = contacts if contacts is not None else [{"id": 500}]
    return {
        "id": lead_id, "pipeline_id": pipeline_id, "status_id": status_id,
        "name": "Тестовая сделка", "responsible_user_id": responsible_user_id,
        "_embedded": embedded,
    }


def _contact(contact_id=500, other_leads=None):
    return {"id": contact_id, "_embedded": {"leads": other_leads or []}}


def _seed_counts(user_source_counts: dict) -> None:
    """user_source_counts: {user_id: {source_id: count}}"""
    state = {"date": ld._today_msk(), "counts": {}, "assignments": {}}
    for uid, per_source in user_source_counts.items():
        state["counts"][str(uid)] = {str(sid): n for sid, n in per_source.items()}
    ld._save_counters_state(state)


# ── фейки amo_service / telegram_bot ────────────────────────────────────

_patch_calls: list = []
_note_calls: list = []
_tag_calls: list = []
_alert_calls: list = []
_lead_by_id: dict = {}
_contact_by_id: dict = {}


def _reset_fakes():
    _patch_calls.clear()
    _note_calls.clear()
    _tag_calls.clear()
    _alert_calls.clear()
    _lead_by_id.clear()
    _contact_by_id.clear()


async def _fake_get_lead_full(lead_id, with_=()):
    return _lead_by_id.get(int(lead_id))


async def _fake_get_contact_by_id(contact_id, with_=()):
    return _contact_by_id.get(int(contact_id))


async def _fake_patch_lead(lead_id, **kwargs):
    _patch_calls.append({"lead_id": lead_id, **kwargs})
    return {"ok": True, "status_code": 200, "retryable": False}


async def _fake_add_note(lead_id, text):
    _note_calls.append((lead_id, text))
    return {"ok": True}


async def _fake_add_tag(lead_id, tag_name):
    _tag_calls.append((lead_id, tag_name))
    return {"ok": True}


async def _fake_send_alert(text, **kw):
    _alert_calls.append(text)
    return True


amo_service.get_lead_full = _fake_get_lead_full
amo_service.get_contact_by_id = _fake_get_contact_by_id
amo_service.patch_lead = _fake_patch_lead
amo_service.add_note = _fake_add_note
amo_service.add_tag = _fake_add_tag
telegram_bot.send_alert = _fake_send_alert


# ════════════════ CRUD профилей ════════════════

def test_create_get_list_profile():
    _reset_fakes()
    p = ld.create_profile(_profile_data(name="Заказ с сайта"))
    assert p.name == "Заказ с сайта"
    assert p.id
    assert ld.get_profile(p.id).name == "Заказ с сайта"
    assert [x.id for x in ld.list_profiles()] == [p.id]


def test_create_profile_validation_errors():
    try:
        ld.create_profile(_profile_data(name=""))
        assert False, "должен упасть без name"
    except ld.ProfileValidationError:
        pass
    try:
        ld.create_profile(_profile_data(participant_ids=[]))
        assert False, "должен упасть без участников"
    except ld.ProfileValidationError:
        pass
    try:
        ld.create_profile(_profile_data(repeat_contact_mode="иногда"))
        assert False, "должен упасть на неизвестном режиме"
    except ld.ProfileValidationError:
        pass
    try:
        ld.create_profile(_profile_data(work_hours={"from": 30, "to": 19}))
        assert False, "должен упасть на часе вне 0..23"
    except ld.ProfileValidationError:
        pass


def test_update_and_delete_profile_applies_immediately():
    p = ld.create_profile(_profile_data(name="Квизы", enabled=False))
    assert ld.get_profile(p.id).enabled is False
    updated = ld.update_profile(p.id, {"enabled": True, "duty_user_id": 9})
    assert updated.enabled is True
    assert updated.duty_user_id == 9
    assert ld.get_profile(p.id).enabled is True  # без явного invalidate — CRUD сам обновляет кэш

    assert ld.delete_profile(p.id) is True
    assert ld.get_profile(p.id) is None
    assert ld.delete_profile(p.id) is False


# ════════════════ уникальность source_id между профилями ════════════════

def test_specific_source_conflict_is_global_across_entry_points():
    ld.create_profile(_profile_data(name="A", status_id=1, source_ids=[111]))
    try:
        ld.create_profile(_profile_data(name="B", status_id=2, source_ids=[111, 222]))
        assert False, "источник 111 уже занят — должен быть конфликт"
    except ld.ProfileConflictError as exc:
        assert exc.conflicting_source_ids == {111}


def test_wildcard_conflicts_only_on_same_entry_point():
    ld.create_profile(_profile_data(name="A", pipeline_id=1, status_id=1, source_ids=[]))
    # тот же вход, любой другой профиль — конфликт (в т.ч. с конкретным источником)
    try:
        ld.create_profile(_profile_data(name="B", pipeline_id=1, status_id=1, source_ids=[7]))
        assert False, "wildcard-профиль резервирует всю точку входа"
    except ld.ProfileConflictError:
        pass
    # другой вход — конфликта нет
    b = ld.create_profile(_profile_data(name="C", pipeline_id=1, status_id=2, source_ids=[7]))
    assert b.id


def test_update_profile_conflict_excludes_self():
    p = ld.create_profile(_profile_data(name="A", status_id=1, source_ids=[111]))
    # обновление профиля своими же источниками — не конфликт с самим собой
    updated = ld.update_profile(p.id, {"source_ids": [111, 222]})
    assert set(updated.source_ids) == {111, 222}


def test_update_profile_conflict_with_other_profile():
    ld.create_profile(_profile_data(name="A", status_id=1, source_ids=[111]))
    b = ld.create_profile(_profile_data(name="B", status_id=1, source_ids=[222]))
    try:
        ld.update_profile(b.id, {"source_ids": [111]})
        assert False
    except ld.ProfileConflictError:
        pass


# ════════════════ матчинг профиля ════════════════

def test_match_profile_by_entry_point_and_source():
    ld.create_profile(_profile_data(name="Квизы", pipeline_id=10, status_id=20, source_ids=[555]))
    assert ld.match_profile(10, 20, 555) is not None
    assert ld.match_profile(10, 20, 999) is None  # чужой источник
    assert ld.match_profile(10, 21, 555) is None  # чужой этап


def test_match_profile_wildcard_matches_any_source():
    ld.create_profile(_profile_data(name="Любой", pipeline_id=10, status_id=20, source_ids=[]))
    assert ld.match_profile(10, 20, 12345) is not None
    assert ld.match_profile(10, 20, None) is not None


def test_match_profile_multiple_matches_picks_lowest_priority():
    # Прямая манипуляция кэшем — обходит валидацию уникальности намеренно,
    # чтобы проверить защитный путь диспетчера (priority), а не саму валидацию.
    a = ld.create_profile(_profile_data(name="A", status_id=1, source_ids=[1], priority=50))
    profiles = dict(ld._profiles())
    b = ld.Profile(id="dup", name="B", enabled=True, priority=10,
                    pipeline_id=10593102, status_id=1, source_ids=[1],
                    participant_ids=[1])
    profiles["dup"] = b
    ld._profiles_cache = profiles
    picked = ld.match_profile(10593102, 1, 1)
    assert picked.id == "dup"  # меньший priority выигрывает


def test_disabled_profile_does_not_match():
    ld.create_profile(_profile_data(name="Выкл", status_id=1, enabled=False))
    assert ld.match_profile(10593102, 1, None) is None


# ════════════════ repeat_contact_mode: random ════════════════

def test_random_mode_round_robin_ignores_history():
    p = ld.create_profile(_profile_data(name="Random", repeat_contact_mode="random",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=None, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    picks = [run(ld.decide_and_record(lead, p)) for _ in range(4)]
    assert picks == [1, 2, 3, 1]  # игнорирует прошлого ответственного (1), просто по кругу


# ════════════════ repeat_contact_mode: always ════════════════

def test_always_mode_gives_to_previous_responsible_unconditionally():
    p = ld.create_profile(_profile_data(name="Always", repeat_contact_mode="always",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 2, "updated_at": 1}])
    # Егор (2) сильно перегружен по этому источнику — always это не волнует
    _seed_counts({2: {42: 50}, 1: {42: 0}, 3: {42: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 2


def test_always_mode_waits_when_previous_responsible_off_shift():
    p = ld.create_profile(_profile_data(name="Always", repeat_contact_mode="always",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 2, "updated_at": 1}])
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # никто не «на месте», в т.ч. R
    target = run(ld.decide_and_record(lead, p))
    assert target is None  # ждём его, не отдаём другому


def test_always_mode_new_client_falls_back_to_round_robin():
    p = ld.create_profile(_profile_data(name="Always", repeat_contact_mode="always",
                                         participant_ids=[1, 2]))
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])  # новый клиент
    target = run(ld.decide_and_record(lead, p))
    assert target in (1, 2)


# ════════════════ repeat_contact_mode: load ════════════════

def test_load_mode_gives_to_repeat_responsible_within_fairness_gap():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    _seed_counts({1: {42: 3}, 2: {42: 4}, 3: {42: 2}})  # разница ≤ 2 со всеми
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


def test_load_mode_skips_repeat_responsible_beyond_fairness_gap():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    # 1 обогнал 3-го на источнике 42 больше, чем на 2 — приоритет снимается
    _seed_counts({1: {42: 5}, 2: {42: 5}, 3: {42: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 3  # наименьший по источнику И по общему


def test_load_mode_x_with_min_total_wins():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])  # новый клиент — без приоритета R
    # X по источнику 42 — только пользователь 3 (0), и он же с наименьшим общим (0)
    _seed_counts({1: {42: 2, 7: 10}, 2: {42: 2, 7: 10}, 3: {42: 0, 7: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 3


def test_load_mode_small_total_gap_still_gives_to_x():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    # X = 1 (наим. по источнику 42), но общий минимум у 2. Разница общих ≤ 2 → всё равно X.
    _seed_counts({1: {42: 0, 9: 2}, 2: {42: 5, 9: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


def test_load_mode_large_total_gap_overrides_x():
    """Уточнение Тианы: если разрыв общих счётчиков X-а с минимальным общим
    превышает LEAD_DISTRIBUTION_FAIRNESS_GAP — отдаём НЕ X, а человеку с
    наименьшим общим счётчиком."""
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2]))
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    # X = 1 (наим. по источнику 42 — 0), но общий у 1 = 10, у 2 = 0 → разрыв 10 > 2
    _seed_counts({1: {42: 0, 9: 10}, 2: {42: 5, 9: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 2  # не X (1), а минимальный общий (2)


def test_load_mode_no_source_id_falls_back_to_round_robin():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2]))
    lead = _lead(source_id=None, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target in (1, 2)


def test_counters_persist_and_reset_by_date():
    p = ld.create_profile(_profile_data(name="Load", repeat_contact_mode="load",
                                         participant_ids=[1, 2, 3]))
    lead = _lead(lead_id=1, source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    _seed_counts({1: {42: 0}, 2: {42: 0}, 3: {42: 0}})
    target = run(ld.decide_and_record(lead, p))
    state = ld._load_counters_state()
    assert ld._source_count(state, target, 42) == 1

    # смена даты в файле → счётчик как будто не существовал
    stale = ld._load_counters_state()
    stale["date"] = "2000-01-01"
    ld._save_counters_state(stale)
    fresh = ld._load_counters_state()
    assert fresh["counts"] == {}


# ════════════════ дежурный / пустой пул ════════════════

def test_duty_user_fallback_when_pool_empty():
    p = ld.create_profile(_profile_data(name="Duty", participant_ids=[1, 2], duty_user_id=9))
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # весь пул вне окна
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 9


def test_no_candidate_waiting_without_duty():
    p = ld.create_profile(_profile_data(name="NoDuty", participant_ids=[1, 2], duty_user_id=None))
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target is None


# ════════════════ work_hours профиля ════════════════

def test_profile_work_hours_gate_skips_outside_window():
    _reset_fakes()
    now_h = datetime.datetime.now(ld._MSK).hour
    p = ld.create_profile(_profile_data(name="WH", work_hours={"from": now_h, "to": now_h}))
    lead = _lead(lead_id=200, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[200] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(200))
    assert outcome == "skipped-outside-work-hours"
    assert not _patch_calls


def test_profile_without_work_hours_always_runs():
    _reset_fakes()
    p = ld.create_profile(_profile_data(name="NoWH", work_hours=None))
    lead = _lead(lead_id=201, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[201] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(201))
    assert outcome == "routed"
    assert _patch_calls and _patch_calls[0]["lead_id"] == 201


# ════════════════ идемпотентность ════════════════

def test_already_routed_tag_is_noop():
    _reset_fakes()
    p = ld.create_profile(_profile_data(name="Idemp"))
    lead = _lead(lead_id=300, source_id=1, tags=[{"name": ld.TAG_LEAD_DISTRIBUTION_ROUTED}])
    _lead_by_id[300] = lead
    outcome = run(ld.process_lead_distribution(300))
    assert outcome == "skipped-already-routed"
    assert not _patch_calls


def test_no_matching_profile_is_noop():
    _reset_fakes()
    lead = _lead(lead_id=301, source_id=1, pipeline_id=1, status_id=1)
    _lead_by_id[301] = lead
    outcome = run(ld.process_lead_distribution(301))
    assert outcome == "no-profile"
    assert not _patch_calls


# ════════════════ защита от гонки amgroup ════════════════

def test_contact_gate_spawns_wait_and_no_decision_without_contact():
    _reset_fakes()
    p = ld.create_profile(_profile_data(name="Race"))
    lead = _lead(lead_id=400, source_id=1, contacts=[])
    _lead_by_id[400] = lead
    outcome = run(ld.process_lead_distribution(400))
    assert outcome == "waiting-for-contact"
    assert not _patch_calls
    ld._bg_tasks.clear()  # не ждём фон в этом тесте — только сам факт неприменения решения


def test_contact_wait_loop_succeeds_when_contact_appears():
    _reset_fakes()
    p = ld.create_profile(_profile_data(name="Race"))
    ld.LEAD_DISTRIBUTION_CONTACT_WAIT_S = 1
    ld.LEAD_DISTRIBUTION_CONTACT_POLL_S = 0.01
    no_contact = _lead(lead_id=401, source_id=1, contacts=[])
    with_contact = _lead(lead_id=401, source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    calls = {"n": 0}

    async def _seq(lead_id, with_=()):
        calls["n"] += 1
        return with_contact if calls["n"] >= 2 else no_contact

    amo_service.get_lead_full = _seq
    try:
        run(ld._contact_wait_loop(401, "webhook"))
    finally:
        amo_service.get_lead_full = _fake_get_lead_full
    assert _patch_calls and _patch_calls[0]["lead_id"] == 401
    assert not _tag_calls  # успех — тега ошибки быть не должно


def test_contact_wait_loop_times_out_and_alerts():
    _reset_fakes()
    ld.create_profile(_profile_data(name="Race"))
    ld.LEAD_DISTRIBUTION_CONTACT_WAIT_S = 0.05
    ld.LEAD_DISTRIBUTION_CONTACT_POLL_S = 0.01
    ld.LEAD_DISTRIBUTION_STALE_ALERT_MIN = 1
    # Порог считается от МОМЕНТА ПЕРВОЙ неудачи (_pending_fail["since"]), не от
    # текущего вызова — «протухшая» сделка backdate'ится, как будто уже не первый раз.
    ld._pending_fail[402] = {"since": time.time() - 3600, "alerted": False}
    lead = _lead(lead_id=402, source_id=1, contacts=[])
    _lead_by_id[402] = lead
    run(ld._contact_wait_loop(402, "webhook"))
    assert not _patch_calls
    assert ("402", ld.TAG_LEAD_DISTRIBUTION_ERROR) in [(str(l), t) for l, t in _tag_calls]
    assert _note_calls
    assert _alert_calls


# ════════════════ коррекция счётчиков при ручной смене ответственного ════════════════

def test_correct_reassignment_moves_counters_same_day():
    state = {"date": ld._today_msk(), "counts": {"1": {"42": 3}}, "assignments": {}}
    ld._apply_assignment(state, lead_id=900, source_id=42, user_id=1)
    ld._save_counters_state(state)
    run(ld.correct_reassignment(900, 2))
    final = ld._load_counters_state()
    assert ld._source_count(final, 1, 42) == 3  # было 4 (3 базовых + 1 от apply), стало 3
    assert ld._source_count(final, 2, 42) == 1
    assert final["assignments"]["900"]["user_id"] == 2


def test_correct_reassignment_ignores_lead_not_assigned_today():
    _seed_counts({1: {42: 5}})
    run(ld.correct_reassignment(999999, 2))  # нет в journal — no-op
    final = ld._load_counters_state()
    assert ld._source_count(final, 1, 42) == 5
    assert ld._source_count(final, 2, 42) == 0


def test_correct_reassignment_ignores_stale_date_record():
    state = {"date": "2000-01-01", "counts": {"1": {"42": 5}},
             "assignments": {"901": {"source_id": 42, "user_id": 1, "date": "2000-01-01"}}}
    ld._save_counters_state(state)  # текущая дата в файле «2000-01-01» → load_counters_state сам увидит несовпадение
    run(ld.correct_reassignment(901, 2))
    final = ld._load_counters_state()
    assert final["counts"] == {}  # состояние было признано устаревшим целиком, поправка не применилась


# ════════════════ reconciliation ════════════════

def test_reconcile_skipped_without_cutover():
    ld.LEAD_DISTRIBUTION_SINCE_TS = 0
    ld._last_reconcile_ts = 0
    outcome = run(ld._reconcile_once())
    assert outcome == "skipped-no-cutover"


def test_reconcile_collects_entry_points_from_enabled_profiles_only():
    _reset_fakes()
    ld.create_profile(_profile_data(name="On", pipeline_id=10, status_id=20, enabled=True))
    ld.create_profile(_profile_data(name="Off", pipeline_id=30, status_id=40, enabled=False))
    ld.LEAD_DISTRIBUTION_SINCE_TS = int(time.time()) - 3600
    ld._last_reconcile_ts = 0

    seen_pairs = []

    async def _fake_do_get(path, params=None):
        params = dict(params or [])
        pid = int(params.get("filter[value_after][leads_statuses][0][pipeline_id]", 0))
        sid = int(params.get("filter[value_after][leads_statuses][0][status_id]", 0))
        seen_pairs.append((pid, sid))
        return {"_embedded": {"events": []}}

    amo_service._do_get = _fake_do_get
    try:
        run(ld._reconcile_once())
    finally:
        pass
    assert (10, 20) in seen_pairs
    assert (30, 40) not in seen_pairs


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    ok = 0
    for fn in fns:
        setup_function()
        try:
            fn()
            print(f"OK {fn.__name__}")
            ok += 1
        except Exception:
            print(f"FAIL {fn.__name__}")
            traceback.print_exc()
    print(f"\n{ok}/{len(fns)} прошли")
