"""Юнит-тесты lead_distribution (без сети/прода — amo_service/telegram_bot фейки,
файлы состояния — во временных путях, изолированных per-test).

09.08.2026: CRUD и валидация профилей (create/update/delete_profile,
ProfileValidationError/ProfileConflictError) переехали в team-panel
(app/lead_distribution/service.py + validation.py, свои тесты там). Здесь
lead_distribution — read-only потребитель: профили попадают в тесты через
_seed_profile(), который пишет прямо в кэш lead_distribution_profiles_client
(тот же кэш, что в проде наполняет опрос team-panel) — так тестируется именно
то, что сервис делает с уже полученными правилами, без повторной валидации.

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
import lead_distribution_log_client
import lead_distribution_profiles_client as ldpc
import team_panel_client
import telegram_bot
from waybill_config import FIELD_PHONE


def run(coro):
    return asyncio.run(coro)


async def _drain_bg_tasks():
    """Ждёт задачи, запущенные через ld._spawn (напр. отправка лога распределения) —
    они создаются в том же событийном цикле, что и вызывающий тест, поэтому ждать
    их нужно ДО того, как run()/asyncio.run() закроет цикл, иначе они просто
    не успеют выполниться (или будут отменены при закрытии цикла)."""
    pending = [t for t in ld._bg_tasks if not t.done()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _call_and_drain(coro):
    result = await coro
    await _drain_bg_tasks()
    return result


_TMP = pathlib.Path(tempfile.mkdtemp(prefix="ld_test_"))
_seed_counter = {"n": 0}


def setup_function(_=None):
    # Изолированные файлы состояния на каждый тест.
    ldpc.CACHE_PATH = _TMP / f"profiles_{time.monotonic_ns()}.json"
    ldpc._cache = {}
    ld.COUNTERS_PATH = _TMP / f"counters_{time.monotonic_ns()}.json"
    ld.ROTATION_PATH = _TMP / f"rotation_{time.monotonic_ns()}.json"
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
    # lead_distribution_log_client/team_panel_client - модули общие с
    # test_lead_distribution_log_client.py/test_team_panel_client.py (тестируют их
    # send()/fetch_for_datetime() напрямую, с настоящей httpx-подменой) - патчим/
    # восстанавливаем per-test, а не один раз на весь модуль, иначе те файлы
    # унаследуют наши заглушки, если тесты идут в одном процессе pytest.
    lead_distribution_log_client.send = _fake_log_send
    team_panel_client.fetch_for_datetime = _fake_fetch_for_datetime
    _tomorrow_statuses.clear()


def teardown_function(_=None):
    lead_distribution_log_client.send = _real_log_send
    team_panel_client.fetch_for_datetime = _real_fetch_for_datetime


# ── билдеры ──────────────────────────────────────────────────────────────

def _seed_profile(**over) -> ld.Profile:
    """Кладёт готовый профиль прямо в write-through кэш lead_distribution_profiles_client
    (в проде его наполняет опрос team-panel) — валидация/уникальность source_id теперь
    проверяются на стороне team-panel до попадания сюда, здесь их сознательно не
    перепроверяем. Принимает и старый плоский pipeline_id/status_id (конвертирует в
    entry_points) для совместимости с существующими вызовами в этом файле."""
    _seed_counter["n"] += 1
    data = {
        "name": "Тест",
        "enabled": True,
        "priority": 100,
        "entry_points": [{"pipeline_id": 10593102, "status_ids": [83537714]}],
        "source_ids": [],
        "participant_ids": [1, 2, 3],
        "duty_user_id": None,
        "repeat_contact_mode": "load",
        "work_hours": None,
    }
    if "pipeline_id" in over or "status_id" in over:
        pipeline_id = over.pop("pipeline_id", data["entry_points"][0]["pipeline_id"])
        status_id = over.pop("status_id", data["entry_points"][0]["status_ids"][0])
        data["entry_points"] = [{"pipeline_id": pipeline_id, "status_ids": [status_id]}]
    data.update(over)
    pid = data.pop("id", None) or f"p{_seed_counter['n']}"
    now = int(time.time())
    profile = {"id": pid, "created_at": now, "updated_at": now, **data}

    cache = dict(ldpc.get_profiles())
    cache[pid] = profile
    ldpc._cache = cache
    return ld.get_profile(pid)


def _lead(*, lead_id=100, pipeline_id=10593102, status_id=83537714, source_id=None,
          tags=None, contacts=None, responsible_user_id=None, delivery_type=None):
    embedded = {}
    if source_id is not None:
        embedded["source"] = {"id": source_id, "name": f"source-{source_id}"}
    embedded["tags"] = tags or []
    embedded["contacts"] = contacts if contacts is not None else [{"id": 500}]
    out = {
        "id": lead_id, "pipeline_id": pipeline_id, "status_id": status_id,
        "name": "Тестовая сделка", "responsible_user_id": responsible_user_id,
        "_embedded": embedded,
    }
    if delivery_type is not None:
        out["custom_fields_values"] = [{"field_id": ld.FIELD_DELIVERY_TYPE, "values": [{"value": delivery_type}]}]
    return out


def _contact(contact_id=500, other_leads=None, name=None, phone=None):
    """`other_leads` — ПОЛНЫЕ объекты сделок ({id, responsible_user_id, updated_at}),
    как их удобно описывать в тестах. Но в сам контакт кладутся ТОЛЬКО ссылки
    {"id", "_links"} — ровно то, что отдаёт живой amoCRM на /contacts/{id}?with=leads
    (проверено на боевом аккаунте 18.08.2026). Полные объекты уходят в реестр
    _leads_full, откуда их достаёт фейк get_leads_by_ids — так путь данных в
    тесте совпадает с продовым, и баг «responsible_user_id читается прямо из
    _embedded.leads» больше не может спрятаться за удобным фейком."""
    refs = []
    for l in other_leads or []:
        lid = int(l["id"])
        _leads_full[lid] = l
        refs.append({"id": lid, "_links": {"self": {"href": f"/api/v4/leads/{lid}"}}})
    c = {"id": contact_id, "_embedded": {"leads": refs}}
    if name is not None:
        c["name"] = name
    if phone is not None:
        c["custom_fields_values"] = [{"field_id": FIELD_PHONE, "values": [{"value": phone}]}]
    return c


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
# lead_id -> полный объект сделки; наполняется _contact(), читается фейком
# get_leads_by_ids (см. докстринг _contact)
_leads_full: dict = {}
_log_calls: list = []
_log_should_raise = False


def _reset_fakes():
    _patch_calls.clear()
    _note_calls.clear()
    _tag_calls.clear()
    _alert_calls.clear()
    _lead_by_id.clear()
    _contact_by_id.clear()
    _leads_full.clear()
    _log_calls.clear()
    _open_deal_counts.clear()
    _open_deal_calls.clear()
    global _log_should_raise
    _log_should_raise = False


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


async def _fake_log_send(payload):
    if _log_should_raise:
        raise RuntimeError("сбой отправки лога (тест)")
    _log_calls.append(payload)


_tomorrow_statuses: dict = {}  # user_id -> bool, наполняется тестами tomorrow-fallback


async def _fake_fetch_for_datetime(user_ids, at):
    return {uid: _tomorrow_statuses.get(uid, False) for uid in user_ids}


# use_open_deals_counter: pipeline_id -> {user_id: {source_id: n}} — «сейчас открытых
# сделок», наполняется тестами напрямую (никакой сети), в отличие от _load_counters_state
# это не runtime-состояние диспетчера, а имитация живого amoCRM на момент запроса.
_open_deal_counts: dict = {}
_open_deal_calls: list = []  # (tuple(user_ids), pipeline_id) — проверить, что зовётся 1 раз на воронку профиля


async def _fake_get_open_deal_counts_by_source(user_ids, pipeline_id):
    _open_deal_calls.append((tuple(user_ids), pipeline_id))
    by_pipeline = _open_deal_counts.get(pipeline_id, {})
    return {uid: dict(sources) for uid, sources in by_pipeline.items() if uid in user_ids}


async def _fake_get_leads_by_ids(lead_ids):
    """Дочитывание сделок батчем — единственный источник responsible_user_id/
    updated_at, ровно как в проде (в _embedded.leads контакта их нет)."""
    _leads_by_ids_calls.append(list(lead_ids))
    return [_leads_full[int(i)] for i in lead_ids if int(i) in _leads_full]


_leads_by_ids_calls: list = []

amo_service.get_lead_full = _fake_get_lead_full
amo_service.get_contact_by_id = _fake_get_contact_by_id
amo_service.get_leads_by_ids = _fake_get_leads_by_ids
amo_service.patch_lead = _fake_patch_lead
amo_service.add_note = _fake_add_note
amo_service.add_tag = _fake_add_tag
amo_service.get_open_deal_counts_by_source = _fake_get_open_deal_counts_by_source
telegram_bot.send_alert = _fake_send_alert
_real_log_send = lead_distribution_log_client.send
_real_fetch_for_datetime = team_panel_client.fetch_for_datetime


# ════════════════ чтение профилей из кэша ════════════════

def test_list_and_get_profile_from_cache():
    p = _seed_profile(name="Заказ с сайта")
    assert p.name == "Заказ с сайта"
    assert p.id
    assert ld.get_profile(p.id).name == "Заказ с сайта"
    assert [x.id for x in ld.list_profiles()] == [p.id]


def test_broken_cache_entry_is_skipped_not_fatal():
    _seed_profile(name="Ок")
    cache = dict(ldpc.get_profiles())
    # pipeline_id нечисловой — int() в _normalize_entry_points упадёт, запись пропускается
    cache["битый"] = {"id": "битый", "entry_points": [{"pipeline_id": "x", "status_ids": [1]}]}
    ldpc._cache = cache
    names = {p.name for p in ld.list_profiles()}
    assert names == {"Ок"}


def test_stale_cache_survives_team_panel_outage():
    """Если опрос team-panel не удался, get_profiles() продолжает отдавать
    последнее, что успело сохраниться (fetch_once не трогает кэш при сбое)."""
    p = _seed_profile(name="Держится")
    ok = run(ldpc.fetch_once())  # TEAM_PANEL_BASE_URL/TOKEN не настроены в тестах → сбой
    assert ok is False
    assert ld.get_profile(p.id) is not None


# ════════════════ матчинг профиля ════════════════

def test_match_profile_by_entry_point_and_source():
    _seed_profile(name="Квизы", pipeline_id=10, status_id=20, source_ids=[555])
    assert ld.match_profile(10, 20, 555) is not None
    assert ld.match_profile(10, 20, 999) is None  # чужой источник
    assert ld.match_profile(10, 21, 555) is None  # чужой этап


def test_match_profile_wildcard_matches_any_source():
    _seed_profile(name="Любой", pipeline_id=10, status_id=20, source_ids=[])
    assert ld.match_profile(10, 20, 12345) is not None
    assert ld.match_profile(10, 20, None) is not None


def test_match_profile_multiple_matches_picks_lowest_priority():
    # Два профиля на один и тот же вход намеренно (в проде такое отсекает
    # валидация team-panel) — проверяем защитный путь диспетчера (priority).
    _seed_profile(name="A", status_id=1, source_ids=[1], priority=50)
    _seed_profile(id="dup", name="B", status_id=1, source_ids=[1], priority=10)
    picked = ld.match_profile(10593102, 1, 1)
    assert picked.id == "dup"  # меньший priority выигрывает


def test_disabled_profile_does_not_match():
    _seed_profile(name="Выкл", status_id=1, enabled=False)
    assert ld.match_profile(10593102, 1, None) is None


# ════════════════ repeat_contact_mode: random ════════════════

def test_random_mode_round_robin_ignores_history():
    p = _seed_profile(name="Random", repeat_contact_mode="random", participant_ids=[1, 2, 3])
    lead = _lead(source_id=None, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    picks = [run(ld.decide_and_record(lead, p)) for _ in range(4)]
    assert picks == [1, 2, 3, 1]  # игнорирует прошлого ответственного (1), просто по кругу


# ════════════════ repeat_contact_mode: always ════════════════

def test_always_mode_gives_to_previous_responsible_unconditionally():
    p = _seed_profile(name="Always", repeat_contact_mode="always", participant_ids=[1, 2, 3])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 2, "updated_at": 1}])
    # Егор (2) сильно перегружен по этому источнику — always это не волнует
    _seed_counts({2: {42: 50}, 1: {42: 0}, 3: {42: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 2


def test_always_mode_waits_when_previous_responsible_off_shift():
    p = _seed_profile(name="Always", repeat_contact_mode="always", participant_ids=[1, 2, 3])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 2, "updated_at": 1}])
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # никто не «на месте», в т.ч. R
    target = run(ld.decide_and_record(lead, p))
    assert target is None  # ждём его, не отдаём другому


def test_always_mode_new_client_falls_back_to_round_robin():
    p = _seed_profile(name="Always", repeat_contact_mode="always", participant_ids=[1, 2])
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])  # новый клиент
    target = run(ld.decide_and_record(lead, p))
    assert target in (1, 2)


# ════════════════ repeat_contact_mode: load ════════════════

def test_load_mode_gives_to_repeat_responsible_within_fairness_gap():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2, 3])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    _seed_counts({1: {42: 3}, 2: {42: 4}, 3: {42: 2}})  # разница ≤ 2 со всеми
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


def test_load_mode_skips_repeat_responsible_beyond_fairness_gap():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2, 3])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    # 1 обогнал 3-го на источнике 42 больше, чем на 2 — приоритет снимается
    _seed_counts({1: {42: 5}, 2: {42: 5}, 3: {42: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 3  # наименьший по источнику И по общему


def test_load_mode_x_with_min_total_wins():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2, 3])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])  # новый клиент — без приоритета R
    # X по источнику 42 — только пользователь 3 (0), и он же с наименьшим общим (0)
    _seed_counts({1: {42: 2, 7: 10}, 2: {42: 2, 7: 10}, 3: {42: 0, 7: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 3


def test_load_mode_small_total_gap_still_gives_to_x():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2])
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
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2])
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    # X = 1 (наим. по источнику 42 — 0), но общий у 1 = 10, у 2 = 0 → разрыв 10 > 2
    _seed_counts({1: {42: 0, 9: 10}, 2: {42: 5, 9: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 2  # не X (1), а минимальный общий (2)


def test_load_mode_weighted_participant_wins_despite_higher_raw_count():
    """1 - вес 2, 2 - вес 1 (дефолт). У 1 сырой счётчик ВЫШЕ (3 против 2), но
    ratio = count/вес у него ниже (1.5 против 2) - без веса выбрали бы 2
    (меньше сырых), с весом - 1 (меньше по отношению к своему весу)."""
    p = _seed_profile(name="Weighted", repeat_contact_mode="load", participant_ids=[1, 2],
                       participant_weights={"1": 2})
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    _seed_counts({1: {42: 3}, 2: {42: 2}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


def test_load_mode_unconfigured_weights_match_unweighted_behavior():
    """Профиль без единого заданного веса - ratio == сырой count для всех,
    решение побайтово то же, что дал бы старый (безвесовой) алгоритм."""
    p = _seed_profile(name="NoWeights", repeat_contact_mode="load", participant_ids=[1, 2, 3])
    assert p.participant_weights == {}
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    _seed_counts({1: {42: 2, 7: 10}, 2: {42: 2, 7: 10}, 3: {42: 0, 7: 0}})
    target = run(ld.decide_and_record(lead, p))
    assert target == 3  # тот же результат, что test_load_mode_x_with_min_total_wins


def test_load_mode_malformed_weight_in_cache_is_clamped_not_fatal():
    """participant_weights приходит из write-through кэша (см. докстринг
    _normalize_participant_weights) - битые записи молча пропускаются, весь
    профиль не роняем."""
    p = _seed_profile(name="Malformed", participant_ids=[1, 2, 3],
                       participant_weights={"1": -5, "2": "не число", "3": 2})
    assert p.participant_weights == {3: 2}


def test_load_mode_no_source_id_falls_back_to_round_robin():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2])
    lead = _lead(source_id=None, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target in (1, 2)


def test_counters_persist_and_reset_by_date():
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2, 3])
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


# ════════════════ use_open_deals_counter (12.08.2026) ════════════════
# Полностью опциональный флаг "load"-режима: вместо "сколько мы раздали
# сегодня" (локальный JSON) — "сколько у сотрудника СЕЙЧАС открытых сделок по
# источнику" (живой запрос к amoCRM, здесь — _fake_get_open_deal_counts_by_source).


def test_open_deals_counter_gives_to_least_loaded():
    p = _seed_profile(name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2],
                       use_open_deals_counter=True)
    _open_deal_counts[10593102] = {1: {42: 7}, 2: {42: 4}}
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 2  # у 2 меньше открытых по этому источнику (4 против 7)


def test_open_deals_counter_ignores_daily_assigned_count():
    """Ключевое отличие от обычного load: даже если ПО ДНЕВНОМУ счётчику
    сотруднику уже отдали сегодня несколько сделок, решение смотрит только на
    открытые в amoCRM - дневной JSON для этого режима не читается вовсе."""
    p = _seed_profile(name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2],
                       use_open_deals_counter=True)
    _seed_counts({1: {42: 0}, 2: {42: 50}})  # по дневному счётчику 2 сильно перегружен
    _open_deal_counts[10593102] = {1: {42: 4}, 2: {42: 1}}  # а по открытым - наоборот
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 2  # решает открытая нагрузка (1 < 4), не дневная


def test_open_deals_counter_does_not_touch_daily_counters_file():
    p = _seed_profile(name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2],
                       use_open_deals_counter=True)
    _open_deal_counts[10593102] = {1: {42: 0}, 2: {42: 0}}
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    run(ld.decide_and_record(lead, p))
    state = ld._load_counters_state()
    assert state["counts"] == {}, "источник правды - amoCRM, дневной JSON не должен получить запись"


def test_open_deals_counter_sums_across_profile_pipelines():
    p = _seed_profile(
        name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2],
        entry_points=[
            {"pipeline_id": 10593102, "status_ids": [83537714]},
            {"pipeline_id": 999999, "status_ids": [888888]},
        ],
        use_open_deals_counter=True,
    )
    _open_deal_counts[10593102] = {1: {42: 2}, 2: {42: 1}}
    _open_deal_counts[999999] = {1: {42: 0}, 2: {42: 4}}
    # суммарно: 1 -> 2, 2 -> 5 - должны отдать 1
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 1
    assert {c[1] for c in _open_deal_calls} == {10593102, 999999}, "запрос должен уйти по каждой воронке профиля"


def test_open_deals_counter_respects_participant_weights():
    p = _seed_profile(name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2],
                       participant_weights={1: 2, 2: 1}, use_open_deals_counter=True)
    # Одинаковый сырой счётчик открытых (2 у обоих), но у 1 вес 2 - его ratio
    # count/вес = 2/2 = 1.0 против 2/1 = 2.0 у 2, т.е. с учётом веса 1 менее
    # загружен - та же Fraction-логика _decide_load_balanced, что у обычного load.
    _open_deal_counts[10593102] = {1: {42: 2}, 2: {42: 2}}
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


def test_open_deals_counter_off_by_default_uses_daily_counters():
    """use_open_deals_counter отсутствует в профиле -> дефолт False, поведение
    не меняется вообще (тот же путь, что был всегда)."""
    _reset_fakes()  # чистый _open_deal_calls - другие тесты этого файла его тоже наполняют
    p = _seed_profile(name="Load", repeat_contact_mode="load", participant_ids=[1, 2])
    assert p.use_open_deals_counter is False
    _open_deal_counts[10593102] = {1: {42: 0}, 2: {42: 99}}  # если бы читался - отдали бы 1
    _seed_counts({1: {42: 5}, 2: {42: 0}})
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 2  # решил дневной счётчик, open_deal_counts не тронут
    assert _open_deal_calls == []


def test_open_deals_counter_repeat_responsible_priority_still_applies():
    p = _seed_profile(name="OpenDeals", repeat_contact_mode="load", participant_ids=[1, 2, 3],
                       use_open_deals_counter=True)
    lead = _lead(source_id=42, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 1, "updated_at": 1}])
    # 1 (повторный) в пределах разрыва со всеми - должен получить приоритет,
    # несмотря на то, что 3 формально меньше загружен.
    _open_deal_counts[10593102] = {1: {42: 3}, 2: {42: 4}, 3: {42: 2}}
    target = run(ld.decide_and_record(lead, p))
    assert target == 1


# ════════════════ дежурный / пустой пул ════════════════

def test_duty_user_fallback_when_pool_empty():
    p = _seed_profile(name="Duty", participant_ids=[1, 2], duty_user_id=9)
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # весь пул вне окна
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target == 9


def test_no_candidate_waiting_without_duty():
    p = _seed_profile(name="NoDuty", participant_ids=[1, 2], duty_user_id=None)
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)
    lead = _lead(source_id=1, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[])
    target = run(ld.decide_and_record(lead, p))
    assert target is None


# ════════════════ work_hours профиля ════════════════

def _hhmm(d: datetime.datetime) -> str:
    return d.strftime("%H:%M")


def test_profile_before_first_interval_routes_via_today_pool():
    """До начала первого интервала - НЕ "рабочий день закончился" (это другое
    состояние, не должно триггерить tomorrow-fallback) - обычный today-пул."""
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [{"start": _hhmm(now + datetime.timedelta(hours=2)), "end": _hhmm(now + datetime.timedelta(hours=3))}]
    _seed_profile(name="BeforeStart", participant_ids=[1, 2], work_hours=work_hours)
    lead = _lead(lead_id=210, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[210] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(210))
    assert outcome == "routed"
    assert _log_calls[-1]["rule"] != "tomorrow_shift_fallback"


def test_profile_gap_between_intervals_routes_via_today_pool():
    """Между двумя интервалами (обеденный перерыв и т.п.) - тоже не "конец дня"
    (последний интервал ещё не кончился) - обычный today-пул, не завтра."""
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [
        {"start": _hhmm(now - datetime.timedelta(hours=2)), "end": _hhmm(now - datetime.timedelta(hours=1))},
        {"start": _hhmm(now + datetime.timedelta(hours=1)), "end": _hhmm(now + datetime.timedelta(hours=2))},
    ]
    _seed_profile(name="Gap", participant_ids=[1, 2], work_hours=work_hours)
    lead = _lead(lead_id=211, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[211] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(211))
    assert outcome == "routed"
    assert _log_calls[-1]["rule"] != "tomorrow_shift_fallback"


def test_profile_past_last_interval_end_routes_to_tomorrow_pool():
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [{"start": _hhmm(now - datetime.timedelta(hours=3)), "end": _hhmm(now - datetime.timedelta(hours=1))}]
    _seed_profile(name="DayEnded", participant_ids=[1, 2], duty_user_id=9, work_hours=work_hours)
    _tomorrow_statuses[2] = True  # только 2 работает завтра
    lead = _lead(lead_id=212, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[212] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(_call_and_drain(ld.process_lead_distribution(212)))
    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 2
    assert _log_calls[-1]["rule"] == "tomorrow_shift_fallback"


def test_profile_past_last_interval_end_tomorrow_pool_empty_falls_to_duty():
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [{"start": _hhmm(now - datetime.timedelta(hours=3)), "end": _hhmm(now - datetime.timedelta(hours=1))}]
    _seed_profile(name="DayEndedNoOne", participant_ids=[1, 2], duty_user_id=9, work_hours=work_hours)
    # _tomorrow_statuses пуст - никто не работает завтра
    lead = _lead(lead_id=213, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[213] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(_call_and_drain(ld.process_lead_distribution(213)))
    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 9
    assert _log_calls[-1]["rule"] == "duty_fallback"


def test_profile_before_first_interval_empty_live_pool_uses_upcoming_pool():
    """Реальный баг 17.08.2026: eligible_pool пуст ПРЯМО СЕЙЧАС (team-panel
    показывает, что реально никто не на месте) ДО начала первого интервала -
    раньше это считалось "обычным путём" (eligible_pool), который просто
    оставался пустым до начала дня, а лид висел без ответственного. Теперь
    пустой пул тоже триггерит upcoming-пул (ближайший будущий старт - сегодня,
    не обязательно "завтра")."""
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now.hour, now.hour)  # никто не «на месте» по плейсхолдеру
    work_hours = [{"start": _hhmm(now + datetime.timedelta(hours=2)), "end": _hhmm(now + datetime.timedelta(hours=3))}]
    _seed_profile(name="BeforeStartEmptyPool", participant_ids=[1, 2], duty_user_id=9, work_hours=work_hours)
    _tomorrow_statuses[2] = True  # доступен на момент старта интервала (сегодня)
    lead = _lead(lead_id=220, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[220] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(_call_and_drain(ld.process_lead_distribution(220)))
    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 2
    assert _log_calls[-1]["rule"] == "tomorrow_shift_fallback"


def test_always_mode_repeat_client_routed_to_tomorrow_pool():
    """"always" раньше проверял _is_on_shift(repeat_responsible) напрямую -
    под tomorrow-пулом это всегда False (сегодняшний день уже кончился),
    повторный клиент завис бы навсегда. Должен смотреть в tomorrow-пул."""
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [{"start": _hhmm(now - datetime.timedelta(hours=3)), "end": _hhmm(now - datetime.timedelta(hours=1))}]
    _seed_profile(name="AlwaysTomorrow", participant_ids=[1, 2], repeat_contact_mode="always", work_hours=work_hours)
    _tomorrow_statuses[1] = True
    lead = _lead(lead_id=214, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[214] = lead
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 900, "responsible_user_id": 1, "updated_at": 1}])
    outcome = run(_call_and_drain(ld.process_lead_distribution(214)))
    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 1
    assert _log_calls[-1]["rule"] == "tomorrow_shift_fallback"


def test_repeat_responsible_read_via_batch_fetch_not_contact_embed():
    """Живой amoCRM кладёт в contact["_embedded"]["leads"] ТОЛЬКО ссылки
    {"id","_links"} — ответственный и updated_at берутся исключительно из
    дочитанных батчем сделок (18.08.2026: раньше читались прямо из ссылок,
    из-за чего повторный клиент не определялся никогда)."""
    _reset_fakes()
    _seed_profile(name="BatchFetch", participant_ids=[1, 2])
    lead = _lead(lead_id=231, source_id=1, contacts=[{"id": 501}])
    _lead_by_id[231] = lead
    _contact_by_id[501] = _contact(501, other_leads=[
        {"id": 901, "responsible_user_id": 1, "updated_at": 10},
        {"id": 902, "responsible_user_id": 2, "updated_at": 99},  # свежее — она и решает
    ])
    embedded = _contact_by_id[501]["_embedded"]["leads"]
    assert all(set(l.keys()) == {"id", "_links"} for l in embedded), "контакт обязан отдавать только ссылки"
    resp = run(ld._find_repeat_responsible(_lead_by_id[231]))
    assert resp == 2
    assert _leads_by_ids_calls, "сделки контакта должны дочитываться батчем"


def test_load_mode_repeat_responsible_wins_under_upcoming_pool():
    """load-режим под forward-пулом: приоритет повторного клиента проверялся
    через _is_on_shift (на смене СЕЙЧАС) — вне рабочих часов всегда False, и
    повторный клиент молча уходил по общей балансировке. Для режима always это
    чинили раньше (_repeat_on_shift), ветку load пропустили (18.08.2026)."""
    _reset_fakes()
    now = datetime.datetime.now(ld._MSK)
    work_hours = [{"start": _hhmm(now - datetime.timedelta(hours=3)), "end": _hhmm(now - datetime.timedelta(hours=1))}]
    _seed_profile(name="RepeatUpcoming", participant_ids=[1, 2], work_hours=work_hours)
    _tomorrow_statuses[1] = True
    _tomorrow_statuses[2] = True
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now.hour, now.hour)  # СЕЙЧАС на смене никого
    _seed_counts({2: {1: 1}})  # у прежнего ответственного даже больше по источнику, но в пределах gap
    lead = _lead(lead_id=232, source_id=1, contacts=[{"id": 502}])
    _lead_by_id[232] = lead
    _contact_by_id[502] = _contact(502, other_leads=[{"id": 903, "responsible_user_id": 2, "updated_at": 7}])
    outcome = run(_call_and_drain(ld.process_lead_distribution(232)))
    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 2, "повторный клиент должен остаться у прежнего ответственного"


def test_profile_without_work_hours_always_runs():
    _reset_fakes()
    _seed_profile(name="NoWH", work_hours=None)
    lead = _lead(lead_id=201, source_id=1, contacts=[{"id": 500}])
    _lead_by_id[201] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(201))
    assert outcome == "routed"
    assert _patch_calls and _patch_calls[0]["lead_id"] == 201


# ════════════════ идемпотентность ════════════════

def test_already_routed_tag_is_noop():
    _reset_fakes()
    _seed_profile(name="Idemp")
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


# ════════════════ доставка «офис» — не распределяем (решение Тианы 19.08.2026) ════════════════

def test_office_pickup_delivery_is_not_distributed():
    """Реальное значение поля 577315 на живом аккаунте: 'Самовывоз из офиса
    Sunscrypt, 1 шт, 0.00 рублей' — с ценой/количеством в той же строке,
    не голое 'офис'. Матч обязан быть по подстроке, не по точному значению."""
    _reset_fakes()
    _seed_profile(name="OfficeGuard", source_ids=[1])
    lead = _lead(lead_id=310, source_id=1, delivery_type="Самовывоз из офиса Sunscrypt, 1 шт, 0.00 рублей")
    _lead_by_id[310] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(310))
    assert outcome == "skipped-office-delivery"
    assert not _patch_calls, "сделка с самовывозом из офиса не должна получать ответственного"


def test_office_pickup_delivery_case_insensitive():
    _reset_fakes()
    _seed_profile(name="OfficeGuardCase", source_ids=[1])
    lead = _lead(lead_id=311, source_id=1, delivery_type="САМОВЫВОЗ ИЗ ОФИСА")
    _lead_by_id[311] = lead
    outcome = run(ld.process_lead_distribution(311))
    assert outcome == "skipped-office-delivery"


def test_non_office_delivery_is_distributed_normally():
    """Контрольный случай — курьер/СДЭК не должны попасть под фильтр офиса."""
    _reset_fakes()
    _seed_profile(name="NonOfficeGuard", source_ids=[1])
    lead = _lead(lead_id=312, source_id=1, delivery_type="Доставка курьером по Москве, 1 шт, 1 000.00 рублей")
    _lead_by_id[312] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(312))
    assert outcome == "routed"
    assert _patch_calls and _patch_calls[0]["lead_id"] == 312


def test_empty_delivery_type_is_distributed_normally():
    _reset_fakes()
    _seed_profile(name="EmptyDeliveryGuard", source_ids=[1])
    lead = _lead(lead_id=313, source_id=1)  # delivery_type не задан вовсе
    _lead_by_id[313] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])
    outcome = run(ld.process_lead_distribution(313))
    assert outcome == "routed"


# ════════════════ защита от гонки amgroup ════════════════

def test_contact_gate_spawns_wait_and_no_decision_without_contact():
    _reset_fakes()
    _seed_profile(name="Race")
    lead = _lead(lead_id=400, source_id=1, contacts=[])
    _lead_by_id[400] = lead
    outcome = run(ld.process_lead_distribution(400))
    assert outcome == "waiting-for-contact"
    assert not _patch_calls
    ld._bg_tasks.clear()  # не ждём фон в этом тесте — только сам факт неприменения решения


def test_contact_wait_loop_succeeds_when_contact_appears():
    _reset_fakes()
    _seed_profile(name="Race")
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
    _seed_profile(name="Race")
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
    _seed_profile(name="On", pipeline_id=10, status_id=20, enabled=True)
    _seed_profile(name="Off", pipeline_id=30, status_id=40, enabled=False)
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


# ════════════════ лог распределений (лог решений в team-panel) ════════════════

def test_log_send_invoked_with_rule_on_routed():
    _reset_fakes()
    _seed_profile(name="Log", participant_ids=[1, 2], repeat_contact_mode="random")
    lead = _lead(lead_id=600, source_id=7, contacts=[{"id": 500}])
    _lead_by_id[600] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])

    outcome = run(_call_and_drain(ld.process_lead_distribution(600)))

    assert outcome == "routed"
    assert len(_log_calls) == 1
    call = _log_calls[0]
    assert call["lead_id"] == 600
    assert call["contact_id"] == 500
    assert call["assigned_user_id"] == _patch_calls[0]["responsible_user_id"]
    assert call["source_id"] == 7
    assert call["rule"] == "random"


def test_log_send_failure_does_not_affect_routing_outcome():
    _reset_fakes()
    global _log_should_raise
    _log_should_raise = True
    _seed_profile(name="LogFail", participant_ids=[1, 2])
    lead = _lead(lead_id=601, source_id=7, contacts=[{"id": 500}])
    _lead_by_id[601] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])

    outcome = run(_call_and_drain(ld.process_lead_distribution(601)))

    assert outcome == "routed"
    assert _patch_calls and _patch_calls[0]["lead_id"] == 601
    assert not _log_calls  # фейк упал внутри, но это не должно было всплыть наружу


def test_log_send_reports_duty_fallback_rule():
    _reset_fakes()
    p = _seed_profile(name="LogDuty", participant_ids=[1, 2], duty_user_id=9)
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # весь пул вне окна
    lead = _lead(lead_id=602, source_id=7, contacts=[{"id": 500}])
    _lead_by_id[602] = lead
    _contact_by_id[500] = _contact(500, other_leads=[])

    outcome = run(_call_and_drain(ld.process_lead_distribution(602)))

    assert outcome == "routed"
    assert _patch_calls[0]["responsible_user_id"] == 9
    assert _log_calls[0]["rule"] == "duty_fallback"
    assert _log_calls[0]["assigned_user_id"] == 9


# ── лог: журнал распределений (11.08.2026) - pipeline/status/контакт/detail ──

def test_find_repeat_responsible_captures_contact_name_and_phone_in_meta():
    _reset_fakes()
    lead = _lead(lead_id=610, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[], name="Иван Иванов", phone="+7 999 123-45-67")
    meta: dict = {}
    found = run(ld._find_repeat_responsible(lead, meta=meta))
    assert found is None  # other_leads пуст - искать репит-ответственного не у кого
    assert meta["contact_name"] == "Иван Иванов"
    assert meta["contact_phone"] == "+7 999 123-45-67"


def test_find_repeat_responsible_meta_none_by_default_is_safe():
    _reset_fakes()
    lead = _lead(lead_id=611, contacts=[{"id": 500}])
    _contact_by_id[500] = _contact(500, other_leads=[], name="Пётр")
    # Без meta= (как во всех остальных вызовах этой функции) - не должно падать.
    found = run(ld._find_repeat_responsible(lead))
    assert found is None


def test_log_send_includes_pipeline_status_contact_and_no_detail_when_empty():
    _reset_fakes()
    _seed_profile(name="LogFields", participant_ids=[1, 2], repeat_contact_mode="random",
                   pipeline_id=111, status_id=222)
    lead = _lead(lead_id=612, pipeline_id=111, status_id=222, source_id=7, contacts=[{"id": 500}])
    _lead_by_id[612] = lead
    _contact_by_id[500] = _contact(500, other_leads=[], name="Клиент", phone="9991234567")

    outcome = run(_call_and_drain(ld.process_lead_distribution(612)))

    assert outcome == "routed"
    call = _log_calls[0]
    assert call["pipeline_id"] == 111
    assert call["status_id"] == 222
    assert call["contact_name"] == "Клиент"
    assert call["contact_phone"] == "9991234567"
    assert call["detail"] is None  # ни repeat, ни prev ответственного - пустой detail не шлём


def test_log_send_detail_carries_repeat_and_prev_responsible():
    _reset_fakes()
    _seed_profile(name="LogDetail", participant_ids=[1, 2], duty_user_id=9,
                   pipeline_id=111, status_id=222)
    now_h = datetime.datetime.now(ld._MSK).hour
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (now_h, now_h)  # весь пул вне окна -> duty_fallback
    lead = _lead(lead_id=613, pipeline_id=111, status_id=222, source_id=7,
                 contacts=[{"id": 500}], responsible_user_id=42)
    _lead_by_id[613] = lead
    _contact_by_id[500] = _contact(500, other_leads=[{"id": 999, "responsible_user_id": 3, "updated_at": 1}])

    outcome = run(_call_and_drain(ld.process_lead_distribution(613)))

    assert outcome == "routed"
    assert _log_calls[0]["detail"] == {"repeat_responsible_user_id": 3, "prev_responsible_user_id": 42}


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
