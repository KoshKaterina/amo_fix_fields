"""Режимы ИИ-менеджера, этап 1 (план 20.11.2026, схема настроек: разделы 1, 7, 8): документ настроек версии 2, режим у сделки,
тихое снятие сделок при смене режима, блок `mode_settings` в ходе к ИИ-сервису.

Что проверяем:
  • документ версии 1 читается как раньше, версия 2 того же содержания даёт те же решения, мусор в блоках даёт безопасные значения;
  • `mode_id` (нет ключа: выводится из `mode`), `review`, справочный `deals`; противоречие `mode` и `mode_id` выключает ИИ;
  • колонка `mode` у `deal_owner` и `deal_given`: миграция идемпотентна и на старой базе, начальное значение по воронке «Тест»;
  • ИИ ведёт сделку, только если она взята в активном режиме: Тест -> Полный и Полный -> Тест при открытой сделке, при идущем ходе,
    во всех точках решения отдельно (разрешение, отправка хода, события, касания, лестница тишины, окна УР, ручная передача);
  • снятие тихое: в amo, клиенту и в чат ОП ничего не уходит, остаётся строка журнала движка;
  • режим `off` ведёт себя как раньше (сделки уходят людям), протухшие настройки ничего не снимают;
  • `mode_settings` в ходе: есть при версии 2, нет при версии 1, настройки берутся на момент отправки.

Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_modes.py -q
"""
import asyncio
import datetime
import json
import sqlite3
import time

import pytest

import ai_engine as E
import ai_engine_client as service
import ai_engine_config as cfg
import ai_engine_followup as F
import ai_engine_give as G
import ai_engine_intercept as I
import ai_engine_modes as M
import ai_engine_settings as settings
import ai_engine_silence as S
import ai_engine_store as store
from test_ai_engine import CHAN, CHAT, CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_RETAIL, PIPE_TEST, _paid_event, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_followup import scheduled
from test_ai_engine_intercept import igor_deal, intercepts, setup as intercept_setup
from test_ai_engine_intercept import client as intercept_client
from test_ai_engine_ur import at, clock, waves

PIPE_TANGEM = 9822330
DEAL2 = DEAL + 1
OPEN_FULL = {"scope": "full", "restricted": False, "items": []}
MSK = datetime.timezone(datetime.timedelta(hours=3))
NOON = datetime.datetime(2026, 11, 20, 12, 0, tzinfo=MSK)
BASES = {
    "test": dict(mode="test", pipelines=[PIPE_TEST]),
    "full": dict(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL),
    "off": dict(mode="off", pipelines=[]),
}


@pytest.fixture(autouse=True)
def _fresh_mode_sweep(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)        # проход «режим сменился» помнит прошлый режим между тестами одного процесса


def apply(env, kind, version=1, **over):
    """Положить в движок документ панели: `kind` это test | full | off, `version` 1 (как до режимов) или 2 (с `mode_id`, `review`, `deals`)."""
    doc = dict(BASES[kind])
    if version == 2:
        doc.update(settings_version=2, mode_id=kind, review={"enabled": True}, deals={"items": []})
    doc.update(over)
    env.set_settings(**doc)


def no_sweeps(env):
    """Выключить проходы «режим сменился»: смотрим, что каждая точка решения справляется сама. Звать ПОСЛЕ смены документа."""
    env.mp.setattr(E, "_last_off_sweep", env.t + 10 ** 9)
    env.mp.setattr(E, "_last_mode_id", settings.active_mode_id())


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


async def take(env, deal=DEAL):
    """Клиент написал, ИИ взял сделку и закончил ход (`continue`): возвращает ход."""
    turn = await env.start_turn()
    await E.handle_result(turn["turn_id"], deal, turn["epoch"], "continue", None, None)
    env.turn_requests.clear()
    return turn


def assert_quiet(env):
    """Снятие тихое: в amo нет примечаний и задач, в чат ОП нет алертов, сервису не ушёл ни один ход."""
    assert env.notes == [] and env.alerts == [] and env.tasks_open == [] and env.turn_requests == []


def released(deal=DEAL):
    owner = store.get_owner(deal)
    return owner["owner"] == "engine" and owner["reason"] == "mode_changed" and not owner["closed"]


# =============================================================================================
# настройки: версия 1 и версия 2
# =============================================================================================

def snapshot():
    probes = [([CONTACT], [PHONE]), ([999], ["79990000000"]), ([], [CHAT])]
    return {
        "mode": settings.effective_mode(), "pipelines": sorted(settings.allowed_pipelines()),
        "restricted": settings.contacts_restricted(), "items": settings.contact_items(),
        "allowed": [settings.contact_allowed(ids, ph) for ids, ph in probes],
        "may_match": [settings.phone_may_match(*ph) for _, ph in probes],
        "timers": settings.timers(), "hours": settings.work_hours(), "window": settings.send_window(NOON),
        "post_sale": settings.post_sale_window_days(), "intercept": settings.intercept_config(),
        "intercept_on": settings.intercept_enabled(),
    }


PILOT = {"scope": "full", "restricted": True, "items": [{"contact_id": CONTACT, "phone_norm": PHONE, "name": "Пилот"}]}
EMPTY_RESTRICTED = {"scope": "full", "restricted": True, "items": []}
SAME_CONTENT = {
    "test, список из одного контакта": ("test", "test", {}),
    "полный, пилотный список": ("full", "full", {"contacts": PILOT}),
    "полный, без ограничения": ("full", "full", {}),
    "полный, пустой список при ограничении": ("full", "full", {"contacts": EMPTY_RESTRICTED}),
    "выключено": ("off", "off", {}),
}


@pytest.mark.parametrize("name", list(SAME_CONTENT))
def test_version_2_document_gives_the_same_decisions_as_version_1(env, name):
    kind, mode_id, over = SAME_CONTENT[name]
    apply(env, kind, 1, intercept={"enabled": True, "mode": "reply", "wait_min": 20}, **over)
    before = snapshot()
    assert settings.settings_version() == 1 and settings.active_mode_id() == mode_id    # v1: режим выведен из `mode`
    apply(env, kind, 2, intercept={"enabled": True, "mode": "reply", "wait_min": 20}, **over)
    assert settings.settings_version() == 2 and settings.active_mode_id() == mode_id
    assert snapshot() == before


def test_version_1_document_reads_as_before(env):
    apply(env, "test")
    assert (settings.settings_version(), settings.is_v2(), settings.mode_settings()) == (1, False, None)
    assert (settings.review_enabled(), settings.deal_items()) == (True, [])
    assert (settings.effective_mode(), settings.active_mode_id()) == ("test", "test")
    apply(env, "full")
    assert (settings.effective_mode(), settings.active_mode_id()) == ("prod", "full")
    apply(env, "off")
    assert (settings.effective_mode(), settings.active_mode_id()) == ("off", "off")


@pytest.mark.parametrize("version", [None, 0, 1, "2", True, 1.5, [2]])
def test_missing_or_broken_settings_version_means_version_1(env, version):
    over = {} if version is None else {"settings_version": version}
    apply(env, "full", **over, mode_id="test", review={"enabled": False}, deals={"items": [{"deal_id": 5}]})
    # ключи версии 2 в документе версии 1 не читаются: `mode_id: test` не спорит с `mode: prod`, пилот остаётся включённым
    assert settings.settings_version() == 1 and settings.effective_mode() == "prod" and settings.active_mode_id() == "full"
    assert settings.review_enabled() is True and settings.deal_items() == [] and settings.mode_settings() is None


def test_newer_versions_are_read_as_version_2(env):
    apply(env, "full", 2, settings_version=3, mode_id="help")
    assert settings.is_v2() and settings.active_mode_id() == "help"


@pytest.mark.parametrize("garbage", [None, "", "weird", 5, ["full"], {"id": "full"}, True])
def test_mode_id_garbage_or_missing_is_derived_from_the_old_mode_key(env, garbage):
    doc = dict(BASES["full"], settings_version=2)
    if garbage is not None:
        doc["mode_id"] = garbage
    env.set_settings(**doc)
    assert settings.effective_mode() == "prod" and settings.active_mode_id() == "full"
    env.set_settings(mode="test", pipelines=[PIPE_TEST], settings_version=2, **({} if garbage is None else {"mode_id": garbage}))
    assert settings.effective_mode() == "test" and settings.active_mode_id() == "test"


@pytest.mark.parametrize("mode_id", ["full", "help", "intercept"])
def test_help_and_intercept_ids_ride_on_the_prod_mode(env, mode_id):
    apply(env, "full", 2, mode_id=mode_id)
    assert settings.effective_mode() == "prod" and settings.active_mode_id() == mode_id
    assert settings.allowed_pipelines() == cfg.MODE_PIPELINES["prod"]


@pytest.mark.parametrize("mode,pipelines,mode_id", [
    ("prod", [PIPE_RETAIL, PIPE_TANGEM], "test"),     # два ответа на вопрос «какой режим»: верить ни одному нельзя
    ("test", [PIPE_TEST], "full"),
    ("test", [PIPE_TEST], "help"),
    ("prod", [PIPE_RETAIL, PIPE_TANGEM], "off"),
])
def test_mode_id_contradicting_mode_switches_the_ai_off(env, mode, pipelines, mode_id):
    env.set_settings(mode=mode, pipelines=pipelines, settings_version=2, mode_id=mode_id)
    assert settings.effective_mode() == "off" and settings.active_mode_id() == "off" and settings.mode_settings() is None


def test_review_block_defaults_to_enabled_and_reads_only_booleans(env):
    apply(env, "test", 2)
    assert settings.review_enabled() is True
    apply(env, "test", 2, review={"enabled": False})
    assert settings.review_enabled() is False
    for junk in ({"enabled": "no"}, {"enabled": 0}, {"enabled": None}, {}, "off", 7, None, [False]):
        apply(env, "test", 2, review=junk)
        assert settings.review_enabled() is True, junk                    # ошибка в эту сторону лишь замедляет ответ
    env.set_settings(settings_version=2)                                  # блока нет вовсе
    assert settings.review_enabled() is True
    apply(env, "test", 1, review={"enabled": False})
    assert settings.review_enabled() is True                              # версия 1 блок не читает


def test_deals_block_is_a_reference_only_and_survives_garbage(env):
    apply(env, "test", 2, deals={"items": [
        {"deal_id": 36587533, "contact_id": 555}, {"deal_id": "36587534", "contact_id": "556"}, {"deal_id": 36587535},
        {"deal_id": 0, "contact_id": 1}, {"deal_id": -3}, {"deal_id": True}, {"deal_id": "abc"}, {"contact_id": 5}, "x", None, 12.5,
    ]})
    assert settings.deal_items() == [
        {"deal_id": 36587533, "contact_id": 555}, {"deal_id": 36587534, "contact_id": 556}, {"deal_id": 36587535, "contact_id": 0}]
    for junk in ("x", 5, None, [], {"items": "x"}, {"items": None}):
        apply(env, "test", 2, deals=junk)
        assert settings.deal_items() == [], junk
    # справочник ничего не решает: сделка в списке, а её контакта в `contacts` нет - контакт не разрешён
    apply(env, "test", 2, deals={"items": [{"deal_id": DEAL, "contact_id": 777}]}, contacts={"scope": "test", "restricted": True, "items": []})
    assert settings.contact_allowed([777], ["79991112233"]) is False


def test_mode_settings_has_exactly_the_agreed_shape(env):
    apply(env, "test", 2)
    assert settings.mode_settings() == {"mode_id": "test", "review": {"enabled": True}}
    apply(env, "full", 2, review={"enabled": False}, mode_id="help")
    assert settings.mode_settings() == {"mode_id": "help", "review": {"enabled": False}}
    apply(env, "full", 2, review={"enabled": "junk"})
    assert settings.mode_settings() == {"mode_id": "full", "review": {"enabled": True}}
    apply(env, "off", 2)
    assert settings.mode_settings() is None
    apply(env, "full", 1)
    assert settings.mode_settings() is None


def test_garbage_in_every_block_gives_safe_defaults_and_does_not_raise(env):
    env.set_settings(
        mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], settings_version=2, mode_id=["full"], contacts="junk", timers=["x"],
        work_hours=5, review="yes", deals=[1, 2], post_sale_window_days={"a": 1}, intercept=7)
    assert settings.effective_mode() == "prod" and settings.active_mode_id() == "full"
    assert settings.contacts_restricted() is True and settings.contact_items() == []        # пустой список при ограничении: никого
    assert settings.contact_allowed([CONTACT], [PHONE]) is False and settings.phone_may_match(PHONE) is False
    assert settings.timers() == settings.DEFAULT_TIMERS
    assert settings.work_hours() == [] and settings.send_window(NOON)["open_now"] is False
    assert settings.review_enabled() is True and settings.deal_items() == []
    assert settings.post_sale_window_days() == 7
    assert settings.intercept_enabled() is False and settings.intercept_config()["managers"]
    assert settings.mode_settings() == {"mode_id": "full", "review": {"enabled": True}}
    env.set_settings(contacts={"scope": "full", "restricted": True, "items": "junk"}, work_hours=[{"start": "10:00"}, {"start": "x", "end": "y"}, 5, "10-19"])
    assert settings.contact_items() == [] and settings.work_hours() == []
    env.set_settings(timers={"debounce_s": "soon", "first_reply_wait_s": None, "debounce_cap_s": -5, "ai_down_release_s": 30})
    assert settings.timers() == dict(settings.DEFAULT_TIMERS, ai_down_release_s=30)


def test_status_shows_the_active_mode_and_the_deals_by_mode(env):
    async def go():
        apply(env, "test", 2)
        await take(env)
        out = E.status()
        assert out["mode"] == "test" and out["mode_id"] == "test" and out["settings_version"] == 2
        assert out["counts"]["ai_by_mode"] == {"test": 1}
    run(go())


# =============================================================================================
# режим у сделки: колонки, миграция
# =============================================================================================

LEGACY_OWNER = (
    "CREATE TABLE deal_owner (deal_id INTEGER PRIMARY KEY, owner TEXT NOT NULL, epoch INTEGER NOT NULL, since TEXT NOT NULL, chat_id TEXT, "
    "chat_type TEXT, channel_id TEXT, reason TEXT, chat_key TEXT, pipeline_id INTEGER, contact_ids TEXT NOT NULL DEFAULT '[]', "
    "closed INTEGER NOT NULL DEFAULT 0{extra})")


def raw(path, sql, args=()):
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(sql, args).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


@pytest.mark.parametrize("with_column", [False, True])
def test_deal_owner_migration_gives_modes_to_old_rows_and_is_idempotent(env, tmp_path, with_column):
    path = str(tmp_path / "legacy.sqlite3")
    env.mp.setattr(cfg, "DB_PATH", path)
    raw(path, LEGACY_OWNER.format(extra=", mode TEXT" if with_column else ""))
    rows = [(1, "ai", PIPE_TEST, "test"), (2, "ai", PIPE_RETAIL, "prod"), (3, "human", None, None), (4, "ai", 7_000_001, "prod"),
            (5, "ai", PIPE_TEST, None), (6, "engine", PIPE_TANGEM, "junk"), (7, "ai", PIPE_RETAIL, "help")]
    for deal, owner, pipeline, mode in rows:
        if with_column:
            raw(path, "INSERT INTO deal_owner (deal_id, owner, epoch, since, pipeline_id, mode) VALUES (?,?,?,?,?,?)",
                (deal, owner, 1, "2026-11-01T00:00:00Z", pipeline, mode))
        else:
            raw(path, "INSERT INTO deal_owner (deal_id, owner, epoch, since, pipeline_id) VALUES (?,?,?,?,?)", (deal, owner, 1, "2026-11-01T00:00:00Z", pipeline))
    store.init()
    got = {r[0]: r[1] for r in raw(path, "SELECT deal_id, mode FROM deal_owner")}
    # воронка «Тест» значит test, любая другая full; уже верный режим (help) и прежний test не трогаются
    want = {1: "test", 2: "full", 3: "full", 4: "full", 5: "test", 6: "full", 7: "help" if with_column else "full"}
    assert got == want
    raw(path, "UPDATE deal_owner SET mode='help' WHERE deal_id=2")
    store.init()
    store.init()
    assert {r[0]: r[1] for r in raw(path, "SELECT deal_id, mode FROM deal_owner")} == {**want, 2: "help"}   # повторные запуски ничего не меняют
    assert store.get_owner(2)["epoch"] == 1


def test_deal_given_migration_adds_the_column_once_and_fills_it_by_pipeline(env):
    path = cfg.DB_PATH
    raw(path, "CREATE TABLE deal_given (deal_id INTEGER PRIMARY KEY, given_at REAL NOT NULL, given_iso TEXT NOT NULL)")     # самая первая версия
    store.set_owner(10, "ai", reason="deal_given", pipeline_id=PIPE_TEST, mode="test")
    store.set_owner(11, "ai", reason="deal_given", pipeline_id=PIPE_RETAIL, mode="prod")
    for deal in (10, 11, 12):
        raw(path, "INSERT INTO deal_given (deal_id, given_at, given_iso) VALUES (?,?,?)", (deal, 1.0, "2026-11-12T10:00:00Z"))
    assert G.cutoff_iso(10) == "2026-11-12T10:00:00Z"                    # первое же обращение добавляет колонки
    assert {r[0]: r[1] for r in raw(path, "SELECT deal_id, mode FROM deal_given")} == {10: "test", 11: "full", 12: "full"}   # сделки 12 нет у владельцев: full
    raw(path, "UPDATE deal_given SET mode='help' WHERE deal_id=11")
    G.is_forced(11)
    G.mode_of(11)
    assert G.mode_of(11) == "help" and G.mode_of(10) == "test"             # колонка уже есть: заполнение второй раз не идёт
    assert G.mode_of(999) is None


def test_set_owner_writes_the_mode_on_birth_and_on_change_but_not_on_repeat(env):
    assert store.set_owner(1, "ai", reason="a", mode="test")["mode"] == "test"
    assert store.set_owner(1, "ai", reason="again", mode="full")["mode"] == "test"        # тот же владелец: режим не переклеивается
    assert store.set_owner(1, "human", reason="b")["mode"] == "test"                      # передача человеку режим не стирает
    assert store.set_owner(1, "ai", reason="c", mode="full")["mode"] == "full"            # новое ведение: режим нового
    store.set_owner(2, "ai", reason="a")                                                  # строка без режима
    assert store.set_owner(2, "ai", reason="again", mode="full")["mode"] == "full"        # режим появился


def test_mode_of_derives_a_missing_mode_the_way_the_migration_does():
    assert M.mode_of({"mode": "help", "pipeline_id": PIPE_TEST}) == "help"
    assert M.mode_of({"mode": None, "pipeline_id": PIPE_TEST}) == "test"
    assert M.mode_of({"mode": "prod", "pipeline_id": PIPE_RETAIL}) == "full"
    assert M.mode_of({"mode": None, "pipeline_id": None}) == "full"
    assert M.mode_of({"mode": "junk", "pipeline_id": PIPE_TEST}) == "test"
    assert M.mode_of(None) is None


def test_only_open_ai_deals_in_a_running_mode_can_be_stale():
    ai = {"owner": "ai", "closed": 0, "mode": "test", "pipeline_id": PIPE_TEST}
    assert M.is_stale(ai, "full") and M.is_stale(ai, "help") and not M.is_stale(ai, "test")
    assert not M.is_stale(ai, "off")                                                      # выключение: своё правило, не «смена режима»
    assert not M.is_stale(dict(ai, owner="human"), "full") and not M.is_stale(dict(ai, owner="engine"), "full")
    assert not M.is_stale(dict(ai, closed=1), "full") and not M.is_stale(None, "full")


def test_given_marks_of_another_mode_do_not_apply(env):
    G.mark(DEAL, env.t, True, "test")
    assert G.mode_of(DEAL) == "test" and G.cutoff_iso(DEAL) and G.is_forced(DEAL)         # режим не назван: как раньше
    assert G.cutoff_iso(DEAL, "test") and G.is_forced(DEAL, "test")
    assert G.cutoff_iso(DEAL, "full") is None and G.is_forced(DEAL, "full") is False
    G.mark(DEAL, env.t, False, "full")
    assert G.mode_of(DEAL) == "full" and G.cutoff_iso(DEAL, "full") and not G.is_forced(DEAL, "full")
    G.mark(DEAL, env.t, False)
    assert G.mode_of(DEAL) == "full"                                                      # передача без режима режим не стирает


# =============================================================================================
# режим записывается при взятии сделки
# =============================================================================================

@pytest.mark.parametrize("version", [1, 2])
def test_a_taken_deal_remembers_the_active_mode(env, version):
    async def go():
        apply(env, "test", version)
        await take(env)
        assert store.get_owner(DEAL)["mode"] == "test"
        apply(env, "full", version)
        env.lead(lead_id=DEAL2, pipeline=PIPE_RETAIL)
        await env.say(env.msg("Здравствуйте", mid="full-1"))
        await env.advance(61)
        assert store.get_owner(DEAL2)["mode"] == "full"
    run(go())


def test_help_and_intercept_are_recorded_as_their_own_modes(env):
    async def go():
        apply(env, "full", 2, mode_id="help")
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        assert store.get_owner(DEAL)["mode"] == "help"
    run(go())


def test_give_and_return_dialog_record_the_active_mode(env):
    async def go():
        apply(env, "full", 2)
        env.lead(pipeline=PIPE_RETAIL)
        code, out = await E.give_deal(DEAL, CHAT, "telegram", CHAN, PHONE, "Тиана")
        assert code == 200 and store.get_owner(DEAL)["mode"] == "full" and G.mode_of(DEAL) == "full"
        await drain()
        await E._to_human(store.get_session(CHAT_KEY), "manager_message", alert=None)
        assert store.get_owner(DEAL)["owner"] == "human"
        code, out = await E.return_dialog(DEAL)
        assert code == 200 and store.get_owner(DEAL)["mode"] == "full"
    run(go())


def test_interception_records_the_active_mode_and_the_next_mode_releases_the_deal(env):
    async def go():
        intercept_setup(env, mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, settings_version=2, mode_id="full",
                        review={"enabled": True})
        igor_deal(env)
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        ev = intercepts(env)[0]
        assert ev["mode_settings"] == {"mode_id": "full", "review": {"enabled": True}}
        assert store.get_owner(DEAL)["mode"] == "full" and I.is_taken(DEAL)
        env.turn_requests.clear()
        intercept_setup(env, mode="test", pipelines=[PIPE_TEST], settings_version=2, mode_id="test", review={"enabled": True})
        await env.advance(2)
        await drain()
        assert released() and store.get_owner(DEAL)["mode"] == "full"
        assert intercepts(env) == [] and env.alerts == []
    run(go())


# =============================================================================================
# смена режима снимает чужие сделки
# =============================================================================================

@pytest.mark.parametrize("version", [1, 2])
def test_switch_from_test_to_full_releases_the_open_test_deal_quietly(env, version):
    async def go():
        apply(env, "test", version)
        turn = await take(env)
        apply(env, "full", version)
        await env.advance(2)                                                  # ближайший проход видит новый режим
        await drain()
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["reason"], owner["closed"], owner["mode"]) == ("engine", 2, "mode_changed", 0, "test")
        assert store.get_session(CHAT_KEY) is None
        assert_quiet(env)
        row, = M.releases()
        assert (row["deal_id"], row["from_mode"], row["to_mode"], row["epoch"], row["note"]) == (DEAL, "test", "full", 2, "снята при смене режима")
        # поздний итог старого хода и поздняя просьба о разрешении отбиваются
        assert (await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None))[0] == 409
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "stale_epoch"
        assert E.decide_permit(DEAL, 2, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "owner_changed"
        # у клиента в новом режиме есть другая подходящая сделка: ИИ берёт её (чат не заблокирован), в режиме full
        env.lead(lead_id=DEAL2, pipeline=PIPE_RETAIL)
        await env.say(env.msg("Здравствуйте", mid="after-switch"))
        await env.advance(61)
        req = env.turn_requests[-1]
        assert req["deal_id"] == DEAL2 and req["mode"] == "prod" and store.get_owner(DEAL2)["mode"] == "full"
        assert store.get_owner(DEAL)["owner"] == "engine"                     # старая сделка так и осталась без ИИ
    run(go())


@pytest.mark.parametrize("version", [1, 2])
def test_switch_from_full_to_test_releases_the_open_full_deal_quietly(env, version):
    async def go():
        apply(env, "full", version)
        env.lead(pipeline=PIPE_RETAIL)
        turn = await take(env)
        assert store.get_owner(DEAL)["mode"] == "full"
        apply(env, "test", version)
        await env.advance(2)
        await drain()
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["reason"], owner["closed"], owner["mode"]) == ("engine", 2, "mode_changed", 0, "full")
        assert_quiet(env)
        row, = M.releases()
        assert (row["from_mode"], row["to_mode"]) == ("full", "test")
        assert E.decide_permit(DEAL, 2, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "owner_changed"
        # а в Тесте ИИ берёт тестовую сделку и записывает режим test
        env.lead(lead_id=DEAL2, pipeline=PIPE_TEST)
        await env.say(env.msg("Привет", mid="after-switch"))
        await env.advance(61)
        assert env.turn_requests[-1]["deal_id"] == DEAL2 and store.get_owner(DEAL2)["mode"] == "test"
    run(go())


def test_switch_during_a_running_turn_cancels_it_and_silences_its_late_result(env):
    async def go():
        turn = await env.start_turn()                                         # ход идёт, итога ещё нет
        apply(env, "full")
        await env.advance(2)
        await drain()
        assert released()
        assert env.cancels == [turn["turn_id"]]                               # сервису ушла отмена хода
        assert store.get_turn(turn["turn_id"])["state"] == "cancelled"
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "stale_epoch"
        assert env.notes == [] and env.alerts == [] and env.tasks_open == []
    run(go())


def test_switch_between_full_and_help_also_releases(env):
    async def go():
        apply(env, "full", 2)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        apply(env, "full", 2, mode_id="help")                                 # тот же `mode: prod`, другой режим
        await env.advance(2)
        assert released() and M.releases()[0]["to_mode"] == "help"
    run(go())


def test_the_same_mode_with_a_new_document_version_releases_nothing(env):
    async def go():
        apply(env, "full", 1)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        apply(env, "full", 2)                                                 # панель перешла на версию 2: режим тот же
        await env.advance(30)
        await env.advance(30)
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["epoch"] == 1 and M.releases() == []
        apply(env, "full", 1)                                                 # и обратно (откат панели)
        await env.advance(30)
        assert store.get_owner(DEAL)["owner"] == "ai" and M.releases() == []
    run(go())


def test_rows_written_by_the_old_engine_survive_the_upgrade_in_the_same_mode(env):
    async def go():
        apply(env, "full", 1)
        env.lead(pipeline=PIPE_RETAIL)
        raw(cfg.DB_PATH, "INSERT INTO deal_owner (deal_id, owner, epoch, since, pipeline_id, mode, chat_key) VALUES (?,?,?,?,?,?,?)",
            (DEAL, "ai", 1, "2026-11-01T00:00:00Z", PIPE_RETAIL, "prod", CHAT_KEY))            # так писал прежний движок
        raw(cfg.DB_PATH, "INSERT INTO deal_owner (deal_id, owner, epoch, since, pipeline_id, mode, chat_key) VALUES (?,?,?,?,?,?,?)",
            (DEAL2, "ai", 1, "2026-11-01T00:00:00Z", PIPE_RETAIL, None, "k2"))
        await env.advance(30)                                                 # до миграции: режим выводится из строки
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL2)["owner"] == "ai"
        store.init()                                                          # рестарт новым кодом
        assert store.get_owner(DEAL)["mode"] == "full" and store.get_owner(DEAL2)["mode"] == "full"
        await env.advance(30)
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL2)["owner"] == "ai" and M.releases() == []
        apply(env, "test", 1)                                                 # а вот смена режима снимает и их
        await env.advance(2)
        assert released(DEAL) and released(DEAL2)
    run(go())


def test_off_still_hands_the_deals_to_humans_and_stale_settings_release_nothing(env):
    async def go():
        apply(env, "test", 2)
        turn = await take(env)
        apply(env, "off", 2)
        assert settings.active_mode_id() == "off" and not M.is_stale(store.get_owner(DEAL), "off")
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "mode_off"
        await env.advance(11)
        await drain()
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["reason"]) == ("human", "mode_off")      # прежнее правило выключения: сделка уходит людям
        assert M.releases() == [] and env.turn_requests == []
        # документ протух (панель молчит), но выключения ещё нет: сделки остаются у ИИ, режим-проход ничего не снимает
        apply(env, "test", 2)
        store.set_owner(DEAL, "ai", reason="back")
        settings.set_document(dict(settings.document(), mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], mode_id="full"), fetched_at=time.time() - 1000)
        assert settings.effective_mode() == "off"
        await env.advance(11)
        assert store.get_owner(DEAL)["owner"] == "ai" and M.releases() == []
    run(go())


def test_a_client_message_in_a_released_deal_does_not_wake_the_ai(env):
    async def go():
        env.lead(pipeline=PIPE_TEST)
        await take(env)
        apply(env, "full")
        await env.advance(2)
        assert released()
        await env.say(env.msg("Ещё вопрос", mid="late-1"))
        await env.advance(61)
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "engine"
    run(go())


def test_a_human_can_give_a_released_deal_back_in_the_new_mode(env):
    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        assert released()
        env.lead(pipeline=PIPE_RETAIL)                                        # сделка теперь в воронке нового режима
        code, out = await E.give_deal(DEAL, CHAT, "telegram", CHAN, PHONE, "Тиана")
        assert code == 200 and out["epoch"] == 3
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["mode"]) == ("ai", "full") and G.mode_of(DEAL) == "full"
        await env.advance(1)
        req = env.turn_requests[-1]
        assert req["event"]["type"] == "deal_given" and req["mode"] == "prod" and req["mode_settings"]["mode_id"] == "full"
        await drain()
    run(go())


# =============================================================================================
# каждая точка решения сама отказывает сделке чужого режима (проходы смены режима выключены)
# =============================================================================================

def test_permit_refuses_a_deal_taken_in_another_mode_and_only_reads(env):
    async def go():
        turn = await env.start_turn()
        args = (DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert E.decide_permit(*args) == (200, {"allowed": True})
        apply(env, "full")
        no_sweeps(env)
        assert E.decide_permit(*args) == (200, {"allowed": False, "reason": "owner_changed"})
        assert store.get_owner(DEAL)["owner"] == "ai"                         # разрешение ничего не меняет
        apply(env, "test")
        assert E.decide_permit(*args) == (200, {"allowed": True})             # вернулся тот же режим: сделка снова своя
    run(go())


def test_followup_registration_is_refused_for_another_mode(env):
    async def go():
        turn = await env.start_turn()
        apply(env, "full")
        no_sweeps(env)
        code, out = await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + 86400, "завтра", "")
        assert out["allowed"] is False and F.pending_for_deal(DEAL) is None
    run(go())


def test_a_queued_turn_is_not_sent_after_the_mode_changed(env):
    async def go():
        env.lead()
        await env.say(env.msg("слово"))
        env.t += 61
        await E.tick_once()                                                   # ход создан, но ещё не отправлен
        queued = store.active_turn_for_deal(DEAL)
        assert queued["state"] == "queued"
        apply(env, "full")
        no_sweeps(env)
        await E.dispatch_once()
        await drain()
        assert env.turn_requests == [] and released()
        assert store.get_turn(queued["turn_id"])["state"] == "cancelled"
        assert env.notes == [] and env.alerts == []
    run(go())


def test_a_pending_client_message_is_not_turned_into_a_turn_after_the_mode_changed(env):
    async def go():
        await take(env)
        await env.say(env.msg("ещё", mid="pending-1"))                        # чат ждёт паузу перед ходом
        apply(env, "full")
        no_sweeps(env)
        env.t += 11
        await E.tick_once()
        await E.dispatch_once()
        await drain()
        assert env.turn_requests == [] and released() and store.get_session(CHAT_KEY) is None
        assert store.pending_inbox(CHAT_KEY) == []
    run(go())


def test_a_client_message_after_the_change_releases_the_deal_and_starts_the_chat_over(env):
    async def go():
        await take(env)
        apply(env, "full")
        no_sweeps(env)
        await env.say(env.msg("Здравствуйте", mid="first-after"))
        assert released()
        session = store.get_session(CHAT_KEY)
        assert session["state"] == "collecting" and session["deal_id"] is None                # разбираем как первое сообщение чата
        await env.advance(61)
        assert env.turn_requests == []                                        # тестовая сделка в боевых воронках не подходит
    run(go())


def test_a_resolution_made_for_the_previous_mode_is_not_applied(env):
    async def go():
        env.lead()
        await env.say(env.msg("слово"))
        real = E.dedup.resolve

        async def switching(*a, **k):
            res = await real(*a, **k)
            apply(env, "full")                                                # панель сменила режим, пока шёл поиск в amo
            return res

        env.mp.setattr(E.dedup, "resolve", switching)
        env.t += 61
        await E.tick_once()
        env.mp.setattr(E.dedup, "resolve", real)
        assert store.get_owner(DEAL) is None and env.turn_requests == []      # сделку по старому выбору не взяли
        assert store.get_session(CHAT_KEY)["state"] == "collecting"          # чат решается заново
    run(go())


def test_a_deal_found_through_another_chat_is_released_when_the_ai_holds_it_from_another_mode(env):
    async def go():
        apply(env, "full", 2)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        apply(env, "full", 2, mode_id="help")                                 # воронки те же, режим другой
        no_sweeps(env)
        other = env.msg("Пишу из другого канала", mid="other-chat-1")
        other["chatId"] = "555000111"                                         # у чата ещё нет сделки: он найдёт её поиском в amo
        await env.say(other)
        env.t += 61
        await E.tick_once()                                                   # без отправки: смотрим, что ход даже не создан
        assert released() and store.active_turn_for_deal(DEAL) is None and env.turn_requests == []
        assert store.get_owner(DEAL)["mode"] == "full"                        # режим старого ведения не переклеен на help
    run(go())


def test_an_event_for_another_mode_is_not_queued_and_a_payment_is_not_consumed(env):
    async def go():
        await take(env)
        apply(env, "full")
        no_sweeps(env)
        await E._handle_lead_event(_paid_event())
        await env.advance(5)
        await drain()
        assert env.turn_requests == [] and released()
        assert store.claim_event(DEAL, "payment_received") is True            # отметка «событие ушло» не поставлена
    run(go())


def test_queue_event_itself_refuses_a_foreign_mode_deal(env):
    async def go():
        await take(env)
        apply(env, "full")
        no_sweeps(env)
        await E._queue_event(store.get_owner(DEAL), "dialog_returned")
        assert store.get_session(CHAT_KEY)["pending_event"] is None
        await env.advance(2)
        assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "ai"     # событие не поставлено, сделку снимет проход
    run(go())


def test_followup_that_comes_due_after_the_change_is_dropped_without_notes_or_alerts(env):
    async def go():
        await scheduled(env)
        notes_before = list(env.notes)
        env.turn_requests.clear()
        apply(env, "full")
        no_sweeps(env)
        await env.advance(25 * 3600)
        await drain()
        assert F.pending_for_deal(DEAL) is None and released()
        assert raw(cfg.DB_PATH, "SELECT status, cancel_reason FROM followup WHERE deal_id=?", (DEAL,)) == [("cancelled", "mode_changed")]
        assert env.notes == notes_before and env.alerts == [] and env.turn_requests == []      # ни примечания «касание не вышло», ни алерта, ни хода
    run(go())


def test_pending_followup_is_cancelled_silently_by_the_release(env):
    async def go():
        await scheduled(env)
        notes_before = list(env.notes)
        apply(env, "full")
        await env.advance(2)
        await drain()
        assert F.pending_for_deal(DEAL) is None and F.cancelled_notes(DEAL) == []             # ИИ о нём уже не узнает: сделка не его
        assert env.notes == notes_before and env.alerts == []
    run(go())


def test_silence_ladder_does_not_fire_for_another_mode(env):
    async def go():
        await take(env)
        await E.silence_arm(DEAL)
        assert S.get(DEAL) is not None
        apply(env, "full")
        no_sweeps(env)
        await env.advance(3600 + 120)
        await env.advance(5)
        await drain()
        assert S.get(DEAL) is None and released()
        assert env.turn_requests == [] and env.alerts == []
    run(go())


def test_ur_windows_skip_a_deal_of_another_mode(env):
    async def go():
        clock(env, at(14))
        await take(env)
        apply(env, "full")
        clock(env, at(17, 55))
        no_sweeps(env)
        for _ in range(3):
            await E.tick_once()
            await E.dispatch_once()
        await drain()
        assert waves(env) == [] and released() and env.alerts == []
    run(go())


def test_lead_event_of_another_mode_releases_instead_of_closing_or_paying(env):
    async def go():
        await take(env)
        apply(env, "full")
        no_sweeps(env)
        ev = E.extract_lead_event(DEAL, PIPE_TEST, 1000, {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        assert await E._lead_event_under_lock(ev) is True
        assert released()
    run(go())


def test_rule_c_does_not_hand_a_deal_of_another_mode_to_a_human(env):
    async def go():
        await take(env)
        apply(env, "full")
        no_sweeps(env)
        env.lead(lead_id=DEAL2, pipeline=PIPE_RETAIL, price=5000)             # у того же контакта появилась сделка с бюджетом
        await E._rule_c_check(DEAL2)
        await drain()
        assert store.get_owner(DEAL)["owner"] == "ai" and env.alerts == []   # правило C чужой сделки не трогает, её снимет проход
    run(go())


def test_give_deal_takes_over_a_deal_the_ai_holds_from_another_mode(env):
    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        no_sweeps(env)
        env.lead(pipeline=PIPE_RETAIL)
        code, out = await E.give_deal(DEAL, CHAT, "telegram", CHAN, PHONE, "Тиана")           # без «перезапуска»: ответ не «already_ai»
        assert code == 200 and out["epoch"] == 3
        assert store.get_owner(DEAL)["mode"] == "full" and G.mode_of(DEAL) == "full"
        assert M.releases()[0]["to_mode"] == "full"
        await drain()
    run(go())


def test_return_dialog_releases_a_deal_from_another_mode_and_then_follows_the_new_mode(env):
    async def go():
        apply(env, "full", 2)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        apply(env, "full", 2, mode_id="help")
        no_sweeps(env)
        code, out = await E.return_dialog(DEAL)
        assert code == 200 and out["epoch"] == 3
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["mode"] == "help"
        apply(env, "test", 2)
        no_sweeps(env)
        code, out = await E.return_dialog(DEAL)                                # теперь воронка сделки вне режима: отказ, но снять успели
        assert code == 409 and out["detail"] == "pipeline_not_allowed" and released()
        await drain()
    run(go())


# =============================================================================================
# блок mode_settings в ходе к ИИ-сервису
# =============================================================================================

def test_turn_has_mode_settings_for_version_2_and_has_none_for_version_1(env):
    async def go():
        turn = await env.start_turn()
        assert "mode_settings" not in turn and turn["mode"] == "test"        # версия 1: тело хода как раньше
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        apply(env, "test", 2)
        await env.say(env.msg("ещё", mid="v2-1"))
        await env.advance(11)
        second = env.turn_requests[-1]
        assert second["mode_settings"] == {"mode_id": "test", "review": {"enabled": True}} and second["mode"] == "test"
        assert sorted(second["mode_settings"]) == ["mode_id", "review"] and sorted(second["mode_settings"]["review"]) == ["enabled"]
    run(go())


@pytest.mark.parametrize("mode_id,review", [("full", True), ("full", False), ("help", True), ("intercept", False)])
def test_turn_mode_settings_follow_the_document_in_prod(env, mode_id, review):
    async def go():
        apply(env, "full", 2, mode_id=mode_id, review={"enabled": review})
        env.lead(pipeline=PIPE_RETAIL)
        turn = await env.start_turn()
        assert turn["mode"] == "prod" and turn["mode_settings"] == {"mode_id": mode_id, "review": {"enabled": review}}
    run(go())


def test_event_turns_carry_mode_settings_too(env):
    async def go():
        apply(env, "test", 2)
        turn = await take(env)
        await E._handle_lead_event(_paid_event())
        await env.advance(1)
        ev = env.turn_requests[-1]
        assert ev["event"]["type"] == "payment_received" and ev["mode_settings"]["mode_id"] == "test"
    run(go())


def test_version_2_without_mode_id_still_sends_the_derived_mode(env):
    async def go():
        env.set_settings(settings_version=2)
        turn = await env.start_turn()
        assert turn["mode_settings"] == {"mode_id": "test", "review": {"enabled": True}}
    run(go())


def test_mode_settings_are_taken_at_send_time_not_at_creation(env):
    async def go():
        apply(env, "test", 2, review={"enabled": True})
        env.lead()
        await env.say(env.msg("слово"))
        env.t += 61
        await E.tick_once()                                                   # ход создан и лежит в очереди
        queued = store.active_turn_for_deal(DEAL)
        assert json.loads(queued["payload"])["mode_settings"]["review"] == {"enabled": True}
        apply(env, "test", 2, review={"enabled": False})                      # пока он лежал, «пилот» выключили
        await E.dispatch_once()
        assert env.turn_requests[-1]["mode_settings"] == {"mode_id": "test", "review": {"enabled": False}}
    run(go())


def test_the_block_disappears_when_the_document_goes_back_to_version_1_before_sending(env):
    async def go():
        apply(env, "test", 2)
        env.lead()
        await env.say(env.msg("слово"))
        env.t += 61
        await E.tick_once()
        assert "mode_settings" in json.loads(store.active_turn_for_deal(DEAL)["payload"])
        apply(env, "test", 1)
        await E.dispatch_once()
        assert "mode_settings" not in env.turn_requests[-1] and env.turn_requests[-1]["mode"] == "test"
    run(go())


def test_contract_version_is_not_raised(env):
    assert cfg.CONTRACT_VERSION == "2"                                        # поле необязательное и обратно совместимое
    assert service._headers()["X-Contract"] == "2"
