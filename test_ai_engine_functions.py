"""Режимы ИИ-менеджера, этап 2 (план 20.11.2026, схема настроек: разделы 3.4, 3.5, 5, 7): функции со временем берут включение и значения
из настроек режима, «только отвечаем» вне двух воронок (инициатива), расширенный `mode_settings` в ходе.

Что проверяем:
  • блоки `functions`, `call`, `eta`, `pipelines_any`, `initiative_pipelines` читаются безопасно: нет блока или мусор, значит значение по
    умолчанию (равное сегодняшнему поведению, для лестницы тишины и окон УР запасное значение это прежние переменные окружения);
    документ версии 1 новых блоков не читает;
  • лестница тишины, окна УР, срок касания, «алерт сразу при неудачной отправке» берут значения из документа и подхватывают правку без
    перезапуска; выключенная функция не создаёт событий и снимает стоящие таймеры; в Перехвате УР, напоминания и касаний нет;
  • `pipeline_allowed` и `initiative_allowed`: две воронки, остальные, «Тест», `off`, ошибки чтения; в воронке «только отвечаем»
    нет лестницы, окон УР и касаний, а ход получает `initiative: false`;
  • `mode_settings`: форма, `initiative` при версии 2, блоки `functions`/`call`/`eta` только если они есть в документе, при версии 1 блока нет;
  • окно УР только в «ОП розница» и TangemShop (в режиме «Тест» ещё «Тест»), переменная `AI_UR_PIPELINES`, остальная инициатива прежняя;
  • совместимость: документ этапа 1 (без новых ключей) ведёт себя как раньше.

Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_functions.py -q
"""
import asyncio
import datetime
import json
import os
import subprocess
import sys

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_followup as F
import ai_engine_intercept as I
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_silence as S
import ai_engine_store as store
import ai_engine_ur as U
from test_ai_engine import CHAN, CHAT, CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_RETAIL, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_intercept import igor_deal, intercepts, setup as intercept_setup
from test_ai_engine_intercept import client as intercept_client
from test_ai_engine_silence import ai_owns_and_wrote
from test_ai_engine_ur import at, owned_and_wrote, to, waves

PIPE_TANGEM = 9822330
PIPE_DB = 11166334                      # «Работа с базой»: инициатива по умолчанию как у розницы (`INITIATIVE_PIPELINES_DEFAULT`)
PIPE_OTHER = 7_000_002                  # воронка вне двух розничных: при `pipelines_any` ИИ в ней только отвечает
HOUR = 3600
MSK = datetime.timezone(datetime.timedelta(hours=3))
OPEN_FULL = {"scope": "full", "restricted": False, "items": []}
TWO = {PIPE_RETAIL, PIPE_TANGEM}
INITIATIVE_DEFAULT = TWO | {PIPE_DB}         # воронки с инициативой, когда панель список не прислала
EVERY = {PIPE_TEST, PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER}
EVERY_FULL = EVERY - {PIPE_TEST}            # в Полном «Тест» не воронка с промптом: тестовые воронки считаются только в режиме `test` (`ai_engine_prompts`)


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    monkeypatch.setattr(cfg, "PIPELINES_ANY_ALLOWED", True)           # «любая воронка» движок разрешает переменной AI_PIPELINES_ANY_ALLOWED=1 (см. test_ai_engine_pipelines_any.py)
    monkeypatch.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER}), test=frozenset(), at=0.0))   # на «чужую» воронку тоже есть промпт:
    monkeypatch.setattr(prompts, "_last_ok", True)                    # здесь проверяется «режим пускает или нет», а правило «нет промпта: не пишем» - в test_ai_engine_prompts.py
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(E, "_send_failed_seen", {})


def v2(env, kind="test", **over):
    """Положить документ версии 2: `kind` это test | full (`mode_id` можно подменить). Остальное, в том числе `functions`, через `over`."""
    if kind == "test":
        doc = dict(mode="test", pipelines=[PIPE_TEST], mode_id="test")
    else:
        doc = dict(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, mode_id="full")
    doc.update(settings_version=2, review={"enabled": True}, deals={"items": []})
    doc.update(over)
    env.set_settings(**doc)


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


async def take(env, deal=DEAL):
    turn = await env.start_turn()
    await E.handle_result(turn["turn_id"], deal, turn["epoch"], "continue", None, None)
    env.turn_requests.clear()
    return turn


def day_of(ts):
    return datetime.datetime.fromtimestamp(ts, MSK).strftime("%Y-%m-%d")


# =============================================================================================
# чтение блоков: значения по умолчанию, документ, мусор, версия 1
# =============================================================================================

def test_defaults_equal_todays_behaviour_when_the_blocks_are_missing(env, monkeypatch):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:50")                    # как на проде: переменные не заданы, действуют значения по умолчанию
    v2(env, "full")
    assert settings.silence_config() == {"enabled": True, "alt_after_s": 3600, "alert_after_s": 3600, "alert_on_failed_send": True}
    assert settings.reminder_config() == {"enabled": True, "after_h": 3}
    assert settings.ur_config() == {"enabled": True, "wave1": "17:50", "wave2": "18:45", "late_from": "17:00"}
    assert (settings.touch_max_days(), settings.touch_max_ahead_s(), settings.touches_available()) == (7, 7 * 86400 + 3600, True)
    assert settings.call_config() == {"route_to_mop": True}
    assert settings.eta_config() == {"source": "stock_date", "person": {"id": 13963494, "name": "Екатерина Зубалий"}}
    assert settings.pipelines_any() is False and settings.initiative_pipelines() == frozenset(INITIATIVE_DEFAULT)
    assert settings.mode_settings(PIPE_RETAIL) == {"mode_id": "full", "review": {"enabled": True}, "initiative": True}   # блоков нет, и в ходе их нет


def test_values_come_from_the_document(env):
    v2(env, "full", functions={
        "silence": {"enabled": False, "alt_after_min": 30, "alert_after_min": 45, "alert_on_failed_send": False},
        "reminder": {"enabled": False, "after_h": 5},
        "ur": {"enabled": False, "wave1": "17:30", "wave2": "18:15", "late_from": "16:45"},
        "touch_max_days": 14,
    }, call={"route_to_mop": False}, eta={"source": "call_person", "person": {"id": 11513202, "name": "Гладков Александер"}},
       pipelines_any=True, initiative_pipelines=[PIPE_RETAIL])
    assert settings.silence_config() == {"enabled": False, "alt_after_s": 1800, "alert_after_s": 2700, "alert_on_failed_send": False}
    assert settings.reminder_config() == {"enabled": False, "after_h": 5}
    assert settings.ur_config() == {"enabled": False, "wave1": "17:30", "wave2": "18:15", "late_from": "16:45"}
    assert (settings.touch_max_days(), settings.touch_max_ahead_s()) == (14, 14 * 86400 + 3600)
    assert settings.call_config() == {"route_to_mop": False}
    assert settings.eta_config() == {"source": "call_person", "person": {"id": 11513202, "name": "Гладков Александер"}}
    assert settings.pipelines_any() is True and settings.initiative_pipelines() == frozenset({PIPE_RETAIL})


def test_version_1_document_ignores_every_new_block(env, monkeypatch):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:50")
    env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, pipelines_any=True, initiative_pipelines=[1],
                     functions={"silence": {"enabled": False, "alt_after_min": 5}, "ur": {"enabled": False}, "touch_max_days": 2},
                     call={"route_to_mop": False}, eta={"source": "call_person"})
    assert settings.settings_version() == 1
    assert settings.silence_config()["enabled"] is True and settings.silence_config()["alt_after_s"] == 3600
    assert settings.ur_config()["enabled"] is True and settings.touch_max_days() == 7
    assert settings.call_config() == {"route_to_mop": True} and settings.eta_config()["source"] == "stock_date"
    assert settings.pipelines_any() is False and settings.initiative_pipelines() == frozenset(INITIATIVE_DEFAULT)
    assert settings.pipeline_allowed(PIPE_OTHER) is False and settings.mode_settings(PIPE_RETAIL) is None


@pytest.mark.parametrize("bad", [4, 1441, 0, -5, "30", 30.0, True, None, [30], {"a": 1}])
def test_silence_minutes_outside_bounds_or_of_the_wrong_type_fall_back(env, bad):
    v2(env, functions={"silence": {"alt_after_min": bad, "alert_after_min": bad}})
    assert settings.silence_config()["alt_after_s"] == 3600 and settings.silence_config()["alert_after_s"] == 3600


@pytest.mark.parametrize("minutes,seconds", [(5, 300), (60, 3600), (1440, 86400)])
def test_silence_minutes_bounds_are_inclusive(env, minutes, seconds):
    v2(env, functions={"silence": {"alt_after_min": minutes, "alert_after_min": minutes}})
    assert settings.silence_config()["alt_after_s"] == seconds and settings.silence_config()["alert_after_s"] == seconds


@pytest.mark.parametrize("bad", ["no", 0, 1, None, "false", [False]])
def test_booleans_read_only_real_booleans(env, bad):
    v2(env, functions={"silence": {"enabled": bad, "alert_on_failed_send": bad}, "reminder": {"enabled": bad}}, call={"route_to_mop": bad})
    assert settings.silence_config()["enabled"] is True and settings.silence_config()["alert_on_failed_send"] is True
    assert settings.reminder_config()["enabled"] is True and settings.call_config() == {"route_to_mop": True}


@pytest.mark.parametrize("bad", [0, 31, -1, "7", 7.0, True, None, [7]])
def test_touch_days_outside_bounds_fall_back_to_seven(env, bad):
    v2(env, functions={"touch_max_days": bad})
    assert settings.touch_max_days() == 7


@pytest.mark.parametrize("days", [1, 7, 30])
def test_touch_days_bounds_are_inclusive(env, days):
    v2(env, functions={"touch_max_days": days})
    assert settings.touch_max_days() == days and settings.touch_max_ahead_s() == days * 86400 + 3600


@pytest.mark.parametrize("bad", [0, 73, -1, "3", 3.5, True, None])
def test_reminder_hours_outside_bounds_fall_back_to_three(env, bad):
    v2(env, functions={"reminder": {"after_h": bad}})
    assert settings.reminder_config()["after_h"] == 3
    v2(env, functions={"reminder": {"after_h": 72}})
    assert settings.reminder_config()["after_h"] == 72
    v2(env, functions={"reminder": {"after_h": 1}})
    assert settings.reminder_config()["after_h"] == 1


UR_DEFAULT = ("17:50", "18:45", "17:00")
UR_TABLE = {
    "свои три времени": (("17:30", "18:15", "16:45"), ("17:30", "18:15", "16:45")),
    "времена записаны без нуля": (("7:05", "18:00", "7:00"), ("07:05", "18:00", "07:00")),
    "late_from равен wave1 допустим": (("17:50", "18:45", "17:50"), ("17:50", "18:45", "17:50")),
    "wave2 равна wave1": (("17:50", "17:50", "17:00"), UR_DEFAULT),
    "wave2 раньше wave1": (("17:50", "17:10", "17:00"), UR_DEFAULT),
    "late_from позже wave1": (("17:50", "18:45", "17:51"), UR_DEFAULT),
    "двадцать пять часов": (("25:00", "26:00", "17:00"), UR_DEFAULT),
    "шестьдесят минут": (("17:60", "18:45", "17:00"), UR_DEFAULT),
    "не время": (("вечером", "18:45", "17:00"), UR_DEFAULT),
    "число вместо строки": ((1750, "18:45", "17:00"), UR_DEFAULT),
    "пустая строка": (("", "18:45", "17:00"), UR_DEFAULT),
    "нет ключа": (("17:30", None, "16:45"), UR_DEFAULT),
}


@pytest.mark.parametrize("name", list(UR_TABLE))
def test_ur_times_table(env, monkeypatch, name):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:50")
    given, expected = UR_TABLE[name]
    block = {"enabled": True, **{k: v for k, v in zip(("wave1", "wave2", "late_from"), given) if v is not None}}
    v2(env, functions={"ur": block})
    got = settings.ur_config()
    assert (got["wave1"], got["wave2"], got["late_from"]) == expected, name


def test_environment_variables_are_the_fallback_when_the_block_or_a_key_is_missing(env, monkeypatch):
    monkeypatch.setattr(cfg, "SILENCE_ALT_S", 1800)
    monkeypatch.setattr(cfg, "SILENCE_ALERT_S", 5400)
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:20")
    monkeypatch.setattr(cfg, "UR_WAVE_2", "18:30")
    monkeypatch.setattr(cfg, "UR_LATE_FROM", "16:40")
    v2(env)                                                                          # нет блока functions вовсе
    assert (settings.silence_config()["alt_after_s"], settings.silence_config()["alert_after_s"]) == (1800, 5400)
    assert settings.ur_config() == {"enabled": True, "wave1": "17:20", "wave2": "18:30", "late_from": "16:40"}
    v2(env, functions={"silence": {"alt_after_min": 20}, "ur": {"wave1": "17:00"}})   # ключи по одному: недостающее берётся из окружения
    assert (settings.silence_config()["alt_after_s"], settings.silence_config()["alert_after_s"]) == (1200, 5400)
    assert settings.ur_config() == {"enabled": True, "wave1": "17:20", "wave2": "18:30", "late_from": "16:40"}    # времена только все вместе
    v2(env, functions={"silence": {"alt_after_min": 20}, "ur": {"wave1": "17:00", "wave2": "18:00", "late_from": "16:00"}})
    assert settings.ur_config() == {"enabled": True, "wave1": "17:00", "wave2": "18:00", "late_from": "16:00"}    # документ главнее окружения


def test_garbage_environment_times_fall_back_to_the_built_in_defaults(env, monkeypatch):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "потом")
    monkeypatch.setattr(cfg, "UR_WAVE_2", "25:99")
    monkeypatch.setattr(cfg, "UR_LATE_FROM", "")
    v2(env)
    assert settings.ur_config() == {"enabled": True, "wave1": "17:50", "wave2": "18:45", "late_from": "17:00"}


def test_empty_wave_variable_means_windows_off_unless_the_document_turns_them_on(env, monkeypatch):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "")                                      # так окна выключены сегодня
    v2(env)
    assert settings.ur_config()["enabled"] is False
    v2(env, functions={"ur": {"wave1": "17:50", "wave2": "18:45", "late_from": "17:00"}})
    assert settings.ur_config()["enabled"] is False                                # ключа `enabled` нет: запасное значение из окружения
    v2(env, functions={"ur": {"enabled": True}})
    assert settings.ur_config()["enabled"] is True
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:50")
    v2(env, functions={"ur": {"enabled": False}})
    assert settings.ur_config()["enabled"] is False


def test_eta_and_call_garbage_gives_the_defaults(env):
    default_person = {"id": 13963494, "name": "Екатерина Зубалий"}
    for junk in ({"source": "tomorrow", "person": {"id": 5, "name": "Пётр"}}, {"source": 5}, {"source": None}, {}, "x", 7, [], None):
        v2(env, eta=junk)
        assert settings.eta_config()["source"] == "stock_date", junk
    for person in ({"id": "abc", "name": "Пётр"}, {"id": -5, "name": "Пётр"}, {"id": True, "name": "Пётр"}, {"id": 5, "name": ""},
                   {"id": 5, "name": "   "}, {"id": 5, "name": 7}, {"id": 5}, {"name": "Пётр"}, "Пётр", 5, None, []):
        v2(env, eta={"source": "call_person", "person": person})
        assert settings.eta_config() == {"source": "call_person", "person": default_person}, person
    v2(env, eta={"source": "call_person", "person": {"id": 77, "name": " Пётр Иванов "}})
    assert settings.eta_config()["person"] == {"id": 77, "name": "Пётр Иванов"}
    for junk in ("x", 0, None, [], {}):
        v2(env, call=junk)
        assert settings.call_config() == {"route_to_mop": True}


def test_garbage_in_the_new_blocks_does_not_raise(env, monkeypatch):
    monkeypatch.setattr(cfg, "UR_WAVE_1", "17:50")
    for junk in ("x", 5, None, [], [1], {"a": 1}):
        # `initiative_pipelines` пустым списком или списком с номером значит «нигде» и «только там»: это ответ панели, а не мусор
        v2(env, "full", functions=junk, call=junk, eta=junk, pipelines_any=junk, initiative_pipelines=junk if not isinstance(junk, list) else "x")
        assert settings.silence_config()["alt_after_s"] == 3600 and settings.ur_config()["wave1"] == "17:50"
        assert settings.touch_max_days() == 7 and settings.call_config() == {"route_to_mop": True}
        assert settings.pipelines_any() is False and settings.initiative_pipelines() == frozenset(INITIATIVE_DEFAULT)
    for junk in ("x", 100, None, [], True):
        v2(env, "full", functions={"silence": junk, "reminder": junk, "ur": junk, "touch_max_days": junk})
        assert settings.silence_config()["enabled"] is True and settings.reminder_config() == {"enabled": True, "after_h": 3}
        assert settings.ur_config()["wave2"] == "18:45" and settings.touch_max_days() == 7


def test_settings_are_read_on_every_call_so_a_panel_edit_needs_no_restart(env):
    v2(env, functions={"silence": {"alt_after_min": 30}})
    assert settings.silence_config()["alt_after_s"] == 1800
    v2(env, functions={"silence": {"alt_after_min": 90}})
    assert settings.silence_config()["alt_after_s"] == 5400
    settings.set_document(None)                                                     # документа нет совсем: значения по умолчанию, не падение
    assert settings.silence_config()["alt_after_s"] == 3600 and settings.pipelines_any() is False
    assert settings.pipeline_allowed(PIPE_TEST) is False and settings.initiative_allowed(PIPE_TEST) is False


# =============================================================================================
# воронки: можно ли вести и можно ли инициативу
# =============================================================================================

PIPE_CASES = {
    # название: (вид документа, ключи, воронки, где можно вести, где можно инициативу)
    "Тест, документ этапа 1": ("test", {}, {PIPE_TEST}, {PIPE_TEST}),
    "Полный, документ этапа 1": ("full", {}, TWO, TWO),
    "Полный, pipelines_any": ("full", {"pipelines_any": True}, EVERY_FULL, TWO),
    "Полный, pipelines_any и список из одной воронки": ("full", {"pipelines_any": True, "initiative_pipelines": [PIPE_RETAIL]}, EVERY_FULL, {PIPE_RETAIL}),
    "Полный, pipelines_any и пустой список": ("full", {"pipelines_any": True, "initiative_pipelines": []}, EVERY_FULL, set()),
    "Полный, pipelines_any и мусорный список": ("full", {"pipelines_any": True, "initiative_pipelines": "x"}, EVERY_FULL, TWO),
    "Полный, pipelines_any и список из одного мусора": ("full", {"pipelines_any": True, "initiative_pipelines": [None, "a", True, -1, 0]}, EVERY_FULL, TWO),
    "Полный, pipelines_any и мусор в списке": ("full", {"pipelines_any": True, "initiative_pipelines": [PIPE_TANGEM, None, "a"]}, EVERY_FULL, {PIPE_TANGEM}),
    "Полный, номера строками": ("full", {"pipelines_any": True, "initiative_pipelines": [str(PIPE_RETAIL)]}, EVERY_FULL, {PIPE_RETAIL}),
    "Полный, pipelines_any ложь": ("full", {"pipelines_any": False}, TWO, TWO),
    "Полный, без pipelines_any, список панели главнее": ("full", {"initiative_pipelines": [PIPE_TANGEM]}, TWO, {PIPE_TANGEM}),
    "Полный, чужая воронка в списке без pipelines_any": ("full", {"initiative_pipelines": [PIPE_OTHER]}, TWO, set()),
    "pipelines_any строкой не считается": ("full", {"pipelines_any": "true"}, TWO, TWO),
    "pipelines_any единицей не считается": ("full", {"pipelines_any": 1}, TWO, TWO),
    "Тест и pipelines_any: в Тесте инициатива как сегодня": ("test", {"pipelines_any": True}, EVERY, {PIPE_TEST, PIPE_RETAIL, PIPE_TANGEM}),
    "Тест, список панели только Тест": ("test", {"pipelines_any": True, "initiative_pipelines": [PIPE_TEST]}, EVERY, {PIPE_TEST}),
}


@pytest.mark.parametrize("name", list(PIPE_CASES))
def test_pipeline_and_initiative_decisions_table(env, name):
    kind, over, may_lead, may_initiate = PIPE_CASES[name]
    v2(env, kind, **over)
    assert {p for p in EVERY if settings.pipeline_allowed(p)} == may_lead, name
    assert {p for p in EVERY if settings.initiative_allowed(p)} == may_initiate, name


@pytest.mark.parametrize("bad", [None, 0, -1, "abc", "", True, 12.5, [PIPE_RETAIL], {}])
def test_an_unknown_pipeline_is_never_allowed(env, bad):
    v2(env, "full", pipelines_any=True)
    assert settings.pipeline_allowed(bad) is False and settings.initiative_allowed(bad) is False


def test_old_and_new_documents_without_the_new_keys_decide_the_same(env):
    for kind in ("test", "full"):
        env.set_settings(**({"mode": "test", "pipelines": [PIPE_TEST]} if kind == "test" else
                            {"mode": "prod", "pipelines": [PIPE_RETAIL, PIPE_TANGEM], "contacts": OPEN_FULL}))
        before = [(settings.pipeline_allowed(p), settings.initiative_allowed(p)) for p in (*EVERY, None, 0)]
        assert before == [(p in settings.allowed_pipelines(), p in settings.allowed_pipelines()) for p in (*EVERY, None, 0)]
        v2(env, kind)
        assert [(settings.pipeline_allowed(p), settings.initiative_allowed(p)) for p in (*EVERY, None, 0)] == before


def test_mode_off_and_read_errors_mean_nowhere(env, monkeypatch):
    v2(env, "full", pipelines_any=True)
    assert settings.pipeline_allowed(PIPE_OTHER) is True
    v2(env, "full", mode="off", mode_id="off", pipelines=[], pipelines_any=True)               # режим выключен
    assert not any(settings.pipeline_allowed(p) or settings.initiative_allowed(p) for p in EVERY)
    v2(env, "full", pipelines=[PIPE_TEST], pipelines_any=True)                                  # воронки панели не совпали с режимом: off
    assert settings.effective_mode() == "off" and not any(settings.pipeline_allowed(p) for p in EVERY)
    v2(env, "full", pipelines=[], pipelines_any=True)
    assert settings.effective_mode() == "off" and not any(settings.pipeline_allowed(p) for p in EVERY)
    v2(env, "full", pipelines_any=True)
    settings._fetched_at -= 10 ** 5                                                             # настройки протухли
    assert not any(settings.pipeline_allowed(p) or settings.initiative_allowed(p) for p in EVERY)
    settings._fetched_at += 10 ** 5
    monkeypatch.setattr(cfg, "ENABLED", False)                                                  # рубильник движка
    assert not any(settings.pipeline_allowed(p) or settings.initiative_allowed(p) for p in EVERY)


# =============================================================================================
# mode_settings: форма
# =============================================================================================

FULL_FUNCTIONS = {
    "silence": {"enabled": True, "alt_after_min": 45, "alert_after_min": 30, "alert_on_failed_send": False},
    "reminder": {"enabled": True, "after_h": 4},
    "ur": {"enabled": True, "wave1": "17:40", "wave2": "18:30", "late_from": "16:50"},
    "touch_max_days": 10,
}


def test_mode_settings_carries_the_blocks_that_are_in_the_document_and_only_those(env):
    v2(env, "full", functions=FULL_FUNCTIONS, call={"route_to_mop": False},
       eta={"source": "call_person", "person": {"id": 13794378, "name": "Сергей"}})
    assert settings.mode_settings(PIPE_RETAIL) == {
        "mode_id": "full", "review": {"enabled": True}, "initiative": True, "functions": FULL_FUNCTIONS,
        "call": {"route_to_mop": False}, "eta": {"source": "call_person", "person": {"id": 13794378, "name": "Сергей"}},
    }
    for only in ("functions", "call", "eta"):                                       # по одному: в ход попадает только то, что есть
        v2(env, "full", **{only: {"functions": FULL_FUNCTIONS, "call": {"route_to_mop": True}, "eta": {"source": "stock_date"}}[only]})
        ms = settings.mode_settings(PIPE_RETAIL)
        assert set(ms) == {"mode_id", "review", "initiative", only}, only
    v2(env, "full", functions={})                                                    # пустой блок есть: значения по умолчанию
    assert settings.mode_settings(PIPE_RETAIL)["functions"] == {
        "silence": {"enabled": True, "alt_after_min": 60, "alert_after_min": 60, "alert_on_failed_send": True},
        "reminder": {"enabled": True, "after_h": 3},
        "ur": {"enabled": bool(cfg.UR_WAVE_1), "wave1": "17:50", "wave2": "18:45", "late_from": "17:00"},
        "touch_max_days": 7,
    }
    json.dumps(settings.mode_settings(PIPE_RETAIL))                                  # уходит в теле хода: должно сериализоваться


def test_mode_settings_initiative_follows_the_pipeline_and_is_there_for_version_2_only(env):
    v2(env, "full", pipelines_any=True)
    assert settings.mode_settings(PIPE_RETAIL)["initiative"] is True
    assert settings.mode_settings(PIPE_OTHER)["initiative"] is False
    assert settings.mode_settings()["initiative"] is False                           # воронка неизвестна: инициативы нет
    assert settings.mode_settings(PIPE_OTHER, initiative=True)["initiative"] is True  # вызывающий решает сам (перехваченная сделка)
    v2(env, "test")
    assert settings.mode_settings(PIPE_TEST)["initiative"] is True and settings.mode_settings(PIPE_RETAIL)["initiative"] is False
    env.set_settings(mode="test", pipelines=[PIPE_TEST])                             # версия 1: блока нет вовсе
    assert settings.mode_settings(PIPE_TEST) is None
    v2(env, "full", mode="off", mode_id="off", pipelines=[])
    assert settings.mode_settings(PIPE_RETAIL) is None


def test_intercept_mode_has_no_ur_reminder_or_touches_and_only_the_ladder_goes_to_the_service(env):
    v2(env, "full", mode_id="intercept", functions=FULL_FUNCTIONS, call={"route_to_mop": False})
    assert settings.active_mode_id() == "intercept"
    assert settings.ur_config()["enabled"] is False and settings.reminder_config()["enabled"] is False
    assert settings.touches_available() is False
    ms = settings.mode_settings(PIPE_RETAIL)
    assert ms["functions"] == {"silence": FULL_FUNCTIONS["silence"]} and ms["call"] == {"route_to_mop": False}
    v2(env, "full", mode_id="help", functions=FULL_FUNCTIONS)
    assert settings.ur_config()["enabled"] is True and settings.touches_available() is True
    assert set(settings.mode_settings(PIPE_RETAIL)["functions"]) == {"silence", "reminder", "ur", "touch_max_days"}


# =============================================================================================
# ход к ИИ-сервису: initiative по воронке сделки, блоки функций
# =============================================================================================

@pytest.mark.parametrize("pipeline,initiative", [(PIPE_OTHER, False), (PIPE_RETAIL, True), (PIPE_TANGEM, True)])
def test_turn_gets_initiative_by_the_pipeline_of_the_deal(env, pipeline, initiative):
    async def go():
        v2(env, "full", pipelines_any=True, functions=FULL_FUNCTIONS, call={"route_to_mop": True})
        env.lead(pipeline=pipeline)
        turn = await env.start_turn()
        assert turn["mode"] == "prod" and turn["mode_settings"]["initiative"] is initiative
        assert turn["mode_settings"]["functions"] == FULL_FUNCTIONS and turn["mode_settings"]["call"] == {"route_to_mop": True}
        assert "eta" not in turn["mode_settings"]                                    # в документе блока нет, в ходе его нет
        assert store.get_owner(DEAL)["pipeline_id"] == pipeline and store.get_owner(DEAL)["mode"] == "full"
    run(go())


def test_a_reply_only_pipeline_is_not_taken_without_pipelines_any(env):
    async def go():
        v2(env, "full")
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None             # как на этапе 1: чужая воронка ИИ не нужна
    run(go())


def test_the_stage_one_document_gives_the_same_turn_as_before_plus_initiative(env):
    async def go():
        v2(env, "full")
        env.lead(pipeline=PIPE_RETAIL)
        turn = await env.start_turn()
        assert turn["mode_settings"] == {"mode_id": "full", "review": {"enabled": True}, "initiative": True}
    run(go())


def test_function_values_in_the_turn_are_taken_at_send_time(env):
    async def go():
        v2(env, "full", functions={"touch_max_days": 7})
        env.lead(pipeline=PIPE_RETAIL)
        await env.say(env.msg("слово"))
        env.t += 61
        await E.tick_once()                                                          # ход создан и лежит в очереди
        assert json.loads(store.active_turn_for_deal(DEAL)["payload"])["mode_settings"]["functions"]["touch_max_days"] == 7
        v2(env, "full", functions={"touch_max_days": 3, "silence": {"alt_after_min": 20}}, pipelines_any=True, initiative_pipelines=[])
        await E.dispatch_once()
        ms = env.turn_requests[-1]["mode_settings"]
        assert ms["functions"]["touch_max_days"] == 3 and ms["functions"]["silence"]["alt_after_min"] == 20 and ms["initiative"] is False
    run(go())


def test_an_intercepted_deal_keeps_initiative_true_in_any_pipeline(env):
    async def go():
        intercept_setup(env, mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, settings_version=2, mode_id="full",
                        review={"enabled": True}, functions=FULL_FUNCTIONS)
        igor_deal(env)                                                               # воронка вне режимов: перехват работает в любой
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        ev = intercepts(env)[0]
        assert I.is_taken(DEAL) and ev["mode_settings"]["initiative"] is True and ev["mode_settings"]["functions"] == FULL_FUNCTIONS
    run(go())


# =============================================================================================
# где ИИ ведёт сделку: возврат диалога, перенос в другую воронку
# =============================================================================================

def human_owned_deal(pipeline):
    store.set_owner(DEAL, store.OWNER_HUMAN, reason="manager_message", pipeline_id=pipeline, mode="full",
                    chat={"chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN, "chat_key": CHAT_KEY})


def test_dialog_return_checks_the_pipeline_through_pipeline_allowed(env):
    async def go():
        v2(env, "full")
        human_owned_deal(PIPE_OTHER)
        assert await E.return_dialog(DEAL) == (409, {"detail": "pipeline_not_allowed"})
        v2(env, "full", pipelines_any=True)
        code, out = await E.return_dialog(DEAL)
        assert code == 200 and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_deal_moved_out_of_the_mode_pipelines_is_released_only_without_pipelines_any(env):
    async def go():
        v2(env, "full")
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        moved = {"lead_id": DEAL, "pipeline_id": PIPE_OTHER, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
        await E._handle_lead_event(moved)
        assert store.get_owner(DEAL)["closed"] == 1 and store.get_owner(DEAL)["owner"] == "engine"      # этап 1: ушла из воронок режима
    run(go())


def test_a_deal_moved_to_another_pipeline_stays_with_the_ai_when_any_pipeline_is_allowed(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        assert store.get_owner(DEAL)["pipeline_id"] == PIPE_RETAIL
        moved = {"lead_id": DEAL, "pipeline_id": PIPE_OTHER, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
        await E._handle_lead_event(moved)
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai" and not owner["closed"] and owner["pipeline_id"] == PIPE_OTHER      # новая воронка запомнена: от неё зависит инициатива
        assert owner["epoch"] == 1
    run(go())


def test_after_a_move_the_ladder_of_that_deal_is_taken_off(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E.silence_arm(DEAL)
        assert S.get(DEAL) is not None
        await E._handle_lead_event({"lead_id": DEAL, "pipeline_id": PIPE_OTHER, "status_id": 1000, "added": False, "paid": False, "work_touched": False})
        await env.advance(HOUR + 120)
        await env.advance(5)
        assert S.get(DEAL) is None and env.turn_requests == [] and env.alerts == []
    run(go())


# =============================================================================================
# лестница тишины
# =============================================================================================

def test_ladder_times_come_from_the_document(env):
    async def go():
        v2(env, functions={"silence": {"alt_after_min": 30, "alert_after_min": 45}})
        await ai_owns_and_wrote(env)
        await env.advance(30 * 60 - 60)
        assert env.turn_requests == [] and env.alerts == []                         # ещё не 30 минут
        await env.advance(120)
        await env.advance(5)
        await env.advance(5)
        ev = env.turn_requests[-1]
        assert ev["event"]["type"] == "silent_alt" and "30 мин" in ev["event"]["reason"] and S.get(DEAL)["stage"] == 1
        await E.handle_result(ev["turn_id"], DEAL, ev["epoch"], "continue", None, None)
        await env.advance(45 * 60 - 60)
        assert env.alerts == []                                                     # ещё не 45
        await env.advance(120)
        await env.advance(5)
        assert len(env.alerts) == 1 and "больше 75 мин" in env.alerts[0] and S.get(DEAL)["stage"] == 2
    run(go())


def test_ladder_defaults_keep_the_old_texts(env):
    async def go():
        v2(env)                                                                     # документ версии 2 без блока: как всегда
        await ai_owns_and_wrote(env)
        await env.advance(HOUR + 120)
        await env.advance(5)
        await env.advance(5)
        ev = env.turn_requests[-1]
        assert ev["event"]["reason"] == "клиент молчит час после нашего сообщения"
        await E.handle_result(ev["turn_id"], DEAL, ev["epoch"], "continue", None, None)
        await env.advance(HOUR + 120)
        await env.advance(5)
        assert len(env.alerts) == 1 and "больше двух часов" in env.alerts[0]
    run(go())


def test_ladder_falls_back_to_the_environment_values(env, monkeypatch):
    async def go():
        monkeypatch.setattr(cfg, "SILENCE_ALT_S", 1200)
        monkeypatch.setattr(cfg, "SILENCE_ALERT_S", 1200)
        v2(env)                                                                     # блока нет: действует окружение
        await ai_owns_and_wrote(env)
        await env.advance(1200 + 60)
        await env.advance(5)
        await env.advance(5)
        assert env.turn_requests[-1]["event"]["type"] == "silent_alt"
    run(go())


def test_a_panel_edit_moves_the_deadline_of_a_countdown_that_is_already_running(env):
    async def go():
        v2(env)
        await ai_owns_and_wrote(env)
        await env.advance(20 * 60)
        assert env.turn_requests == []
        v2(env, functions={"silence": {"alt_after_min": 15}})                         # срок уменьшили, пока шёл отсчёт: перезапуск не нужен
        await env.advance(5)
        await env.advance(5)
        await env.advance(5)
        assert env.turn_requests and env.turn_requests[-1]["event"]["type"] == "silent_alt"
    run(go())


def test_disabling_the_ladder_removes_standing_countdowns_and_creates_no_events(env):
    async def go():
        v2(env)
        await ai_owns_and_wrote(env)
        assert S.get(DEAL) is not None
        notes_before = len(env.notes)
        v2(env, functions={"silence": {"enabled": False}})
        await env.advance(5)
        assert S.get(DEAL) is None                                                  # стоящий отсчёт снят
        await env.advance(5 * HOUR)
        assert env.turn_requests == [] and env.alerts == [] and len(env.notes) == notes_before and env.tasks_open == []
        await E.silence_arm(DEAL)                                                   # и новый не взводится, пока функция выключена
        assert S.get(DEAL) is None and U.last_sent(DEAL) is not None                # время сообщения для окон УР при этом пишется
        v2(env)                                                                     # включили снова
        await env.advance(5 * HOUR)
        assert S.get(DEAL) is None and env.turn_requests == []                      # старый отсчёт не воскрес
        await E.silence_arm(DEAL)
        assert S.get(DEAL)["armed_at"] == env.t                                     # новый начался с нового сообщения ИИ
    run(go())


def test_disabling_in_the_second_hour_means_no_alert(env):
    async def go():
        v2(env)
        await ai_owns_and_wrote(env)
        await env.advance(HOUR + 120)
        await env.advance(5)
        await env.advance(5)
        ev = env.turn_requests[-1]
        assert ev["event"]["type"] == "silent_alt" and S.get(DEAL)["stage"] == 1
        await E.handle_result(ev["turn_id"], DEAL, ev["epoch"], "continue", None, None)
        v2(env, functions={"silence": {"enabled": False}})
        await env.advance(3 * HOUR)
        assert env.alerts == [] and S.get(DEAL) is None
    run(go())


def test_the_ladder_is_off_in_a_reply_only_pipeline(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await take(env)
        await E.silence_arm(DEAL)
        assert S.get(DEAL) is None and U.last_sent(DEAL) is not None
        await env.advance(5 * HOUR)
        assert env.turn_requests == [] and env.alerts == []
    run(go())


def test_the_ladder_in_the_two_retail_pipelines_works_with_pipelines_any(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_TANGEM)
        await take(env)
        await E.silence_arm(DEAL)
        assert S.get(DEAL) is not None
        await env.advance(HOUR + 120)
        await env.advance(5)
        await env.advance(5)
        assert env.turn_requests[-1]["event"]["type"] == "silent_alt"
    run(go())


def test_queue_event_refuses_initiative_events_when_the_function_is_off_or_the_pipeline_is_reply_only(env):
    async def go():
        v2(env, "full", pipelines_any=True, functions={"ur": {"enabled": False}, "silence": {"enabled": False}})
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        owner = store.get_owner(DEAL)
        for kind in ("ur_wave", "silent_alt"):
            await E._queue_event(owner, kind)
            assert not store.get_session(CHAT_KEY).get("pending_event"), kind
        v2(env, "full", pipelines_any=True, functions={"ur": {"enabled": True}, "silence": {"enabled": True}}, initiative_pipelines=[PIPE_TANGEM])
        for kind in ("ur_wave", "silent_alt"):
            await E._queue_event(owner, kind)
            assert not store.get_session(CHAT_KEY).get("pending_event"), kind        # воронка сделки «только отвечаем»
        v2(env, "full", pipelines_any=True, functions={"ur": {"enabled": True}, "silence": {"enabled": True}})
        await E._queue_event(owner, "ur_wave")
        assert json.loads(store.get_session(CHAT_KEY)["pending_event"])["type"] == "ur_wave"
        await E._queue_event(owner, "payment_received")                              # оплата и возврат диалога не инициатива: функциями не гасятся
    run(go())


# =============================================================================================
# «алерт сразу при неудачной отправке»
# =============================================================================================

REASON = "в Telegram по этому номеру нет аккаунта"


def own_deal():
    store.set_owner(DEAL, store.OWNER_AI, reason="t", chat={"chat_key": "k", "chat_id": "1", "channel_id": "c"}, pipeline_id=PIPE_TEST)


def test_failed_send_alert_can_be_switched_off(env):
    async def go():
        env.lead()
        own_deal()
        v2(env, functions={"silence": {"alert_on_failed_send": False}})
        out = await E.send_failed(DEAL, "X", REASON)
        assert out == {"ok": True, "deduped": False, "skipped": "alert_on_failed_send_off"}
        assert env.notes == [] and env.alerts == [] and env.tasks_open == []
        assert store.get_owner(DEAL)["owner"] == store.OWNER_AI                       # ИИ остаётся владельцем, как и при включённом
        v2(env, functions={"silence": {"alert_on_failed_send": True}})                # включили: то же событие уже не считается повтором
        out = await E.send_failed(DEAL, "X", REASON)
        assert out == {"ok": True, "deduped": False} and len(env.alerts) == 1 and len(env.notes) == 1 and len(env.tasks_open) == 1
    run(go())


def test_failed_send_alert_does_not_depend_on_the_ladder_switch(env):
    async def go():
        env.lead()
        own_deal()
        v2(env, functions={"silence": {"enabled": False}})                            # лестница выключена, ключ «алерт сразу» по умолчанию да
        out = await E.send_failed(DEAL, "X", REASON)
        assert out == {"ok": True, "deduped": False} and len(env.alerts) == 1
    run(go())


# =============================================================================================
# окна УР
# =============================================================================================

def test_ur_times_come_from_the_document(env):
    async def go():
        v2(env, functions={"ur": {"enabled": True, "wave1": "17:30", "wave2": "18:15", "late_from": "16:45"}})
        await owned_and_wrote(env, 14)
        await to(env, 17, 29)
        assert waves(env) == []
        await to(env, 17, 31)
        assert len(waves(env)) == 1 and "волна 1" in waves(env)[0]["event"]["reason"] and "17:30" in waves(env)[0]["event"]["reason"]
        await to(env, 17, 55)
        assert len(waves(env)) == 1                                                  # прежнее 17:50 уже ничего не добавляет
    run(go())


def test_the_late_border_from_the_document_moves_a_deal_to_the_second_window(env):
    async def go():
        v2(env, functions={"ur": {"enabled": True, "wave1": "17:30", "wave2": "18:15", "late_from": "16:45"}})
        await owned_and_wrote(env, 16, 50)                                           # писали в 16:50: позже границы 16:45
        await to(env, 17, 31)
        assert waves(env) == []                                                      # первое окно пропущено
        await to(env, 18, 14)
        assert waves(env) == []
        await to(env, 18, 16)
        assert len(waves(env)) == 1 and "волна 2" in waves(env)[0]["event"]["reason"] and "18:15" in waves(env)[0]["event"]["reason"]
    run(go())


def test_ur_times_fall_back_to_the_environment_when_the_document_has_none(env, monkeypatch):
    async def go():
        monkeypatch.setattr(cfg, "UR_WAVE_2", "19:10")
        monkeypatch.setattr(cfg, "UR_LATE_FROM", "17:30")
        v2(env)
        await owned_and_wrote(env, 17, 40)                                           # писали в 17:40: для окружения (граница 17:30) это поздно
        await to(env, 17, 55)
        assert waves(env) == []
        await to(env, 19, 5)
        assert waves(env) == []
        await to(env, 19, 11)
        assert len(waves(env)) == 1 and "волна 2" in waves(env)[0]["event"]["reason"] and "19:10" in waves(env)[0]["event"]["reason"]
    run(go())


def test_a_broken_set_of_ur_times_in_the_document_means_the_defaults(env):
    async def go():
        v2(env, functions={"ur": {"enabled": True, "wave1": "18:50", "wave2": "17:45", "late_from": "17:00"}})      # wave2 раньше wave1
        await owned_and_wrote(env, 14)
        await to(env, 17, 51)
        assert len(waves(env)) == 1 and "17:50" in waves(env)[0]["event"]["reason"]
    run(go())


def test_ur_windows_can_be_switched_off_in_the_document(env):
    async def go():
        v2(env, functions={"ur": {"enabled": False}})
        await owned_and_wrote(env, 14)
        await to(env, 17, 55)
        await to(env, 19, 0)
        assert waves(env) == [] and U.state(day_of(at(12)), DEAL, 1) is None
    run(go())


def test_switching_ur_off_removes_the_standing_second_window_for_today(env):
    async def go():
        v2(env)
        await owned_and_wrote(env, 17, 10)                                           # писали поздно: сделка ждёт второго окна
        await to(env, 17, 51)
        day = day_of(at(12))
        assert U.state(day, DEAL, 1) == "deferred"
        v2(env, functions={"ur": {"enabled": False}})
        await to(env, 18, 0)
        assert U.state(day, DEAL, 1) == "cancelled"                                  # стоящий таймер снят
        v2(env)                                                                      # включили снова до вечера
        await to(env, 18, 50)
        await to(env, 19, 30)
        assert waves(env) == []                                                      # второе окно по этой сделке сегодня уже не придёт
    run(go())


def test_switching_ur_on_before_the_first_window_works_normally(env):
    async def go():
        v2(env, functions={"ur": {"enabled": False}})
        await owned_and_wrote(env, 14)
        await to(env, 16, 0)
        v2(env)
        await to(env, 17, 51)
        assert len(waves(env)) == 1 and "волна 1" in waves(env)[0]["event"]["reason"]
    run(go())


@pytest.mark.parametrize("kind,mode_id,expected", [("full", "full", 1), ("full", "help", 1), ("test", "test", 1), ("full", "intercept", 0)])
def test_ur_windows_exist_in_every_mode_but_intercept(env, kind, mode_id, expected):
    async def go():
        v2(env, kind, mode_id=mode_id)
        env.lead(pipeline=PIPE_TEST if kind == "test" else PIPE_RETAIL)
        await owned_and_wrote(env, 14)
        await to(env, 17, 55)
        assert len(waves(env)) == expected
    run(go())


def test_no_ur_window_in_a_reply_only_pipeline(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await owned_and_wrote(env, 14)
        await to(env, 17, 55)
        await to(env, 19, 30)
        assert waves(env) == [] and U.state(day_of(at(12)), DEAL, 1) is None
        v2(env, "full", pipelines_any=True, initiative_pipelines=[PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER])      # панель дала воронке инициативу
        await to(env, 19, 40)
        assert waves(env) == []                                                      # но окно УР только в двух розничных воронках
    run(go())


# --- воронки окна УР (решение Кати 26.11.2026): «ОП розница» и TangemShop, в режиме «Тест» ещё «Тест» -----------------------------

PIPE_WAITLIST = 10611934                # «Лист ожидания»
LED = [PIPE_RETAIL, PIPE_TANGEM, PIPE_DB, PIPE_WAITLIST, PIPE_OTHER]


def led_with_initiative(env, monkeypatch):
    """Режим «Полный»: на все воронки `LED` есть промпт (ИИ вправе вести их сделки) и панель дала им инициативу."""
    monkeypatch.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset(LED), test=frozenset(), at=0.0))
    v2(env, "full", pipelines_any=True, initiative_pipelines=LED)


@pytest.mark.parametrize("pipeline,expected", [(PIPE_RETAIL, 1), (PIPE_TANGEM, 1), (PIPE_DB, 0), (PIPE_WAITLIST, 0), (PIPE_OTHER, 0)])
def test_ur_window_goes_only_to_the_two_retail_pipelines(env, monkeypatch, pipeline, expected):
    async def go():
        led_with_initiative(env, monkeypatch)
        env.lead(pipeline=pipeline)
        await owned_and_wrote(env, 14)
        assert store.get_owner(DEAL)["owner"] == store.OWNER_AI and settings.initiative_allowed(pipeline)
        await to(env, 17, 55)
        assert len(waves(env)) == expected
        if not expected:
            await to(env, 19, 30)
            assert waves(env) == [] and U.state(day_of(at(12)), DEAL, 1) is None
    run(go())


@pytest.mark.parametrize("moved_to,expected", [(PIPE_DB, 0), (PIPE_WAITLIST, 0), (PIPE_TANGEM, 1)])
def test_a_deal_moved_between_the_windows_gets_the_second_one_only_in_a_window_pipeline(env, monkeypatch, moved_to, expected):
    async def go():
        led_with_initiative(env, monkeypatch)
        env.lead(pipeline=PIPE_RETAIL)
        await owned_and_wrote(env, 17, 10)                                           # писали поздно: сделка ждёт второго окна
        await to(env, 17, 55)
        day = day_of(at(12))
        assert U.state(day, DEAL, 1) == "deferred" and waves(env) == []
        env.lead(pipeline=moved_to)
        await E._handle_lead_event(E.extract_lead_event(DEAL, moved_to, 1000, {"leads": {"update": {"0": {"id": str(DEAL)}}}}))
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["pipeline_id"]) == (store.OWNER_AI, moved_to)       # ИИ сделку ведёт и дальше
        await to(env, 18, 46)
        assert len(waves(env)) == expected
        assert (U.state(day, DEAL, 2) == "fired") is bool(expected)
    run(go())


def test_queue_event_refuses_the_ur_wave_outside_the_window_pipelines_but_keeps_other_initiative(env, monkeypatch):
    async def go():
        led_with_initiative(env, monkeypatch)
        env.lead(pipeline=PIPE_DB)
        await take(env)
        owner = store.get_owner(DEAL)
        await E._queue_event(owner, "ur_wave")
        assert not store.get_session(CHAT_KEY).get("pending_event")
        await E._queue_event(owner, "silent_alt")                                    # лестница тишины в «Работе с базой» как была
        assert json.loads(store.get_session(CHAT_KEY)["pending_event"])["type"] == "silent_alt"
    run(go())


def test_the_test_pipeline_is_in_the_ur_window_only_in_the_test_mode(env):
    v2(env)
    assert settings.ur_window_pipelines() == frozenset(TWO | {PIPE_TEST}) and settings.ur_window_allowed(PIPE_TEST)
    v2(env, "full", pipelines_any=True)
    assert settings.ur_window_pipelines() == frozenset(TWO) and not settings.ur_window_allowed(PIPE_TEST)
    assert not settings.ur_window_allowed(None) and settings.ur_window_allowed(str(PIPE_RETAIL))
    env.set_settings(mode="off")
    assert settings.ur_window_pipelines() == frozenset(TWO)


@pytest.mark.parametrize("raw,expected", [
    ("", TWO), ("   ", TWO), (",,", TWO), ("all", TWO), ("*", TWO), ("0,-5", TWO), ("10593102;9822330", TWO), ("²", TWO),
    ("11166334", {PIPE_DB}), (" 9822330 , x,", {PIPE_TANGEM}), ("10593102,9822330,11166334", TWO | {PIPE_DB}),
])
def test_ur_pipelines_variable_never_means_all_pipelines(raw, expected):
    assert cfg._parse_pipelines(raw, cfg.UR_PIPELINES_DEFAULT) == frozenset(expected)


@pytest.mark.parametrize("raw,expected", [("11166334", [PIPE_DB]), ("все", sorted(TWO))])
def test_ur_pipelines_are_read_from_the_environment(raw, expected):
    out = subprocess.run([sys.executable, "-c", "import ai_engine_config as c; print(sorted(c.UR_PIPELINES))"], cwd=os.path.dirname(os.path.abspath(__file__)),
                         env={**os.environ, "AI_UR_PIPELINES": raw}, capture_output=True, text=True, check=True)
    assert out.stdout.strip() == str(expected)


# =============================================================================================
# отложенные касания
# =============================================================================================

HOURS = 3600


async def register(env, hours):
    """Ход ИИ и регистрация касания через `hours` часов от сейчас (как это делает сервис до итога хода): возвращает ход, код и тело ответа."""
    if not env.turn_requests:
        turn = await env.start_turn()
    else:
        turn = env.turn_requests[-1]
    code, out = await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + hours * HOURS, "просьба", "черновик")
    return turn, code, out


@pytest.mark.parametrize("days,ok", [(1, True), (2, True), (3, False), (8, False)])
def test_touch_horizon_comes_from_touch_max_days(env, days, ok):
    async def go():
        v2(env, functions={"touch_max_days": 2})
        turn, code, out = await register(env, days * 24)
        assert out["allowed"] is ok and (ok or (code, out["reason"]) == (422, "too_far"))
    run(go())


def test_touch_horizon_can_be_the_maximum(env):
    async def go():
        v2(env, functions={"touch_max_days": 30})
        assert (await register(env, 29 * 24))[2]["allowed"] is True
    run(go())


def test_touch_horizon_stays_seven_days_for_an_old_document(env):
    async def go():
        turn, code, out = await register(env, 8 * 24)                                # документ версии 1: прежние семь суток
        assert out == {"allowed": False, "reason": "too_far"}
        turn, code, out = await register(env, 6 * 24)
        assert out["allowed"] is True
    run(go())


def test_touch_too_soon_is_unchanged(env):
    async def go():
        v2(env, functions={"touch_max_days": 30})
        turn, code, out = await register(env, 0.05)
        assert out == {"allowed": False, "reason": "too_soon"}
    run(go())


def test_no_touches_in_the_intercept_mode(env):
    async def go():
        v2(env, "full", mode_id="intercept")
        env.lead(pipeline=PIPE_RETAIL)
        turn, code, out = await register(env, 24)
        assert (code, out) == (422, {"allowed": False, "reason": "touch_unavailable"}) and F.pending_for_deal(DEAL) is None
    run(go())


def test_no_touches_in_a_reply_only_pipeline(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        turn, code, out = await register(env, 24)
        assert (code, out) == (422, {"allowed": False, "reason": "initiative_off"}) and F.pending_for_deal(DEAL) is None
        v2(env, "full", pipelines_any=True, initiative_pipelines=[PIPE_RETAIL, PIPE_OTHER])
        code, out = (await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + 24 * HOURS, "просьба", ""))
        assert code == 200 and out["allowed"] is True
    run(go())


def test_a_waiting_touch_is_cancelled_loudly_when_its_pipeline_became_reply_only(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        turn, code, out = await register(env, 24)
        assert out["allowed"] is True
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        await drain()
        env.turn_requests.clear()
        v2(env, "full", pipelines_any=True, initiative_pipelines=[PIPE_TANGEM])        # панель перевела «ОП розница» в «только отвечаем»
        await env.advance(24 * HOURS + 120)
        await env.advance(5)
        await env.advance(5)
        await drain()
        assert env.turn_requests == [] and F.pending_for_deal(DEAL) is None             # касание не ушло
        assert any("ИИ не смог написать клиенту" in a for a in env.alerts)               # а людям об этом сказано: обещание клиенту не выполнено
        assert any("в воронке сделки ИИ только отвечает" in text for _, text in env.notes)
    run(go())


def test_a_waiting_touch_still_fires_in_the_two_pipelines(env):
    async def go():
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        turn, code, out = await register(env, 24)
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        await drain()
        env.turn_requests.clear()
        await env.advance(24 * HOURS + 120)
        await env.advance(5)
        await env.advance(5)
        assert env.turn_requests and env.turn_requests[-1]["event"]["type"] == "followup_due"
    run(go())


# =============================================================================================
# срез состояния
# =============================================================================================

def test_status_shows_what_the_engine_takes_from_the_settings(env):
    v2(env, "full", pipelines_any=True, functions={"touch_max_days": 5, "silence": {"alt_after_min": 25}})
    out = E.status()
    assert out["pipelines_any"] is True and out["pipelines_any_allowed"] is True and out["initiative_pipelines"] == sorted(INITIATIVE_DEFAULT)
    assert out["ur_window_pipelines"] == sorted(TWO)
    assert out["functions"]["touch_max_days"] == 5 and out["functions"]["silence"]["alt_after_min"] == 25


def test_status_shows_the_ur_window_pipelines_of_the_active_mode_and_the_override(env, monkeypatch):
    v2(env)
    assert E.status()["ur_window_pipelines"] == sorted(TWO | {PIPE_TEST})
    monkeypatch.setattr(cfg, "UR_PIPELINES", frozenset({PIPE_DB}))
    v2(env, "full", pipelines_any=True)
    assert E.status()["ur_window_pipelines"] == [PIPE_DB]
