"""Шлюз «любая воронка»: `pipelines_any` документа панели действует, только если оператор разрешил это в .env движка (`AI_PIPELINES_ANY_ALLOWED=1`).

Зачем. ИИ-сервис до своего выката отвечает `mode_mismatch` на воронки вне двух розничных, и включённый `pipelines_any` породил бы передачу
человеку и алерт на каждый ход. Пока переменная не равна «1», `settings.pipelines_any()` всегда ложь, и все точки решения (`pipeline_allowed`,
`dedup.resolve`, событие сделки из amo, `return_dialog`, инициатива, срез состояния) ведут себя как при документе без `pipelines_any`
(две воронки режима, поведение этапа 1). Всё остальное (функции из настроек, `initiative`, `mode_settings`, фильтры, бюджет, товары,
существующие сделки, подтверждения) от переменной не зависит. При «1» поведение то же, что проверяют тесты `test_ai_engine_functions.py`.

Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_pipelines_any.py -q
"""
import os
import subprocess
import sys

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store
from test_ai_engine import CHAN, CHAT, DEAL, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_functions import INITIATIVE_DEFAULT, PIPE_OTHER, PIPE_TANGEM, TWO, take, v2

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER}), test=frozenset(), at=0.0))   # на «чужую» воронку промпт есть:
    monkeypatch.setattr(prompts, "_last_ok", True)                    # здесь проверяется «режим пускает или нет», а правило «нет промпта: не пишем» - в test_ai_engine_prompts.py


def allow(env, on):
    env.mp.setattr(cfg, "PIPELINES_ANY_ALLOWED", on)


# --- сама переменная ------------------------------------------------------------------------

@pytest.mark.parametrize("value, expected", [(None, False), ("", False), ("0", False), ("true", False), ("yes", False), ("1", True), (" 1 ", True)])
def test_the_variable_is_on_only_when_it_equals_one(value, expected):
    env_vars = {k: v for k, v in os.environ.items() if k != "AI_PIPELINES_ANY_ALLOWED"}
    if value is not None:
        env_vars["AI_PIPELINES_ANY_ALLOWED"] = value
    out = subprocess.run([sys.executable, "-X", "utf8", "-c", "import ai_engine_config as c; print(c.PIPELINES_ANY_ALLOWED)"],
                         cwd=HERE, env=env_vars, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.strip().splitlines()[-1] == str(expected)


def test_the_default_in_the_test_process_is_off():
    if os.environ.get("AI_PIPELINES_ANY_ALLOWED"):
        pytest.skip("переменная задана в окружении запуска")
    assert cfg.PIPELINES_ANY_ALLOWED is False                                          # без переменной в окружении гейт закрыт


# --- настройки ------------------------------------------------------------------------------

def test_without_the_variable_pipelines_any_is_false_whatever_the_panel_sends(env):
    allow(env, False)
    v2(env, "full", pipelines_any=True)
    assert settings.pipelines_any_allowed() is False and settings.pipelines_any() is False
    assert settings.pipeline_allowed(PIPE_RETAIL) and settings.pipeline_allowed(PIPE_TANGEM)
    assert not settings.pipeline_allowed(PIPE_OTHER)                                  # как при документе без pipelines_any
    assert settings.initiative_allowed(PIPE_RETAIL) and not settings.initiative_allowed(PIPE_OTHER)
    assert settings.initiative_pipelines() == frozenset(INITIATIVE_DEFAULT)


def test_with_the_variable_pipelines_any_works_as_the_panel_asks(env):
    allow(env, True)
    v2(env, "full", pipelines_any=True)
    assert settings.pipelines_any_allowed() is True and settings.pipelines_any() is True
    assert settings.pipeline_allowed(PIPE_OTHER) and not settings.initiative_allowed(PIPE_OTHER)       # ведёт, но «только отвечает»
    v2(env, "full")
    assert settings.pipelines_any() is False and not settings.pipeline_allowed(PIPE_OTHER)             # переменная одна ничего не включает


# --- точки решения --------------------------------------------------------------------------

def test_a_foreign_pipeline_is_not_taken_without_the_variable_and_nothing_goes_to_people(env):
    async def go():
        allow(env, False)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
        assert env.alerts == [] and env.notes == [] and env.tasks_open == []
    run(go())


def test_a_foreign_pipeline_is_taken_with_the_variable_and_the_turn_says_reply_only(env):
    async def go():
        allow(env, True)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
        assert env.turn_requests[0]["mode_settings"]["initiative"] is False
    run(go())


def test_the_two_retail_pipelines_work_the_same_with_and_without_the_variable(env):
    async def go():
        for on in (False, True):
            allow(env, on)
            v2(env, "full", pipelines_any=True)
            env.lead(pipeline=PIPE_RETAIL)
            await env.say(env.msg(f"слово {on}"))
            await env.advance(61)
            assert store.get_owner(DEAL)["owner"] == "ai", on
            assert env.turn_requests[-1]["mode_settings"]["initiative"] is True, on
            store.drop_session(f"{CHAN}:{CHAT}")
            with store._connect() as conn:
                conn.execute("DELETE FROM deal_owner")
            env.turn_requests.clear()
    run(go())


def test_a_deal_moved_to_a_foreign_pipeline_is_released_without_the_variable(env):
    async def go():
        moved = {"lead_id": DEAL, "pipeline_id": PIPE_OTHER, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
        allow(env, False)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E._handle_lead_event(moved)
        assert store.get_owner(DEAL)["closed"] == 1 and store.get_owner(DEAL)["owner"] == "engine"      # поведение этапа 1
    run(go())


def test_a_deal_moved_to_a_foreign_pipeline_stays_with_the_ai_when_the_variable_is_on(env):
    async def go():
        moved = {"lead_id": DEAL, "pipeline_id": PIPE_OTHER, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
        allow(env, True)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E._handle_lead_event(moved)
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai" and not owner["closed"] and owner["pipeline_id"] == PIPE_OTHER
    run(go())


@pytest.mark.parametrize("on, code", [(False, 409), (True, 200)])
def test_return_dialog_for_a_foreign_pipeline_follows_the_variable(env, on, code):
    async def go():
        allow(env, on)
        v2(env, "full", pipelines_any=True)
        store.set_owner(DEAL, store.OWNER_HUMAN, reason="rule_A", pipeline_id=PIPE_OTHER, mode="full")
        got, out = await E.return_dialog(DEAL)
        assert got == code and (out.get("detail") == "pipeline_not_allowed" if code == 409 else out.get("ok") is True)
    run(go())


# --- срез состояния -------------------------------------------------------------------------

@pytest.mark.parametrize("on", [False, True])
def test_the_status_shows_both_the_allowance_and_the_effective_value(env, on):
    allow(env, on)
    v2(env, "full", pipelines_any=True)
    out = E.status()
    assert out["pipelines_any_allowed"] is on and out["pipelines_any"] is on
    v2(env, "full")
    out = E.status()
    assert out["pipelines_any_allowed"] is on and out["pipelines_any"] is False


def test_everything_else_of_stage_two_does_not_depend_on_the_variable(env):
    allow(env, False)
    v2(env, "full", functions={"touch_max_days": 5, "silence": {"alt_after_min": 25}}, call={"route_to_mop": False},
       filters={"payment_status": ["paid"]}, budget={"skip_from_rub": 100}, pipelines_any=True)
    assert settings.silence_config()["alt_after_s"] == 25 * 60 and settings.touch_max_ahead_s() >= 5 * 86400
    ms = settings.mode_settings(PIPE_RETAIL)
    assert ms["initiative"] is True and ms["functions"]["touch_max_days"] == 5 and ms["call"] == {"route_to_mop": False}
