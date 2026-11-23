"""Нет промпта: не пишем (решение Кати 23.11.2026, `ai_engine_prompts`): ИИ берёт и ведёт сделки только в воронках, для которых в ИИ-сервисе есть промпт,
независимо от настроек режима (`pipelines_any`, воронки режима).

Что проверяем:
  • разбор ответа `GET /v1/funnels`: объединение `pipelines` у воронок с `has_prompt: true`; мусор не принимается целиком;
  • отказоустойчивость, строго «не шире, чем сейчас»: сервис не ответил или ответил мусором - последний удачный ответ, удачного не было - статический запас
    (две воронки розницы); ни одна ошибка множество не расширяет; опрос не чаще раза в минуту;
  • шлюз `settings.pipeline_allowed` и всё, что за ним: `pipelines_any` не открывает воронку без промпта, воронка с промптом открывается только если её пускает режим;
    сделка в воронке без промпта не берётся (сообщение клиента, `give_deal`, `return_dialog`), перенос взятой сделки в такую воронку снимает её (`left_pipeline`),
    `permit` отказывает, «Тест» в режиме `test` работает всегда, перехват живёт по своим правилам;
  • срез `GET /`: `prompt_pipelines`, `prompt_pipelines_source`, `prompt_pipelines_age_s`.

Сети нет: подставный мир из test_ai_engine.py, ручка `/v1/funnels` подставная. Запуск: python -m pytest test_ai_engine_prompts.py -q
"""
import asyncio
import types

import httpx
import pytest

import ai_engine as E
import ai_engine_client as client
import ai_engine_config as cfg
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store
from test_ai_engine import CHAN, CHAT, DEAL, PIPE_RETAIL, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_functions import PIPE_OTHER, PIPE_TANGEM, TWO, drain, take, v2
from test_ai_engine_give import give
from test_ai_engine_intercept import PIPE_ANY, igor_deal, intercepts, setup as intercept_setup
from test_ai_engine_intercept import client as intercept_client

OPT = {"slug": "opt", "title": "Опт", "pipelines": [PIPE_OTHER], "has_prompt": True, "prompt_source": "fallback"}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(prompts, "_good", None)                       # состояние модуля: как после старта процесса, а после теста прежнее
    monkeypatch.setattr(prompts, "_last_ok", None)
    monkeypatch.setattr(cfg, "PIPELINES_ANY_ALLOWED", True)           # «любая воронка» разрешена: проверяем, что и она не открывает воронку без промпта
    monkeypatch.setattr(E, "_last_mode_id", None)


def answer(retail=True, tangem=True, extra=(), test=(PIPE_TEST,), **over):
    body = {
        "contract": "2", "generated_at": "2026-11-23T10:00:00Z",
        "funnels": [
            {"slug": "sunscrypt", "title": "Sunscrypt (ОП розница)", "pipelines": [PIPE_RETAIL], "has_prompt": retail, "prompt_source": "var:SUNSCRYPT_PROMPT"},
            {"slug": "tangem", "title": "Tangem (TangemShop)", "pipelines": [PIPE_TANGEM], "has_prompt": tangem, "prompt_source": "image:tangem"},
            *extra,
        ],
        "test_pipelines": list(test),
    }
    body.update(over)
    return body


class Svc:
    """Подставной ИИ-сервис: `GET /v1/funnels` отвечает по команде теста, остальные ручки отдаёт обработчик мира из `Env`."""

    def __init__(self, env, body=None):
        self.requests: list[httpx.Request] = []
        self.body = answer() if body is None else body
        self.status = 200
        self.raw: bytes | None = None          # сырое тело вместо JSON
        self.down = False
        self.timeout = False
        original = client._transport.handler

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path != "/v1/funnels":
                return original(request)
            self.requests.append(request)
            if self.down:
                raise httpx.ConnectError("нет связи")
            if self.timeout:
                raise httpx.ReadTimeout("ответ не пришёл")
            if self.raw is not None:
                return httpx.Response(self.status, content=self.raw)
            return httpx.Response(self.status, json=self.body)

        env.mp.setattr(client, "_transport", httpx.MockTransport(handler))

    async def poll(self):
        return await prompts.fetch_once()


def known(env, *pipelines, test=()):
    """Словарь сервиса уже получен: ответ принят, воронки с промптом `pipelines`."""
    env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset(pipelines), test=frozenset(test), at=prompts._now()))
    env.mp.setattr(prompts, "_last_ok", True)


def moved(pipeline, lead_id=DEAL):
    return {"lead_id": lead_id, "pipeline_id": pipeline, "status_id": 1000, "added": False, "paid": False, "work_touched": False}


# =============================================================================================
# разбор ответа
# =============================================================================================

def test_parse_takes_the_union_of_funnels_with_a_prompt():
    snap = prompts.parse(answer(extra=(OPT,)))
    assert snap.prompt == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER}) and snap.test == frozenset({PIPE_TEST})


def test_parse_ignores_funnels_without_a_prompt():
    snap = prompts.parse(answer(tangem=False, extra=({**OPT, "has_prompt": False},)))
    assert snap.prompt == frozenset({PIPE_RETAIL})


def test_one_funnel_may_have_several_pipelines_and_a_pipeline_may_repeat():
    body = answer(extra=({**OPT, "pipelines": [PIPE_OTHER, PIPE_RETAIL, PIPE_OTHER]},))
    assert prompts.parse(body).prompt == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})


def test_parse_accepts_an_answer_without_contract_and_an_empty_dictionary():
    body = answer()
    del body["contract"]
    assert prompts.parse(body).prompt == frozenset(TWO)
    empty = prompts.parse({"contract": "2", "funnels": [], "test_pipelines": []})
    assert empty is not None and empty.prompt == frozenset() and empty.test == frozenset()      # «нигде» это сужение, а не мусор


def test_parse_accepts_an_answer_without_test_pipelines():
    body = answer()
    del body["test_pipelines"]
    assert prompts.parse(body).test == frozenset()


GARBAGE = [
    pytest.param(None, id="none"),
    pytest.param([], id="список"),
    pytest.param("funnels", id="строка"),
    pytest.param({}, id="пустой объект"),
    pytest.param({"contract": "2"}, id="нет funnels"),
    pytest.param(answer(contract="3"), id="чужой contract"),
    pytest.param(answer(funnels="sunscrypt"), id="funnels не список"),
    pytest.param(answer(funnels=[["sunscrypt"]]), id="воронка не объект"),
    pytest.param(answer(extra=({"slug": "x", "has_prompt": True},)), id="нет pipelines"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [PIPE_OTHER]},)), id="нет has_prompt"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": PIPE_OTHER, "has_prompt": True},)), id="pipelines не список"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [str(PIPE_OTHER)], "has_prompt": True},)), id="номер строкой"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [True], "has_prompt": True},)), id="номер bool"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [0], "has_prompt": True},)), id="номер ноль"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [-5], "has_prompt": True},)), id="номер отрицательный"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [1.5], "has_prompt": True},)), id="номер дробный"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [PIPE_OTHER], "has_prompt": "true"},)), id="has_prompt строкой"),
    pytest.param(answer(extra=({"slug": "x", "pipelines": [PIPE_OTHER], "has_prompt": 1},)), id="has_prompt числом"),
    pytest.param(answer(test="8642414"), id="test_pipelines не список"),
    pytest.param(answer(test=["8642414"]), id="test_pipelines строкой"),
]


@pytest.mark.parametrize("body", GARBAGE)
def test_garbage_is_rejected_whole(body):
    assert prompts.parse(body) is None


# =============================================================================================
# опрос и отказоустойчивость
# =============================================================================================

def test_before_any_answer_the_two_retail_pipelines_are_the_reserve():
    assert prompts.source() == "static" and prompts.age_s() is None
    assert prompts.pipelines_for("prod") == cfg.MODE_PIPELINES["prod"] == frozenset(TWO)
    assert prompts.pipelines_for("test") == frozenset(TWO) | {PIPE_TEST}


def test_a_good_answer_replaces_the_reserve_and_the_request_has_the_agreed_shape(env):
    async def go():
        svc = Svc(env, answer(extra=(OPT,)))
        assert await svc.poll() is True
        assert prompts.source() == "service"
        assert prompts.pipelines_for("prod") == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})
        assert len(svc.requests) == 1
        req = svc.requests[0]
        assert (req.method, req.url.path) == ("GET", "/v1/funnels")
        assert req.headers["x-service-token"] == "svc-token" and req.headers["x-contract"] == "2"
    run(go())


def test_the_answer_may_narrow_the_set_below_the_reserve(env):
    async def go():
        svc = Svc(env, answer(tangem=False))
        assert await svc.poll() is True
        assert prompts.pipelines_for("prod") == frozenset({PIPE_RETAIL})            # TangemShop лишился промпта: сужение принимается сразу
    run(go())


def test_the_service_going_down_keeps_the_last_good_answer(env):
    async def go():
        svc = Svc(env, answer(extra=(OPT,)))
        await svc.poll()
        env.mp.setattr(prompts, "_now", lambda: prompts._good.at + 125)
        svc.down = True
        assert await svc.poll() is False
        assert prompts.source() == "cached" and prompts.age_s() == 125
        assert prompts.pipelines_for("prod") == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})
        svc.down = False
        assert await svc.poll() is True and prompts.source() == "service"                  # ожил: снова свежие данные
    run(go())


FAILURES = [
    pytest.param(dict(down=True), id="сеть"),
    pytest.param(dict(timeout=True), id="таймаут"),
    pytest.param(dict(status=404), id="404"),
    pytest.param(dict(status=426), id="426 версия контракта"),
    pytest.param(dict(status=500), id="500"),
    pytest.param(dict(raw=b"<html>502</html>"), id="не JSON"),
    pytest.param(dict(raw=b"[1, 2, 3]"), id="JSON-список"),
    pytest.param(dict(raw=b"null"), id="null"),
    pytest.param(dict(body={"contract": "2", "funnels": "много"}), id="мусор в funnels"),
    pytest.param(dict(body=answer(extra=({"slug": "x", "pipelines": ["7000002"], "has_prompt": True},))), id="мусор в номере"),
    pytest.param(dict(body=answer(contract="9", extra=(OPT,))), id="чужой contract"),
]


@pytest.mark.parametrize("fail", FAILURES)
def test_a_failure_never_widens_the_set_after_a_narrow_answer(env, fail):
    """Последний удачный ответ уже уже запаса (только розница): любой сбой оставляет ЕГО, а не расширяет до запаса или до того, что в мусоре."""
    async def go():
        svc = Svc(env, answer(tangem=False))
        await svc.poll()
        narrow = prompts.pipelines_for("prod")
        assert narrow == frozenset({PIPE_RETAIL})
        for key, value in fail.items():
            setattr(svc, key, value)
        assert await svc.poll() is False
        assert prompts.pipelines_for("prod") == narrow and prompts.source() == "cached"
        assert prompts.pipelines_for("test") <= narrow | {PIPE_TEST}
        assert not settings.pipeline_allowed(PIPE_OTHER)
    run(go())


@pytest.mark.parametrize("fail", FAILURES)
def test_without_any_good_answer_a_failure_leaves_the_static_reserve(env, fail):
    async def go():
        svc = Svc(env)
        for key, value in fail.items():
            setattr(svc, key, value)
        assert await svc.poll() is False
        assert prompts.source() == "static" and prompts.age_s() is None
        assert prompts.pipelines_for("prod") == frozenset(TWO)
    run(go())


def test_an_unconfigured_service_leaves_the_static_reserve(env):
    async def go():
        env.mp.setattr(cfg, "SERVICE_URL", "")
        svc = Svc(env)
        assert await svc.poll() is False and svc.requests == []
        assert prompts.source() == "static" and prompts.pipelines_for("prod") == frozenset(TWO)
    run(go())


def test_the_loop_asks_once_per_minute_and_survives_a_crash(env, monkeypatch):
    async def go():
        svc = Svc(env, answer(extra=(OPT,)))
        delays = []

        async def fake_sleep(delay):
            delays.append(delay)
            if len(delays) == 2:
                svc.down = True
            if len(delays) == 3:
                async def boom():
                    raise RuntimeError("неожиданный сбой")

                monkeypatch.setattr(client, "get_funnels", boom)                          # даже непредвиденная ошибка не убивает цикл
            if len(delays) == 5:
                raise asyncio.CancelledError

        monkeypatch.setattr(prompts, "asyncio", types.SimpleNamespace(sleep=fake_sleep, CancelledError=asyncio.CancelledError))
        with pytest.raises(asyncio.CancelledError):
            await prompts.poll_loop()
        assert delays == [60] * 5 and prompts.REFRESH_S == 60                              # между опросами ровно минута
        assert len(svc.requests) == 3                                                       # первые два удались, третий упёрся в упавший сервис, четвёртый и пятый упали в `boom` до сети
        assert prompts.source() == "cached" and prompts.pipelines_for("prod") == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})
    run(go())


def test_the_engine_starts_the_poll_loop_and_stops_it(env):
    async def go():
        svc = Svc(env)
        env.mp.setattr(cfg, "PANEL_URL", "")
        await E.init()
        try:
            await asyncio.sleep(0.2)
            assert len(svc.requests) == 1 and prompts.source() == "service"                 # первый опрос сразу при старте, дальше раз в минуту
            assert E.status()["prompt_pipelines_source"] == "service"
        finally:
            await E.shutdown()
        assert E._tasks == []
    run(go())


# =============================================================================================
# воронки по режимам
# =============================================================================================

def test_test_pipelines_count_only_in_the_test_mode(env):
    known(env, PIPE_RETAIL, PIPE_TANGEM, test=(PIPE_OTHER,))
    assert PIPE_TEST in prompts.pipelines_for("test") and PIPE_OTHER in prompts.pipelines_for("test")
    assert PIPE_TEST not in prompts.pipelines_for("prod") and PIPE_OTHER not in prompts.pipelines_for("prod")
    assert prompts.pipelines_for("off") == prompts.pipelines_for("prod")


def test_a_test_pipeline_listed_inside_a_funnel_is_not_a_prompt_pipeline_in_full(env):
    async def go():
        svc = Svc(env, answer(extra=({"slug": "t", "pipelines": [PIPE_TEST], "has_prompt": True, "prompt_source": "fallback"},), test=()))
        await svc.poll()
        v2(env, "full", pipelines_any=True)
        assert prompts.pipelines_for("prod") == frozenset(TWO) and not settings.pipeline_allowed(PIPE_TEST)
    run(go())


@pytest.mark.parametrize("how", ["static", "service", "no_test_in_answer", "service_down", "service_only_other"])
def test_the_test_pipeline_works_in_the_test_mode_whatever_the_service_says(env, how):
    async def go():
        svc = Svc(env, answer(retail=False, tangem=False, test=()) if how == "service_only_other" else answer(test=() if how == "no_test_in_answer" else (PIPE_TEST,)))
        if how != "static":
            await svc.poll()
        if how == "service_down":
            svc.down = True
            await svc.poll()
        assert settings.effective_mode() == "test" and settings.pipeline_allowed(PIPE_TEST)
        turn = await env.start_turn()
        assert turn["mode"] == "test" and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


# =============================================================================================
# шлюз pipeline_allowed
# =============================================================================================

def test_pipelines_any_does_not_open_a_pipeline_without_a_prompt(env):
    known(env, PIPE_RETAIL, PIPE_TANGEM)
    v2(env, "full", pipelines_any=True)
    assert settings.pipelines_any() is True
    assert settings.pipeline_allowed(PIPE_RETAIL) and settings.pipeline_allowed(PIPE_TANGEM)
    assert not settings.pipeline_allowed(PIPE_OTHER) and not settings.pipeline_allowed(PIPE_TEST)
    assert not settings.initiative_allowed(PIPE_OTHER)


def test_a_pipeline_with_a_prompt_is_open_only_when_the_mode_lets_it_in(env):
    known(env, PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER)
    v2(env, "full", pipelines_any=True)
    assert settings.pipeline_allowed(PIPE_OTHER)                                           # режим пускает (любая воронка), промпт есть
    v2(env, "full")
    assert not settings.pipeline_allowed(PIPE_OTHER) and settings.pipeline_allowed(PIPE_RETAIL)    # промпт есть, режим не пускает
    env.mp.setattr(cfg, "PIPELINES_ANY_ALLOWED", False)
    v2(env, "full", pipelines_any=True)
    assert not settings.pipeline_allowed(PIPE_OTHER)                                       # переменная движка не стоит: `pipelines_any` не действует


def test_a_mode_pipeline_without_a_prompt_is_closed_even_without_pipelines_any(env):
    known(env, PIPE_RETAIL)
    v2(env, "full")
    assert settings.pipeline_allowed(PIPE_RETAIL) and not settings.pipeline_allowed(PIPE_TANGEM)
    assert settings.initiative_allowed(PIPE_RETAIL) and not settings.initiative_allowed(PIPE_TANGEM)


def test_nothing_is_allowed_when_no_pipeline_has_a_prompt(env):
    known(env)
    v2(env, "full", pipelines_any=True)
    assert not any(settings.pipeline_allowed(p) for p in (PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER, PIPE_TEST))


def test_the_off_mode_and_bad_values_allow_nothing(env):
    known(env, PIPE_RETAIL, PIPE_TANGEM)
    v2(env, "full", pipelines_any=True)
    for bad in (None, "", 0, -1, True, "abc", 1.5):
        assert not settings.pipeline_allowed(bad), bad
    env.mp.setattr(cfg, "ENABLED", False)
    assert not settings.pipeline_allowed(PIPE_RETAIL)


def test_the_test_mode_takes_the_test_pipeline_and_not_the_retail_ones(env):
    known(env, PIPE_RETAIL, PIPE_TANGEM)
    assert settings.effective_mode() == "test"
    assert settings.pipeline_allowed(PIPE_TEST)
    assert not settings.pipeline_allowed(PIPE_RETAIL) and not settings.pipeline_allowed(PIPE_OTHER)


# =============================================================================================
# движок
# =============================================================================================

def test_a_client_in_a_pipeline_without_a_prompt_is_not_taken_even_with_pipelines_any(env):
    async def go():
        svc = Svc(env)
        await svc.poll()
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
        assert env.alerts == [] and env.notes == [] and env.tasks_open == []
    run(go())


def test_a_client_in_a_pipeline_with_a_prompt_is_taken_and_the_turn_says_reply_only(env):
    async def go():
        svc = Svc(env, answer(extra=(OPT,)))
        await svc.poll()
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
        assert env.turn_requests[0]["mode_settings"]["initiative"] is False             # вне двух розничных воронок ИИ только отвечает
    run(go())


def test_a_prompt_that_appears_later_opens_the_pipeline_on_the_next_poll(env):
    async def go():
        svc = Svc(env)
        await svc.poll()
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert store.get_owner(DEAL) is None
        svc.body = answer(extra=(OPT,))
        await svc.poll()
        await env.advance(61)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_deal_moved_to_a_pipeline_without_a_prompt_is_released(env):
    async def go():
        known(env, PIPE_RETAIL, PIPE_TANGEM)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E._handle_lead_event(moved(PIPE_OTHER))
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["closed"], owner["reason"]) == ("engine", 1, "left_pipeline")
    run(go())


def test_a_deal_moved_to_a_pipeline_with_a_prompt_stays_with_the_ai(env):
    async def go():
        known(env, PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E._handle_lead_event(moved(PIPE_OTHER))
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai" and not owner["closed"] and owner["pipeline_id"] == PIPE_OTHER
    run(go())


def test_a_deal_moved_between_the_two_prompt_pipelines_stays_without_pipelines_any(env):
    async def go():
        known(env, PIPE_RETAIL, PIPE_TANGEM)
        v2(env, "full")
        env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        await E._handle_lead_event(moved(PIPE_TANGEM))
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_prompt_withdrawn_from_the_pipeline_of_a_taken_deal_stops_the_reply_and_the_next_event_releases_it(env):
    async def go():
        svc = Svc(env)
        await svc.poll()
        v2(env, "full")
        env.lead(pipeline=PIPE_TANGEM)
        turn = await env.start_turn()
        args = (DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert E.decide_permit(*args) == (200, {"allowed": True})
        svc.body = answer(tangem=False)                                                    # промпт tangem убрали, пока ход шёл
        await svc.poll()
        assert E.decide_permit(*args) == (200, {"allowed": False, "reason": "owner_changed"})      # клиенту ничего не уходит
        assert store.get_owner(DEAL)["owner"] == "ai"                                      # снимает уже событие сделки или следующее сообщение клиента
        await E._handle_lead_event({**moved(PIPE_TANGEM), "work_touched": True})
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["closed"], owner["reason"]) == ("engine", 1, "left_pipeline")
    run(go())


def test_a_prompt_withdrawn_releases_the_deal_at_the_next_client_message(env):
    async def go():
        svc = Svc(env)
        await svc.poll()
        v2(env, "full")
        env.lead(pipeline=PIPE_TANGEM)
        turn = await take(env)
        svc.body = answer(tangem=False)
        await svc.poll()
        await env.say(env.msg("ещё вопрос", mid="again"))
        await env.advance(70)
        assert env.turn_requests == []                                                     # нового хода нет: сделка уже не подходящая
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["closed"]) == ("engine", 1)
    run(go())


def test_a_prompt_pipeline_is_not_affected_when_another_one_loses_its_prompt(env):
    async def go():
        svc = Svc(env)
        await svc.poll()
        v2(env, "full")
        env.lead(pipeline=PIPE_RETAIL)
        turn = await env.start_turn()
        svc.body = answer(tangem=False)
        await svc.poll()
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"]) == (200, {"allowed": True})
        await E._handle_lead_event({**moved(PIPE_RETAIL), "work_touched": True})
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_return_dialog_is_refused_in_a_pipeline_without_a_prompt(env):
    async def go():
        known(env, PIPE_RETAIL, PIPE_TANGEM)
        v2(env, "full", pipelines_any=True)
        store.set_owner(DEAL, store.OWNER_HUMAN, reason="rule_A", pipeline_id=PIPE_OTHER, mode="full")
        assert await E.return_dialog(DEAL) == (409, {"detail": "pipeline_not_allowed"})
        known(env, PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER)
        code, out = await E.return_dialog(DEAL)
        assert code == 200 and out["ok"] is True
    run(go())


def test_give_deal_is_refused_in_a_pipeline_without_a_prompt(env):
    async def go():
        known(env, PIPE_RETAIL, PIPE_TANGEM)
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        code, out = await give()
        assert (code, out) == (404, {"detail": "deal_not_found"}) and store.get_owner(DEAL) is None
        known(env, PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER)
        code, out = await give()
        assert code == 200 and out["ok"] is True
        await drain()
    run(go())


def test_the_poll_failing_after_a_take_does_not_release_anything(env):
    async def go():
        svc = Svc(env, answer(extra=(OPT,)))
        await svc.poll()
        v2(env, "full", pipelines_any=True)
        env.lead(pipeline=PIPE_OTHER)
        await take(env)
        svc.down = True
        await svc.poll()
        await E._handle_lead_event({**moved(PIPE_OTHER), "work_touched": True})
        assert store.get_owner(DEAL)["owner"] == "ai" and prompts.source() == "cached"
    run(go())


# --- перехват ------------------------------------------------------------------------------

def test_the_intercept_keeps_its_own_pipeline_rules(env):
    """Перехват берёт сделки выбранных менеджеров в любой воронке, в том числе без промпта (воронка `PIPE_ANY` нигде в словаре не значится),
    и перехваченная сделка проходит `permit`, событие сделки и разбор клиента, как и до правила «нет промпта: не пишем»."""
    async def go():
        svc = Svc(env)
        await svc.poll()
        intercept_setup(env)
        igor_deal(env)
        assert PIPE_ANY not in prompts.pipelines_for("prod")
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        ev = intercepts(env)
        assert len(ev) == 1 and store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["pipeline_id"] == PIPE_ANY
        turn = ev[0]
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"]) == (200, {"allowed": True})
        await E._handle_lead_event({**moved(PIPE_ANY), "work_touched": True})
        assert store.get_owner(DEAL)["owner"] == "ai" and not store.get_owner(DEAL)["closed"]
        assert await E._initiative_ok(store.get_owner(DEAL))                                 # перехваченной сделке «инициатива» по правилам перехвата
    run(go())


# =============================================================================================
# срез состояния
# =============================================================================================

def test_the_status_shows_the_prompt_pipelines_their_source_and_age(env):
    async def go():
        v2(env, "full")
        out = E.status()
        assert out["prompt_pipelines"] == sorted(TWO) and out["prompt_pipelines_source"] == "static" and out["prompt_pipelines_age_s"] is None
        svc = Svc(env, answer(extra=(OPT,)))
        await svc.poll()
        env.mp.setattr(prompts, "_now", lambda: prompts._good.at + 12.34)
        out = E.status()
        assert out["prompt_pipelines"] == sorted({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})
        assert out["prompt_pipelines_source"] == "service" and out["prompt_pipelines_age_s"] == 12.3
        svc.down = True
        await svc.poll()
        out = E.status()
        assert out["prompt_pipelines_source"] == "cached" and out["prompt_pipelines"] == sorted({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER})
        assert out["prompt_pipelines_age_s"] == 12.3
    run(go())


def test_the_status_in_the_test_mode_includes_the_test_pipeline(env):
    assert PIPE_TEST in E.status()["prompt_pipelines"]
    v2(env, "full")
    assert PIPE_TEST not in E.status()["prompt_pipelines"]
