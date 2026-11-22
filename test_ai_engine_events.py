"""События движка для панели (`ai_engine_events`): снятие сделки при смене режима попадает в журнал ИИ на экране панели.

Что проверяем:
  • тело события ровно такое, как договорено с панелью (поля, типы, `at` тот же, что в `mode_release`), без личных данных клиента;
    адрес `/api/ingest/ai/engine-event`, метод POST, заголовок `X-Ingest-Token` тот же, что у чтения настроек;
  • событие уходит в обе стороны (Тест -> Полный, Полный -> Тест, Полный -> Помощь), одно снятие даёт одно событие;
  • отправка идёт фоном и снятие не задерживает; сбой сети, 5xx и ошибка внутри отправителя снятие не ломают, строка `mode_release` остаётся;
  • повторы ограничены (паузы 1, 5, 25 с, четыре попытки), 404 и 401/403 и прочие 4xx не повторяются, лог такого отказа не чаще раза в 10 минут;
  • очередь ограничена, лишнее отбрасывается;
  • нет токена, адреса, движок выключен или режим `off`: ничего не уходит; без подставного транспорта под pytest сети нет вовсе.

Сети нет: панель подставная (httpx.MockTransport), паузы повторов записываются, но не ждутся. Запуск: python -m pytest test_ai_engine_events.py -q
"""
import asyncio
import collections
import datetime
import json
import logging
import re

import httpx
import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_events as ev
import ai_engine_modes as M
import ai_engine_settings as settings
import ai_engine_store as store
from test_ai_engine import CHAT, DEAL, PHONE, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_modes import DEAL2, apply, drain, released, take

URL = "http://panel.test/api/ingest/ai/engine-event"
KEYS = {"kind", "deal_id", "from_mode", "to_mode", "epoch", "at", "note"}


@pytest.fixture(autouse=True)
def _fresh_mode_sweep(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)        # проход «режим сменился» помнит прошлый режим между тестами одного процесса


class Panel:
    """Подставная панель: записывает запросы, отвечает по сценарию (`script`: статусы или исключения по порядку, потом `default`)."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.bodies: list[dict] = []
        self.script: list = []
        self.default = 200
        self.gate: asyncio.Event | None = None            # пока не выставлен, ответ висит: панель «думает»
        self.sleeps: list[float] = []                     # паузы повторов (в тестах не ждутся)
        self.clock = 1000.0                               # часы ограничения лога

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.bodies.append(json.loads(request.content))
        if self.gate is not None:
            await self.gate.wait()
        step = self.script.pop(0) if self.script else self.default
        if isinstance(step, Exception):
            raise step
        return httpx.Response(step, json={"ok": step < 300})

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


@pytest.fixture
def panel(env):
    mp = env.mp
    p = Panel()
    mp.setattr(cfg, "PANEL_URL", "http://panel.test")
    mp.setattr(cfg, "PANEL_TOKEN", "panel-token")
    mp.setattr(ev, "_transport", httpx.MockTransport(p.handler))
    mp.setattr(ev, "_sleep", p.sleep)
    mp.setattr(ev, "_now", lambda: p.clock)
    mp.setattr(ev, "_queue", collections.deque())         # состояние модуля не должно переходить из теста в тест
    mp.setattr(ev, "_worker", None)
    mp.setattr(ev, "_log_seen", {})
    mp.setattr(ev, "_suppressed", {})
    return p


def push(deal=1, frm="test", to="full", epoch=2) -> bool:
    return ev.enqueue(M.release_event(deal, frm, to, epoch, 1_700_000_000.0), E._spawn)


def lines(caplog, text: str) -> list[str]:
    """Предупреждения и ошибки модуля событий, в которых есть `text`."""
    return [r.getMessage() for r in caplog.records
            if r.name == "uvicorn" and r.levelno >= logging.WARNING and "ai_engine_events" in r.getMessage() and text in r.getMessage()]


def iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def set_mode(env, mode_id, version=2):
    if mode_id in ("test", "full"):
        apply(env, mode_id, version)
    else:
        apply(env, "full", 2, mode_id=mode_id)             # `help` и `intercept` едут на `mode: prod`


# =============================================================================================
# тело события и запрос
# =============================================================================================

def test_release_event_has_exactly_the_agreed_shape():
    now = datetime.datetime(2026, 11, 23, 10, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    assert M.release_event(36587533, "test", "full", 5, now) == {
        "kind": "mode_release", "deal_id": 36587533, "from_mode": "test", "to_mode": "full", "epoch": 5,
        "at": "2026-11-23T10:00:00Z", "note": "снята при смене режима",
    }
    e = M.release_event("36587533", None, "test", None, now + 0.9)         # режима у сделки и эпохи может не быть: null, секунды усечены
    assert (e["deal_id"], e["from_mode"], e["epoch"], e["at"]) == (36587533, None, None, "2026-11-23T10:00:00Z")
    assert set(e) == KEYS and len(e["note"]) <= 255
    assert json.loads(json.dumps(e, ensure_ascii=False)) == e


@pytest.mark.parametrize("frm,to,version", [("test", "full", 1), ("test", "full", 2), ("full", "test", 1), ("full", "test", 2), ("full", "help", 2)])
def test_a_release_sends_the_event_in_both_directions(env, panel, frm, to, version):
    async def go():
        set_mode(env, frm, version)
        if frm != "test":
            env.lead(pipeline=PIPE_RETAIL)
        await take(env)
        set_mode(env, to, version)
        await env.advance(2)
        await drain()
        assert released()
        row, = M.releases()
        body, = panel.bodies
        assert body == {
            "kind": "mode_release", "deal_id": DEAL, "from_mode": frm, "to_mode": to, "epoch": 2,
            "at": iso(row["at"]), "note": "снята при смене режима",
        }
        assert (row["from_mode"], row["to_mode"], row["epoch"]) == (body["from_mode"], body["to_mode"], body["epoch"])
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", body["at"])
        assert isinstance(body["deal_id"], int) and isinstance(body["epoch"], int)
    run(go())


def test_the_request_goes_to_the_agreed_address_with_the_ingest_token_and_no_client_data(env, panel):
    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        await drain()
        req, = panel.requests
        assert (req.method, str(req.url)) == ("POST", URL)
        assert req.headers["X-Ingest-Token"] == "panel-token"
        assert req.headers["content-type"].startswith("application/json")
        body, = panel.bodies
        assert set(body) == KEYS
        blob = json.dumps(body, ensure_ascii=False)
        assert PHONE not in blob and CHAT not in blob and "Тиана" not in blob and "panel-token" not in blob
    run(go())


def test_one_release_gives_one_event_and_each_release_its_own(env, panel):
    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        for _ in range(3):                                       # дальнейшие проходы той же сделки не трогают
            await env.advance(11)
        await drain()
        assert [b["deal_id"] for b in panel.bodies] == [DEAL] and len(M.releases()) == 1
        # вторая сделка снимается своим событием: в режиме Полный ИИ её взял, потом включили Тест
        env.lead(lead_id=DEAL2, pipeline=PIPE_RETAIL)
        await env.say(env.msg("Здравствуйте", mid="second-chat"))
        await env.advance(61)
        assert store.get_owner(DEAL2)["mode"] == "full"
        apply(env, "test", 2)
        await env.advance(2)
        await drain()
        assert [(b["deal_id"], b["from_mode"], b["to_mode"]) for b in panel.bodies] == [(DEAL, "test", "full"), (DEAL2, "full", "test")]
        assert len(M.releases()) == 2
    run(go())


# =============================================================================================
# фон и устойчивость: отправка не мешает снятию
# =============================================================================================

def test_sending_runs_in_the_background_and_does_not_hold_the_release(env, panel):
    async def go():
        panel.gate = asyncio.Event()                             # панель молчит, пока не разрешим
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)                                     # проход вернулся, хотя ответа панели нет
        assert released() and len(M.releases()) == 1
        assert any(not t.done() for t in E._bg)                  # отправка висит отдельной задачей
        panel.gate.set()
        await drain()
        assert len(panel.bodies) == 1
    run(go())


@pytest.mark.parametrize("failure", [httpx.ConnectError("нет связи"), httpx.ReadTimeout("таймаут"), RuntimeError("что угодно")])
def test_a_network_failure_does_not_break_the_release_and_retries_are_bounded(env, panel, caplog, failure):
    caplog.set_level(logging.DEBUG, logger="uvicorn")
    panel.default = failure                                      # исключение вместо ответа на каждой попытке

    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        await drain()
        assert released() and [r["to_mode"] for r in M.releases()] == ["full"]         # правда осталась в таблице
        assert len(panel.requests) == 4 and panel.sleeps == [1, 5, 25]                 # первая попытка и три повтора, пятой нет
        assert not ev._queue and (ev._worker is None or ev._worker.done())
        assert len(lines(caplog, "недоступна")) == 1
        await env.advance(30)
        await drain()
        assert len(panel.requests) == 4                                                # само по себе ничего не повторяется
    run(go())


def test_an_error_inside_the_publisher_never_reaches_the_release(env, panel, caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="uvicorn")

    def boom(*a, **k):
        raise RuntimeError("сломалось при постановке")
    monkeypatch.setattr(ev, "enqueue", boom)

    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        await drain()
        assert released() and len(M.releases()) == 1 and panel.requests == []
        assert any("не поставил событие" in r.getMessage() for r in caplog.records)
    run(go())


def test_an_error_in_the_worker_is_logged_and_the_next_event_still_goes(env, panel, caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="uvicorn")
    real = ev.deliver
    calls = []

    async def flaky(event):
        calls.append(event["deal_id"])
        if len(calls) == 1:
            raise RuntimeError("сбой внутри отправки")
        return await real(event)
    monkeypatch.setattr(ev, "deliver", flaky)

    async def go():
        apply(env, "full")
        assert push(1) and push(2)
        await drain()
        assert calls == [1, 2] and [b["deal_id"] for b in panel.bodies] == [2]
        assert len(lines(caplog, "сбой отправки")) == 1
    run(go())


# =============================================================================================
# повторы и отказы панели
# =============================================================================================

def test_server_errors_and_429_are_retried_until_the_panel_takes_the_event(env, panel, caplog):
    caplog.set_level(logging.DEBUG, logger="uvicorn")
    panel.script = [500, 429, 200]

    async def go():
        apply(env, "full")
        assert push(7)
        await drain()
        assert len(panel.requests) == 3 and panel.sleeps == [1, 5]
        assert lines(caplog, "ai_engine_events") == []
    run(go())


def test_the_retries_stop_after_the_last_pause(env, panel):
    panel.default = 503

    async def go():
        apply(env, "full")
        assert await ev.deliver(M.release_event(1, "test", "full", 2, 1.0)) == ev.GAVE_UP
        assert len(panel.requests) == 1 + len(ev.RETRY_DELAYS_S) == 4 and panel.sleeps == [1, 5, 25]
    run(go())


@pytest.mark.parametrize("status", [404, 401, 403, 400, 422])
def test_a_refusal_is_not_retried(env, panel, status):
    panel.default = status

    async def go():
        apply(env, "full")
        assert await ev.deliver(M.release_event(1, "test", "full", 2, 1.0)) == ev.REFUSED
        assert len(panel.requests) == 1 and panel.sleeps == []
    run(go())


def test_404_before_the_panel_is_deployed_is_dropped_quietly_and_the_release_is_intact(env, panel, caplog):
    caplog.set_level(logging.DEBUG, logger="uvicorn")
    panel.default = 404                                          # движок выкачен раньше панели: ручки ещё нет

    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        await env.advance(2)
        await drain()
        assert released() and len(M.releases()) == 1
        assert len(panel.requests) == 1 and panel.sleeps == []
        assert len(lines(caplog, "404")) == 1
        assert not ev._queue
    run(go())


def test_a_refusal_is_logged_at_most_once_per_ten_minutes_per_kind(env, panel, caplog):
    caplog.set_level(logging.DEBUG, logger="uvicorn")

    async def go():
        apply(env, "full")
        panel.default = 404
        for deal in (1, 2, 3):
            assert push(deal)
            await drain()
        assert len(panel.requests) == 3 and len(lines(caplog, "404")) == 1                # три отказа, одна строка
        panel.clock += 599
        assert push(4)
        await drain()
        assert len(lines(caplog, "404")) == 1                                             # десяти минут ещё нет
        panel.clock += 2
        assert push(5)
        await drain()
        again = lines(caplog, "404")
        assert len(again) == 2 and "ещё 3 таких" in again[1]                              # снова записали и сказали, сколько проглочено
        # другой вид отказа пишется отдельно: 401 и 403 это один вид
        panel.default = 401
        assert push(6)
        await drain()
        panel.default = 403
        assert push(7)
        await drain()
        assert len(lines(caplog, "токен не принят")) == 1
        panel.default = 422
        assert push(8)
        await drain()
        assert len(lines(caplog, "422")) == 1
        assert panel.sleeps == []                                                         # ни один отказ не повторялся
    run(go())


def test_a_retry_is_dropped_when_the_mode_went_off_during_the_pause(env, panel):
    async def go():
        apply(env, "full")
        panel.default = 503

        async def sleep(seconds):
            panel.sleeps.append(seconds)
            apply(env, "off")                                    # панель погасили между попытками
        env.mp.setattr(ev, "_sleep", sleep)
        assert await ev.deliver(M.release_event(1, "test", "full", 2, 1.0)) == ev.SKIPPED
        assert len(panel.requests) == 1 and panel.sleeps == [1]
    run(go())


# =============================================================================================
# очередь ограничена
# =============================================================================================

def test_the_queue_is_bounded_and_the_overflow_is_dropped_with_one_log_line(env, panel, caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="uvicorn")
    monkeypatch.setattr(ev, "QUEUE_MAX", 3)

    async def go():
        apply(env, "full")
        accepted = [push(deal) for deal in range(1, 11)]
        assert accepted == [True] * 3 + [False] * 7 and len(ev._queue) == 3
        assert len([t for t in E._bg if not t.done()]) == 1                               # рабочая задача одна
        await drain()
        assert [b["deal_id"] for b in panel.bodies] == [1, 2, 3]
        assert len(lines(caplog, "очередь событий в панель полна")) == 1
        assert push(11)                                                                   # освободилась: снова принимает
        await drain()
        assert panel.bodies[-1]["deal_id"] == 11
    run(go())


def test_a_long_outage_does_not_pile_up_events(env, panel, monkeypatch):
    monkeypatch.setattr(ev, "QUEUE_MAX", 5)
    panel.default = httpx.ConnectError("панель лежит")

    async def go():
        apply(env, "full")
        accepted = [push(deal) for deal in range(1, 21)]
        assert accepted.count(True) == 5 and len(ev._queue) == 5
        await drain()
        assert not ev._queue and len(panel.requests) == 5 * 4                             # каждое принятое: четыре попытки и отказ
    run(go())


# =============================================================================================
# когда не отправляем
# =============================================================================================

@pytest.mark.parametrize("what", ["no_token", "no_url", "engine_off", "mode_off"])
def test_nothing_is_sent_without_a_token_an_address_an_enabled_engine_or_an_active_mode(env, panel, what):
    async def go():
        apply(env, "test", 2)
        await take(env)
        apply(env, "full", 2)
        owner = store.get_owner(DEAL)
        {
            "no_token": lambda: env.mp.setattr(cfg, "PANEL_TOKEN", ""),
            "no_url": lambda: env.mp.setattr(cfg, "PANEL_URL", ""),
            "engine_off": lambda: env.mp.setattr(cfg, "ENABLED", False),
            "mode_off": lambda: apply(env, "off", 2),
        }[what]()
        await E._release_for_mode(owner, "full")                 # снятие само проходит, как и без панели
        await drain()
        assert panel.requests == [] and not ev._queue
        assert released() and [r["to_mode"] for r in M.releases()] == ["full"]
    run(go())


def test_without_a_substitute_transport_nothing_goes_to_the_network_under_pytest(env, panel, monkeypatch):
    apply(env, "full")
    assert ev.may_send() is True
    monkeypatch.setattr(ev, "_transport", None)
    assert ev.may_send() is False                                # PYTEST_CURRENT_TEST стоит: настоящей сети нет
    assert push(1) is False and not ev._queue
    assert settings.active_mode_id() == "full"


def test_the_shared_env_has_no_panel_token_so_other_tests_send_nothing(env):
    assert cfg.PANEL_TOKEN == "" and ev.may_send() is False
