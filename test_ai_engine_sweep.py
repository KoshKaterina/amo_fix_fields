"""Подбор существующих сделок для режима «Полный» (`ai_engine_sweep`, решение Кати 23.11.2026).

Что проверяем:
  • настройки: блок `existing.sweep` (нет блока и мусор выключают подбор, мусорное поле в лог один раз), границы, умолчания;
  • когда подбор вообще работает: движок, режим Полный, рабочие часы, рубильник `AI_SWEEP_DISABLED`, ИИ-сервис; он встроен в таймеры, шестого цикла нет;
  • бюджет: `per_hour` взятых за скользящий час, не больше двух за прогон, потолок просмотров, очередь пилота;
  • запрос в панель: тело, `exclude` (владельцы, журнал, `retry_at`, предложения, отметки, потолок), сбои панели не роняют прогон;
  • пул «розница»: открытые задачи на сотрудников (`blocked_by_task`, `amo_unavailable`), молчание клиента (`too_early`, порог напоминания), задачи пользователя ИИ не мешают;
  • пул «остальные»: только ответ за `other_days` суток, задачи не смотрим, ИИ отвечает без инициативы;
  • исключения: запись владельца (в том числе снятая с ИИ), отметки людей, журнал за 24 часа, воронка без промпта, канал, чат в работе;
  • те же правила взятия, что при сообщении клиента: `leave` и `redistribute`, пилот и подтверждение, тишина менеджера, фильтры, бюджет, дубли без алертов и задач;
  • событие хода `deal_given` без новых событий, задачи amo не создаются и не закрываются, рестарт ничего не дублирует, срез состояния.

Сети нет: amo, МойСклад, панель и ИИ-сервис подставные. Запуск: python -m pytest test_ai_engine_sweep.py -q
"""
import asyncio
import datetime
import inspect
import json
import logging
import os
import subprocess
import sys

import httpx
import pytest

import ai_engine as E
import ai_engine_approval as A
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_filters as F
import ai_engine_give as G
import ai_engine_marks as M
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_silence as silence
import ai_engine_store as store
import ai_engine_sweep as S
import amo_service
from test_ai_engine import CHAN, CHAT, CONTACT, DEAL, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_filters import AI_USER, OLD_MANAGER, Amo, put_lead
from test_ai_engine_followup import drain

PIPE_TANGEM = 9822330
PIPE_OTHER = 7_000_002
PIPE_DB = 11166334                                                                      # «Работа с базой»
OPEN_FULL = {"scope": "full", "restricted": False, "items": []}
ALL_DAY = [{"start": "00:00", "end": "24:00"}]
ON = {"enabled": True, "per_hour": 10, "retail": True, "other": True, "other_days": 3}
HOUR = 3600
UTC = datetime.timezone.utc


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    F._cache.clear()
    F._inflight.clear()
    F._warned.clear()
    settings.set_document(None)
    A._sending.clear()
    A._deciding.clear()
    monkeypatch.setattr(A, "_last_orphans", 0.0)
    monkeypatch.setattr(F, "REASSIGN_DISABLED", False)
    monkeypatch.setattr(cfg, "SWEEP_DISABLED", False)
    monkeypatch.setattr(cfg, "PIPELINES_ANY_ALLOWED", False)
    monkeypatch.setattr(prompts, "_good", None)                       # словаря сервиса нет: статический запас, две розничные воронки
    monkeypatch.setattr(prompts, "_last_ok", None)
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(S, "_transport", None)
    monkeypatch.setattr(S, "_task", None)
    monkeypatch.setattr(S, "_next_run_at", float("inf"))              # таймеры теста сами подбор не запускают; тесты `tick` включают его явно
    monkeypatch.setattr(S, "_state", {"last_run_at": None, "last_candidates": None, "total_open": None, "last_result": ""})
    monkeypatch.setattr(S, "_warn_seen", {})
    monkeypatch.setattr(S, "_disabled_noted", False)
    monkeypatch.setattr(S, "_last_status", 0)
    yield
    A._sending.clear()
    A._deciding.clear()


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, UTC).isoformat()


def setup(env, sweep=ON, policy="redistribute", review=False, armed=False, **over):
    """Документ версии 2 для Полного режима: `existing` с `sweep`, пилот выключен, рабочие часы круглосуточно. `sweep=None`: блока нет. Первый проход по умолчанию уже закончен
    (`armed=False`: тесты обычного режима с `per_hour`); `armed=True` взводит его, `armed=None` состояние не трогает."""
    existing = {"policy": policy, "since": env.t - 100, "manager_quiet_min": 15}
    if sweep is not None:
        existing["sweep"] = sweep
    doc = dict(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, settings_version=2, mode_id="full", review={"enabled": review},
               deals={"items": []}, existing=existing, ai_user=AI_USER, work_hours=ALL_DAY)
    doc.update(over)
    env.set_settings(**doc)
    env.mp.setattr(F, "_now", lambda: env.t)
    if armed is True:
        S.pass_arm()
    elif armed is False:
        S.pass_finish(env.t)


def allow_other(env):
    """«Остальные» воронки: оператор разрешил `AI_PIPELINES_ANY_ALLOWED=1`, на воронку есть промпт."""
    env.mp.setattr(cfg, "PIPELINES_ANY_ALLOWED", True)
    env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_OTHER, PIPE_DB}), test=frozenset(), at=0.0))
    env.mp.setattr(prompts, "_last_ok", True)


class World:
    """Подставной мир подбора: amo по чатам, задачи по сделкам, панель с кандидатами."""

    def __init__(self, env):
        self.env = env
        self.candidates: list[dict] = []
        self.requests: list[dict] = []
        self.tasks: dict[int, list | None] = {}
        self.tasks_calls: list[int] = []
        self.status = 200
        self.raw: bytes | None = None
        self.fail = False
        self.ignore_exclude = False
        self.raw_candidates = False                                                            # отдать список как есть, без отбора по воронкам и exclude (проверка защиты движка)
        self.max_limit: int | None = None                                                      # прежняя панель с меньшим потолком `limit`: больше этого отвечает 422
        self.amo = Amo(env)
        self.chats = 0

        async def find(query, with_=(), page_limit=250, max_pages=5):
            return None if env.amo_down else [ld for ld in env.leads if ld.get("_q") == query]

        async def get_open(deal_id):
            self.tasks_calls.append(int(deal_id))
            return self.tasks.get(int(deal_id), [])

        def handler(request: httpx.Request) -> httpx.Response:
            if self.fail:
                raise httpx.ConnectError("нет связи")
            body = json.loads(request.content)
            self.requests.append({"path": request.url.path, "token": request.headers.get("x-ingest-token"), "body": body})
            if self.status != 200:
                return httpx.Response(self.status, json={"detail": "нет"})
            if self.max_limit is not None and body["limit"] > self.max_limit:
                return httpx.Response(422, json={"detail": "limit"})
            if self.raw is not None:
                return httpx.Response(200, content=self.raw)
            skip = set() if self.ignore_exclude else set(body["exclude"])
            pipes = set(body["retail_pipelines"]) | set(body["other_pipelines"])
            rows = list(self.candidates) if self.raw_candidates else [c for c in self.candidates if c["deal_id"] not in skip and c["pipeline_id"] in pipes][:body["limit"]]
            return httpx.Response(200, json={"candidates": rows, "total_open": len(self.candidates), "generated_at": iso(env.t)})

        env.mp.setattr(amo_service, "find_leads_by_query_all", find)
        env.mp.setattr(E.ai_engine_tasks, "_get_open", get_open)
        env.mp.setattr(S, "_transport", httpx.MockTransport(handler))
        env.mp.setattr(cfg, "PANEL_URL", "http://panel.test")
        env.mp.setattr(cfg, "PANEL_TOKEN", "ingest-token")

    def chat_key(self, chat):
        return f"{CHAN}:{chat}"

    def deal(self, deal_id=DEAL, chat=None, pipeline=PIPE_RETAIL, age_days=30, responsible=OLD_MANAGER, waiting=True, client_ago=2 * HOUR, out_ago=None,
             channel=CHAN, price=0, **named):
        """Открытая сделка в amo (создана `age_days` суток назад) и её кандидат в ответе панели."""
        env = self.env
        self.chats += 1
        chat = chat or f"19200{self.chats:05d}"
        ld = put_lead(env, lead_id=deal_id, price=price, created_at=int(env.t) - age_days * 86400, responsible=responsible, pipeline=pipeline, **named)
        ld["_q"] = chat
        if waiting:
            last_client, last_out = env.t - client_ago, (env.t - out_ago if out_ago is not None else None)
            last_msg = last_client
        else:
            last_out = env.t - (out_ago if out_ago is not None else 5 * HOUR)
            last_client = env.t - client_ago if client_ago > 5 * HOUR else env.t - 2 * 86400
            last_msg = last_out
        cand = {"deal_id": deal_id, "pool": "other" if pipeline == PIPE_OTHER else "retail", "pipeline_id": pipeline, "status_id": 1000,
                "responsible_user_id": responsible, "contact_id": CONTACT, "contact_name": "Тестовый Клиент", "phone": f"7926{self.chats:07d}",
                "chat_id": chat, "chat_type": "telegram", "channel_id": channel, "last_msg_at": iso(last_msg), "last_client_at": iso(last_client),
                "last_out_at": iso(last_out) if last_out else None, "waiting": waiting}
        self.candidates.append(cand)
        return cand

    def outcomes(self):
        return [(r["deal_id"], r["outcome"]) for r in reversed(S.journal())]


@pytest.fixture
def w(env):
    return World(env)


def last_row(deal=DEAL):
    return next(r for r in S.journal() if r["deal_id"] == deal)


# =============================================================================================
# настройки
# =============================================================================================

def test_sweep_is_off_without_a_block_and_the_panel_is_not_asked(env, w):
    async def go():
        setup(env, sweep=None)
        w.deal()
        assert S.config() is None and S.blocked_by() == "подбор выключен в настройках"
        out = await S.run_once(E)
        assert out["skipped"] == "подбор выключен в настройках" and w.requests == [] and store.get_owner(DEAL) is None
    run(go())


@pytest.mark.parametrize("sweep", [
    "да", ["enabled"], {"enabled": "yes"}, {"enabled": 1}, {"enabled": True, "per_hour": 0}, {"enabled": True, "per_hour": 61}, {"enabled": True, "per_hour": True},
    {"enabled": True, "per_hour": 5.5}, {"enabled": True, "per_hour": "10"}, {"enabled": True, "retail": "да"}, {"enabled": True, "other": 1},
    {"enabled": True, "other_days": 0}, {"enabled": True, "other_days": 8}, {"enabled": True, "other_days": False}, {"enabled": True, "other_days": "3"},
])
def test_garbage_in_the_block_switches_the_sweep_off(env, sweep):
    setup(env, sweep=sweep)
    assert S.config() is None and S.blocked_by() is not None


def test_a_broken_existing_block_or_policy_switches_the_sweep_off(env):
    setup(env, existing="не словарь")
    assert S.config() is None
    setup(env, existing={"sweep": dict(ON)})                                    # политики нет: без неё подбор брал бы старые сделки как новые
    assert S.config() is None
    setup(env, existing={"policy": "всё подряд", "sweep": dict(ON)})
    assert S.config() is None


def test_a_garbage_field_goes_to_the_log_once(env, caplog):
    caplog.set_level(logging.ERROR, logger="uvicorn")
    setup(env, sweep={"enabled": True, "per_hour": 99})
    for _ in range(3):
        assert S.config() is None
    assert len([r for r in caplog.records if "existing.sweep.per_hour" in r.getMessage()]) == 1


def test_defaults_and_bounds(env):
    setup(env, sweep={"enabled": True})
    assert S.config() == {"enabled": True, "per_hour": 10, "retail": True, "other": True, "other_days": 3, "retail_pipelines": frozenset({PIPE_RETAIL, PIPE_TANGEM})}
    for per_hour, days in ((1, 1), (60, 7)):
        setup(env, sweep={"enabled": True, "per_hour": per_hour, "other_days": days})
        assert S.config()["per_hour"] == per_hour and S.config()["other_days"] == days
    setup(env, sweep={"enabled": False, "per_hour": 10})
    assert S.config() is None
    setup(env, sweep={"enabled": True, "retail": False, "other": False})
    assert S.config() is not None and S.blocked_by() == "оба пула выключены в настройках"


def test_the_version_one_document_has_no_sweep(env):
    env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, existing={"policy": "redistribute", "sweep": dict(ON)})
    assert S.config() is None                                                   # блоки версии 2 документ версии 1 не читает


# =============================================================================================
# когда подбор работает
# =============================================================================================

def test_the_sweep_runs_only_when_everything_allows_it(env, w):
    async def go():
        setup(env)
        assert S.blocked_by() is None
        env.mp.setattr(cfg, "SWEEP_DISABLED", True)
        assert "AI_SWEEP_DISABLED" in S.blocked_by()
        env.mp.setattr(cfg, "SWEEP_DISABLED", False)
        env.mp.setattr(cfg, "ENABLED", False)
        assert S.blocked_by() == "движок выключен"
        env.mp.setattr(cfg, "ENABLED", True)
        setup(env, work_hours=[])                                               # пустые часы: «никогда», а не круглосуточно
        assert S.blocked_by() == "сейчас вне рабочих часов"
        setup(env, mode_id="test", mode="test", pipelines=[8642414])
        assert S.blocked_by() == "активный режим не «Полный»"
        for mode_id in ("help", "intercept"):
            setup(env, mode_id=mode_id)
            assert S.blocked_by() == "активный режим не «Полный»"
        settings.set_document(None)
        assert "режим «Выключено»" in S.blocked_by()
        setup(env)
        settings.set_document(settings.document(), fetched_at=env.t - 100000)     # настройки протухли
        assert "режим «Выключено»" in S.blocked_by()
    run(go())


@pytest.mark.parametrize("kind", ["rubilnik", "hours", "test_mode", "help_mode", "off", "stale"])
def test_a_blocked_sweep_does_not_call_the_panel_and_takes_nothing(env, w, kind):
    async def go():
        setup(env)
        w.deal()
        if kind == "rubilnik":
            env.mp.setattr(cfg, "SWEEP_DISABLED", True)
        elif kind == "hours":
            setup(env, work_hours=[])
        elif kind == "test_mode":
            setup(env, mode_id="test", mode="test", pipelines=[8642414])
        elif kind == "help_mode":
            setup(env, mode_id="help")
        elif kind == "off":
            setup(env, mode="off", mode_id="off")
        else:
            settings.set_document(settings.document(), fetched_at=env.t - 100000)
        out = await S.run_once(E)
        assert out["skipped"] and w.requests == [] and store.get_owner(DEAL) is None and S.journal() == []
    run(go())


def test_the_ai_service_down_means_no_run(env, w):
    async def go():
        setup(env)
        w.deal()
        env.health_ok = False
        env.mp.setattr(E, "_ai_ok_at", env.t - 1000)
        out = await S.run_once(E)
        assert out["skipped"] == "ИИ-сервис недоступен" and w.requests == [] and store.get_owner(DEAL) is None
    run(go())


def test_the_sweep_is_part_of_the_timer_loop_not_a_sixth_loop(env, w):
    async def go():
        setup(env)
        w.deal()
        env.mp.setattr(S, "_next_run_at", 0.0)
        env.mp.setattr(S, "INTERVAL_S", 300)
        await env.advance(1)                                                    # `tick_once` запускает прогон фоновой задачей
        await drain()
        assert len(w.requests) == 1 and store.get_owner(DEAL)["owner"] == "ai"
        await env.advance(100)
        await drain()
        assert len(w.requests) == 1                                             # раньше чем через 5 минут прогона нет
        w.deal(DEAL + 1)
        await env.advance(301)
        await drain()
        assert len(w.requests) == 2 and store.get_owner(DEAL + 1)["owner"] == "ai"
        source = inspect.getsource(E.init)
        assert source.count("create_task(") == 5 and "sweep" not in source      # те же пять циклов, `loops_alive` не меняется
    run(go())


def test_a_run_does_not_overlap_itself(env, w):
    async def go():
        setup(env)
        w.deal()
        env.mp.setattr(S, "_next_run_at", 0.0)
        gate = asyncio.Event()
        real = S.fetch_candidates

        async def slow(body):
            await gate.wait()
            return await real(body)

        env.mp.setattr(S, "fetch_candidates", slow)
        await S.tick(E, env.t)
        await S.tick(E, env.t + 1000)                                           # прошлый прогон ещё идёт
        await asyncio.sleep(0)
        gate.set()
        await drain()
        assert len(w.requests) == 1
    run(go())


# =============================================================================================
# бюджет
# =============================================================================================

def test_two_per_run_and_the_hourly_limit_slides(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 3})
        for i in range(6):
            w.deal(DEAL + i)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 2} and S.count_taken(env.t) == 2                    # не больше двух за прогон
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1} and S.count_taken(env.t) == 3                    # лимит часа: остался один
        out = await S.run_once(E)
        assert out["skipped"].startswith("лимит за час исчерпан (3 из 3)") and out["outcomes"] == {}
        block = S.status_block(env.t)
        assert (block["taken_last_hour"], block["budget_left"], block["per_hour"]) == (3, 0, 3)
        env.t += 30 * 60
        assert S.count_taken(env.t) == 3
        env.t += 31 * 60                                                                        # час прошёл: окно скользит
        assert S.count_taken(env.t) == 0
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 2}
        assert sum(1 for i in range(6) if store.get_owner(DEAL + i)) == 5
    run(go())


def test_a_proposal_counts_against_the_hourly_limit(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 2}, review=True)
        for i in range(4):
            w.deal(DEAL + i)
        out = await S.run_once(E)
        assert out["outcomes"] == {"reassign_pending": 2} and S.count_taken(env.t) == 2
        out = await S.run_once(E)
        assert out["skipped"].startswith("лимит за час исчерпан") and A.count_open() == 2
    run(go())


def test_the_pilot_queue_cap_stops_new_proposals(env, w):
    async def go():
        setup(env, review=True)
        env.mp.setattr(S, "MAX_OPEN_PROPOSALS", 2)
        for i in range(6):
            w.deal(DEAL + i)
        await S.run_once(E)
        await S.run_once(E)
        assert A.count_open() == 2
        out = await S.run_once(E)
        assert "в очереди пилота уже 2" in out["skipped"] and A.count_open() == 2
    run(go())


def test_the_examine_cap_counts_only_candidates_that_reach_the_network(env, w):
    async def go():
        setup(env)
        for i in range(9):
            w.deal(DEAL + i)
        env.leads = []                                                                          # в amo сделок нет: каждый кандидат уходит в сеть и получает `closed`
        out = await S.run_once(E)
        assert out["examined"] == S.max_examine(10) == 6 and out["outcomes"] == {"closed": 6}  # per_hour 10: шесть просмотров, остальные в следующий раз
        out = await S.run_once(E)
        assert out["examined"] == 3 and out["outcomes"] == {"closed": 3}
    run(go())


# =============================================================================================
# запрос в панель и exclude
# =============================================================================================

def test_the_request_to_the_panel_has_the_agreed_shape(env, w):
    async def go():
        setup(env)
        w.deal()
        await S.run_once(E)
        req = w.requests[0]
        assert req["path"] == "/api/ingest/ai/sweep-candidates" and req["token"] == "ingest-token"
        body = req["body"]
        assert set(body) == {"retail_pipelines", "other_pipelines", "ai_user_id", "other_days", "exclude", "limit"}
        assert body["retail_pipelines"] == sorted([PIPE_RETAIL, PIPE_TANGEM]) and body["other_pipelines"] == []
        assert (body["ai_user_id"], body["other_days"], body["limit"], body["exclude"]) == (AI_USER["id"], 3, 30, [])
        assert S._state["last_candidates"] == 1 and S._state["total_open"] == 1
    run(go())


def test_the_pools_follow_the_settings_and_the_dictionary(env, w):
    async def go():
        setup(env, pipelines_any=True)
        w.deal()
        await S.run_once(E)
        assert w.requests[-1]["body"]["other_pipelines"] == []                                 # `pipelines_any` без разрешения оператора ничего не открывает
        allow_other(env)
        setup(env, pipelines_any=True)
        await S.run_once(E)
        body = w.requests[-1]["body"]
        assert body["retail_pipelines"] == sorted([PIPE_RETAIL, PIPE_TANGEM]) and body["other_pipelines"] == sorted([PIPE_OTHER, PIPE_DB])
        setup(env, sweep={**ON, "retail": False}, pipelines_any=True)
        await S.run_once(E)
        body = w.requests[-1]["body"]
        assert body["retail_pipelines"] == [] and body["other_pipelines"] == sorted([PIPE_OTHER, PIPE_DB])
        setup(env, sweep={**ON, "other": False}, pipelines_any=True)
        await S.run_once(E)
        body = w.requests[-1]["body"]
        assert body["retail_pipelines"] == sorted([PIPE_RETAIL, PIPE_TANGEM]) and body["other_pipelines"] == []
    run(go())


def test_the_retail_pool_is_the_two_retail_pipelines_whatever_the_initiative_list_says(env, w):
    async def go():
        allow_other(env)
        setup(env, pipelines_any=True, initiative_pipelines=[PIPE_RETAIL, PIPE_TANGEM, PIPE_DB, PIPE_OTHER])      # инициатива у четырёх воронок, пул «розница» всё равно две
        w.deal()
        await S.run_once(E)
        body = w.requests[-1]["body"]
        assert body["retail_pipelines"] == sorted([PIPE_RETAIL, PIPE_TANGEM])
        assert body["other_pipelines"] == sorted([PIPE_OTHER, PIPE_DB]) and PIPE_DB not in body["retail_pipelines"]
        assert cfg.SWEEP_RETAIL_PIPELINES == frozenset({PIPE_RETAIL, PIPE_TANGEM})
        setup(env, pipelines_any=True, initiative_pipelines=[PIPE_TANGEM])                       # инициатива только у одной: пул тот же
        await S.run_once(E)
        assert w.requests[-1]["body"]["retail_pipelines"] == sorted([PIPE_RETAIL, PIPE_TANGEM])
    run(go())


def test_the_retail_list_can_be_overridden_by_the_panel_and_garbage_switches_the_sweep_off(env, w):
    async def go():
        allow_other(env)
        setup(env, sweep={**ON, "retail_pipelines": [PIPE_TANGEM]}, pipelines_any=True)
        assert S.config()["retail_pipelines"] == frozenset({PIPE_TANGEM})
        w.deal()
        await S.run_once(E)
        body = w.requests[-1]["body"]
        assert body["retail_pipelines"] == [PIPE_TANGEM] and PIPE_RETAIL in body["other_pipelines"]
        for junk in ([], "x", [True], [0], [-1], [1.5], ["a"], {"id": 1}, 5):
            setup(env, sweep={**ON, "retail_pipelines": junk})
            assert S.config() is None, junk
    run(go())


def test_work_with_the_base_deals_are_pool_two_answer_only_and_keep_the_initiative_once_taken(env, w):
    async def go():
        allow_other(env)
        setup(env, pipelines_any=True, initiative_pipelines=[PIPE_RETAIL, PIPE_TANGEM, PIPE_DB])
        w.deal(DEAL, pipeline=PIPE_DB, waiting=True, client_ago=HOUR)                           # старая сделка «Работы с базой», клиент ждёт ответа
        w.deal(DEAL + 1, pipeline=PIPE_DB, waiting=False, out_ago=20 * HOUR)                   # молчащий клиент: в пуле «остальные» только ответ
        w.deal(DEAL + 2, pipeline=PIPE_DB, waiting=True, client_ago=5 * 86400)                 # написал давно: за пределами `other_days`
        w.tasks[DEAL] = [{"id": 1, "responsible_user_id": AI_USER["id"]}]                       # задача только на пользователе ИИ не мешает
        w.tasks[DEAL + 1] = w.tasks[DEAL + 2] = [{"id": 2, "responsible_user_id": OLD_MANAGER}]
        out = await S.run_once(E)
        assert w.requests[-1]["body"]["other_pipelines"] == sorted([PIPE_OTHER, PIPE_DB]) and PIPE_DB not in w.requests[-1]["body"]["retail_pipelines"]
        assert out["outcomes"] == {"given": 1, "refused": 2} and w.tasks_calls == [DEAL]         # живой запрос задач один и только там, где сделка дошла до сети
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL + 1) is None and store.get_owner(DEAL + 2) is None
        assert "только ответ" in last_row(DEAL + 1)["note"] and "старше 3 сут." in last_row(DEAL + 2)["note"]
        # Взятая сделка живёт по правилам воронки с инициативой: как у розницы.
        await env.advance(30)
        turn = env.turn_requests[-1]
        assert turn["event"]["type"] == "deal_given" and turn["mode_settings"]["initiative"] is True
        assert settings.initiative_allowed(PIPE_DB) is True and await E._initiative_ok(store.get_owner(DEAL)) is True
        await E.silence_arm(DEAL)
        assert silence.get(DEAL) is not None                                                    # лестница тишины идёт и по сделке «Работы с базой»
    run(go())


def test_the_initiative_default_is_the_two_retail_pipelines_and_work_with_the_base(env):
    assert cfg.INITIATIVE_PIPELINES_DEFAULT == frozenset({PIPE_RETAIL, PIPE_TANGEM, PIPE_DB})
    assert cfg.MODE_PIPELINES["prod"] == frozenset({PIPE_RETAIL, PIPE_TANGEM})                 # воронки режима прежние: «Работа с базой» их не расширяет
    setup(env, pipelines_any=True)
    assert settings.initiative_pipelines() == cfg.INITIATIVE_PIPELINES_DEFAULT
    assert settings.initiative_allowed(PIPE_DB) is False                                       # `AI_PIPELINES_ANY_ALLOWED` не включён: ИИ в этой воронке вообще не ведёт сделки
    env.mp.setattr(cfg, "PIPELINES_ANY_ALLOWED", True)
    env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM}), test=frozenset(), at=0.0))
    assert settings.initiative_allowed(PIPE_DB) is False                                       # промпта на воронку нет: не пишем
    allow_other(env)
    assert settings.initiative_allowed(PIPE_DB) is True and settings.initiative_allowed(PIPE_OTHER) is False


def test_a_silent_retail_client_is_not_taken_when_the_panel_took_the_initiative_away(env, w):
    async def go():
        setup(env, initiative_pipelines=[PIPE_TANGEM])                                         # «ОП розница» переведена в «только отвечаем»
        w.deal(waiting=False, out_ago=10 * HOUR)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "инициатива выключена" in last_row()["note"] and store.get_owner(DEAL) is None
        w.deal(DEAL + 1, waiting=True)                                                         # а ответ клиенту, который ждёт, по-прежнему берётся
        assert (await S.run_once(E))["outcomes"] == {"given": 1}
    run(go())


def test_a_pipeline_without_a_prompt_is_not_a_candidate_even_if_the_panel_returns_it(env, w):
    async def go():
        setup(env, pipelines_any=True)
        env.mp.setattr(cfg, "PIPELINES_ANY_ALLOWED", True)
        env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM}), test=frozenset(), at=0.0))   # на «чужую» воронку промпта нет
        env.mp.setattr(prompts, "_last_ok", True)
        w.candidates.append({"deal_id": DEAL, "pool": "other", "pipeline_id": PIPE_OTHER, "chat_id": "1920099999", "channel_id": CHAN, "waiting": True,
                             "chat_type": "telegram", "last_client_at": iso(env.t - 60), "last_msg_at": iso(env.t - 60)})
        w.raw_candidates = True                                                                 # панель вернула сделку чужой воронки, хотя её не просили
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and store.get_owner(DEAL) is None
        assert "не ведётся ИИ" in last_row()["note"] and DEAL in S.excluded_ids(env.t)           # журнал держит сделку сутки
    run(go())


def test_exclude_holds_what_the_engine_knows(env, w):
    async def go():
        setup(env)
        store.set_owner(101, store.OWNER_AI, reason="client_message", mode="full")
        store.set_owner(102, store.OWNER_HUMAN, reason="manager_message", mode="full")
        store.set_owner(103, store.OWNER_ENGINE, reason="sale_completed", closed=True)
        store.set_owner(104, store.OWNER_ENGINE, reason="mode_changed")                        # снята с ИИ
        S.log(201, "refused", "причина", env.t - 3600)                                         # попытка за последние 24 часа
        S.log(202, "refused", "давно", env.t - 25 * HOUR)                                      # старая: уже не держит
        S.log(203, "too_early", "рано", env.t - 100, retry_at=env.t + 5 * HOUR)               # отложена до retry_at
        S.log(204, "refused", "менеджер", env.t - 100, retry_at=env.t - 10)                    # срок отсрочки прошёл
        S.log(205, "blocked_by_task", "задача", env.t - 100, retry_at=env.t + 99 * HOUR)       # дальше суток не держим
        M.put(301, M.KIND_REMOVED, env.t - 40 * 86400)
        M.put(302, M.KIND_NO_REPLY, env.t - 100)
        M.put(303, M.KIND_NO_REPLY, env.t - 2 * 86400)                                         # давняя: решает время последнего сообщения клиента
        A.create(deal_id=401, from_id=1, from_name="x", to_id=2, to_name="y", chat={}, now=env.t - 86400)
        old = store.set_owner(105, store.OWNER_HUMAN, reason="давно")
        with store._connect() as conn:
            conn.execute("UPDATE deal_owner SET since=? WHERE deal_id=105", (S._iso(env.t - 121 * 86400),))     # старше 120 суток: в запрос не идёт, но движок откажет сам
        assert old["deal_id"] == 105
        got = set(S.excluded_ids(env.t))
        assert got == {101, 102, 103, 104, 201, 203, 205, 301, 302, 401}                      # 205: отсрочка длиннее суток держит не дольше суток с попытки
        assert 203 in S.excluded_ids(env.t + 4 * HOUR) and 203 not in S.excluded_ids(env.t + 6 * HOUR)
        assert 205 in S.excluded_ids(env.t + 23 * HOUR) and 205 not in S.excluded_ids(env.t + 25 * HOUR)
        w.deal(DEAL)
        await S.run_once(E)
        assert set(w.requests[-1]["body"]["exclude"]) == got
    run(go())


def test_exclude_is_cut_by_importance_at_the_cap(env, w):
    setup(env)
    env.mp.setattr(S, "EXCLUDE_MAX", 3)
    store.set_owner(101, store.OWNER_AI, reason="client_message", mode="full")
    S.log(201, "refused", "x", env.t - 100)
    M.put(301, M.KIND_REMOVED, env.t - 100)
    M.put(302, M.KIND_NO_REPLY, env.t - 100)
    assert S.excluded_ids(env.t) == [301, 302, 201]                                            # отметки, потом журнал, владельцы в хвосте


def test_the_deal_is_not_asked_again_for_a_day_and_comes_back_after(env, w):
    async def go():
        setup(env)
        w.deal(DEAL, client_ago=5 * 86400)
        w.candidates[0]["waiting"] = True
        env.mp.setattr(cfg, "CHANNELS", {"другой-канал"})                                      # канал не слушаем: отказ
        await S.run_once(E)
        assert w.outcomes() == [(DEAL, "refused")]
        await S.run_once(E)
        assert len(w.outcomes()) == 1 and DEAL in w.requests[-1]["body"]["exclude"]            # сутки сделку не просим
        env.t += 25 * HOUR
        await S.run_once(E)
        assert len(w.outcomes()) == 2 and DEAL not in w.requests[-1]["body"]["exclude"]
    run(go())


# =============================================================================================
# сбои панели
# =============================================================================================

@pytest.mark.parametrize("kind", ["404", "500", "401", "network", "not_json", "list", "no_candidates"])
def test_a_bad_panel_never_breaks_the_run(env, w, kind, caplog):
    caplog.set_level(logging.WARNING, logger="uvicorn")

    async def go():
        setup(env)
        w.deal()
        if kind in ("404", "500", "401"):
            w.status = int(kind)
        elif kind == "network":
            w.fail = True
        elif kind == "not_json":
            w.raw = b"<html>404</html>"
        elif kind == "list":
            w.raw = b"[]"
        else:
            w.raw = b'{"total_open": 3}'
        out = await S.run_once(E)
        assert out["skipped"] == "панель не отдала кандидатов" and out["outcomes"] == {} and store.get_owner(DEAL) is None and S.journal() == []
        await S.run_once(E)
        await S.run_once(E)
        warnings = [r for r in caplog.records if "ai_engine_sweep" in r.getMessage() and r.levelno == logging.WARNING and "прогон" not in r.getMessage()]
        assert len(warnings) == 1                                                              # не чаще раза в час
        S._warn_seen.clear()
        await S.run_once(E)
        assert len([r for r in caplog.records if "ai_engine_sweep" in r.getMessage() and r.levelno == logging.WARNING and "прогон" not in r.getMessage()]) == 2
    run(go())


def test_garbage_candidates_are_skipped_and_duplicates_counted_once(env, w):
    async def go():
        setup(env)
        good = w.deal()
        w.candidates[:0] = ["строка", {"deal_id": "x"}, {"deal_id": 5, "pipeline_id": PIPE_RETAIL}, {"deal_id": 6, "pipeline_id": PIPE_RETAIL, "chat_id": "1"},
                            dict(good), {"deal_id": True, "pipeline_id": 1, "chat_id": "1", "channel_id": "2"}]
        w.raw_candidates = True
        out = await S.run_once(E)
        assert out["candidates"] == 1 and out["outcomes"] == {"given": 1}
    run(go())


# =============================================================================================
# пул «розница»: задачи
# =============================================================================================

def test_an_open_task_of_an_employee_blocks_the_deal_for_six_hours(env, w):
    async def go():
        setup(env)
        w.deal()
        w.tasks[DEAL] = [{"id": 1, "responsible_user_id": OLD_MANAGER, "text": "позвонить клиенту"}]
        out = await S.run_once(E)
        assert out["outcomes"] == {"blocked_by_task": 1} and store.get_owner(DEAL) is None
        assert w.tasks_calls == [DEAL]                                                         # один живой запрос по сделке
        row = last_row()
        assert row["outcome"] == "blocked_by_task" and abs(row["retry_at"] - (env.t + 6 * HOUR)) < 1
        assert w.amo.patches == [] and env.notes == [] and env.alerts == []
        await S.run_once(E)
        assert DEAL in w.requests[-1]["body"]["exclude"] and w.tasks_calls == [DEAL]
        w.tasks[DEAL] = []                                                                     # задачу закрыли
        env.t += 6 * HOUR + 1
        assert DEAL not in S.excluded_ids(env.t)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1} and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_tasks_of_the_ai_user_do_not_block_and_are_not_touched(env, w):
    async def go():
        setup(env)
        w.deal()
        intake = {"id": 50, "text": "Новый заказ с сайта", "task_type_id": 4232862, "created_by": 0, "responsible_user_id": AI_USER["id"], "deal_id": DEAL}
        env.tasks_open.append(intake)
        w.tasks[DEAL] = [intake]
        out = await S.run_once(E)
        await drain()
        assert out["outcomes"] == {"given": 1}
        assert "is_completed" not in intake and len(env.tasks_open) == 1                       # задачи приёма не закрыты, новых нет
        assert w.tasks_calls == [DEAL]
    run(go())


def test_one_foreign_task_among_the_ai_ones_blocks(env, w):
    async def go():
        setup(env)
        w.deal()
        w.tasks[DEAL] = [{"id": 1, "responsible_user_id": AI_USER["id"]}, {"id": 2, "responsible_user_id": 4242}, {"id": 3, "responsible_user_id": None}]
        out = await S.run_once(E)
        assert out["outcomes"] == {"blocked_by_task": 1} and "2" in last_row()["note"]
    run(go())


def test_amo_not_answering_about_tasks_is_not_read_as_no_tasks(env, w):
    async def go():
        setup(env)
        w.deal()
        w.tasks[DEAL] = None
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"amo_unavailable": 1} and store.get_owner(DEAL) is None
        assert abs(row["retry_at"] - (env.t + 15 * 60)) < 1
        assert DEAL not in S.excluded_ids(env.t + 16 * 60) and DEAL in S.excluded_ids(env.t + 10 * 60)
    run(go())


def test_two_amo_failures_in_a_row_stop_the_run(env, w):
    async def go():
        setup(env)
        for i in range(4):
            w.deal(DEAL + i)
            w.tasks[DEAL + i] = None
        out = await S.run_once(E)
        assert out["outcomes"] == {"amo_unavailable": 2} and "две неудачи подряд" in out["skipped"]
        env.amo_down = True
        for i in range(4):
            w.tasks[DEAL + i] = []
        w.tasks.clear()
        S._state["last_result"] = ""
        out = await S.run_once(E)
        assert out["outcomes"] == {"amo_unavailable": 2}                                       # поиск сделок в amo тоже `amo_unavailable`
    run(go())


def other_world(env, w):
    """Пул «остальные»: `pipelines_any` разрешён, на воронку есть промпт."""
    setup(env, pipelines_any=True)
    allow_other(env)


def test_the_other_pool_looks_at_tasks_too_and_a_manager_task_blocks_the_deal(env, w):
    async def go():
        other_world(env, w)
        w.deal(pipeline=PIPE_OTHER)
        w.tasks[DEAL] = [{"id": 1, "responsible_user_id": OLD_MANAGER, "text": "позвонить клиенту"}]
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"blocked_by_task": 1} and store.get_owner(DEAL) is None and w.tasks_calls == [DEAL]     # один живой запрос
        assert abs(row["retry_at"] - (env.t + 6 * HOUR)) < 1 and "задач" in row["note"]
        assert w.amo.patches == [] and env.turn_requests == [] and env.notes == [] and env.alerts == []
        assert DEAL in S.excluded_ids(env.t + 5 * HOUR) and DEAL not in S.excluded_ids(env.t + 6 * HOUR + 1)
        await S.run_once(E)
        assert w.tasks_calls == [DEAL]                                                          # до конца срока сделку не просим и задачи не читаем
    run(go())


def test_the_other_pool_takes_a_deal_without_tasks_with_one_live_request(env, w):
    async def go():
        other_world(env, w)
        w.deal(pipeline=PIPE_OTHER)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1} and store.get_owner(DEAL)["owner"] == "ai" and w.tasks_calls == [DEAL]
    run(go())


def test_the_other_pool_ignores_a_task_that_belongs_to_the_ai_user(env, w):
    async def go():
        other_world(env, w)
        w.deal(pipeline=PIPE_OTHER)
        w.tasks[DEAL] = [{"id": 5, "responsible_user_id": AI_USER["id"], "text": "задача ИИ"}]
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1} and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_the_other_pool_does_not_read_amo_silence_as_no_tasks(env, w):
    async def go():
        other_world(env, w)
        w.deal(pipeline=PIPE_OTHER)
        w.tasks[DEAL] = None                                                                    # amo не ответила по задачам
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"amo_unavailable": 1} and store.get_owner(DEAL) is None
        assert abs(row["retry_at"] - (env.t + 15 * 60)) < 1
        assert DEAL in S.excluded_ids(env.t + 10 * 60) and DEAL not in S.excluded_ids(env.t + 16 * 60)
        w.tasks[DEAL] = []                                                                      # amo ожила: через 15 минут сделку берут
        env.t += 16 * 60
        assert (await S.run_once(E))["outcomes"] == {"given": 1}
    run(go())


def test_both_pools_use_the_same_task_rule_in_one_run(env, w):
    async def go():
        other_world(env, w)
        w.deal(DEAL, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 1, pipeline=PIPE_OTHER)
        w.deal(DEAL + 2, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 3, pipeline=PIPE_OTHER)
        w.tasks[DEAL] = w.tasks[DEAL + 1] = [{"id": 1, "responsible_user_id": OLD_MANAGER}]
        w.tasks[DEAL + 2] = [{"id": 2, "responsible_user_id": AI_USER["id"]}]
        env.mp.setattr(S, "MAX_PER_RUN", 4)
        out = await S.run_once(E)
        assert out["outcomes"] == {"blocked_by_task": 2, "given": 2}
        assert sorted(w.tasks_calls) == [DEAL, DEAL + 1, DEAL + 2, DEAL + 3]                    # по одному запросу на сделку, не два
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1) is None
        assert store.get_owner(DEAL + 2)["owner"] == "ai" and store.get_owner(DEAL + 3)["owner"] == "ai"
        assert env.tasks_open == []                                                             # задачи amo не создаём и не закрываем
    run(go())


# =============================================================================================
# пул «розница»: молчание клиента
# =============================================================================================

def test_a_silent_client_is_taken_only_after_the_standard_silence_threshold(env, w):
    async def go():
        setup(env)
        w.deal(waiting=False, out_ago=HOUR)                                                    # последнее сообщение наше, час назад; порог по умолчанию 3 часа
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"too_early": 1} and store.get_owner(DEAL) is None and w.tasks_calls == []     # до сети дело не дошло
        assert abs(row["retry_at"] - (env.t + 2 * HOUR)) < 1 and "рано" in row["note"]
        assert DEAL in S.excluded_ids(env.t + HOUR) and DEAL not in S.excluded_ids(env.t + 2 * HOUR + 1)
        env.t += 2 * HOUR + 1
        w.candidates[0]["last_out_at"] = iso(env.t - 3 * HOUR - 1)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1}
        await env.advance(30)
        assert env.turn_requests[-1]["event"]["type"] == "deal_given"
    run(go())


def test_the_threshold_is_the_reminder_setting_of_the_panel(env, w):
    async def go():
        setup(env, functions={"reminder": {"enabled": True, "after_h": 1}})
        w.deal(waiting=False, out_ago=HOUR + 60)
        assert (await S.run_once(E))["outcomes"] == {"given": 1}
        setup(env, functions={"reminder": {"enabled": True, "after_h": 24}})
        w.deal(DEAL + 1, waiting=False, out_ago=20 * HOUR)
        out = await S.run_once(E)
        assert out["outcomes"] == {"too_early": 1} and abs(last_row(DEAL + 1)["retry_at"] - (env.t + 4 * HOUR)) < 1
        assert S.silence_threshold_s() == (24 * HOUR, "")
    run(go())


def test_a_disabled_reminder_means_no_initiative_toward_silent_clients(env, w):
    async def go():
        setup(env, functions={"reminder": {"enabled": False}})
        w.deal(waiting=False, out_ago=10 * HOUR)
        w.deal(DEAL + 1, waiting=True)                                                         # а ответ клиенту, который ждёт, по-прежнему берётся
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1, "given": 1}
        assert "напоминание клиенту выключено" in last_row(DEAL)["note"] and store.get_owner(DEAL) is None and store.get_owner(DEAL + 1)["owner"] == "ai"
    run(go())


def test_a_waiting_client_has_no_threshold_at_all(env, w):
    async def go():
        setup(env)
        w.deal(waiting=True, client_ago=30)                                                    # написал полминуты назад: ждёт ответа, порога нет
        assert (await S.run_once(E))["outcomes"] == {"given": 1}
    run(go())


def test_the_silence_start_falls_back_to_the_last_message(env, w):
    async def go():
        setup(env)
        cand = w.deal(waiting=False, out_ago=10 * HOUR)
        cand["last_out_at"] = None
        cand["last_msg_at"] = iso(env.t - HOUR)
        assert (await S.run_once(E))["outcomes"] == {"too_early": 1}
        cand["last_msg_at"] = cand["last_out_at"] = None
        w.ignore_exclude = True
        assert (await S.run_once(E))["outcomes"] == {"refused": 1} and "неизвестно" in last_row()["note"]
    run(go())


# =============================================================================================
# пул «остальные»
# =============================================================================================

def test_the_other_pool_answers_only_a_fresh_unanswered_client_message(env, w):
    async def go():
        setup(env, pipelines_any=True)
        allow_other(env)
        w.deal(DEAL, pipeline=PIPE_OTHER, waiting=True, client_ago=2 * 86400)                  # в пределах трёх суток
        w.deal(DEAL + 1, pipeline=PIPE_OTHER, waiting=True, client_ago=4 * 86400)              # старше
        w.deal(DEAL + 2, pipeline=PIPE_OTHER, waiting=False, out_ago=2 * HOUR)                 # последнее сообщение не клиентское
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1, "refused": 2}
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL + 1) is None and store.get_owner(DEAL + 2) is None
        assert "только ответ" in last_row(DEAL + 2)["note"] and "старше 3 сут." in last_row(DEAL + 1)["note"]
        await env.advance(30)
        turn = env.turn_requests[-1]
        assert turn["event"]["type"] == "deal_given" and turn["mode_settings"]["initiative"] is False      # ИИ в этой воронке только отвечает
    run(go())


def test_other_days_is_the_window(env, w):
    async def go():
        setup(env, sweep={**ON, "other_days": 1}, pipelines_any=True)
        allow_other(env)
        w.deal(DEAL, pipeline=PIPE_OTHER, client_ago=23 * HOUR)
        w.deal(DEAL + 1, pipeline=PIPE_OTHER, client_ago=25 * HOUR)
        assert (await S.run_once(E))["outcomes"] == {"given": 1, "refused": 1} and store.get_owner(DEAL) is not None and store.get_owner(DEAL + 1) is None
    run(go())


def test_a_pool_switched_off_takes_nothing_from_it(env, w):
    async def go():
        setup(env, sweep={**ON, "retail": False}, pipelines_any=True)
        allow_other(env)
        w.deal(DEAL, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 1, pipeline=PIPE_OTHER)
        w.raw_candidates = True                                                                 # панель вернула и «розницу», хотя пул выключен: движок откажет сам
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1, "given": 1} and "выключен в настройках" in last_row(DEAL)["note"]
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1)["owner"] == "ai"
    run(go())


# =============================================================================================
# исключения
# =============================================================================================

@pytest.mark.parametrize("owner, closed", [("ai", False), ("human", False), ("engine", False), ("engine", True), ("human", True)])
def test_a_deal_with_an_owner_record_is_never_taken_again(env, w, owner, closed):
    async def go():
        setup(env)
        w.deal()
        w.ignore_exclude = True                                                                # панель даже вернула её: движок отказывает сам
        before = store.set_owner(DEAL, owner, reason="было", mode="full", closed=closed)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "запись владельца" in last_row()["note"]
        after = store.get_owner(DEAL)
        assert (after["owner"], after["epoch"], after["closed"]) == (before["owner"], before["epoch"], before["closed"])
        assert w.amo.patches == [] and env.turn_requests == []
    run(go())


def test_a_deal_taken_off_the_ai_is_not_taken_back_by_the_sweep(env, w):
    async def go():
        setup(env)
        w.deal(chat=CHAT)
        assert (await S.run_once(E))["outcomes"] == {"given": 1}
        await env.advance(1)
        await env.say(env.msg("Здравствуйте", mid="c-1"))
        await env.say(env.msg("Это менеджер", echo=True, author="Иван Иванов", mid="m-1"))      # менеджер забрал диалог у ИИ
        assert store.get_owner(DEAL)["owner"] == "human"
        S.log(999, "refused", "прочее", env.t)
        w.ignore_exclude = True
        env.t += 2 * 86400
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and store.get_owner(DEAL)["owner"] == "human" and store.get_owner(DEAL)["epoch"] == 2
    run(go())


def test_human_marks_keep_a_deal_out_of_the_sweep(env, w):
    async def go():
        setup(env)
        w.deal(DEAL, client_ago=5 * HOUR)
        w.deal(DEAL + 1, client_ago=5 * HOUR)
        w.deal(DEAL + 2, client_ago=5 * HOUR)
        M.put(DEAL, M.KIND_REMOVED, env.t - HOUR)
        M.put(DEAL + 1, M.KIND_NO_REPLY, env.t - HOUR)                                         # клиент писал ДО отметки
        w.candidates[2]["last_client_at"] = iso(env.t - 30 * 60)
        M.put(DEAL + 2, M.KIND_NO_REPLY, env.t - HOUR)                                         # а здесь клиент написал ПОСЛЕ отметки: она не действует
        w.ignore_exclude = True
        out = await S.run_once(E)
        assert out["outcomes"] == {"marked": 2, "given": 1}
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1) is None and store.get_owner(DEAL + 2)["owner"] == "ai"
        assert "удалить" in last_row(DEAL)["note"] and "не требует ответа" in last_row(DEAL + 1)["note"]
        assert w.tasks_calls == [DEAL + 2]                                                     # отмеченные не стоили запросов в amo
    run(go())


def test_a_mark_set_while_the_deal_was_being_checked_stops_it(env, w):
    async def go():
        setup(env)
        w.deal()
        real = F.decide_take

        async def mark_meanwhile(*a, **k):
            M.put(DEAL, M.KIND_REMOVED, env.t)
            return await real(*a, **k)

        env.mp.setattr(F, "decide_take", mark_meanwhile)
        out = await S.run_once(E)
        assert out["outcomes"] == {"marked": 1} and store.get_owner(DEAL) is None and w.amo.patches == []
    run(go())


def test_a_foreign_channel_is_not_taken(env, w):
    async def go():
        setup(env)
        w.deal(channel="чужой-канал")
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "канал" in last_row()["note"] and store.get_owner(DEAL) is None
    run(go())


def test_a_chat_the_engine_is_busy_with_is_left_alone_for_a_while(env, w):
    async def go():
        setup(env)
        cand = w.deal()
        store.put_session(w.chat_key(cand["chat_id"]), channel_id=CHAN, chat_id=cand["chat_id"], state=store.S_COLLECTING, next_at=env.t + 30, deal_id=None)
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"refused": 1} and "в работе движка" in row["note"] and abs(row["retry_at"] - (env.t + 600)) < 1
        store.put_session(w.chat_key(cand["chat_id"]), state=store.S_HUMAN, skip_until=env.t + 2 * HOUR)
        w.ignore_exclude = True
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "человеческий" in last_row()["note"] and abs(last_row()["retry_at"] - (env.t + 2 * HOUR)) < 1
        assert store.get_owner(DEAL) is None
    run(go())


def test_a_deal_the_autopilot_runs_is_left_alone(env, w):
    async def go():
        setup(env)
        w.deal()
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: True)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "авто-режим" in last_row()["note"] and store.get_owner(DEAL) is None
    run(go())


def test_the_client_writing_meanwhile_is_not_overridden(env, w):
    async def go():
        setup(env)
        w.deal()
        real = F.decide_take

        async def client_wrote(*a, **k):
            store.set_owner(DEAL, store.OWNER_AI, reason="client_message", mode="full")        # обычный вход по сообщению клиента успел раньше
            return await real(*a, **k)

        env.mp.setattr(F, "decide_take", client_wrote)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "появился владелец" in last_row()["note"]
        assert store.get_owner(DEAL)["epoch"] == 1 and w.amo.patches == []
    run(go())


# =============================================================================================
# те же правила взятия, что при сообщении клиента
# =============================================================================================

def test_a_taken_deal_gets_the_same_event_as_the_give_button(env, w):
    async def go():
        setup(env)
        w.deal()
        out = await S.run_once(E)
        await drain()
        assert out["outcomes"] == {"given": 1}
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["reason"], owner["mode"], owner["chat_id"]) == ("ai", 1, "sweep", "full", w.candidates[0]["chat_id"])
        assert w.amo.patches == [(DEAL, AI_USER["id"])]                                        # «перераспределить»: ответственный стал пользователем ИИ
        assert G.cutoff_iso(DEAL) and G.is_forced(DEAL) is False and F.guard_row(DEAL)["first_msg_at"] is None
        assert last_row()["outcome"] == "given"
        await env.advance(30)
        req = env.turn_requests[-1]
        assert req["reason"] == "event" and req["event"]["type"] == "deal_given" and req["messages"] == [] and req["up_to_message_id"] == ""
        assert req["epoch"] == 1 and req["deal_id"] == DEAL and req["manager_cutoff"] and "given_forced" not in req
        assert req["chat"] == {"chat_id": w.candidates[0]["chat_id"], "chat_type": "telegram", "channel_id": CHAN}
        assert req["mode_settings"]["initiative"] is True
        texts = [t for d, t in env.notes if d == DEAL]
        assert any("подобрана ИИ-менеджером" in t for t in texts) and any("Ответственный изменён" in t for t in texts)
        assert env.alerts == [] and env.tasks_open == []
    run(go())


def test_leave_takes_new_deals_only_and_leaves_the_existing_ones(env, w):
    async def go():
        setup(env, policy="leave")
        w.deal(DEAL, age_days=30)
        w.deal(DEAL + 1, age_days=0)                                                           # создана после границы: новая
        w.deal(DEAL + 2, age_days=30)
        w.candidates[1]["last_client_at"] = iso(env.t - 60)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 2, "given": 1}
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1)["owner"] == "ai"
        assert "существующая сделка" in last_row(DEAL)["note"]
        assert w.amo.patches == []                                                             # ответственного не меняли
    run(go())


def test_redistribute_without_the_pilot_assigns_at_once_and_with_the_pilot_proposes(env, w):
    async def go():
        setup(env, review=True)
        w.deal()
        out = await S.run_once(E)
        assert out["outcomes"] == {"reassign_pending": 1}
        row = A.journal()[0]
        assert row["deal_id"] == DEAL and row["state"] == "pending" and row["source"] == "sweep" and row["reason"] == "existing_deal_redistribute"
        assert (row["from_user_id"], row["to_user_id"]) == (OLD_MANAGER, AI_USER["id"])
        assert store.get_owner(DEAL) is None and w.amo.patches == [] and env.turn_requests == [] and env.tasks_open == []   # в amo ничего, ИИ не берёт
        assert len(env.alerts) == 1 and "ИИ просит разрешения" in env.alerts[0] and "@" not in env.alerts[0]               # одна строка без пинга, как у обычного предложения
        assert store.get_session(w.chat_key(w.candidates[0]["chat_id"]))["state"] == "awaiting"
        assert A.wire(row)["reason"] == "existing_deal_redistribute" and set(A.wire(row)) == {"ref", "deal_id", "from_user", "to_user", "reason", "at"}   # форма для панели прежняя
    run(go())


def test_the_ai_user_already_responsible_needs_no_proposal_even_with_the_pilot(env, w):
    async def go():
        setup(env, review=True)
        w.deal(responsible=AI_USER["id"])
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1} and A.journal() == [] and w.amo.patches == []
    run(go())


def test_an_approved_proposal_makes_the_ai_read_the_history_and_leaves_the_tasks_alone(env, w):
    async def go():
        setup(env, review=True)
        w.deal()
        intake = {"id": 50, "text": "Новый заказ с сайта", "task_type_id": 4232862, "created_by": 0, "responsible_user_id": AI_USER["id"], "deal_id": DEAL}
        env.tasks_open.append(intake)
        w.tasks[DEAL] = [intake]
        await S.run_once(E)
        ref = A.journal()[0]["ref"]
        code, out = await E.reassign_decision(ref, DEAL, "approve", "Катя Иванова")
        await drain()
        assert code == 200 and out["state"] == "approved" and "перечитает переписку" in out["detail"]
        assert w.amo.patches == [(DEAL, AI_USER["id"])] and store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["reason"] == "reassign_approved"
        assert G.cutoff_iso(DEAL) and F.guard_row(DEAL)["first_msg_at"] is None
        await env.advance(30)
        req = env.turn_requests[-1]
        assert req["reason"] == "event" and req["event"]["type"] == "deal_given" and req["messages"] == []
        assert "is_completed" not in intake and len(env.tasks_open) == 1                       # задачи amo не закрыты
        assert [t for d, t in env.notes if "подобрана ИИ-менеджером" in t]
    run(go())


def test_a_rejected_proposal_is_not_offered_again(env, w):
    async def go():
        setup(env, review=True)
        w.deal()
        await S.run_once(E)
        ref = A.journal()[0]["ref"]
        assert (await E.reassign_decision(ref, DEAL, "reject", "Катя Иванова"))[1]["state"] == "rejected"
        assert store.get_owner(DEAL) is None
        env.t += 3 * 86400
        assert DEAL in S.excluded_ids(env.t)                                                   # предложения за 7 дней держат сделку вне подбора
        await S.run_once(E)
        assert DEAL in w.requests[-1]["body"]["exclude"] and len(A.journal()) == 1
    run(go())


def test_a_manager_who_wrote_recently_postpones_the_deal_until_the_quiet_time_passes(env, w):
    async def go():
        setup(env)
        cand = w.deal()
        F.note_manager_out(w.chat_key(cand["chat_id"]), env.t - 60)
        out = await S.run_once(E)
        row = last_row()
        assert out["outcomes"] == {"refused": 1} and "менеджер писал клиенту" in row["note"]
        assert abs(row["retry_at"] - (env.t - 60 + 15 * 60)) < 1 and store.get_owner(DEAL) is None and w.amo.patches == []
        assert DEAL in S.excluded_ids(env.t + 10 * 60) and DEAL not in S.excluded_ids(env.t + 15 * 60)       # откладывается до конца тишины, а не на сутки
        env.t += 15 * 60
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1}
    run(go())


def test_several_open_deals_of_one_client_go_to_nobody_and_call_nobody(env, w):
    async def go():
        setup(env)
        first = w.deal(DEAL)
        w.deal(DEAL + 1)
        for ld in env.leads:
            ld["_q"] = first["chat_id"]                                                        # у клиента две подходящие сделки: правило D
        for cand in w.candidates:
            cand["chat_id"] = first["chat_id"]
        out = await S.run_once(E)
        assert out["outcomes"] == {"needs_human": 2} and "правило D" in last_row(DEAL)["note"]
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1) is None
        assert env.alerts == [] and env.tasks_open == [] and env.notes == [] and env.bot_messages == [] and w.amo.patches == []
        assert w.tasks_calls == [DEAL, DEAL + 1]
        assert store.get_session(w.chat_key(first["chat_id"])) is None
    run(go())


def test_a_parallel_deal_with_work_signs_is_rule_a_and_calls_nobody(env, w):
    async def go():
        setup(env)
        first = w.deal(DEAL)
        busy = w.deal(DEAL + 1, price=5000, pipeline=PIPE_TANGEM)
        busy["chat_id"] = first["chat_id"]
        env.leads[1]["_q"] = first["chat_id"]
        w.candidates.pop()                                                                     # кандидат один, но у клиента вторая сделка с бюджетом
        out = await S.run_once(E)
        assert out["outcomes"] == {"needs_human": 1} and "правило A" in last_row(DEAL)["note"]
        assert env.alerts == [] and env.tasks_open == [] and store.get_owner(DEAL) is None
    run(go())


def test_a_deal_that_closed_meanwhile_is_closed_in_the_log(env, w):
    async def go():
        setup(env)
        w.deal()
        env.leads[0]["status_id"] = 142
        out = await S.run_once(E)
        assert out["outcomes"] == {"closed": 1} and store.get_owner(DEAL) is None
    run(go())


def test_budget_and_filters_of_the_mode_apply(env, w):
    async def go():
        setup(env, budget={"skip_from_rub": 1000}, filters={"request_type": ["order"]})
        w.deal(DEAL, price=5000)
        w.deal(DEAL + 1, type="Предзаказ")
        w.deal(DEAL + 2, type="Заказ")
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 2, "given": 1}
        assert "бюджет" in last_row(DEAL)["note"] and "тип заявки" in last_row(DEAL + 1)["note"] and store.get_owner(DEAL + 2)["owner"] == "ai"
    run(go())


def test_the_contact_list_of_the_mode_applies(env, w):
    async def go():
        setup(env, contacts={"scope": "full", "restricted": True, "items": [{"contact_id": 999, "phone_norm": "79990000000", "name": "Другой"}]})
        w.deal()
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "контакта нет в списке" in last_row()["note"] and store.get_owner(DEAL) is None
    run(go())


def test_the_ai_service_going_down_during_the_run_is_an_error_not_a_take(env, w):
    async def go():
        setup(env)
        w.deal()
        real = F.decide_take

        async def service_died(*a, **k):
            env.health_ok = False
            env.mp.setattr(E, "_ai_ok_at", env.t - 1000)
            return await real(*a, **k)

        env.mp.setattr(F, "decide_take", service_died)
        out = await S.run_once(E)
        assert out["outcomes"] == {"error": 1} and store.get_owner(DEAL) is None and w.amo.patches == []
    run(go())


def test_a_failed_responsible_change_is_an_error_and_the_deal_is_not_taken(env, w):
    async def go():
        setup(env)
        w.deal()
        w.amo.ok, w.amo.status = False, 403
        out = await S.run_once(E)
        assert out["outcomes"] == {"error": 1} and store.get_owner(DEAL) is None and "смена ответственного" in last_row()["note"]
        assert abs(last_row()["retry_at"] - (env.t + 15 * 60)) < 1
    run(go())


# =============================================================================================
# устойчивость
# =============================================================================================

def test_an_exception_in_one_deal_does_not_stop_the_run_or_lose_deals(env, w):
    async def go():
        setup(env)
        w.deal(DEAL)
        w.deal(DEAL + 1)
        real = dedup.resolve
        calls = []

        async def flaky(chat_id, phone, **kw):
            calls.append(chat_id)
            if len(calls) == 1:
                raise RuntimeError("сбой")
            return await real(chat_id, phone, **kw)

        env.mp.setattr(dedup, "resolve", flaky)
        out = await S.run_once(E)
        assert out["outcomes"] == {"error": 1, "given": 1} and store.get_owner(DEAL) is None and store.get_owner(DEAL + 1)["owner"] == "ai"
        assert abs(last_row(DEAL)["retry_at"] - (env.t + 15 * 60)) < 1
    run(go())


def test_a_crash_of_the_whole_run_does_not_reach_the_timer_loop(env, w):
    async def go():
        setup(env)
        w.deal()

        async def boom(body):
            raise RuntimeError("всё упало")

        env.mp.setattr(S, "fetch_candidates", boom)
        out = await S.run_once(E)
        assert out["skipped"] == "прогон упал (см. лог движка)"
        env.mp.setattr(S, "_next_run_at", 0.0)
        await env.advance(1)                                                                   # `tick_once` целиком отрабатывает
        await drain()
        env.mp.setattr(S, "blocked_by", lambda: (_ for _ in ()).throw(RuntimeError("сбой")))
        await S.tick(E, env.t + 1000)                                                          # даже сбой самого `tick` не бросает
    run(go())


def test_a_restart_repeats_nothing(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 3})
        for i in range(5):
            w.deal(DEAL + i)
        await S.run_once(E)
        assert S.count_taken(env.t) == 2
        epochs = {DEAL + i: store.get_owner(DEAL + i) for i in range(5)}
        env.mp.setattr(S, "_task", None)                                                       # контейнер перезапустили: память пуста, база на месте
        env.mp.setattr(S, "_next_run_at", 0.0)
        env.mp.setattr(S, "_state", {"last_run_at": None, "last_candidates": None, "total_open": None, "last_result": ""})
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 1}                                                 # бюджет часа считается по базе: осталась одна
        taken = {d for d, o in epochs.items() if o}
        assert taken <= set(w.requests[-1]["body"]["exclude"])                                 # уже взятые панели не просим
        assert all(store.get_owner(d)["epoch"] == 1 for d in range(DEAL, DEAL + 5) if store.get_owner(d))
        assert S.count_taken(env.t) == 3 and len([r for r in S.journal() if r["outcome"] == "given"]) == 3
    run(go())


def test_a_second_pass_over_the_same_deal_does_not_double_it(env, w):
    async def go():
        setup(env)
        w.deal()
        w.ignore_exclude = True
        await S.run_once(E)
        await env.advance(30)
        turns = len(env.turn_requests)
        out = await S.run_once(E)
        assert out["outcomes"] == {"refused": 1} and "запись владельца" in last_row()["note"]
        await env.advance(5)
        assert len(env.turn_requests) == turns and store.get_owner(DEAL)["epoch"] == 1 and w.amo.patches == [(DEAL, AI_USER["id"])]
    run(go())


def test_the_sweep_log_is_pruned_after_a_week(env, w):
    async def go():
        setup(env)
        S.log(1, "refused", "давно", env.t - 8 * 86400)
        S.log(2, "refused", "недавно", env.t - 86400)
        w.deal(DEAL + 10)
        await S.run_once(E)
        assert {r["deal_id"] for r in S.journal()} == {2, DEAL + 10}
    run(go())


# =============================================================================================
# срез состояния
# =============================================================================================

def test_the_status_slice_shows_the_sweep(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 4, "other_days": 2})
        for i in range(3):
            w.deal(DEAL + i)
        w.tasks[DEAL + 2] = [{"id": 1, "responsible_user_id": OLD_MANAGER}]
        M.put(900, M.KIND_NO_REPLY, env.t)
        M.put(901, M.KIND_REMOVED, env.t)
        M.put(902, M.KIND_REMOVED, env.t)
        before = E.status()["sweep"]
        assert (before["enabled"], before["taken_last_hour"], before["budget_left"], before["last_run_at"], before["outcomes_24h"]) == (True, 0, 4, None, {})
        await S.run_once(E)
        sw = E.status()["sweep"]
        assert sw["enabled"] is True and sw["per_hour"] == 4 and sw["pools"] == {"retail": True, "other": True, "other_days": 2}
        assert (sw["taken_last_hour"], sw["budget_left"]) == (2, 2)
        assert sw["outcomes_24h"] == {"given": 2} and sw["last_candidates"] == 3 and sw["total_open"] == 3
        assert sw["last_run_at"] == iso(env.t).replace("+00:00", "Z") or sw["last_run_at"].startswith(iso(env.t)[:16])
        assert sw["marks"] == {"no_reply": 1, "removed": 2, "total": 3} and sw["blocked_by"] is None and "кандидатов 3" in sw["last_result"]
        await S.run_once(E)
        sw = E.status()["sweep"]
        assert sw["outcomes_24h"] == {"given": 2, "blocked_by_task": 1} and sw["budget_left"] == 2
        setup(env, sweep=None)
        sw = E.status()["sweep"]
        assert sw["enabled"] is False and sw["per_hour"] is None and sw["budget_left"] is None and sw["blocked_by"] == "подбор выключен в настройках"
        assert "loops_alive" in E.status()                                                      # срез по-прежнему считает ровно циклы `_tasks`, подбор среди них не числится
    run(go())


def test_the_status_slice_survives_an_unreadable_base(env, w):
    setup(env)
    env.mp.setattr(S, "count_taken", lambda now: (_ for _ in ()).throw(RuntimeError("база")))
    assert E.status()["sweep"] == "ошибка чтения базы"


def test_the_sweep_log_columns_are_the_agreed_ones(env):
    S.log(1, "given", "ok", env.t)
    with S._Db() as conn:
        assert [r[1] for r in conn.execute("PRAGMA table_info(sweep_log)")] == ["id", "deal_id", "at", "outcome", "note", "retry_at"]


# =============================================================================================
# константы, рубильник, миграция
# =============================================================================================

def test_the_agreed_numbers():
    assert (S.INTERVAL_S, S.MAX_PER_RUN, S.CANDIDATES_LIMIT, S.EXCLUDE_MAX) == (300, 2, 30, 3000)
    assert (S.DEFAULT_PER_HOUR, S.DEFAULT_OTHER_DAYS, S.RETRY_TASK_S, S.RETRY_ERROR_S) == (10, 3, 6 * 3600, 15 * 60)
    assert S.PATH == "/api/ingest/ai/sweep-candidates"


@pytest.mark.parametrize("value, expected", [(None, False), ("", False), ("0", False), ("true", False), ("1", True), (" 1 ", True)])
def test_the_kill_switch_is_on_only_when_it_equals_one(value, expected):
    here = os.path.dirname(os.path.abspath(__file__))
    env_vars = {k: v for k, v in os.environ.items() if k != "AI_SWEEP_DISABLED"}
    if value is not None:
        env_vars["AI_SWEEP_DISABLED"] = value
    out = subprocess.run([sys.executable, "-X", "utf8", "-c", "import ai_engine_config as c; print(c.SWEEP_DISABLED)"],
                         cwd=here, env=env_vars, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.strip().splitlines()[-1] == str(expected)


def test_the_proposal_table_of_a_live_base_gets_its_source_column(env):
    with store._connect() as conn:                                                              # таблица прежнего выката: колонки `source` нет
        conn.executescript(
            "CREATE TABLE reassign_proposal (ref TEXT PRIMARY KEY, deal_id INTEGER NOT NULL, from_user_id INTEGER, from_user_name TEXT NOT NULL DEFAULT '', "
            "to_user_id INTEGER NOT NULL, to_user_name TEXT NOT NULL, reason TEXT NOT NULL, proposed_at REAL NOT NULL, state TEXT NOT NULL, decided_by TEXT, "
            "decided_at REAL, note TEXT NOT NULL DEFAULT '', chat_key TEXT NOT NULL DEFAULT '', chat_id TEXT NOT NULL DEFAULT '', chat_type TEXT NOT NULL DEFAULT '', "
            "channel_id TEXT NOT NULL DEFAULT '', phone TEXT NOT NULL DEFAULT '', contact_name TEXT NOT NULL DEFAULT '', delivered_at REAL, "
            "delivery_tries INTEGER NOT NULL DEFAULT 0, delivery_next_at REAL NOT NULL DEFAULT 0);"
            "INSERT INTO reassign_proposal (ref, deal_id, to_user_id, to_user_name, reason, proposed_at, state) VALUES ('rp-1-1', 1, 2, 'x', 'r', 1, 'pending');")
    assert A.get("rp-1-1")["source"] == "" and A.get("rp-1-1")["state"] == "pending"            # прежняя строка читается, источник пуст
    row = A.create(deal_id=2, from_id=None, from_name="", to_id=2, to_name="x", chat={}, now=5.0, source=A.SOURCE_SWEEP)
    assert row["source"] == "sweep" and A.get(row["ref"])["source"] == "sweep"
    assert sorted(A.recent_deal_ids(0.0)) == [1, 2]
    assert A.count_open() == 2                                                                  # повторное открытие таблицы миграцию не ломает


def test_a_proposal_of_an_ordinary_client_message_has_an_empty_source(env):
    async def go():
        from test_ai_engine_approval import make_proposal
        _, _, row = await make_proposal(env)
        assert row is not None and row["source"] == ""
    run(go())
