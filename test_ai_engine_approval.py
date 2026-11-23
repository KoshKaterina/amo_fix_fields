"""Подтверждение человеком смены ответственного («Перераспределить существующие» при включённом пилоте, `ai_engine_approval`, решение Кати 23.11.2026).

Что проверяем:
  • пилот включён: вместо отказа создаётся предложение, сделка «ждёт решения» (ИИ не отвечает и не берёт её, в amo ничего не пишется, сообщения клиента
    копятся); пилот выключен: ответственный назначается сразу; одно открытое предложение на сделку; 24 часа после отказа;
  • доставка в панель: тело по договорённости, повторы 1, 5, 25 секунд и раз в минуту, отказы панели (404, 401, 403) редко, рестарт ничего не теряет;
  • решение человека: approve (актуально, устарело по менеджеру, по фильтрам, по закрытой сделке, по смене ответственного, сбой amo и ИИ-сервиса с повтором),
    reject, идемпотентность, неверный токен, узкий токен не открывает прочие ручки;
  • устаревание само: режим, политика, рубильник, менеджер, закрытая сделка, владелец; протухшие настройки ничего не снимают;
  • аварийный рубильник AI_REASSIGN_DISABLED, срез состояния, совместимость (документ без redistribute ведёт себя как сегодня).

Сети нет: amo, МойСклад, панель и ИИ-сервис подставные. Запуск: python -m pytest test_ai_engine_approval.py -q
"""
import asyncio
import json
import logging
import re
import time

import httpx
import pytest

import ai_engine as E
import ai_engine_api as API
import ai_engine_approval as A
import ai_engine_config as cfg
import ai_engine_events as panel_events
import ai_engine_filters as F
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from test_ai_engine import CHAT_KEY, DEAL, PHONE, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_filters import AI_USER, OLD_MANAGER, Amo, client_writes, manager_writes, put_lead, quiet, set_full
from test_ai_engine_followup import drain

GIVE_TOKEN = "give-token"
STATUS_CLOSED = 142
EXPECTED_LINE = ("ИИ просит разрешения взять сделку {deal}: сменить ответственного с Иванова Мария на Гладков Александер. "
                 "Подтвердите во вкладке «Пилот» панели Team.")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    F._cache.clear()
    F._inflight.clear()
    F._warned.clear()
    settings.set_document(None)
    A._sending.clear()
    A._deciding.clear()
    panel_events._log_seen.clear()
    panel_events._suppressed.clear()
    monkeypatch.setattr(A, "_last_orphans", 0.0)
    monkeypatch.setattr(F, "REASSIGN_DISABLED", False)
    yield
    A._sending.clear()
    A._deciding.clear()


class FakePanel:
    """Подставная панель: принимает предложения, отвечает заданным кодом, помнит запросы."""

    def __init__(self, env, status=200):
        self.requests: list[dict] = []
        self.status = status
        self.down = False

        def handler(request: httpx.Request) -> httpx.Response:
            if self.down:
                raise httpx.ConnectError("нет связи")
            self.requests.append({"path": request.url.path, "token": request.headers.get("x-ingest-token"), "body": json.loads(request.content), "at": env.t})
            return httpx.Response(self.status, json={"ok": True, "duplicate": False} if self.status == 200 else {"detail": "нет"})

        env.mp.setattr(A, "_transport", httpx.MockTransport(handler))
        env.mp.setattr(cfg, "PANEL_URL", "http://panel.test")
        env.mp.setattr(cfg, "PANEL_TOKEN", "ingest-token")


def pilot(env, review=True, **over):
    """Полный режим, `redistribute`, пилот включён (или выключен: `review=False`)."""
    blocks = dict(existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, review={"enabled": review})
    blocks.update(over)
    set_full(env, **blocks)


async def make_proposal(env, review=True, ok=True, responsible=OLD_MANAGER, **over):
    amo = Amo(env, ok=ok)
    pilot(env, review=review, **over)
    ld = put_lead(env, created_at=int(env.t) - 3600, responsible=responsible)
    await client_writes(env)
    rows = A.journal()
    return amo, ld, (rows[0] if rows else None)


async def decide(ref, decision="approve", by="Катя Иванова", deal=DEAL):
    return await E.reassign_decision(ref, deal, decision, by)


def turn_events(env):
    return [r for r in env.turn_requests]


# =============================================================================================
# создание предложения
# =============================================================================================

def test_the_pilot_turns_a_refusal_into_a_proposal_and_the_deal_waits(env):
    async def go():
        amo, ld, row = await make_proposal(env)
        assert row is not None and row["state"] == "pending" and row["deal_id"] == DEAL
        assert re.fullmatch(rf"rp-{DEAL}-\d+", row["ref"]) and len(row["ref"]) <= 64
        assert (row["from_user_id"], row["from_user_name"], row["to_user_id"], row["to_user_name"]) == (OLD_MANAGER, "Иванова Мария", 11513202, "Гладков Александер")
        assert row["reason"] == "existing_deal_redistribute" and row["decided_by"] is None and row["decided_at"] is None
        assert amo.patches == [] and store.get_owner(DEAL) is None                            # в amo не пишется, сделку ИИ не берёт
        assert env.turn_requests == [] and env.notes == [] and env.tasks_open == []
        assert store.get_session(CHAT_KEY)["state"] == "awaiting"
        assert [r["text"] for r in store.pending_inbox(CHAT_KEY)] == ["Здравствуйте"]          # сообщение клиента ждёт решения
        assert F.guard_row(DEAL) is None and F.reassign_journal() == []
    run(go())


def test_the_alert_is_one_line_without_a_ping(env):
    async def go():
        await make_proposal(env)
        assert len(env.alerts) == 1
        text = env.alerts[0]
        assert "\n" not in text and "@" not in text and "ИИ-менеджер" not in text           # без шапки, пингов и второй строки
        plain = re.sub(r"<[^>]+>", "", text)
        assert plain == EXPECTED_LINE.format(deal=DEAL)
        assert f"/leads/detail/{DEAL}" in text                                                 # номер сделки ссылкой
    run(go())


def test_an_alert_with_a_ping_keeps_its_head_and_mention(env):
    async def go():
        await E._alert("whatever", ["строка"])
        await E._alert("whatever2", ["строка"], ping=False)
        assert env.alerts[0].startswith("🤖 ИИ-менеджер") and env.alerts[0].count("\n") >= 2
        assert env.alerts[1] == "строка"
    run(go())


def test_without_the_pilot_the_responsible_is_assigned_at_once(env):
    async def go():
        amo, ld, row = await make_proposal(env, review=False)
        assert row is None and A.journal() == []                                              # предложения нет
        assert amo.patches == [(DEAL, 11513202)] and store.get_owner(DEAL)["owner"] == "ai"
        assert [t for _, t in env.notes if "Ответственный изменён" in t] == ["Ответственный изменён с Иванова Мария на Гладков Александер (ИИ, режим Полный)"]
        assert len(env.turn_requests) == 1
    run(go())


def test_the_ai_user_already_responsible_needs_no_proposal_even_with_the_pilot(env):
    async def go():
        amo, ld, row = await make_proposal(env, responsible=AI_USER["id"])
        assert row is None and amo.patches == [] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_deal_without_a_responsible_is_proposed_from_nobody(env):
    async def go():
        amo, ld, row = await make_proposal(env, responsible=None)
        assert row["from_user_id"] is None and row["from_user_name"] == ""
        assert A.wire(row)["from_user"] is None
        assert "без ответственного" in env.alerts[0]
    run(go())


def test_new_messages_while_waiting_pile_up_without_a_second_proposal_or_alert(env):
    async def go():
        await make_proposal(env)
        await env.say(env.msg("Алло?", mid="m2"))
        await env.advance(61)
        await env.say(env.msg("Кто-нибудь здесь?", mid="m3"))
        await env.advance(61)
        assert len(A.journal()) == 1 and len(env.alerts) == 1 and env.turn_requests == []
        assert [r["text"] for r in store.pending_inbox(CHAT_KEY)] == ["Здравствуйте", "Алло?", "Кто-нибудь здесь?"]
        assert store.get_session(CHAT_KEY)["state"] == "awaiting"
    run(go())


def test_only_one_proposal_can_be_open_per_deal(env):
    async def go():
        _, _, row = await make_proposal(env)
        assert A.create(deal_id=DEAL, from_id=1, from_name="x", to_id=2, to_name="y", chat={}, now=env.t) is None
        assert A.open_for_deal(DEAL)["ref"] == row["ref"] and A.count_open() == 1
        assert "ждёт решения" in A.block_reason(DEAL, env.t) and row["ref"] in A.block_reason(DEAL, env.t)
        with pytest.raises(Exception):                                                          # индекс не даёт обойти проверку
            with A._Db() as conn:
                conn.execute("INSERT INTO reassign_proposal (ref, deal_id, to_user_id, to_user_name, reason, proposed_at, state) VALUES ('x', ?, 1, 'y', 'r', 1, 'pending')", (DEAL,))
    run(go())


def test_a_media_message_does_not_start_a_second_proposal(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        await make_proposal(env)
        await env.say(env.msg("", mid="geo-1", typ="geo"))                                        # вложение, которое ИИ не читает: чат пересматривается
        await env.advance(61)
        assert len(A.journal()) == 1 and A.journal()[0]["state"] == "pending" and len(env.alerts) == 1
        assert any("ждёт решения человека" in r.getMessage() and "не взята" in r.getMessage() for r in caplog.records)
    run(go())


def test_proposals_in_the_same_second_get_different_refs(env):
    async def go():
        a = A.create(deal_id=1, from_id=None, from_name="", to_id=2, to_name="y", chat={}, now=1000.5)
        A.transition(a["ref"], "rejected", now=1000.6)
        b = A.create(deal_id=1, from_id=None, from_name="", to_id=2, to_name="y", chat={}, now=1000.9)
        assert a["ref"] == "rp-1-1000" and b["ref"] == "rp-1-1000-2"
    run(go())


def test_no_proposal_for_a_new_deal_or_with_the_leave_policy(env):
    async def go():
        Amo(env)
        pilot(env, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert A.journal() == [] and store.get_owner(DEAL) is None and quiet(env)             # leave: существующие не трогаем, как раньше
    run(go())


def test_a_document_without_the_existing_block_behaves_as_before(env):
    async def go():
        Amo(env)
        set_full(env, review={"enabled": True})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert A.journal() == [] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_the_active_manager_still_blocks_before_any_proposal(env):
    async def go():
        Amo(env)
        pilot(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await manager_writes(env)
        env.t += 60
        await client_writes(env)
        assert A.journal() == [] and quiet(env)
    run(go())


def test_the_emergency_switch_neither_takes_nor_proposes(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        env.mp.setattr(F, "REASSIGN_DISABLED", True)
        for review in (True, False):
            amo, _, row = await make_proposal(env, review=review)
            assert row is None and amo.patches == [] and store.get_owner(DEAL) is None and quiet(env)
            env.t += 700                                                                        # окно пропуска прошло
        assert any("AI_REASSIGN_DISABLED" in r.getMessage() for r in caplog.records)
    run(go())


def test_the_proposal_waits_while_the_ai_service_is_down_and_is_made_after(env):
    async def go():
        Amo(env)
        pilot(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        env.mp.setattr(E, "_ai_ok_at", env.t - 10 ** 6)
        env.health_ok = False
        await client_writes(env)
        assert A.journal() == [] and store.get_owner(DEAL) is None                              # ИИ не отвечает: не просим разрешения на то, чего не сделаем
    run(go())


# =============================================================================================
# доставка в панель
# =============================================================================================

def test_the_proposal_goes_to_the_panel_with_the_agreed_body_and_only_once(env):
    async def go():
        panel = FakePanel(env)
        _, _, row = await make_proposal(env)
        await drain()
        assert len(panel.requests) == 1
        req = panel.requests[0]
        assert req["path"] == "/api/ingest/ai/reassign-proposal" and req["token"] == "ingest-token"
        assert req["body"] == {
            "ref": row["ref"], "deal_id": DEAL, "from_user": {"id": OLD_MANAGER, "name": "Иванова Мария"},
            "to_user": {"id": 11513202, "name": "Гладков Александер"}, "reason": "existing_deal_redistribute",
            "at": time_iso(row["proposed_at"]),
        }
        dump = json.dumps(req["body"], ensure_ascii=False)
        assert PHONE not in dump and "Тиана" not in dump and "Здравствуйте" not in dump       # личных данных клиента в теле нет
        assert A.get(row["ref"])["delivered_at"] is not None
        for _ in range(5):
            await env.advance(30)
        await drain()
        assert len(panel.requests) == 1                                                        # доставленное не шлётся снова
    run(go())


def time_iso(ts):
    return A._iso(ts)


def test_delivery_retries_after_1_5_25_seconds_and_then_every_minute(env):
    async def go():
        panel = FakePanel(env, status=500)
        _, _, row = await make_proposal(env)
        await drain()
        delays = []
        for _ in range(6):
            cur = A.get(row["ref"])
            delays.append(round(cur["delivery_next_at"] - env.t))
            env.t = cur["delivery_next_at"]
            await A._deliver(E, row["ref"])
        assert delays == [1, 5, 25, 60, 60, 60] and A.get(row["ref"])["delivered_at"] is None
        panel.status = 200
        env.t = A.get(row["ref"])["delivery_next_at"]
        await A._deliver(E, row["ref"])
        assert A.get(row["ref"])["delivered_at"] is not None and panel.requests[-1]["body"]["ref"] == row["ref"]
    run(go())


def test_a_network_error_is_retried_like_a_server_error(env):
    async def go():
        panel = FakePanel(env)
        panel.down = True
        _, _, row = await make_proposal(env)
        await drain()
        first = A.get(row["ref"])
        assert first["delivered_at"] is None and first["delivery_tries"] == 1 and round(first["delivery_next_at"] - env.t) == 1
        panel.down = False
        await env.advance(2)
        await drain()
        assert A.get(row["ref"])["delivered_at"] is not None
    run(go())


@pytest.mark.parametrize("status", [404, 401, 403, 400])
def test_a_refusing_panel_is_knocked_on_rarely_and_logged_once(env, caplog, status):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        panel = FakePanel(env, status=status)
        _, _, row = await make_proposal(env)
        await drain()
        assert len(panel.requests) == 1 and round(A.get(row["ref"])["delivery_next_at"] - env.t) == 600
        for _ in range(9):
            await env.advance(60)                                                               # девять минут тишины
        await drain()
        assert len(panel.requests) == 1
        await env.advance(60)
        await drain()
        assert len(panel.requests) == 2                                                         # через десять минут одна новая попытка
        warns = [r for r in caplog.records if r.levelno >= logging.WARNING and "ai_engine_approval" in r.getMessage()]
        assert len(warns) == 1                                                                  # без спама в логе
    run(go())


def test_after_the_panel_is_deployed_the_stored_proposal_is_delivered(env):
    async def go():
        panel = FakePanel(env, status=404)
        _, _, row = await make_proposal(env)
        await drain()
        panel.status = 200
        await env.advance(601)
        await drain()
        assert A.get(row["ref"])["delivered_at"] is not None and len(panel.requests) == 2
    run(go())


def test_nothing_is_sent_when_the_panel_is_not_configured(env):
    async def go():
        _, _, row = await make_proposal(env)                                                    # адреса и токена панели в стенде нет
        await env.advance(120)
        await drain()
        assert A.get(row["ref"])["delivered_at"] is None and A.get(row["ref"])["delivery_tries"] == 0
        assert A.may_send() is False
    run(go())


def test_a_decided_proposal_is_not_sent_late(env):
    async def go():
        panel = FakePanel(env)
        panel.down = True
        _, _, row = await make_proposal(env)
        await drain()
        await decide(row["ref"], "reject")
        panel.down = False
        await env.advance(120)
        await drain()
        assert panel.requests == [] and A.get(row["ref"])["delivered_at"] is None
    run(go())


def test_delivery_state_survives_a_restart(env):
    async def go():
        panel = FakePanel(env)
        panel.down = True
        _, _, row = await make_proposal(env)
        await drain()
        A._sending.clear()                                                                      # рестарт: память пуста, база осталась
        panel.down = False
        env.t = A.get(row["ref"])["delivery_next_at"]
        await E.tick_once()
        await drain()
        assert A.get(row["ref"])["delivered_at"] is not None
    run(go())


# =============================================================================================
# решение человека: подтвердить
# =============================================================================================

def test_approve_assigns_takes_the_deal_and_starts_the_turn_on_the_waiting_messages(env):
    async def go():
        amo, ld, row = await make_proposal(env)
        code, out = await decide(row["ref"])
        assert code == 200 and out["ok"] is True and out["state"] == "approved" and out["detail"]
        assert amo.patches == [(DEAL, 11513202)]
        assert [t for _, t in env.notes if "Ответственный изменён" in t] == [
            "Ответственный изменён с Иванова Мария на Гладков Александер (ИИ, режим Полный, подтвердил Катя Иванова)"]
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["mode"], owner["reason"]) == ("ai", "full", "reassign_approved")
        assert F.guard_row(DEAL)["first_msg_at"] is None                                         # существующая сделка: инициатива ждёт первого сообщения ИИ
        journal = F.reassign_journal()
        assert len(journal) == 1 and (journal[0]["deal_id"], journal[0]["to_id"], journal[0]["ok"]) == (DEAL, 11513202, 1)
        cur = A.get(row["ref"])
        assert (cur["state"], cur["decided_by"]) == ("approved", "Катя Иванова") and cur["decided_at"] is not None
        await env.advance(1)
        assert len(env.turn_requests) == 1
        turn = env.turn_requests[0]
        assert turn["reason"] == "first_message" and turn["deal_id"] == DEAL and [m["text"] for m in turn["messages"]] == ["Здравствуйте"]
        assert turn["mode"] == "prod" and turn["epoch"] == owner["epoch"]
        assert env.alerts and len(env.alerts) == 1                                               # новых алертов нет (только о предложении)
    run(go())


def test_approve_twice_is_idempotent(env):
    async def go():
        amo, _, row = await make_proposal(env)
        await decide(row["ref"])
        await env.advance(1)
        notes, turns = list(env.notes), len(env.turn_requests)
        code, out = await decide(row["ref"])
        assert (code, out["ok"], out["state"]) == (200, True, "approved")
        code, out = await decide(row["ref"], "reject")                                          # противоположное решение по решённому: тоже без действий
        assert (code, out["state"]) == (200, "approved")
        await env.advance(1)
        assert amo.patches == [(DEAL, 11513202)] and env.notes == notes and len(env.turn_requests) == turns
    run(go())


def test_two_simultaneous_approvals_assign_once(env):
    async def go():
        amo, _, row = await make_proposal(env)
        results = await asyncio.gather(decide(row["ref"]), decide(row["ref"]))
        assert amo.patches == [(DEAL, 11513202)]
        assert sorted(code for code, _ in results) in ([200, 200], [200, 409])
        assert any(out.get("state") == "approved" for _, out in results)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_approve_without_waiting_messages_takes_the_deal_and_waits_for_the_client(env):
    async def go():
        _, _, row = await make_proposal(env)
        store.discard_pending(CHAT_KEY)
        code, out = await decide(row["ref"])
        assert out["state"] == "approved" and "следующее сообщение" in out["detail"]
        await env.advance(1)
        assert store.get_owner(DEAL)["owner"] == "ai" and env.turn_requests == []
        await client_writes(env, "А вот и я", mid="late")
        assert len(env.turn_requests) == 1 and env.turn_requests[0]["messages"][0]["text"] == "А вот и я"
    run(go())


def test_approve_when_someone_already_made_the_ai_user_responsible_skips_the_write(env):
    async def go():
        amo, ld, row = await make_proposal(env)
        ld["responsible_user_id"] = AI_USER["id"]
        code, out = await decide(row["ref"])
        assert out["state"] == "approved" and amo.patches == [] and store.get_owner(DEAL)["owner"] == "ai"
        assert not any("Ответственный изменён" in t for _, t in env.notes)
    run(go())


def test_approve_goes_stale_when_the_manager_wrote_after_the_proposal(env):
    async def go():
        amo, _, row = await make_proposal(env)
        env.t += 5
        await manager_writes(env)
        code, out = await decide(row["ref"])
        assert (code, out["state"]) == (200, "stale") and "менеджер" in out["detail"]
        assert amo.patches == [] and A.get(row["ref"])["state"] == "stale" and env.notes == []
        assert F.guard_row(DEAL) is None
    run(go())


@pytest.mark.parametrize("what, expect, change", [
    ("фильтр", "больше не подходит", lambda env, ld: set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15},
                                                             ai_user=AI_USER, review={"enabled": True}, filters={"payment_status": ["paid"]})),
    ("бюджет", "больше не подходит", lambda env, ld: ld.update(price=10 ** 6) or set_full(
        env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, review={"enabled": True},
        budget={"skip_from_rub": 1000})),
    ("закрыта", "сделка закрыта", lambda env, ld: ld.update(status_id=STATUS_CLOSED)),
    ("ответственный", "ответственного уже сменили", lambda env, ld: ld.update(responsible_user_id=999)),
])
def test_approve_goes_stale_when_the_deal_no_longer_fits(env, what, expect, change):
    async def go():
        amo, ld, row = await make_proposal(env)
        change(env, ld)
        code, out = await decide(row["ref"])
        assert (code, out["state"]) == (200, "stale") and expect in out["detail"], out
        assert amo.patches == [] and store.get_owner(DEAL) is None and A.get(row["ref"])["state"] in ("stale", "cancelled")
        assert store.get_session(CHAT_KEY)["state"] == "skip"                                    # чат отпущен, отвечает менеджер
    run(go())


def test_approve_goes_stale_when_the_pipeline_lost_its_prompt(env):
    """Правило «нет промпта: не пишем» (`ai_engine_prompts`): пока предложение ждало человека, в воронке сделки убрали промпт; ответственного не меняем, сделку не берём."""
    async def go():
        amo, ld, row = await make_proposal(env)
        env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL + 1}), test=frozenset(), at=0.0))
        env.mp.setattr(prompts, "_last_ok", True)
        code, out = await decide(row["ref"])
        assert (code, out["state"]) == (200, "stale") and "воронка" in out["detail"], out
        assert amo.patches == [] and store.get_owner(DEAL) is None and A.get(row["ref"])["state"] == "stale"
        assert env.notes == [] and store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_approve_goes_stale_when_the_mode_or_the_policy_changed_or_the_switch_is_on(env):
    async def go():
        for what in ("mode", "policy", "switch", "user"):
            env.t += 1000
            amo, ld, row = await make_proposal(env)
            if what == "mode":
                env.set_settings(settings_version=2, mode_id="test", review={"enabled": True})
            elif what == "policy":
                set_full(env, existing={"policy": "leave", "since": env.t - 100}, review={"enabled": True})
            elif what == "switch":
                env.mp.setattr(F, "REASSIGN_DISABLED", True)
            else:
                pilot(env, ai_user={"id": 42, "name": "Другой"})
            code, out = await decide(row["ref"])
            assert (code, out["state"]) == (200, "stale"), (what, out)
            assert amo.patches == [] and A.get(row["ref"])["state"] == "cancelled", what
            env.mp.setattr(F, "REASSIGN_DISABLED", False)
            store.set_owner(DEAL, store.OWNER_ENGINE, reason="тест", pipeline_id=PIPE_RETAIL, mode="full")
            env.leads.clear()
            store.drop_session(CHAT_KEY)
            with A._Db() as conn:
                conn.execute("DELETE FROM reassign_proposal")
                conn.execute("DELETE FROM deal_owner")
    run(go())


def test_approve_fails_when_amo_refuses_the_write_and_can_be_repeated(env):
    async def go():
        amo, _, row = await make_proposal(env)
        amo.ok, amo.status = False, 403
        code, out = await decide(row["ref"])
        assert (code, out["ok"], out["state"]) == (200, True, "failed") and out["detail"]
        assert store.get_owner(DEAL) is None and env.notes == [] and F.guard_row(DEAL) is None
        cur = A.get(row["ref"])
        assert cur["state"] == "failed" and cur["decided_by"] == "Катя Иванова"
        assert A.open_for_deal(DEAL)["ref"] == row["ref"] and "ждёт решения" in A.block_reason(DEAL, env.t)        # открытое: нового предложения нет
        assert store.get_session(CHAT_KEY)["state"] == "awaiting"                                  # сообщения клиента всё ещё ждут
        amo.ok, amo.status = True, 200
        code, out = await decide(row["ref"], by="Сергей Петров")
        assert out["state"] == "approved" and store.get_owner(DEAL)["owner"] == "ai"
        assert A.get(row["ref"])["decided_by"] == "Сергей Петров"
        await env.advance(1)
        assert len(env.turn_requests) == 1 and env.turn_requests[0]["messages"][0]["text"] == "Здравствуйте"
    run(go())


def test_approve_fails_when_amo_does_not_return_the_deal_or_the_ai_service_is_down(env):
    async def go():
        amo, _, row = await make_proposal(env)
        real = amo_service.get_lead_full

        async def gone(*a, **k):
            return None

        env.mp.setattr(amo_service, "get_lead_full", gone)
        assert (await decide(row["ref"]))[1]["state"] == "failed"
        env.mp.setattr(amo_service, "get_lead_full", real)
        env.mp.setattr(E, "_ai_ok_at", env.t - 10 ** 6)
        env.health_ok = False
        code, out = await decide(row["ref"])
        assert out["state"] == "failed" and "ИИ-сервис" in out["detail"] and amo.patches == [] and A.get(row["ref"])["state"] == "failed"
        env.health_ok = True
        assert (await decide(row["ref"]))[1]["state"] == "approved"
    run(go())


def test_approve_fails_while_the_settings_are_outdated_and_closes_nothing(env):
    async def go():
        amo, _, row = await make_proposal(env)
        env.mp.setattr(settings, "_now", lambda: time.time() + 10 ** 6)
        code, out = await decide(row["ref"])
        assert out["state"] == "failed" and amo.patches == [] and A.get(row["ref"])["state"] == "failed"
    run(go())


def test_the_write_error_of_amo_is_a_failure_not_a_crash(env):
    async def go():
        amo, _, row = await make_proposal(env)

        async def boom(*a, **k):
            raise RuntimeError("amo упала")

        env.mp.setattr(amo_service, "patch_lead", boom)
        code, out = await decide(row["ref"])
        assert (code, out["state"]) == (200, "failed") and store.get_owner(DEAL) is None
    run(go())


def test_an_autopilot_deal_makes_the_proposal_stale(env):
    async def go():
        _, _, row = await make_proposal(env)
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: True)
        code, out = await decide(row["ref"])
        assert out["state"] == "stale" and "авто-режим" in out["detail"]
    run(go())


def test_approve_rechecks_products_in_moysklad(env, monkeypatch):
    async def go():
        calls = []

        async def ms_get(path, params=None):
            calls.append(path)
            return {"rows": [{"assortment": {"name": "Ledger Stax", "article": "LD-9", "meta": {"type": "product"}}}], "meta": {"size": 1}}

        monkeypatch.setattr(F, "_ms_get", ms_get)
        amo, ld, row = await make_proposal(env)
        ld["custom_fields_values"] = [{"field_id": F.FIELD_MS_ORDER, "values": [{"value": "78245ce7-dfda-11f1-0a80-09520056c39a"}]}]
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, review={"enabled": True},
                 filters={"products": {"enabled": True, "allow_empty": True, "allow_unknown": False}}, target_skus=["HW-26"])
        code, out = await decide(row["ref"])
        assert out["state"] == "stale" and "вне целевого списка" in out["detail"] and calls and amo.patches == []
    run(go())


# =============================================================================================
# решение человека: отклонить, ошибки запроса
# =============================================================================================

def test_reject_writes_nothing_and_releases_the_chat(env):
    async def go():
        amo, _, row = await make_proposal(env)
        code, out = await decide(row["ref"], "reject", by="Катя Иванова")
        assert (code, out["ok"], out["state"]) == (200, True, "rejected") and out["detail"]
        assert amo.patches == [] and env.notes == [] and store.get_owner(DEAL) is None and env.turn_requests == []
        cur = A.get(row["ref"])
        assert (cur["state"], cur["decided_by"]) == ("rejected", "Катя Иванова") and cur["decided_at"] is not None
        session = store.get_session(CHAT_KEY)
        assert session["state"] == "skip" and store.pending_inbox(CHAT_KEY) == []
        assert (await decide(row["ref"], "reject"))[1]["state"] == "rejected"                    # повтор: тот же ответ
        assert (await decide(row["ref"], "approve"))[1]["state"] == "rejected"                   # одобрить отклонённое нельзя
        assert amo.patches == [] and store.get_owner(DEAL) is None
    run(go())


def test_after_a_rejection_there_is_no_new_proposal_for_24_hours(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        amo, _, row = await make_proposal(env)
        await decide(row["ref"], "reject")
        env.t += 3600
        await client_writes(env, "Алло", mid="again")
        assert len(A.journal()) == 1 and store.get_owner(DEAL) is None
        assert any("отклонено, новое не раньше чем через 24 часа" in r.getMessage() for r in caplog.records)
        env.t += 20 * 3600
        await client_writes(env, "Алло 2", mid="again2")
        assert len(A.journal()) == 1                                                           # прошло около 21 часа: ещё нельзя
        env.t += 4 * 3600
        await client_writes(env, "Алло 3", mid="again3")                                       # больше 24 часов после отказа
        assert len(A.journal()) == 2 and A.journal()[0]["state"] == "pending" and A.journal()[0]["ref"] != row["ref"]
        assert len(env.alerts) == 2
    run(go())


def test_a_failed_proposal_can_be_rejected_too(env):
    async def go():
        amo, _, row = await make_proposal(env)
        amo.ok, amo.status = False, 403
        assert (await decide(row["ref"]))[1]["state"] == "failed"
        assert (await decide(row["ref"], "reject"))[1]["state"] == "rejected" and A.count_open() == 0
    run(go())


def test_unknown_ref_mismatched_deal_and_bad_decision(env):
    async def go():
        _, _, row = await make_proposal(env)
        assert (await decide("rp-0-0"))[0] == 404 and (await decide(""))[0] == 404 and (await decide("x" * 80))[0] == 404
        assert (await decide(row["ref"], deal=DEAL + 5))[0] == 409
        assert (await decide(row["ref"], "maybe"))[0] == 422 and (await decide(row["ref"], ""))[0] == 422
        A._deciding.add(row["ref"])
        assert (await decide(row["ref"]))[0] == 409                                              # то же предложение уже решается
        A._deciding.clear()
        assert A.get(row["ref"])["state"] == "pending"
        code, out = await decide(row["ref"], by="  ")
        assert out["state"] == "approved" and A.get(row["ref"])["decided_by"] == "не указан"
    run(go())


# =============================================================================================
# ручка и токены
# =============================================================================================

def body(ref, decision="approve", deal=DEAL, by="Катя Иванова"):
    return API.ReassignDecisionIn(ref=ref, deal_id=deal, decision=decision, decided_by=by)


def status_of(resp):
    return resp.status_code, json.loads(resp.body)


def test_the_route_accepts_the_narrow_give_token_and_rejects_the_rest(env):
    async def go():
        env.mp.setattr(cfg, "GIVE_TOKEN", GIVE_TOKEN)
        _, _, row = await make_proposal(env)
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token="", x_give_token="wrong"))[0] == 403
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token="", x_give_token=""))[0] == 403
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token="wrong", x_give_token=""))[0] == 403
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token=GIVE_TOKEN, x_give_token=""))[0] == 403       # токен не в своём заголовке
        assert A.get(row["ref"])["state"] == "pending"
        code, out = status_of(await API.reassign_decision(body(row["ref"], "reject"), x_admin_token="", x_give_token=GIVE_TOKEN))
        assert (code, out["ok"], out["state"]) == (200, True, "rejected")
        code, out = status_of(await API.reassign_decision(body(row["ref"], "approve"), x_admin_token="", x_give_token=GIVE_TOKEN))
        assert (code, out["state"]) == (200, "rejected")                                         # идемпотентность через ручку
        assert status_of(await API.reassign_decision(body("rp-1-1"), x_admin_token="", x_give_token=GIVE_TOKEN))[0] == 404
        assert status_of(await API.reassign_decision(body(row["ref"], deal=1), x_admin_token="", x_give_token=GIVE_TOKEN))[0] == 409
    run(go())


def test_the_admin_token_also_opens_the_route_and_without_a_give_token_the_route_is_closed(env):
    async def go():
        _, _, row = await make_proposal(env)
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token="admin-token", x_give_token=""))[0] == 200
        env.mp.setattr(cfg, "GIVE_TOKEN", "")
        assert status_of(await API.reassign_decision(body(row["ref"]), x_admin_token="", x_give_token=GIVE_TOKEN))[0] == 503
    run(go())


def test_the_narrow_token_opens_only_give_deal_and_the_reassign_route(env):
    async def go():
        env.mp.setattr(cfg, "GIVE_TOKEN", GIVE_TOKEN)
        deny = GIVE_TOKEN
        assert (await API.return_dialog(API.ReturnIn(deal_id=1), x_admin_token=deny)).status_code == 403
        assert (await API.alert(API.AlertIn(lines=["x"]), x_admin_token=deny)).status_code == 403
        assert (await API.state(1, x_admin_token=deny)).status_code == 403
        permit = API.PermitIn(deal_id=1, epoch=1, turn_id=1)
        assert (await API.permit(permit, deny, "2")).status_code == 403
        assert (await API.result(API.ResultIn(turn_id=1, deal_id=1, epoch=1, outcome="continue"), deny, "2")).status_code == 403
        assert (await API.followup(API.FollowupIn(deal_id=1, epoch=1, turn_id=1, due_at_ts=1.0), deny, "2")).status_code == 403
        assert (await API.send_failed(API.SendFailedIn(deal_id=1, code="c", reason="r"), deny, "2")).status_code == 403
        give = API.GiveIn(deal_id=1, chat_id="c", chat_type="telegram", channel_id="ch")
        resp = await API.give_deal(give, x_admin_token="", x_give_token=GIVE_TOKEN)
        assert resp.status_code != 403                                                          # give_deal по узкому токену открыт, как раньше
        assert (await API.give_deal(give, x_admin_token=GIVE_TOKEN, x_give_token="")).status_code == 403
    run(go())


# =============================================================================================
# устаревание само
# =============================================================================================

@pytest.mark.parametrize("what", ["mode", "policy", "switch"])
def test_the_proposal_is_cancelled_when_the_conditions_disappear(env, what):
    async def go():
        _, _, row = await make_proposal(env)
        if what == "mode":
            env.set_settings(settings_version=2, mode_id="test", review={"enabled": True})
        elif what == "policy":
            set_full(env, existing={"policy": "leave", "since": env.t - 100}, review={"enabled": True})
        else:
            env.mp.setattr(F, "REASSIGN_DISABLED", True)
        await env.advance(1)
        cur = A.get(row["ref"])
        assert cur["state"] == "cancelled" and cur["note"] and A.count_open() == 0
        assert store.get_session(CHAT_KEY)["state"] == "skip" and store.pending_inbox(CHAT_KEY) == []
        assert (await decide(row["ref"]))[1]["state"] == "stale"                                  # cancelled наружу отдаётся как stale
    run(go())


def test_the_proposal_goes_stale_when_the_manager_answers_and_the_chat_stays_with_the_manager(env):
    async def go():
        amo, _, row = await make_proposal(env)
        env.t += 5
        await manager_writes(env)
        await env.advance(1)
        assert A.get(row["ref"])["state"] == "stale" and A.count_open() == 0
        assert (await decide(row["ref"]))[1]["state"] == "stale" and amo.patches == [] and env.turn_requests == []
        await client_writes(env, "Спасибо", mid="thanks")                                         # менеджер ответил: диалог его, ИИ молчит
        assert store.get_owner(DEAL)["owner"] == "human" and env.turn_requests == []
    run(go())


def test_the_proposal_is_cancelled_when_the_deal_is_closed(env):
    async def go():
        _, _, row = await make_proposal(env)
        await E._handle_lead_event({"lead_id": DEAL, "pipeline_id": PIPE_RETAIL, "status_id": STATUS_CLOSED, "added": False, "paid": False, "work_touched": False})
        cur = A.get(row["ref"])
        assert cur["state"] == "cancelled" and cur["note"] == "сделка закрыта" and A.count_open() == 0
        assert store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_a_lead_event_for_an_open_deal_does_not_touch_the_proposal(env):
    async def go():
        _, _, row = await make_proposal(env)
        await E._handle_lead_event({"lead_id": DEAL, "pipeline_id": PIPE_RETAIL, "status_id": 1000, "added": False, "paid": False, "work_touched": False})
        assert A.get(row["ref"])["state"] == "pending"
    run(go())


def test_the_proposal_is_cancelled_when_the_deal_gets_an_owner_in_the_engine(env):
    async def go():
        _, _, row = await make_proposal(env)
        store.set_owner(DEAL, store.OWNER_HUMAN, reason="rule_A", pipeline_id=PIPE_RETAIL, mode="full")
        await env.advance(1)
        assert A.get(row["ref"])["state"] == "cancelled"
    run(go())


def test_outdated_settings_close_nothing(env):
    async def go():
        _, _, row = await make_proposal(env)
        env.set_settings(settings_version=2, mode_id="test", review={"enabled": True})
        env.mp.setattr(settings, "_now", lambda: time.time() + 10 ** 6)                           # панель молчит: настройки протухли
        await env.advance(1)
        assert A.get(row["ref"])["state"] == "pending" and A.count_open() == 1
    run(go())


def test_a_waiting_chat_without_an_open_proposal_is_released(env):
    async def go():
        store.put_session(CHAT_KEY, channel_id="c", chat_id="1", chat_type="telegram", state=A.AWAITING, deal_id=DEAL, next_at=None)
        store.add_inbox("orphan-1", CHAT_KEY, "ждал", "text", "", env.t)
        await env.advance(1)
        assert store.get_session(CHAT_KEY)["state"] == "skip" and store.pending_inbox(CHAT_KEY) == []
    run(go())


def test_a_waiting_chat_with_an_open_proposal_is_left_alone(env):
    async def go():
        await make_proposal(env)
        for _ in range(3):
            await env.advance(61)
        assert store.get_session(CHAT_KEY)["state"] == "awaiting" and A.count_open() == 1
    run(go())


# =============================================================================================
# срез и контракт
# =============================================================================================

def test_the_status_shows_the_number_of_open_proposals(env):
    async def go():
        assert E.status()["reassign_proposals_open"] == 0
        _, _, row = await make_proposal(env)
        assert E.status()["reassign_proposals_open"] == 1
        await decide(row["ref"], "reject")
        assert E.status()["reassign_proposals_open"] == 0
    run(go())


def test_the_wire_form_has_exactly_the_agreed_keys():
    row = {"ref": "rp-1-2", "deal_id": 1, "from_user_id": 7, "from_user_name": "А Б", "to_user_id": 9, "to_user_name": "В Г",
           "reason": "existing_deal_redistribute", "proposed_at": 1_700_000_000.4}
    assert A.wire(row) == {"ref": "rp-1-2", "deal_id": 1, "from_user": {"id": 7, "name": "А Б"}, "to_user": {"id": 9, "name": "В Г"},
                           "reason": "existing_deal_redistribute", "at": "2023-11-14T22:13:20Z"}
    assert A.wire({**row, "from_user_id": None})["from_user"] is None
    assert A.wire({**row, "from_user_name": ""})["from_user"] == {"id": 7, "name": "пользователь 7"}
    assert A.PATH == "/api/ingest/ai/reassign-proposal" and A.REASON == "existing_deal_redistribute"


def test_the_table_has_the_agreed_columns(env):
    with A._Db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(reassign_proposal)")]
    for name in ("ref", "deal_id", "from_user_id", "from_user_name", "to_user_id", "to_user_name", "reason", "proposed_at", "state", "decided_by", "decided_at", "note"):
        assert name in cols
    assert A.OPEN_STATES == ("pending", "failed") and A.REJECT_COOLDOWN_S == 86400
