"""Причина итога `stage_not_served` (решение 23.11.2026): ИИ-сервис вернул `aborted` с этой причиной, потому что на ЭТОМ этапе сделки (ОПТ, Академия) ИИ не отвечает.

Что проверяем: ход закрыт без ответа клиенту; нет алерта, задачи, примечания и передачи человеку; повтора хода нет (сообщения остаются потреблёнными);
владелец и эпоха прежние, чат ждёт клиента; в логе только код причины; событие, которое встало в очередь, пока шёл ход, не теряется; следующее сообщение клиента
обрабатывается как обычно; `intercept.finish_reply` вызывается, как для прочих неизвестных причин.

Сети нет: подставной мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_stage_not_served.py -q
"""
import logging

import pytest

import ai_engine as E
import ai_engine_store as store
from test_ai_engine import CHAT_KEY, DEAL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_followup import drain

DETAIL = "stage_not_served: этап «Согласование оптового заказа» ОПТ, ИИ здесь не отвечает"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)


@pytest.mark.parametrize("reason", ["stage_not_served", DETAIL])
def test_the_turn_is_closed_without_an_answer_and_nobody_is_called(env, reason):
    async def go():
        turn = await env.start_turn()
        before = store.get_owner(DEAL)
        code, out = await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", reason, None)
        await drain()
        assert (code, out) == (200, {"ok": True})
        done = store.get_turn(turn["turn_id"])
        assert done["state"] == "done" and done["outcome"] == "aborted" and done["outcome_reason"] == reason
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["closed"]) == (before["owner"], before["epoch"], 0) and owner["owner"] == "ai"     # ИИ остаётся владельцем
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "ai" and s["next_at"] is None and s["current_turn"] is None                      # чат ждёт следующего сообщения клиента
        assert env.alerts == [] and env.tasks_open == [] and env.notes == [] and env.bot_messages == [] and env.reactions == []
        assert store.pending_inbox(CHAT_KEY) == []                                                             # сообщения хода потреблены, не возвращены
        count = len(env.turn_requests)
        await env.advance(120)
        assert len(env.turn_requests) == count and env.cancels == []                                          # повтора хода нет
    run(go())


def test_the_log_has_only_the_code_not_the_text_of_the_service(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", DETAIL, None)
        lines = [r.getMessage() for r in caplog.records if "stage_not_served" in r.getMessage()]
        assert len(lines) == 1 and str(turn["turn_id"]) in lines[0] and str(DEAL) in lines[0]
        assert "ОПТ" not in lines[0] and "Согласование" not in lines[0] and "Тиана" not in caplog.text and "79260000001" not in caplog.text
    run(go())


def test_the_next_message_of_the_client_is_handled_as_usual(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", "stage_not_served", None)
        env.turn_requests.clear()
        await env.say(env.msg("Подскажите по оптовой цене", mid="c-2"))
        await env.advance(11)
        req = env.turn_requests[-1]
        assert req["reason"] == "client_message" and [m["message_id"] for m in req["messages"]] == ["c-2"] and req["epoch"] == turn["epoch"]
    run(go())


def test_an_event_queued_during_the_turn_is_not_lost_but_the_turn_is_not_redone(env):
    async def go():
        turn = await env.start_turn()
        owner = store.get_owner(DEAL)
        await E._queue_event(owner, "payment_received")                                                       # пока шёл ход, пришла оплата
        env.turn_requests.clear()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", "stage_not_served", None)
        await env.advance(2)
        assert [r["event"]["type"] for r in env.turn_requests] == ["payment_received"] and env.turn_requests[0]["messages"] == []    # событие ушло, сообщений прежнего хода нет
    run(go())


def test_finish_reply_is_called_as_for_other_unknown_reasons(env):
    async def go():
        calls = []

        async def finish_reply(eng, deal_id):
            calls.append(deal_id)

        env.mp.setattr(E.intercept, "finish_reply", finish_reply)
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", "stage_not_served", None)
        assert calls == [DEAL]
    run(go())


def test_an_unknown_reason_still_behaves_the_same_way(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", "что-то_новое", None)
        assert store.get_session(CHAT_KEY)["state"] == "ai" and env.alerts == [] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_the_other_reasons_are_not_swallowed(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "aborted", "mode_mismatch", None)       # прежние причины работают по-своему: сделка уходит человеку
        assert store.get_owner(DEAL)["owner"] == "human"
    run(go())
