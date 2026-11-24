"""Отметки людей по сделке из панели (`ai_engine_marks`, `ai_engine.mark_deal`, `POST /internal/ai/v1/deal_mark`, решение Кати 23.11.2026).

Что проверяем:
  • хранилище: поставить, повторить, повысить `no_reply` до `removed`, не понизить `removed`, журнал, снятие;
  • «не требует ответа»: ждущий ход и черновик сняты (ход отменён у сервиса, ждущие сообщения сброшены, `permit` по нему отказывает), владелец остаётся, новое сообщение
    клиента ПОЗЖЕ отметки обрабатывается как всегда, сообщение до отметки у сделки без владельца не берёт её, открытое предложение смены ответственного снято;
  • «удалить»: ИИ освобождает сделку (владелец `human`, без алертов и задач, ход, касание и лестница тишины сняты), по сообщению клиента и в подборе сделка человеческая навсегда;
  • ручная передача «Дать сделку ИИ» снимает обе отметки (строка в журнале), в том числе при перезапуске хода;
  • ручка: токены (узкий и админский), 422, идемпотентность, ответ `{ok, state}`; в лог личных данных нет.

Сети нет: подставной мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_marks.py -q
"""
import json
import logging

import pytest

import ai_engine as E
import ai_engine_api as API
import ai_engine_approval as A
import ai_engine_config as cfg
import ai_engine_filters as F
import ai_engine_followup as FU
import ai_engine_marks as M
import ai_engine_settings as settings
import ai_engine_silence as silence
import ai_engine_store as store
from test_ai_engine import CHAT_KEY, DEAL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_approval import make_proposal
from test_ai_engine_followup import drain, scheduled
from test_ai_engine_give import give

GIVE_TOKEN = "give-token"
BY = "Секретная Фамилия"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    F._cache.clear()
    F._inflight.clear()
    F._warned.clear()
    A._sending.clear()
    A._deciding.clear()
    monkeypatch.setattr(A, "_last_orphans", 0.0)
    monkeypatch.setattr(F, "REASSIGN_DISABLED", False)
    monkeypatch.setattr(E, "_last_mode_id", None)
    yield
    A._sending.clear()
    A._deciding.clear()


async def mark(kind, deal=DEAL, by=BY):
    return await E.mark_deal(deal, kind, by)


# =============================================================================================
# хранилище
# =============================================================================================

def test_put_repeat_upgrade_and_the_final_removed(env):
    first = M.put(DEAL, "no_reply", 100.0, BY)
    assert first == {"state": "marked", "kind": "no_reply", "prev": None, "repeat": False}
    again = M.put(DEAL, "no_reply", 200.0, BY)
    assert again["state"] == "marked" and again["repeat"] is True and M.get(DEAL)["at"] == 200.0          # повтор обновляет время
    up = M.put(DEAL, "removed", 300.0, BY)
    assert up == {"state": "marked", "kind": "removed", "prev": "no_reply", "repeat": False}
    for kind in ("removed", "no_reply"):
        same = M.put(DEAL, kind, 400.0, BY)
        assert same["state"] == "already_removed" and same["kind"] == "removed" and M.get(DEAL)["kind"] == "removed" and M.get(DEAL)["at"] == 300.0
    assert [r["event"] for r in reversed(M.journal())] == ["set", "repeat", "set", "repeat", "repeat"]


def test_suppresses_only_what_it_should(env):
    assert M.suppresses(DEAL, 1.0) is None
    M.put(DEAL, "no_reply", 100.0)
    assert M.suppresses(DEAL, 99.0) and M.suppresses(DEAL, 100.0) and M.suppresses(DEAL, None)           # до отметки и неизвестное время
    assert M.suppresses(DEAL, 100.5) is None                                                              # сообщение позже отметки отметка не касается
    M.put(DEAL, "removed", 200.0)
    assert "удалить" in M.suppresses(DEAL, 10_000.0)                                                      # «удалить» мешает всегда


def test_clear_and_counts(env):
    M.put(1, "no_reply", 1.0)
    M.put(2, "removed", 1.0)
    M.put(3, "removed", 1.0)
    assert M.counts() == {"no_reply": 1, "removed": 2}
    assert M.clear(2, "передача", 5.0) == ["removed"] and M.clear(2, "ещё раз", 6.0) == [] and M.get(2) is None
    assert M.counts() == {"no_reply": 1, "removed": 1}
    assert M.journal()[0]["event"] == "cleared" and M.journal()[0]["note"] == "передача"


def test_the_tables_are_the_agreed_ones(env):
    M.put(1, "no_reply", 1.0)
    with M._Db() as conn:
        assert [r[1] for r in conn.execute("PRAGMA table_info(deal_mark)")] == ["deal_id", "kind", "at", "by_user", "note"]


def test_by_is_cleaned_and_cut(env):
    M.put(1, "no_reply", 1.0, "  Катя \n  Иванова " + "я" * 200)
    assert len(M.get(1)["by_user"]) == 80 and "\n" not in M.get(1)["by_user"] and M.get(1)["by_user"].startswith("Катя Иванова ")


# =============================================================================================
# «не требует ответа»
# =============================================================================================

def test_no_reply_cancels_the_waiting_turn_and_keeps_the_owner(env):
    async def go():
        turn = await env.start_turn()                                                                     # ИИ ведёт сделку, ход ушёл сервису
        assert store.get_session(CHAT_KEY)["state"] == "in_turn"
        code, out = await mark("no_reply")
        await drain()
        assert code == 200 and out["ok"] is True and out["state"] == "marked" and out["kind"] == "no_reply" and out["repeat"] is False
        assert out["cancelled_turns"] == 1 and out["released"] is False
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["epoch"] == 1             # владелец остался
        assert env.cancels == [turn["turn_id"]]                                                           # сервису ушла отмена: черновик не отправится
        assert store.get_turn(turn["turn_id"])["state"] == "cancelled"
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"]) == (200, {"allowed": False, "reason": "owner_changed"})
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "ai" and s["next_at"] is None and s["current_turn"] is None
        assert env.alerts == [] and env.tasks_open == [] and env.notes == []
    run(go())


def test_no_reply_drops_the_messages_that_wait_for_the_next_turn(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        env.turn_requests.clear()
        await env.say(env.msg("Подскажите ещё", mid="c-2"))                                                  # накапливаем, ход ещё не назначен
        assert store.get_session(CHAT_KEY)["state"] == "collecting" and store.pending_inbox(CHAT_KEY)
        code, out = await mark("no_reply")
        assert out["dropped_messages"] == 1 and store.pending_inbox(CHAT_KEY) == [] and store.get_session(CHAT_KEY)["state"] == "ai"
        await env.advance(61)
        assert env.turn_requests == []                                                                      # по этому сообщению хода не будет
        env.t += 5
        await env.say(env.msg("А ещё вопрос", mid="c-3"))                                                    # новое сообщение позже отметки: всё как всегда
        await env.advance(11)
        assert [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["c-3"] and env.turn_requests[-1]["reason"] == "client_message"
    run(go())


def test_no_reply_drops_a_waiting_deal_given_event_too(env):
    async def go():
        env.lead()
        code, _ = await give()
        assert code == 200 and store.get_session(CHAT_KEY)["pending_event"]
        out = (await mark("no_reply"))[1]
        assert out["ok"] and store.get_session(CHAT_KEY)["pending_event"] is None and store.get_session(CHAT_KEY)["state"] == "ai"
        await env.advance(5)
        assert env.turn_requests == []
    run(go())


def test_a_message_before_the_mark_does_not_take_a_deal_nobody_owns_yet(env):
    async def go():
        env.lead()
        await env.say(env.msg("Здравствуйте", mid="c-1"))                                                    # клиент написал, ИИ ещё ждёт тишину и не взял сделку
        assert store.get_owner(DEAL) is None
        out = (await mark("no_reply"))[1]
        assert out["ok"] and out["cancelled_turns"] == 0
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None and store.get_session(CHAT_KEY) is None
        assert env.alerts == [] and env.tasks_open == []
        await env.say(env.msg("Алло, вы тут?", mid="c-2"))                                                   # новое сообщение позже отметки обрабатывается как обычно
        await env.advance(61)
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["c-2"]
    run(go())


def test_no_reply_repeat_is_harmless_and_moves_the_time_forward(env):
    async def go():
        await env.start_turn()
        first = (await mark("no_reply"))[1]
        env.t += 100
        second = (await mark("no_reply"))[1]
        third = (await mark("no_reply", by=""))[1]
        assert (first["repeat"], second["repeat"], third["repeat"]) == (False, True, True)
        assert second["state"] == "marked" and second["cancelled_turns"] == 0 and M.get(DEAL)["at"] == env.t
        assert [r["event"] for r in reversed(M.journal())] == ["set", "repeat", "repeat"]
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_no_reply_cancels_an_open_reassign_proposal_and_does_not_leave_the_chat_in_skip(env):
    async def go():
        _, _, row = await make_proposal(env)
        assert row["state"] == "pending" and store.get_session(CHAT_KEY)["state"] == "awaiting"
        out = (await mark("no_reply"))[1]
        assert out["ok"] and A.get(row["ref"])["state"] == "cancelled" and "не требует ответа" in A.get(row["ref"])["note"]
        assert store.get_session(CHAT_KEY) is None and store.pending_inbox(CHAT_KEY) == []                  # не `skip`: следующее сообщение клиента не потеряется
        assert store.get_owner(DEAL) is None
        assert (await mark("no_reply"))[1]["repeat"] is True
    run(go())


def test_a_no_reply_set_before_a_proposal_does_not_close_a_later_one(env):
    async def go():
        M.put(DEAL, "no_reply", env.t - 1000)
        _, _, row = await make_proposal(env)                                                                 # клиент написал позже отметки: предложение создано как обычно
        assert row is not None and row["state"] == "pending"
        assert A._stale_reason(row) is None
        M.put(DEAL, "no_reply", env.t + 1)
        assert A._stale_reason(row)[0] == "cancelled"                                                        # а отметка после предложения его устаревает
    run(go())


# =============================================================================================
# «удалить»
# =============================================================================================

def test_removed_frees_a_deal_the_ai_owns_without_alerts_and_tasks(env):
    async def go():
        turn = await scheduled(env)
        silence.arm(DEAL, CHAT_KEY, env.t)
        assert FU.pending_for_deal(DEAL) and store.get_owner(DEAL)["owner"] == "ai"
        env.notes.clear()
        code, out = await mark("removed")
        await drain()
        assert code == 200 and out == {"ok": True, "state": "marked", "kind": "removed", "repeat": False, "released": True}
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["reason"], owner["closed"]) == ("human", turn["epoch"] + 1, "removed", 0)
        assert FU.pending_for_deal(DEAL) is None and silence.get(DEAL) is None                              # касание и лестница тишины сняты
        assert store.get_session(CHAT_KEY)["state"] == "human"
        assert env.alerts == [] and env.tasks_open == [] and env.bot_messages == [] and env.reactions == []
        assert len(env.notes) == 1 and "касание" in env.notes[0][1]                                          # единственная запись: отложенное касание отменено (как при любой передаче человеку)
    run(go())


def test_removed_cancels_the_waiting_turn(env):
    async def go():
        turn = await env.start_turn()
        await mark("removed")
        await drain()
        assert env.cancels == [turn["turn_id"]] and store.get_turn(turn["turn_id"])["state"] == "cancelled"
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])[1]["allowed"] is False
    run(go())


def test_removed_makes_the_deal_human_for_ever_for_new_messages(env):
    async def go():
        await env.start_turn()
        await mark("removed")
        env.turn_requests.clear()
        for i in range(2):
            await env.say(env.msg(f"Алло {i}", mid=f"c-{i + 5}"))
            await env.advance(61)
            assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "human"
        store.drop_session(CHAT_KEY)                                                                         # сессию чата убрала уборка: запись владельца всё равно держит
        await env.say(env.msg("Это снова я", mid="c-9"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "human" and env.alerts == [] and env.tasks_open == []
    run(go())


def test_removed_on_a_deal_nobody_owns_creates_a_human_owner(env):
    async def go():
        env.lead()
        code, out = await mark("removed")
        assert out["released"] is False and store.get_owner(DEAL)["owner"] == "human" and store.get_owner(DEAL)["reason"] == "removed"
        await env.say(env.msg("Здравствуйте", mid="c-1"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL)["epoch"] == 1 and env.alerts == [] and env.tasks_open == []
    run(go())


def test_removed_repeat_changes_nothing(env):
    async def go():
        await env.start_turn()
        first = (await mark("removed"))[1]
        owner = store.get_owner(DEAL)
        env.t += 50
        for kind in ("removed", "no_reply"):
            code, out = await mark(kind)
            assert code == 200 and out["state"] == "already_removed" and out["kind"] == "removed" and out["repeat"] is True and "released" not in out
        assert store.get_owner(DEAL) == owner and first["released"] is True and M.get(DEAL)["kind"] == "removed"
    run(go())


def test_removed_after_no_reply_upgrades_and_no_reply_after_removed_does_not_downgrade(env):
    async def go():
        await env.start_turn()
        await mark("no_reply")
        out = (await mark("removed"))[1]
        assert out["state"] == "marked" and out["kind"] == "removed" and out["released"] is True and store.get_owner(DEAL)["owner"] == "human"
    run(go())


def test_removed_cancels_an_open_proposal(env):
    async def go():
        _, _, row = await make_proposal(env)
        await mark("removed")
        assert A.get(row["ref"])["state"] == "cancelled" and "удалить" in A.get(row["ref"])["note"]
        assert store.get_owner(DEAL)["owner"] == "human" and store.get_session(CHAT_KEY)["state"] == "human"
        assert store.pending_inbox(CHAT_KEY) == [] and env.turn_requests == []
    run(go())


def test_removed_does_not_touch_a_closed_deal(env):
    async def go():
        store.set_owner(DEAL, store.OWNER_ENGINE, reason="sale_completed", closed=True)
        out = (await mark("removed"))[1]
        owner = store.get_owner(DEAL)
        assert out["released"] is False and (owner["owner"], owner["closed"], owner["epoch"]) == ("engine", 1, 1) and M.get(DEAL)["kind"] == "removed"
    run(go())


def test_a_mark_works_while_the_mode_is_off_and_without_crashing(env):
    async def go():
        env.set_settings(mode="off")
        assert settings.effective_mode() == "off"
        assert (await mark("no_reply"))[0] == 200 and (await mark("removed"))[0] == 200 and store.get_owner(DEAL)["owner"] == "human"
    run(go())


# =============================================================================================
# «Дать сделку ИИ» снимает отметки
# =============================================================================================

@pytest.mark.parametrize("kind", ["no_reply", "removed"])
def test_the_give_button_clears_the_marks(env, kind):
    async def go():
        env.lead()
        await mark(kind)
        assert M.get(DEAL)["kind"] == kind
        code, out = await give()
        await drain()
        assert code == 200 and M.get(DEAL) is None
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_owner(DEAL)["reason"] == "deal_given"
        events = [(r["event"], r["kind"]) for r in M.journal()]
        assert events[0] == ("cleared", kind) and "Дать сделку ИИ" in M.journal()[0]["note"]
        await env.advance(30)
        assert env.turn_requests[-1]["event"]["type"] == "deal_given"
    run(go())


def test_a_refused_give_keeps_the_marks(env):
    async def go():
        env.lead()
        await mark("removed")
        env.set_settings(mode="off")
        code, out = await give()
        assert code == 409 and M.get(DEAL)["kind"] == "removed"
    run(go())


def test_the_give_restart_clears_a_no_reply_too(env):
    async def go():
        await env.start_turn()
        await mark("no_reply")
        assert M.get(DEAL) is not None
        code, out = await give(retrigger=True)
        assert code == 200 and out["retriggered"] is True and M.get(DEAL) is None
    run(go())


def test_marks_are_independent_per_deal(env):
    async def go():
        await mark("removed", deal=DEAL + 7)
        await mark("no_reply", deal=DEAL + 8)
        env.lead()
        assert (await give())[0] == 200
        assert M.get(DEAL + 7)["kind"] == "removed" and M.get(DEAL + 8)["kind"] == "no_reply"
    run(go())


# =============================================================================================
# ручка
# =============================================================================================

def status_of(resp):
    return resp.status_code, json.loads(resp.body)


def test_the_route_accepts_the_narrow_token_and_the_admin_one_and_rejects_the_rest(env):
    async def go():
        env.mp.setattr(cfg, "GIVE_TOKEN", GIVE_TOKEN)
        body = API.DealMarkIn(deal_id=DEAL, kind="no_reply", by=BY)
        assert status_of(await API.deal_mark(body, x_admin_token="", x_give_token="wrong"))[0] == 403
        assert status_of(await API.deal_mark(body, x_admin_token="", x_give_token=""))[0] == 403
        assert status_of(await API.deal_mark(body, x_admin_token="wrong", x_give_token=""))[0] == 403
        assert status_of(await API.deal_mark(body, x_admin_token=GIVE_TOKEN, x_give_token=""))[0] == 403    # токен не в своём заголовке
        assert M.get(DEAL) is None
        code, out = status_of(await API.deal_mark(body, x_admin_token="", x_give_token=GIVE_TOKEN))
        assert code == 200 and out["ok"] is True and out["state"] == "marked" and M.get(DEAL)["by_user"] == BY
        assert status_of(await API.deal_mark(body, x_admin_token="admin-token", x_give_token=""))[0] == 200
        env.mp.setattr(cfg, "GIVE_TOKEN", "")
        assert status_of(await API.deal_mark(body, x_admin_token="", x_give_token=GIVE_TOKEN))[0] == 503   # токена нет в окружении: как у give_deal
        env.mp.setattr(cfg, "ENABLED", False)
        assert status_of(await API.deal_mark(body, x_admin_token="admin-token", x_give_token=""))[0] == 503
    run(go())


def test_the_route_validates_the_body_and_survives_errors(env):
    async def go():
        env.mp.setattr(cfg, "GIVE_TOKEN", GIVE_TOKEN)

        async def call(deal, kind):
            return status_of(await API.deal_mark(API.DealMarkIn(deal_id=deal, kind=kind, by=BY), x_admin_token="", x_give_token=GIVE_TOKEN))

        assert await call(DEAL, "maybe") == (422, {"detail": "bad_kind"})
        assert await call(DEAL, "") == (422, {"detail": "bad_kind"})
        assert await call(DEAL, "NO_REPLY") == (422, {"detail": "bad_kind"})
        assert await call(0, "removed") == (422, {"detail": "bad_deal"})
        assert await call(-5, "removed") == (422, {"detail": "bad_deal"})
        assert M.counts() == {"no_reply": 0, "removed": 0}

        async def boom(*a, **k):
            raise RuntimeError("сбой")

        env.mp.setattr(E, "mark_deal", boom)
        assert await call(DEAL, "removed") == (500, {"detail": "engine_error"})
    run(go())


def test_the_route_is_one_of_the_three_the_narrow_token_opens(env):
    async def go():
        env.mp.setattr(cfg, "GIVE_TOKEN", GIVE_TOKEN)
        deny = GIVE_TOKEN
        assert (await API.return_dialog(API.ReturnIn(deal_id=1), x_admin_token=deny)).status_code == 403    # прочие ручки человека узкий токен не открывает
        assert (await API.state(1, x_admin_token=deny)).status_code == 403
        assert (await API.alert(API.AlertIn(lines=["x"]), x_admin_token=deny)).status_code == 403
        assert (await API.permit(API.PermitIn(deal_id=1, epoch=1, turn_id=1), deny, "2")).status_code == 403
        assert status_of(await API.deal_mark(API.DealMarkIn(deal_id=1, kind="no_reply"), x_admin_token="", x_give_token=GIVE_TOKEN))[0] == 200
    run(go())


def test_the_log_has_no_personal_data(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        await env.start_turn()
        await mark("no_reply")
        await mark("removed")
        await mark("removed")
        log = caplog.text
        assert "36587533" in log and "отметка человека" in log
        assert BY not in log and "Тиана" not in log and "79260000001" not in log and "1920391385" not in log
    run(go())
