"""Перехват сделок Игоря Оанча (`ai_engine_intercept`, решение Кати 12.11.2026): 15 минут без ответа, заметка «ИИ взял сделку», отказ ИИ при ответе менеджера.
Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_intercept.py -q
"""
import datetime

import pytest

import ai_engine as E
import ai_engine_intercept as I
import ai_engine_silence as S
import ai_engine_store as store
from test_ai_engine import CONTACT, DEAL, PHONE, PIPE_OFFICE, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)

IGOR = 9291546
OTHER_CHAN = "7c0ffee0-0000-4000-8000-000000000001"           # канал, которого нет в AI_ENGINE_CHANNELS
ALL_DAY = [{"start": "00:00", "end": "24:00"}]
MSK = datetime.timezone(datetime.timedelta(hours=3))


def at(h, m=0, day=1):
    return datetime.datetime(2026, 11, day, h, m, tzinfo=MSK).timestamp()


def igor_deal(env, lead_id=DEAL, pipeline=PIPE_OFFICE, responsible=IGOR, status=1000):
    lead = env.lead(lead_id=lead_id, pipeline=pipeline, status=status)
    lead["responsible_user_id"] = responsible
    return lead


def setup(env, enabled=True, **over):
    env.set_settings(intercept={"enabled": enabled}, work_hours=ALL_DAY, **over)


def client(env, text="Здравствуйте, подскажите по кошельку", chan=OTHER_CHAN):
    m = env.msg(text)
    m["channelId"] = chan
    return m


def manager(env, text="Добрый день!", author="Игорь Оанча", chan=OTHER_CHAN):
    m = env.msg(text, echo=True, author=author)
    m["channelId"] = chan
    return m


def intercepts(env):
    return [r for r in env.turn_requests if r.get("event", {}).get("type") == "deal_intercepted"]


def taken_notes(env):
    return [n for n in env.notes if "ИИ взял сделку" in n[1]]


# ---------------------------------------------------------------------------------------------
# 15 минут рабочего времени
# ---------------------------------------------------------------------------------------------

def test_due_at_counts_working_time_only():
    hours = [{"start": "10:00", "end": "19:00"}]
    assert I.due_at(at(12, 0), 900, hours) == at(12, 15)
    assert I.due_at(at(18, 50), 900, hours) == at(10, 5, day=2)          # 10 минут до закрытия окна, ещё 5 после открытия
    assert I.due_at(at(23, 30), 900, hours) == at(10, 15, day=2)
    assert I.due_at(at(3, 0), 900, hours) == at(10, 15)
    assert I.due_at(at(12, 0), 900, []) is None                          # нет рабочих часов: никогда
    two = [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "19:00"}]
    assert I.due_at(at(12, 55), 900, two) == at(14, 10)
    assert I.due_at(at(12, 0), 900, ALL_DAY) == at(12, 15)


# ---------------------------------------------------------------------------------------------
# основной путь
# ---------------------------------------------------------------------------------------------

def test_unanswered_client_after_15_minutes_gets_note_then_ai_event(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(14 * 60)
        assert taken_notes(env) == [] and intercepts(env) == []
        await env.advance(2 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and taken_notes(env)[0][0] == DEAL
        assert env.notes.index(taken_notes(env)[0]) == 0                    # заметка до всего остального
        ev = intercepts(env)
        assert len(ev) == 1
        body = ev[0]
        assert body["reason"] == "event" and body["intercepted"] is True and body["given_forced"] is True
        assert body["manager_cutoff"] and body["event"]["reason"]
        assert body["chat"]["channel_id"] == OTHER_CHAN and body["messages"] == []
        owner = store.get_owner(DEAL)
        assert owner["owner"] == store.OWNER_AI and owner["pipeline_id"] == PIPE_OFFICE
        assert I.is_taken(DEAL) and I.is_tracked(f"{OTHER_CHAN}:1920391385")
    run(go())


def test_the_note_is_written_before_the_deal_changes_hands(env):
    async def go():
        setup(env)
        igor_deal(env)
        seen = []

        async def add_note(lead_id, text):
            seen.append((int(lead_id), store.get_owner(DEAL), len(env.turn_requests)))     # на момент заметки владельца ещё нет, хода нет
            env.notes.append((int(lead_id), text))
            return {}

        env.mp.setattr(E.amo_service, "add_note", add_note)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert seen == [(DEAL, None, 0)]
        assert len(intercepts(env)) == 1
    run(go())


def test_manager_answer_during_the_note_means_no_takeover_and_a_correcting_note(env):
    async def go():
        setup(env)
        igor_deal(env)

        async def add_note(lead_id, text):
            env.notes.append((int(lead_id), text))
            if "ИИ взял сделку" in text:                                          # менеджер ответил, пока писалась заметка
                I.observe_human_out(f"{OTHER_CHAN}:1920391385", env.t)
            return {}

        env.mp.setattr(E.amo_service, "add_note", add_note)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and any("сделку не ведёт" in n[1] for n in env.notes)
        assert store.get_owner(DEAL) is None and intercepts(env) == [] and not I.is_taken(DEAL)
    run(go())


def test_manager_answer_within_15_minutes_cancels_the_wait(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(10 * 60)
        await env.say(manager(env))
        await env.advance(30 * 60)
        assert taken_notes(env) == [] and intercepts(env) == [] and store.get_owner(DEAL) is None
    run(go())


def test_robot_echo_is_not_an_answer(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(5 * 60)
        await env.say(manager(env, "Здравствуйте! Мы получили ваше сообщение", author="Admin"))
        await env.advance(11 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and len(intercepts(env)) == 1
    run(go())


def test_following_client_messages_do_not_move_the_deadline(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env, "раз"))
        await env.advance(10 * 60)
        await env.say(client(env, "два"))
        await env.advance(6 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


# ---------------------------------------------------------------------------------------------
# кого не берём
# ---------------------------------------------------------------------------------------------

def test_other_responsible_manager_is_not_intercepted(env):
    async def go():
        setup(env)
        igor_deal(env, responsible=123)
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
    run(go())


def test_switch_off_and_mode_off_and_contact_outside_the_list_do_nothing(env):
    async def go():
        igor_deal(env)
        setup(env, enabled=False)
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
        setup(env, mode="off")
        await env.say(client(env, "ещё"))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
        env.set_settings(intercept={"enabled": True}, work_hours=ALL_DAY,
                         contacts={"scope": "test", "restricted": True, "items": [{"contact_id": 999, "phone_norm": "79990000000", "name": "Другой"}]})
        await env.say(client(env, "и ещё"))
        await env.advance(20 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
    run(go())


def test_closed_deal_and_two_open_deals_of_igor_are_skipped(env):
    async def go():
        setup(env)
        igor_deal(env, status=142)
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
        igor_deal(env, lead_id=DEAL)
        igor_deal(env, lead_id=DEAL + 1)
        await env.say(client(env, "снова"))
        await env.advance(20 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None and store.get_owner(DEAL + 1) is None
    run(go())


def test_a_deal_is_taken_only_once_and_known_owners_are_left_alone(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
        await E._to_human(await E._db(store.get_session, f"{OTHER_CHAN}:1920391385"), "manager_message", alert=None)
        await env.say(client(env, "ответьте"))
        await env.advance(20 * 60)
        assert len(taken_notes(env)) == 1 and len(intercepts(env)) == 1          # повторно не берём
        igor_deal(env, lead_id=DEAL + 5)
        store.set_owner(DEAL + 5, store.OWNER_HUMAN, reason="x")
    run(go())


def test_chat_already_led_by_the_regular_entry_is_not_intercepted(env):
    async def go():
        setup(env)
        env.lead()                                                           # воронка «Тест»: обычный вход ИИ
        env.leads[0]["responsible_user_id"] = IGOR
        await env.say(env.msg("слово"))                                      # канал CHAN входит в AI_ENGINE_CHANNELS
        await env.advance(61)
        assert env.turn_requests
        await env.advance(20 * 60)
        assert taken_notes(env) == []
    run(go())


# ---------------------------------------------------------------------------------------------
# сбои
# ---------------------------------------------------------------------------------------------

def test_failed_note_means_the_deal_is_not_taken_and_retried_later(env):
    async def go():
        setup(env)
        igor_deal(env)
        fails = {"n": 2}

        async def add_note(lead_id, text):
            if fails["n"] > 0:
                fails["n"] -= 1
                raise RuntimeError("amo недоступна")
            env.notes.append((int(lead_id), text))
            return {}

        env.mp.setattr(E.amo_service, "add_note", add_note)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert store.get_owner(DEAL) is None and intercepts(env) == []            # заметки нет: сделку не берём
        await env.advance(61)
        await env.advance(61)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and len(intercepts(env)) == 1
    run(go())


def test_amo_down_and_ai_down_are_retried(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        env.amo_down = True
        await env.advance(16 * 60)
        assert taken_notes(env) == []
        env.amo_down = False
        await env.advance(61)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


# ---------------------------------------------------------------------------------------------
# менеджер написал после заметки
# ---------------------------------------------------------------------------------------------

def test_manager_message_after_the_note_denies_permit_and_takes_the_dialog(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        turn = intercepts(env)[0]
        code, out = E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], "")
        assert (code, out["allowed"]) == (200, True)
        await env.say(manager(env, "Игорь здесь, отвечу сам"))
        code, out = E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], "")
        assert out["allowed"] is False
        assert store.get_owner(DEAL)["owner"] == store.OWNER_HUMAN
    run(go())


def test_manager_replied_after_flag_alone_denies_permit(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        turn = intercepts(env)[0]
        I.observe_human_out(f"{OTHER_CHAN}:1920391385", env.t + 1)          # событие менеджера дошло, а владельца ещё не сменили
        assert I.manager_replied_after(DEAL)
        code, out = E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], "")
        assert out == {"allowed": False, "reason": "owner_changed"}
    run(go())


def test_manager_message_before_the_note_does_not_stop_the_ai(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(manager(env, "Добрый день, сейчас уточню"))
        await env.advance(2 * 60)
        await env.say(client(env, "жду"))
        await env.advance(16 * 60)
        await env.advance(5)
        turn = intercepts(env)[0]
        assert not I.manager_replied_after(DEAL)
        code, out = E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], "")
        assert out["allowed"] is True
    run(go())


# ---------------------------------------------------------------------------------------------
# перехваченная сделка живёт в чужой воронке
# ---------------------------------------------------------------------------------------------

def test_client_follow_up_in_the_intercepted_chat_goes_to_the_ai_in_any_pipeline(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        first = intercepts(env)[0]
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        env.turn_requests.clear()
        await env.say(client(env, "Какая цена?"))
        await env.advance(61)
        assert env.turn_requests and env.turn_requests[-1]["reason"] == "client_message"
        assert env.turn_requests[-1]["intercepted"] is True
        assert store.get_owner(DEAL)["owner"] == store.OWNER_AI                 # не K_GONE: воронка «Офис» вне режима
    run(go())


def test_lead_event_in_a_foreign_pipeline_does_not_release_an_intercepted_deal(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        ev = {"lead_id": DEAL, "pipeline_id": PIPE_OFFICE, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
        await E._lead_event_under_lock(ev)
        assert store.get_owner(DEAL)["owner"] == store.OWNER_AI and not store.get_owner(DEAL)["closed"]
        ev["status_id"] = 142                                                       # а закрытие по-прежнему отпускает
        await E._lead_event_under_lock(ev)
        assert store.get_owner(DEAL)["closed"]
    run(go())


def test_silence_ladder_and_ur_windows_skip_intercepted_deals(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        await E.silence_arm(DEAL)
        assert S.get(DEAL) is None
    run(go())


def test_dialog_can_be_returned_to_the_ai_for_an_intercepted_deal_in_a_foreign_pipeline(env):
    async def go():
        setup(env)
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        await E._to_human(await E._db(store.get_session, f"{OTHER_CHAN}:1920391385"), "manager_message", alert=None)
        assert store.get_owner(DEAL)["owner"] == store.OWNER_HUMAN
        code, out = await E.return_dialog(DEAL)
        assert code == 200 and store.get_owner(DEAL)["owner"] == store.OWNER_AI
    run(go())
