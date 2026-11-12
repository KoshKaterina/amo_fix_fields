"""Перехват сделок Игоря Оанча (`ai_engine_intercept`, решение Кати 12.11.2026): 15 минут без ответа, заметка «ИИ взял сделку», отказ ИИ при ответе менеджера.
Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_intercept.py -q
"""
import datetime
import re

import pytest

import ai_engine as E
import ai_engine_intercept as I
import ai_engine_silence as S
import ai_engine_store as store
from test_ai_engine import CONTACT, DEAL, PHONE, PIPE_OFFICE, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)

IGOR = 9291546
PIPE_ANY = 7_000_001                                             # воронка вне режимов: перехват работает в любой
MOP = 578151                                                    # «Ответственный МОП»
OTHER_CHAN = "7c0ffee0-0000-4000-8000-000000000001"           # канал, которого нет в AI_ENGINE_CHANNELS
ALL_DAY = [{"start": "00:00", "end": "24:00"}]
MSK = datetime.timezone(datetime.timedelta(hours=3))


def at(h, m=0, day=1):
    return datetime.datetime(2026, 11, day, h, m, tzinfo=MSK).timestamp()


def igor_deal(env, lead_id=DEAL, pipeline=PIPE_ANY, responsible=IGOR, status=1000, mop=None):
    lead = env.lead(lead_id=lead_id, pipeline=pipeline, status=status)
    lead["responsible_user_id"] = responsible
    lead["created_at"] = int(env.t) - 7200                                     # сделка создана два часа назад
    if mop is not None:
        lead["custom_fields_values"] = [{"field_id": MOP, "values": [{"value": mop}]}]
    return lead


def setup(env, enabled=True, icfg=None, **over):
    """Настройки движка с перехватом; `icfg` дополняет блок `intercept` (менеджеры, office, mode, wait_min, with_calls). Звонки в amo подставные."""
    env.set_settings(intercept={"enabled": enabled, **(icfg or {})}, work_hours=ALL_DAY, **over)
    if not hasattr(env, "contact_notes"):
        env.contact_notes, env.notes_down = {}, False

        async def fake_get(path, params=None):
            m = re.match(r"/api/v4/contacts/(\d+)/notes", path)
            assert m, path
            if env.notes_down:
                return None
            notes = env.contact_notes.get(int(m.group(1)), [])
            return {"_embedded": {"notes": notes}} if notes else {}

        env.mp.setattr(E.amo_service, "_do_get", fake_get)


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
        assert owner["owner"] == store.OWNER_AI and owner["pipeline_id"] == PIPE_ANY
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
        assert len(taken_notes(env)) == 1 and any("ИИ клиенту не отвечал" in n[1] for n in env.notes)
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
        ev = {"lead_id": DEAL, "pipeline_id": PIPE_ANY, "status_id": 1000, "added": False, "paid": False, "work_touched": False}
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


# ---------------------------------------------------------------------------------------------
# настройки из панели
# ---------------------------------------------------------------------------------------------

def test_config_defaults_keep_the_previous_behaviour_and_bad_values_fall_back(env):
    from ai_engine_settings import intercept_config
    env.set_settings(intercept={"enabled": True})
    assert intercept_config() == {"enabled": True, "managers": [{"id": IGOR, "name": "Оанча Игорь"}], "office": False, "mode": "take",
                                  "wait_min": 15, "with_calls": False}
    env.set_settings(intercept={"enabled": "yes", "managers": [{"id": "x"}, {"id": 5, "name": "  "}, {"id": 7, "name": "Егор"}],
                                "office": 1, "mode": "both", "wait_min": 0, "with_calls": "да"})
    cfg = intercept_config()
    assert cfg == {"enabled": False, "managers": [{"id": 7, "name": "Егор"}], "office": False, "mode": "take", "wait_min": 15, "with_calls": False}
    env.set_settings(intercept={"enabled": True, "managers": [], "wait_min": 240, "mode": "reply"})
    assert intercept_config()["managers"] == [] and intercept_config()["wait_min"] == 240 and intercept_config()["mode"] == "reply"


def test_name_key_ignores_case_yo_and_word_order():
    assert I.name_key("Оанча Игорь") == I.name_key("игорь  ОАНЧА") == I.name_key("Игорь Оанча")
    assert I.name_key("Артём Коннов") == I.name_key("Артем Коннов")
    assert I.name_key("") == frozenset() and I.name_key(None) == frozenset()


# ---------------------------------------------------------------------------------------------
# чьи сделки
# ---------------------------------------------------------------------------------------------

def test_managers_come_from_the_panel_list(env):
    async def go():
        setup(env, icfg={"managers": [{"id": 321, "name": "Егор Константинов"}]})
        igor_deal(env)                                                       # Игорь больше не в списке
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
        igor_deal(env, responsible=321)
        await env.say(client(env, "ещё"))
        await env.advance(20 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


def test_empty_manager_list_touches_nobody(env):
    async def go():
        setup(env, icfg={"managers": []})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
    run(go())


def test_office_is_ignored_until_its_switch_is_on(env):
    async def go():
        setup(env)                                                          # office выключен по умолчанию
        igor_deal(env, pipeline=PIPE_OFFICE, mop="Оанча Игорь")
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
    run(go())


@pytest.mark.parametrize("responsible,mop,expected", [
    (13963494, "Оанча Игорь", True),             # ответственный в «Офисе» другой, МОП Игорь: берём
    (13963494, "игорь   оанча", True),            # регистр, пробелы и порядок слов не важны
    (IGOR, "Егор Константинов", False),          # ответственный Игорь, но МОП другой: «Офис» смотрит на МОП
    (IGOR, None, False),                          # поле МОП пусто: не берём
    (13963494, "Кто-то Другой", False),
])
def test_office_looks_at_the_mop_field_not_at_the_responsible(env, responsible, mop, expected):
    async def go():
        setup(env, icfg={"office": True})
        igor_deal(env, pipeline=PIPE_OFFICE, responsible=responsible, mop=mop)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert (len(taken_notes(env)) == 1) is expected
        assert (store.get_owner(DEAL) is not None) is expected
    run(go())


def test_other_pipelines_still_use_the_responsible_even_with_office_on(env):
    async def go():
        setup(env, icfg={"office": True})
        igor_deal(env, pipeline=PIPE_ANY, responsible=IGOR, mop="Егор Константинов")      # МОП вне «Офиса» не смотрим
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


# ---------------------------------------------------------------------------------------------
# время молчания
# ---------------------------------------------------------------------------------------------

def test_silence_minutes_come_from_the_panel_and_apply_without_restart(env):
    async def go():
        setup(env, icfg={"wait_min": 30})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []                                        # 30 минут, 20 прошло
        setup(env, icfg={"wait_min": 5})                                      # панель поменяла настройку, перезапуска нет
        await env.advance(5)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and "5 минут" in taken_notes(env)[0][1]
    run(go())


# ---------------------------------------------------------------------------------------------
# звонки
# ---------------------------------------------------------------------------------------------

def _call(env, created_offset, note_type="call_in", contact=CONTACT):
    env.contact_notes.setdefault(contact, []).append({"note_type": note_type, "created_at": int(env.t) - 7200 + created_offset})


def test_deal_with_calls_is_not_touched_by_default(env):
    async def go():
        setup(env)
        igor_deal(env)
        _call(env, 600)                                                      # звонок через десять минут после создания сделки
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
    run(go())


def test_calls_do_not_matter_when_the_switch_is_on(env):
    async def go():
        setup(env, icfg={"with_calls": True})
        igor_deal(env)
        _call(env, 600, "call_out")
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


def test_old_calls_and_other_notes_are_not_calls_of_this_deal(env):
    async def go():
        setup(env)
        igor_deal(env)
        _call(env, -86400)                                                   # звонок за сутки до создания сделки: не её
        env.contact_notes[CONTACT].append({"note_type": "common", "created_at": int(env.t) - 3600})
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


def test_a_call_just_before_the_deal_was_created_still_counts(env):
    async def go():
        setup(env)
        igor_deal(env)
        _call(env, -120)                                                     # звонок за две минуты до сделки: из него и родилась сделка
        await env.say(client(env))
        await env.advance(20 * 60)
        assert taken_notes(env) == []
    run(go())


def test_unreadable_notes_postpone_the_decision(env):
    async def go():
        setup(env)
        igor_deal(env)
        env.notes_down = True
        await env.say(client(env))
        await env.advance(16 * 60)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
        env.notes_down = False
        await env.advance(61)
        await env.advance(5)
        assert len(taken_notes(env)) == 1
    run(go())


# ---------------------------------------------------------------------------------------------
# режим «отвечать только при молчании»
# ---------------------------------------------------------------------------------------------

def replies(env):
    return [r for r in env.turn_requests if r.get("event", {}).get("type") == "silence_reply"]


def test_reply_mode_answers_once_writes_its_own_note_and_gives_the_deal_back(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(replies(env)) == 1 and intercepts(env) == []
        assert any("ответил клиенту вместо менеджера" in n[1] for n in env.notes) and taken_notes(env) == []
        body = replies(env)[0]
        assert body["intercepted"] is True and body["given_forced"] is True and body["manager_cutoff"]
        assert store.get_owner(DEAL)["owner"] == store.OWNER_AI                       # на время хода владелец нужен для permit
        assert not I.is_tracked(f"{OTHER_CHAN}:1920391385")                            # чат не отслеживается, сделка не захвачена
        code, out = E.decide_permit(DEAL, body["epoch"], body["turn_id"], "")
        assert out["allowed"] is True
        await E.handle_result(body["turn_id"], DEAL, body["epoch"], "continue", None, None)
        assert store.get_owner(DEAL) is None                                          # владельца как не было
        assert store.session_for_deal(DEAL) is None and not I.is_taken(DEAL)
    run(go())


def test_reply_mode_answers_again_only_if_the_manager_stays_silent_again(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env, "первый вопрос"))
        await env.advance(16 * 60)
        await env.advance(5)
        first = replies(env)[0]
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        # следующее сообщение клиента: менеджер отвечает вовремя, ИИ молчит
        await env.say(client(env, "второй вопрос"))
        await env.advance(8 * 60)
        await env.say(manager(env, "Я отвечу сам"))
        await env.advance(20 * 60)
        assert len(replies(env)) == 1
        # ещё одно сообщение: менеджер опять молчит, ИИ отвечает второй раз
        await env.say(client(env, "третий вопрос"))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(replies(env)) == 2
        assert len([n for n in env.notes if "вместо менеджера" in n[1]]) == 2
    run(go())


def test_reply_mode_message_while_the_ai_was_answering_starts_a_new_wait(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env, "вопрос"))
        await env.advance(16 * 60)
        await env.advance(5)
        first = replies(env)[0]
        await env.say(client(env, "и ещё одно"))                                    # пока ИИ готовил ответ
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        assert I.has_wait(f"{OTHER_CHAN}:1920391385")                              # ожидание по новому сообщению живо
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(replies(env)) == 2
    run(go())


def test_reply_mode_manager_message_after_the_note_denies_the_answer(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        body = replies(env)[0]
        await env.say(manager(env, "уже отвечаю"))
        code, out = E.decide_permit(DEAL, body["epoch"], body["turn_id"], "")
        assert out["allowed"] is False
    run(go())


def test_reply_mode_handoff_keeps_the_human_owner_and_drops_the_intercept_row(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        body = replies(env)[0]
        await E.handle_result(body["turn_id"], DEAL, body["epoch"], "handoff_to_human", "call_request", None)
        assert store.get_owner(DEAL)["owner"] == store.OWNER_HUMAN and not I.is_taken(DEAL)
    run(go())


def test_reply_mode_hung_turn_is_cleaned_up(env):
    async def go():
        setup(env, icfg={"mode": "reply"})
        igor_deal(env)
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert store.get_owner(DEAL) is not None
        await E._db(store.cancel_active_turns, DEAL)                               # итог хода так и не пришёл, ходов в работе нет
        await E._db(store.put_session, f"{OTHER_CHAN}:1920391385", state=store.S_AI, next_at=None, current_turn=None, pending_event=None)
        env.t += I.REPLY_STALE_S - 60
        await E.tick_once()
        assert store.get_owner(DEAL) is not None                                    # полчаса ещё не прошло
        env.t += 120
        await E.tick_once()
        assert store.get_owner(DEAL) is None and not I.is_taken(DEAL) and store.session_for_deal(DEAL) is None
    run(go())


def test_reply_mode_in_the_office_uses_the_mop_field(env):
    async def go():
        setup(env, icfg={"mode": "reply", "office": True})
        igor_deal(env, pipeline=PIPE_OFFICE, responsible=13963494, mop="Оанча Игорь")
        await env.say(client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(replies(env)) == 1
    run(go())


def test_switching_the_mode_applies_to_the_next_decision(env):
    async def go():
        setup(env, icfg={"mode": "take"})
        igor_deal(env)
        await env.say(client(env))
        setup(env, icfg={"mode": "reply"})                                           # переключили до срабатывания
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(replies(env)) == 1 and intercepts(env) == []
    run(go())
