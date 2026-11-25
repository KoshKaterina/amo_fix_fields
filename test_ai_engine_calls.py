"""Звонок менеджера как вмешательство и примечание о снятии (`ai_engine_calls`, просьба Кати 25.11.2026).

Проверяем то, ошибка в чём значит ИИ, пишущего поверх менеджера, ИИ, молча брошенного из-за чужого звонка, или неправду в карточке:

  • исходящий звонок любой и входящий с разговором снимают ИИ тем же путём, что сообщение менеджера; пропущенный входящий и звонки ДО взятия сделки не считаются;
  • примечание о снятии: дословный текст, одно на снятие, только если ИИ вёл сделку, сбой amo не ломает снятие и повторяется;
  • «старт» возвращает такую сделку (в том числе перехваченную), своё примечание движок командой не считает, закреплённая заметка открепляется;
  • пустые контакты записи владельца дописываются; сбой amo не теряет курсор и не мешает таймерам;
  • после «Выключено» сделка со звонком остаётся людям.

Сети нет: amo, сервис и Telegram подставные, время ручное. Запуск: python -m pytest test_ai_engine_calls.py -q
"""
import asyncio
import datetime
import logging

import pytest

import ai_engine as E
import ai_engine_calls as C
import ai_engine_config as cfg
import ai_engine_filters as F
import ai_engine_intercept as intercept
import ai_engine_mode_return as MR
import ai_engine_pause as P
import ai_engine_prompts as prompts
import ai_engine_store as store
import amo_service
from test_ai_engine import CHAN, CHAT, CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_mode_return import World
from test_ai_engine_pause import MANAGER as PAUSE_USER, Amo

MANAGER = "Иванов Иван"
OTHER_CONTACT = 999
CALLER_PHONE = "79995554433"
TEXT_MESSAGE = ("ИИ-менеджер приостановил ведение сделки, причина: живой менеджер отправил сообщение клиенту. "
                "Чтобы вернуть ИИ, напишите в сделку примечание «старт».")
TEXT_CALL_OUT = ("ИИ-менеджер приостановил ведение сделки, причина: живой менеджер совершил звонок. "
                 "Чтобы вернуть ИИ, напишите в сделку примечание «старт».")
TEXT_CALL_IN = ("ИИ-менеджер приостановил ведение сделки, причина: живой менеджер ответил на звонок клиента. "
                "Чтобы вернуть ИИ, напишите в сделку примечание «старт».")


async def drain():
    """Дождаться фоновых задач ЭТОГО цикла событий: в `_bg` могут остаться задачи прежних тестов, их ждать нельзя."""
    loop = asyncio.get_running_loop()
    for _ in range(100):
        await asyncio.sleep(0)
        mine = [t for t in E._bg if t.get_loop() is loop and not t.done()]
        if not mine:
            return
        await asyncio.wait(mine, timeout=5)
    raise AssertionError("фоновые задачи движка не кончаются")


class Telephony:
    """Подставные звонки amo: `GET /api/v4/contacts/notes` с фильтрами, страницами и `_links.next`."""

    def __init__(self, env, mp):
        self.env = env
        self.feed: list[dict] = []
        self.requests: list[dict] = []
        self.down = False
        self.next_id = 900000

        async def do_get(path, params=None):
            assert path == "/api/v4/contacts/notes", path
            query = dict(params)
            query["types"] = sorted(v for k, v in params if k == "filter[note_type][]")
            self.requests.append(query)
            if self.down:
                return None
            since, limit, page = int(query["filter[updated_at][from]"]), int(query["limit"]), int(query["page"])
            rows = sorted((n for n in self.feed if n["note_type"] in query["types"] and n["updated_at"] >= since), key=lambda n: n["updated_at"])
            chunk = rows[(page - 1) * limit:page * limit]
            if not chunk:
                return {}                                            # amo отвечает 204
            return {"_embedded": {"notes": chunk}, "_links": {"next": {"href": "x"}} if page * limit < len(rows) else {}}

        mp.setattr(amo_service, "_do_get", do_get)

    def call(self, note_type="call_out", status=4, contact=CONTACT, at=None, updated=None, duration=30):
        at = int(self.env.t if at is None else at)
        self.next_id += 1
        self.feed.append({"id": self.next_id, "entity_id": contact, "note_type": note_type, "created_by": 0, "created_at": at,
                          "updated_at": int(at if updated is None else updated),
                          "params": {"call_status": status, "duration": duration, "phone": CALLER_PHONE, "source": "sipuni", "uniq": "u"}})
        return self.next_id


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(prompts, "_good", None)
    monkeypatch.setattr(prompts, "_last_ok", None)
    monkeypatch.setattr(cfg, "MODE_RETURN_DISABLED", False)
    F._warned.clear()


@pytest.fixture
def phone(env, monkeypatch):
    monkeypatch.setattr(cfg, "CALLS_TAKEOVER_ENABLED", True)
    monkeypatch.setattr(C, "_next_run_at", float("inf"))             # опрос гоняем руками (`run_once`): `tick` только досылает примечания
    monkeypatch.setattr(store, "_now", lambda: env.t)                # `deal_owner.since` в тех же часах, что таймеры и звонки
    return Telephony(env, monkeypatch)


async def poll(env):
    await C.run_once(E)
    await drain()


async def led(env):
    """ИИ взял сделку по сообщению клиента и закончил ход."""
    turn = await env.start_turn()
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    await drain()
    return turn


async def led_and_watched(env):
    """Опрос уже смотрит звонки, ИИ ведёт сделку, с взятия прошло несколько секунд."""
    await poll(env)
    env.t += 5
    turn = await led(env)
    env.t += 5
    return turn


def owner(deal=DEAL):
    row = store.get_owner(deal)
    return row["owner"], row["reason"]


def human(env, text="Отвечу сам", **extra):
    return env.msg(text, echo=True, author=MANAGER, **extra)


def _iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================================
# что считается вмешательством
# =============================================================================================

@pytest.mark.parametrize("note,kind", [
    ({"note_type": "call_out", "params": {"call_status": 4}}, "call_out"),
    ({"note_type": "call_out", "params": {"call_status": 6, "duration": 0}}, "call_out"),      # менеджер взялся за клиента и без дозвона
    ({"note_type": "call_out"}, "call_out"),
    ({"note_type": "call_in", "params": {"call_status": 4, "duration": 0}}, "call_in"),
    ({"note_type": "call_in", "params": {"call_status": "4"}}, "call_in"),
    ({"note_type": "call_in", "params": {"call_status": 2, "duration": 25}}, None),            # не взяли: длительность не разговор
    ({"note_type": "call_in", "params": {"call_status": 6, "duration": 10}}, None),
    ({"note_type": "call_in", "params": {"duration": 120}}, None),
    ({"note_type": "common", "params": {"call_status": 4}}, None),
])
def test_what_counts_as_an_intervention(note, kind):
    assert C.intervention_kind(note) == kind


# =============================================================================================
# звонок снимает ИИ
# =============================================================================================

@pytest.mark.parametrize("note_type,status,text", [
    ("call_out", 4, TEXT_CALL_OUT), ("call_out", 6, TEXT_CALL_OUT), ("call_in", 4, TEXT_CALL_IN),
], ids=["outgoing", "outgoing-no-answer", "incoming-answered"])
def test_a_call_after_the_take_releases_the_deal_and_leaves_one_note(env, phone, note_type, status, text):
    async def go():
        await poll(env)
        env.t += 5
        env.lead()
        await env.say(env.msg("Здравствуйте"))
        await env.advance(61)
        turn = env.turn_requests[-1]                                # ход ещё идёт у сервиса
        env.t += 5
        phone.call(note_type, status)
        await poll(env)
        assert owner() == ("human", "manager_call") and store.get_owner(DEAL)["epoch"] == 2
        assert env.cancels == [turn["turn_id"]]
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])[1]["allowed"] is False
        assert store.get_session(CHAT_KEY)["state"] == store.S_HUMAN
        assert env.notes == [(DEAL, text)]
        assert env.alerts == [] and env.tasks_open == []             # как при сообщении менеджера: без алерта и задачи
        await env.say(env.msg("Алло, а доставка?"))
        await env.advance(120)
        await drain()
        assert len(env.turn_requests) == 1                          # ИИ молчит

    run(go())


@pytest.mark.parametrize("status", [2, 6])
def test_a_missed_incoming_call_does_not_count(env, phone, status):
    async def go():
        await led_and_watched(env)
        phone.call("call_in", status, duration=40)
        await poll(env)
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_repeated_polls_and_a_second_call_write_one_note(env, phone):
    async def go():
        await led_and_watched(env)
        phone.call("call_in", 4)
        env.t += 1
        phone.call("call_out", 4)                                    # два звонка в одном опросе: называем первый
        await poll(env)
        await poll(env)
        env.t += 5
        phone.call("call_out", 4)
        await poll(env)
        assert env.notes == [(DEAL, TEXT_CALL_IN)] and store.get_owner(DEAL)["epoch"] == 2
        assert [r["deal_id"] for r in C.journal()] == [DEAL]

    run(go())


def test_calls_before_the_take_do_not_count(env, phone):
    """Лид из звонка: клиент позвонил, потом написал. Иначе ИИ не взял бы такую сделку никогда."""
    async def go():
        await poll(env)
        env.t += 5
        old = phone.call("call_in", 4)
        env.t += 30
        await led(env)
        await poll(env)
        assert owner()[0] == "ai"
        env.t += 5
        next(n for n in phone.feed if n["id"] == old)["updated_at"] = int(env.t)      # amo обновила старое примечание
        await poll(env)
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_later_turns_of_the_ai_do_not_move_the_moment_of_the_take(env, phone):
    """Звонок был, а опрос ещё не прошёл: клиент написал, ИИ поставил новый ход. `since` не сдвигается, пока владелец ИИ, и звонок не теряется."""
    async def go():
        await led_and_watched(env)
        taken_at = store.get_owner(DEAL)["since"]
        phone.call("call_out")
        env.t += 30
        await env.say(env.msg("ещё вопрос", mid="later-1"))
        await env.advance(30)
        assert len(env.turn_requests) == 2 and store.get_owner(DEAL)["since"] == taken_at
        await poll(env)
        assert owner() == ("human", "manager_call")

    run(go())


def test_the_first_poll_starts_from_now(env, phone):
    async def go():
        await led(env)
        env.t += 5
        phone.call("call_out")                                       # после взятия, но до первого опроса (до выката)
        env.t += 5
        await poll(env)
        assert owner()[0] == "ai" and phone.requests, "первый опрос уже спрашивает amo: «удачный опрос» должен значить ответ amo"
        env.t += 5
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call")

    run(go())


def test_a_note_that_appears_late_is_still_found(env, phone):
    async def go():
        await led_and_watched(env)
        called_at = int(env.t)
        env.t += 400
        phone.call("call_in", 2, contact=OTHER_CONTACT)              # чужой звонок увёл курсор вперёд
        await poll(env)
        phone.call("call_out", at=called_at)                         # а это примечание появилось только теперь, с прежним временем
        await poll(env)
        assert owner() == ("human", "manager_call")

    run(go())


def test_a_call_of_another_contact_is_ignored(env, phone):
    async def go():
        await led_and_watched(env)
        phone.call("call_out", contact=OTHER_CONTACT)
        await poll(env)
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_every_ai_deal_of_the_contact_is_released(env, phone):
    async def go():
        await led_and_watched(env)
        chat = {"chat_id": "777", "chat_type": "telegram", "channel_id": CHAN, "chat_key": f"{CHAN}:777"}
        store.set_owner(DEAL + 1, "ai", reason="client_message", chat=chat, pipeline_id=PIPE_TEST, contact_ids=[CONTACT], mode="test")
        env.t += 5
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call") and owner(DEAL + 1) == ("human", "manager_call")
        assert sorted(d for d, _ in env.notes) == [DEAL, DEAL + 1]

    run(go())


def test_an_intercepted_deal_is_released_by_a_call(env, phone):
    async def go():
        await led_and_watched(env)
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t, intercept.KIND_TAKE)
        env.t += 5
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call")

    run(go())


def test_a_call_counts_while_the_settings_are_off_or_stale(env, phone):
    async def go():
        await led_and_watched(env)
        env.set_settings(mode="off", pipelines=[])                   # проход `_sweep_if_off` ещё не отпустил сделку
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call")                  # не `mode_off`: после включения она не вернётся

    run(go())


# =============================================================================================
# пустые контакты записи владельца
# =============================================================================================

async def led_without_contacts(env):
    """У сделки в момент взятия не было контакта (чат совпал по телефону), позже его привязали: в записи владельца `contact_ids` пуст."""
    env.lead(contacts=())
    await led(env)
    assert owner()[0] == "ai" and store.get_owner(DEAL)["contact_ids"] == "[]"
    env.lead()


def test_a_deal_without_contacts_gets_them_from_amo(env, phone):
    async def go():
        await poll(env)
        env.t += 5
        await led_without_contacts(env)
        env.t += 5
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call") and store.get_owner(DEAL)["contact_ids"] == f"[{CONTACT}]"

    run(go())


def test_a_deal_with_no_contact_at_all_is_not_asked_on_every_poll(env, phone, monkeypatch):
    async def go():
        await poll(env)
        env.t += 5
        await led_without_contacts(env)
        env.lead(contacts=())
        asked: list[int] = []
        real = amo_service.get_lead_full

        async def get_full(lead_id, with_=()):
            asked.append(int(lead_id))
            return await real(lead_id, with_=with_)

        monkeypatch.setattr(amo_service, "get_lead_full", get_full)
        env.t += 5
        phone.call("call_out")
        await poll(env)
        await poll(env)
        assert owner()[0] == "ai" and asked == [DEAL] and C.status_block(env.t)["last_error"] is None

    run(go())


def test_the_cursor_waits_while_the_contacts_cannot_be_read(env, phone):
    async def go():
        await poll(env)
        env.t += 5
        await led_without_contacts(env)
        env.t += 5
        phone.call("call_out")
        env.leads = []                                               # amo не отвечает по сделке
        before = C.cursor_get()[0]
        env.t += 1200
        phone.call("call_in", 2, contact=OTHER_CONTACT)
        await poll(env)
        assert owner()[0] == "ai" and C.cursor_get()[0] == before and C.status_block(env.t)["last_error"] == "incomplete"
        env.lead()
        await poll(env)
        assert owner() == ("human", "manager_call") and C.cursor_get()[0] > before

    run(go())


# =============================================================================================
# amo, страницы, цикл, рубильник, срез
# =============================================================================================

def test_a_silent_amo_keeps_the_cursor_and_the_timers_work(env, phone):
    async def go():
        await led_and_watched(env)
        before = C.cursor_get()[0]
        phone.call("call_out")
        phone.down = True
        env.t += 3600
        await poll(env)
        assert C.cursor_get()[0] == before and C.status_block(env.t)["last_error"] == "amo_unavailable"
        await env.say(env.msg("ещё вопрос", mid="q-1"))
        await env.advance(30)
        assert [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["q-1"]
        phone.down = False
        await poll(env)
        assert owner() == ("human", "manager_call") and C.status_block(env.t)["last_error"] is None

    run(go())


def test_all_pages_are_read(env, phone, monkeypatch):
    async def go():
        monkeypatch.setattr(C, "PAGE_LIMIT", 2)
        await led_and_watched(env)
        for i in range(4):
            env.t += 1
            phone.call("call_out", contact=OTHER_CONTACT + i)
        env.t += 1
        phone.call("call_out")
        phone.requests.clear()
        await poll(env)
        assert [q["page"] for q in phone.requests] == ["1", "2", "3"] and owner() == ("human", "manager_call")
        assert phone.requests[0]["types"] == ["call_in", "call_out"]

    run(go())


def test_the_poll_is_a_background_task_of_the_tick_and_not_a_sixth_loop(env, phone, monkeypatch):
    async def go():
        monkeypatch.setattr(C, "_next_run_at", 0.0)
        monkeypatch.setattr(cfg, "CALLS_POLL_S", 20)
        loops = list(E._tasks)
        await env.advance(1)
        await drain()
        assert C._task is not None and E._tasks == loops and len(phone.requests) == 1
        await env.advance(5)
        await drain()
        assert len(phone.requests) == 1                              # не чаще раза в 20 с
        await env.advance(20)
        await drain()
        assert len(phone.requests) == 2

    run(go())


def test_the_switch_turns_calls_off_but_keeps_the_message_note(env, phone, monkeypatch):
    async def go():
        monkeypatch.setattr(cfg, "CALLS_TAKEOVER_ENABLED", False)
        monkeypatch.setattr(C, "_next_run_at", 0.0)
        await led(env)
        env.t += 5
        phone.call("call_out")
        await env.advance(60)
        await drain()
        assert phone.requests == [] and owner()[0] == "ai" and E.status()["calls"]["enabled"] is False
        await env.say(human(env))
        await drain()
        assert env.notes == [(DEAL, TEXT_MESSAGE)]

    run(go())


def test_the_status_block(env, phone):
    async def go():
        block = E.status()["calls"]
        assert block["enabled"] is True and block["last_poll_at"] is None and block["removed_24h"] == 0
        await led_and_watched(env)
        phone.call("call_out")
        await poll(env)
        block = E.status()["calls"]
        assert (block["removed_24h"], block["last_error"], block["notes_pending"], block["poll_s"]) == (1, None, 0, cfg.CALLS_POLL_S)
        assert block["last_poll_at"] == _iso(env.t)

    run(go())


def test_no_phones_or_names_in_the_logs(env, phone, caplog):
    async def go():
        await led_and_watched(env)
        caplog.clear()
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call")

    with caplog.at_level(logging.INFO):
        run(go())
    mine = [r.getMessage() for r in caplog.records if "ai_engine_calls" in r.getMessage()]
    assert mine and not any(CALLER_PHONE in r.getMessage() or PHONE in r.getMessage() or "Тиана" in r.getMessage() for r in caplog.records)


# =============================================================================================
# примечание после сообщения менеджера
# =============================================================================================

def test_a_manager_message_leaves_one_note_even_with_repeated_webhooks(env, phone):
    async def go():
        await led(env)
        message = human(env, mid="mgr-1")
        await env.say(message)
        await env.say(message)
        await env.say(human(env, "И ещё раз"))
        await drain()
        await env.advance(60)
        await drain()
        assert owner() == ("human", "manager_message") and env.notes == [(DEAL, TEXT_MESSAGE)]
        assert env.alerts == [] and env.tasks_open == []

    run(go())


def test_robot_echo_and_a_chat_the_ai_does_not_lead_leave_no_note(env, phone):
    async def go():
        env.lead()
        await env.say(env.msg("Здравствуйте"))                       # ИИ сделку ещё не взял
        await env.say(human(env))
        await drain()
        assert env.notes == [] and store.get_owner(DEAL) is None
        store.drop_session(CHAT_KEY)
        await led(env)
        await env.say(env.msg("Шаблон", echo=True, author="Admin"))
        await drain()
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_a_manager_message_while_off_leaves_the_note_too(env, phone):
    async def go():
        await led(env)
        env.set_settings(mode="off", pipelines=[])
        await E._handle_off_payload({"messages": [human(env)]})
        await E._handle_off_payload({"messages": [human(env, "ещё")]})
        await drain()
        assert owner() == ("human", "manager_message") and env.notes == [(DEAL, TEXT_MESSAGE)]

    run(go())


def _failing_notes(env, monkeypatch, answer):
    attempts: list[int] = []

    async def add_note(deal_id, text):
        attempts.append(int(deal_id))
        value = answer[0]
        if isinstance(value, Exception):
            raise value
        if value.get("ok") is not False:
            env.notes.append((int(deal_id), text))
        return value

    monkeypatch.setattr(amo_service, "add_note", add_note)
    return attempts


@pytest.mark.parametrize("failure", [{"ok": False, "status_code": None, "retryable": True}, RuntimeError("amo")], ids=["refused", "raised"])
def test_a_failed_note_does_not_break_the_release_and_is_retried(env, phone, monkeypatch, failure):
    async def go():
        await led(env)
        answer = [failure]
        attempts = _failing_notes(env, monkeypatch, answer)
        await env.say(human(env))
        await drain()
        assert owner() == ("human", "manager_message") and env.notes == [] and len(attempts) == 1
        assert C.status_block(env.t)["notes_pending"] == 1
        await env.advance(30)
        await drain()
        assert len(attempts) == 1                                    # пауза перед повтором
        answer[0] = {"ok": True, "status_code": 200}
        await env.advance(C.NOTE_RETRY_S)
        await drain()
        await env.advance(3600)
        await drain()
        assert env.notes == [(DEAL, TEXT_MESSAGE)] and len(attempts) == 2 and C.status_block(env.t)["notes_pending"] == 0

    run(go())


@pytest.mark.parametrize("answer,wait", [
    ({"ok": False, "status_code": 400, "retryable": False}, 0), ({"ok": False, "status_code": None, "retryable": True}, C.NOTE_GIVE_UP_S + 1),
], ids=["rejected-for-good", "a-day-of-failures"])
def test_a_hopeless_note_is_given_up(env, phone, monkeypatch, answer, wait):
    async def go():
        await led(env)
        _failing_notes(env, monkeypatch, [answer])
        await env.say(human(env))
        await drain()
        env.t += wait
        await C.flush_notes(E)
        assert C.status_block(env.t)["notes_pending"] == 0 and env.notes == []

    run(go())


def test_a_note_not_yet_written_is_dropped_if_the_ai_is_back(env, phone, monkeypatch):
    async def go():
        await led(env)
        answer = [{"ok": False, "status_code": None, "retryable": True}]
        _failing_notes(env, monkeypatch, answer)
        await env.say(human(env))
        await drain()
        assert (await E.manager_start(DEAL))["state"] == "returned"
        answer[0] = {"ok": True}
        env.t += C.NOTE_RETRY_S + 1
        await C.flush_notes(E)
        assert env.notes == [] and C.status_block(env.t)["notes_pending"] == 0

    run(go())


# =============================================================================================
# «старт», «стоп», закреплённая заметка
# =============================================================================================

@pytest.fixture
def pins(env, phone, monkeypatch):
    monkeypatch.setattr(cfg, "PAUSE_NOTES_ENABLED", True)
    return Amo(env, monkeypatch)


async def commands(env):
    await P.run_once(E)
    await drain()


@pytest.mark.parametrize("how", ["call", "message"])
def test_start_returns_a_released_deal_and_only_a_new_call_releases_it_again(env, phone, pins, how):
    async def go():
        await commands(env)
        await led_and_watched(env)
        assert pins.pinned_texts() == ["ИИ-менеджер: " + P.TEXT_AI]
        if how == "call":
            phone.call("call_out")
            await poll(env)
        else:
            await env.say(human(env))
            await drain()
        assert owner()[0] == "human"
        await commands(env)
        assert pins.pinned_texts() == []                             # заметка «ИИ ведёт сделку» откреплена сверкой
        env.t += 60
        pins.command("старт")
        await commands(env)
        assert owner() == ("ai", "dialog_returned")
        env.turn_requests.clear()
        await env.advance(1)
        turn = env.turn_requests[-1]
        assert turn["event"]["type"] == "dialog_returned"
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])[1]["allowed"] is True
        await poll(env)
        assert owner()[0] == "ai"                                    # прежний звонок был ДО возврата
        env.t += 5
        phone.call("call_out")
        await poll(env)
        assert owner() == ("human", "manager_call")
        assert [t for _, t in env.notes].count(TEXT_CALL_OUT) == (2 if how == "call" else 1)

    run(go())


def test_start_works_for_an_intercepted_deal_the_manager_wrote_to(env, phone, pins):
    async def go():
        await commands(env)
        await led(env)
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t, intercept.KIND_TAKE)
        env.t += 5
        await env.say(human(env))
        await drain()
        assert owner() == ("human", "manager_message") and intercept.manager_replied_after(DEAL) is True
        env.t += 60
        start_at = env.t
        pins.command("старт")
        await commands(env)
        assert owner()[0] == "ai"
        env.turn_requests.clear()
        await env.advance(1)
        turn = env.turn_requests[-1]
        assert turn["intercepted"] is True and turn["manager_cutoff"] == _iso(start_at)
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])[1]["allowed"] is True

    run(go())


def test_the_engines_own_note_is_never_a_command(env, phone, pins):
    for text in C.NOTE_TEXT.values():
        assert P.parse_command(text) is None

    async def go():
        await commands(env)
        await led_and_watched(env)
        phone.call("call_out")
        await poll(env)
        env.t += 5
        pins.command(TEXT_CALL_OUT, by=0)
        pins.command(TEXT_CALL_OUT, by=PAUSE_USER)                   # даже если бы amo записала его на сотрудника
        await commands(env)
        assert owner() == ("human", "manager_call") and P.pause_get(DEAL) is None

    run(go())


def test_stop_and_removed_do_not_write_the_takeover_note(env, phone):
    async def go():
        await led(env)
        await E.manager_stop(DEAL)
        await drain()
        assert owner() == ("human", "paused") and env.notes == [] and C.status_block(env.t)["notes_pending"] == 0

    run(go())


# =============================================================================================
# возврат после «Выключено»
# =============================================================================================

@pytest.fixture
def w(env, phone):
    return World(env)


@pytest.mark.parametrize("note_type,status,expected", [
    ("call_out", 6, ("human", "mode_off_kept")), ("call_in", 4, ("human", "mode_off_kept")),
    ("call_in", 2, ("ai", "dialog_returned")), ("call_in", 6, ("ai", "dialog_returned")),
], ids=["outgoing", "answered", "missed", "not-reached"])
def test_a_call_while_off_decides_the_return(w, phone, note_type, status, expected):
    env = w.env

    async def go():
        await w.led_off()
        env.t += 100
        phone.call(note_type, status)
        env.t += 100
        w.on()
        await w.sweep()
        assert w.owner() == expected
        if expected[1] == "mode_off_kept":
            assert "звонок" in w.note() and env.tasks_open == [] and env.notes == [] and w.summaries() == []

    run(go())


@pytest.mark.parametrize("case", ["other-contact", "long-before-the-removal"])
def test_a_foreign_or_old_call_does_not_hold_the_deal(w, phone, case):
    env = w.env

    async def go():
        await w.led()
        if case == "long-before-the-removal":
            env.t += 5
            phone.call("call_out")
            env.t += 600
        await w.off()
        if case == "other-contact":
            env.t += 5
            phone.call("call_out", contact=OTHER_CONTACT)
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


def test_a_call_seconds_before_the_removal_is_not_missed(w, phone):
    """Опрос звонков не успел до выключения: сделку сняли `mode_off`, но менеджер ей уже занялся."""
    env = w.env

    async def go():
        await w.led()
        env.t += 100
        phone.call("call_out")
        env.t += 5
        await w.off()
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept")

    run(go())


def test_a_silent_amo_on_calls_postpones_the_return(w, phone):
    env = w.env

    async def go():
        await w.led_off()
        env.t += 5
        phone.call("call_out")
        phone.down = True
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and len(phone.requests) == 1
        await w.sweep()
        assert len(phone.requests) == 1                              # не раньше чем через RETRY_S
        phone.down = False
        await w.sweep(MR.RETRY_S)
        assert w.owner() == ("human", "mode_off_kept")

    run(go())


def test_the_switch_skips_the_call_check_on_return(w, phone, monkeypatch):
    env = w.env

    async def go():
        await w.led_off()
        env.t += 5
        phone.call("call_out")
        monkeypatch.setattr(cfg, "CALLS_TAKEOVER_ENABLED", False)
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned") and phone.requests == []

    run(go())
