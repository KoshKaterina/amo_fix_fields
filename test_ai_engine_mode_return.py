"""Возврат ИИ сделок, снятых выключением режима (`ai_engine_mode_return`, решение Кати 25.11.2026).

Проверяем то, ошибка в чём значит сообщение клиенту от ИИ поверх менеджера или молчащего ИИ после включения:

  • режим включили: сделка возвращается тем же путём, что человеком (`dialog_returned`), следующее сообщение клиента обрабатывается, повторное выключение снова снимает;
  • менеджер писал после снятия (в том числе пока вебхук при «Выключено» раньше отбрасывался), задача сотрудника, отметки, «стоп», закрытая сделка, чужая воронка, правила дублей: остаётся у людей (`mode_off_kept`);
  • доказать нечем (снята больше трёх суток назад, до этой версии или перед простоем): остаётся у людей;
  • временное (amo молчит, авто-режим, нет часов, ИИ-сервис лежит): `mode_off` остаётся, повтор с паузой, без долбёжки amo;
  • нагрузка и итог: не больше нескольких сделок за проход, одна строка в чат ОП, срез `GET /`, рубильник.

Сети нет: amo, сервис и Telegram подставные, время ручное. Запуск: python -m pytest test_ai_engine_mode_return.py -q
"""
import asyncio
import datetime
import time

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_filters as F
import ai_engine_intercept as intercept
import ai_engine_mode_return as MR
import ai_engine_pause as P
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from test_ai_engine import CHAN, CHAT, CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_RETAIL, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_modes import PIPE_TANGEM, apply

MANAGER = "Иванов Иван"
REAL_OFF_HANDLER = E._handle_off_payload


async def drain():
    """Дождаться фоновых задач движка. `sleep(0)` обязателен: `gather` по уже законченным задачам не отдаёт цикл, и колбэк, убирающий задачу из `_bg`, не выполнился бы."""
    while E._bg:
        await asyncio.gather(*list(E._bg))
        await asyncio.sleep(0)


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(prompts, "_good", None)                      # словаря сервиса нет: статический запас воронок
    monkeypatch.setattr(prompts, "_last_ok", None)
    monkeypatch.setattr(cfg, "MODE_RETURN_DISABLED", False)
    F._warned.clear()


class World:
    """Подставной мир возврата: время базы движка равно ручному, счётчики запросов amo."""

    def __init__(self, env):
        self.env = env
        self.lead_calls: list[int] = []
        self.task_calls: list[int] = []
        self.lead_down = False
        env.mp.setattr(store, "_now", lambda: env.t)                  # `deal_owner.since` в тех же часах, что и таймеры

        async def get_full(lead_id, with_=()):
            self.lead_calls.append(int(lead_id))
            if self.lead_down:
                return None
            return next((ld for ld in env.leads if ld["id"] == int(lead_id)), {})       # удалённой сделке amo отвечает пустым телом

        real_tasks = E.ai_engine_tasks._get_open

        async def get_open(deal_id):
            self.task_calls.append(int(deal_id))
            return await real_tasks(deal_id)

        env.mp.setattr(amo_service, "get_lead_full", get_full)
        env.mp.setattr(E.ai_engine_tasks, "_get_open", get_open)

    async def led(self):
        """ИИ взял сделку по сообщению клиента и закончил ход."""
        turn = await self.env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        await drain()
        self.lead_calls.clear()
        self.task_calls.clear()
        return turn

    async def off(self):
        self.env.set_settings(mode="off", pipelines=[])
        await self.env.advance(11)
        await drain()

    def on(self, **over):
        self.env.set_settings(**over)

    async def sweep(self, seconds=61):
        """Проход таймеров: возврат запускается фоновой задачей, дожидаемся её."""
        await self.env.advance(seconds)
        await drain()

    async def led_off(self):
        await self.led()
        await self.off()
        assert self.owner() == ("human", "mode_off")

    def owner(self, deal=DEAL):
        row = store.get_owner(deal)
        return row["owner"], row["reason"]

    def note(self, deal=DEAL):
        return next(r["note"] for r in MR.journal() if r["deal_id"] == deal)

    def summaries(self):
        return [a for a in self.env.alerts if "возвращено ИИ" in a]

    def many(self, n):
        """`n` клиентов со своими чатами и сделками, которые ведёт ИИ; поиск amo отвечает только по чату своего клиента."""
        env = self.env

        async def find(query, with_=(), page_limit=250, max_pages=5):
            return [ld for ld in env.leads if ld.get("_q") == query]

        env.mp.setattr(amo_service, "find_leads_by_query_all", find)
        ids = []
        for i in range(n):
            deal, chat = 1000 + i, f"55500{i:02d}"
            env.lead(lead_id=deal, pipeline=PIPE_TEST)["_q"] = chat
            key = f"{CHAN}:{chat}"
            store.set_owner(deal, "ai", reason="client_message", chat={"chat_id": chat, "chat_type": "telegram", "channel_id": CHAN, "chat_key": key},
                            pipeline_id=PIPE_TEST, contact_ids=[CONTACT], mode="test")
            store.put_session(key, channel_id=CHAN, chat_id=chat, chat_type="telegram", phone=PHONE, state=store.S_AI, deal_id=deal)
            ids.append(deal)
        return ids


@pytest.fixture
def w(env):
    return World(env)


async def webhook(env, *messages) -> bool:
    """Тот же путь, что у настоящего вебхука: `on_wazzup` и цикл приёма. False: в очередь ничего не легло."""
    done = asyncio.Event()

    async def spy(payload):
        await REAL_OFF_HANDLER(payload)
        done.set()

    env.mp.setattr(E, "_handle_off_payload", spy)
    env.mp.setattr(E, "_queue", asyncio.Queue())
    E.on_wazzup({"messages": list(messages)})
    if E._queue.empty():
        return False
    task = asyncio.create_task(E._ingest_loop())
    try:
        await asyncio.wait_for(done.wait(), 5)
    finally:
        task.cancel()
    return True


def human(env, text="Отвечу сам", **extra):
    return {**env.msg(text, echo=True, author=MANAGER), **extra}


def set_since(deal, ts, watch_from=None):
    iso = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with store._connect() as conn:
        conn.execute("UPDATE deal_owner SET since=? WHERE deal_id=?", (iso, deal))
        if watch_from is not None:
            conn.execute("UPDATE mode_return_meta SET v=? WHERE k='watch_from'", (watch_from,))


# =============================================================================================
# возврат
# =============================================================================================

def test_the_deal_comes_back_and_the_next_client_message_is_processed_and_off_takes_it_again(w):
    env = w.env

    async def go():
        await w.led_off()
        assert store.get_session(CHAT_KEY)["state"] == store.S_HUMAN
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and w.lead_calls == []          # пока выключено, ничего не происходит
        w.on()
        await w.sweep()
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["reason"], owner["epoch"], owner["mode"]) == ("ai", "dialog_returned", 3, "test")
        env.turn_requests.clear()
        await env.advance(1)
        event = env.turn_requests[-1]
        assert event["reason"] == "event" and event["event"]["type"] == "dialog_returned" and event["epoch"] == 3 and event["messages"] == []
        await E.handle_result(event["turn_id"], DEAL, 3, "continue", None, None)
        await env.say(env.msg("А когда доставка?", mid="after-1"))                  # чат был `human`: сообщение больше не теряется
        await env.advance(30)
        turn = env.turn_requests[-1]
        assert [m["message_id"] for m in turn["messages"]] == ["after-1"] and turn["epoch"] == 3
        assert E.decide_permit(DEAL, 3, turn["turn_id"], "after-1")[1]["allowed"] is True
        await E.handle_result(turn["turn_id"], DEAL, 3, "continue", None, None)
        assert w.summaries() == ["ИИ-менеджер (тест): после включения режима сделок возвращено ИИ: 1, оставлено менеджерам: 0."]

        await w.off()                                                              # повторное выключение снова снимает
        assert w.owner() == ("human", "mode_off") and store.get_owner(DEAL)["epoch"] == 4
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned") and store.get_owner(DEAL)["epoch"] == 5
        assert len(w.summaries()) == 2 and env.tasks_open == [] and env.notes == []

    run(go())


def test_a_deal_moved_to_another_allowed_pipeline_returns_with_the_new_pipeline(w):
    env = w.env

    async def go():
        apply(env, "full", 2)
        env.lead(pipeline=PIPE_RETAIL)
        await w.led()
        apply(env, "off", 2)
        await w.sweep(11)
        env.lead(pipeline=PIPE_TANGEM)
        apply(env, "full", 2)
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned") and store.get_owner(DEAL)["pipeline_id"] == PIPE_TANGEM

    run(go())


def test_other_chats_of_the_deal_marked_human_while_waiting_wake_up(w):
    env = w.env

    async def go():
        await w.led_off()
        second = f"{CHAN}:999"
        store.put_session(second, channel_id=CHAN, chat_id="999", chat_type="telegram", phone=PHONE, state=store.S_HUMAN, deal_id=DEAL)
        env.t += 1
        store.put_session(CHAT_KEY, state=store.S_HUMAN)                           # ход-событие пойдёт в этот чат (он свежее)
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")
        assert store.get_session(CHAT_KEY)["state"] == store.S_COLLECTING
        assert store.get_session(second)["state"] == store.S_AI
        await env.advance(1)
        await E.handle_result(env.turn_requests[-1]["turn_id"], DEAL, 3, "continue", None, None)
        message = env.msg("Пишу из другого чата", mid="second-1")
        message["chatId"] = "999"
        await env.say(message)
        await env.advance(30)
        assert [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["second-1"]

    run(go())


def test_an_intercepted_deal_returns_as_intercepted(w):
    env = w.env

    async def go():
        await w.led()
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t, intercept.KIND_TAKE)
        await w.off()
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")
        await env.advance(1)
        assert env.turn_requests[-1]["intercepted"] is True

    run(go())


def test_reply_instead_of_manager_is_never_returned(w):
    env = w.env

    async def go():
        env.lead()
        await env.advance(1)
        store.set_owner(DEAL, "ai", reason="intercept_reply", chat={"chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN, "chat_key": CHAT_KEY},
                        pipeline_id=PIPE_TEST, mode="test")
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t, intercept.KIND_REPLY)
        await w.off()
        assert w.owner() == ("human", "mode_off_kept")
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and w.lead_calls == []

    run(go())


# =============================================================================================
# менеджер писал клиенту
# =============================================================================================

def test_a_manager_message_during_off_keeps_the_deal_with_people(w):
    env = w.env

    async def go():
        await w.led_off()
        env.t += 100
        assert await webhook(env, human(env)) is True                              # раньше вебхук при «Выключено» отбрасывался целиком
        assert F.manager_last_out(CHAT_KEY) == pytest.approx(env.t)
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "писал менеджер" in w.note()
        assert w.lead_calls == [] and w.summaries() == [] and env.tasks_open == []

    run(go())


@pytest.mark.parametrize("message", [dict(echo=True, author="Admin"), dict(echo=True, author=""), dict(echo=False)], ids=["admin", "empty", "client"])
def test_robot_echo_and_client_messages_during_off_do_not_hold_the_deal(w, message):
    env = w.env

    async def go():
        await w.led_off()
        assert await webhook(env, env.msg("что-то", **message)) is False
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


@pytest.mark.parametrize("stale", [False, True], ids=["off-before-the-sweep", "stale-settings"])
def test_a_manager_message_while_the_ai_still_owns_the_deal_in_off_hands_it_over_at_once(w, stale):
    env = w.env

    async def go():
        await w.led()
        if stale:
            settings.set_document(settings.document(), fetched_at=time.time() - cfg.SETTINGS_STALE_S - 5)
        else:
            env.set_settings(mode="off", pipelines=[])
        assert settings.effective_mode() == "off" and store.get_owner(DEAL)["owner"] == "ai"
        assert await webhook(env, human(env)) is True
        assert w.owner() == ("human", "manager_message")                           # не `mode_off`: иначе вернулась бы поверх ответа менеджера
        await w.sweep(11)
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "manager_message")

    run(go())


def test_a_manager_message_in_another_listened_channel_counts(w):
    env = w.env

    async def go():
        env.mp.setattr(cfg, "CHANNELS", {CHAN, "second-channel"})
        await w.led_off()
        env.t += 100
        await webhook(env, human(env, channelId="second-channel"))
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "писал менеджер" in w.note()

    run(go())


def test_a_manager_message_after_the_mode_is_back_but_before_the_pass_counts(w):
    env = w.env

    async def go():
        await w.led_off()
        env.t += 5
        w.on()
        await env.say(human(env))
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept")

    run(go())


def test_a_manager_message_before_the_removal_does_not_count(w):
    env = w.env

    async def go():
        await w.led()
        F.note_manager_out(CHAT_KEY, env.t - 100)
        await w.off()
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


def test_a_manager_replying_after_an_interception_keeps_it_with_people(w):
    env = w.env

    async def go():
        await w.led()
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t - 10, intercept.KIND_TAKE)
        await w.off()
        env.t += 100
        await webhook(env, human(env))
        assert intercept.manager_replied_after(DEAL) is True
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept")

    run(go())


# =============================================================================================
# условия по сделке
# =============================================================================================

def test_an_open_task_of_an_employee_keeps_the_deal_and_is_not_touched(w):
    env = w.env

    async def go():
        await w.led_off()
        env.tasks_open.append({"id": 1, "text": "Перезвонить клиенту", "responsible_user_id": 7})
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "задача сотрудника" in w.note()
        assert env.tasks_open == [{"id": 1, "text": "Перезвонить клиенту", "responsible_user_id": 7}]
        w.task_calls.clear()
        await w.sweep(601)
        assert w.task_calls == [] and w.summaries() == []                           # постоянная причина: больше не проверяется; ничего не вернули: строки в чат нет

    run(go())


def test_a_task_of_the_ai_user_does_not_block(w):
    env = w.env

    async def go():
        await w.led_off()
        env.tasks_open.append({"id": 1, "text": "Задача ИИ", "responsible_user_id": F.DEFAULT_AI_USER["id"]})
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


@pytest.mark.parametrize("silent", ["tasks", "lead", "search"])
def test_a_silent_amo_leaves_the_deal_waiting_and_is_asked_again_only_after_a_pause(w, silent):
    env = w.env

    async def go():
        await w.led_off()
        env.tasks_down, w.lead_down, env.amo_down = silent == "tasks", silent == "lead", silent == "search"
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and len(w.lead_calls) == 1
        for _ in range(4):
            await w.sweep()
        assert len(w.lead_calls) == 1                                              # без долбёжки amo каждый тик
        env.tasks_down = w.lead_down = env.amo_down = False
        await w.sweep(MR.RETRY_S)
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


@pytest.mark.parametrize("change,why", [("closed", "закрыта"), ("gone", "не найдена"), ("pipeline", "воронку")])
def test_a_closed_missing_or_foreign_deal_stays_with_people(w, change, why):
    env = w.env

    async def go():
        env.mp.setattr(prompts, "_good", prompts.Snapshot(prompt=frozenset({PIPE_RETAIL, PIPE_TANGEM}), test=frozenset(), at=0.0))
        env.mp.setattr(prompts, "_last_ok", True)
        await w.led_off()
        if change == "closed":
            env.lead(status=143)
        elif change == "gone":
            env.leads = []
        else:
            env.lead(pipeline=PIPE_RETAIL)                                         # перенесли из «Теста»: режим «Тест» её не ведёт
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and why in w.note()
        assert not store.get_owner(DEAL)["closed"] and len(env.turn_requests) == 1

    run(go())


def test_without_the_prompt_dictionary_a_foreign_pipeline_is_not_decided_yet(w):
    env = w.env

    async def go():
        await w.led_off()
        env.lead(pipeline=PIPE_RETAIL)                                             # статический запас уже настоящего словаря: навсегда по нему не решаем
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and prompts.source() == prompts.SOURCE_STATIC

    run(go())


@pytest.mark.parametrize("kind", ["no_reply", "removed"])
def test_a_mark_of_a_person_keeps_the_deal(w, kind):
    async def go():
        await w.led_off()
        code, _ = await E.mark_deal(DEAL, kind, "РОП")
        assert code == 200
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and kind in w.note()

    run(go())


def test_stop_during_off_replaces_the_reason_and_the_deal_is_not_returned(w):
    env = w.env

    async def go():
        await w.led_off()
        await E.manager_stop(DEAL)
        assert w.owner() == ("human", "paused")                                    # `_to_human` причину человеческой записи сам не меняет
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "paused") and w.lead_calls == [] and len(env.turn_requests) == 1

    run(go())


def test_a_recorded_pause_alone_keeps_the_deal(w):
    async def go():
        await w.led_off()
        P.pause_put(DEAL, w.env.t, MANAGER)
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "стоп" in w.note()

    run(go())


def test_a_person_can_still_start_a_kept_deal(w):
    env = w.env

    async def go():
        await w.led_off()
        env.tasks_open.append({"id": 1, "text": "Перезвонить", "responsible_user_id": 7})
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept")
        assert (await E.manager_start(DEAL))["state"] == "returned" and w.owner() == ("ai", "dialog_returned")

    run(go())


def test_another_deal_of_the_client_keeps_it_with_people_without_alerts_and_tasks(w):
    env = w.env

    async def go():
        await w.led_off()
        env.lead(lead_id=DEAL + 1, price=1000)                                     # пока ИИ был выключен, у клиента появился заказ
        w.on()
        env.alerts.clear()
        await w.sweep()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "правило C" in w.note()
        assert env.alerts == [] and env.tasks_open == [] and env.notes == [] and len(env.turn_requests) == 1

    run(go())


def test_a_contact_removed_from_the_list_keeps_the_deal(w):
    async def go():
        await w.led_off()
        w.on(contacts={"scope": "test", "restricted": True, "items": []})
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "списке" in w.note()

    run(go())


def test_a_channel_that_is_not_listened_keeps_the_deal(w):
    env = w.env

    async def go():
        await w.led_off()
        env.mp.setattr(cfg, "CHANNELS", {"another-channel"})
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "не слушает" in w.note()

    run(go())


def test_a_chat_where_the_ai_leads_another_deal_keeps_it(w):
    async def go():
        await w.led_off()
        store.set_owner(DEAL + 1, "ai", reason="client_message", chat={"chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN, "chat_key": CHAT_KEY},
                        pipeline_id=PIPE_TEST, mode="test")
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "другую сделку" in w.note()

    run(go())


def test_an_autopilot_chat_waits(w):
    env = w.env

    async def go():
        await w.led_off()
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: True)
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and w.lead_calls == []
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: False)
        await w.sweep(MR.RETRY_S)
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


# =============================================================================================
# режим записи и доказуемость
# =============================================================================================

def test_a_test_deal_is_not_returned_in_the_full_mode_and_waits_for_the_test_mode(w):
    env = w.env

    async def go():
        await w.led_off()
        apply(env, "full", 2)
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and w.lead_calls == []           # не вернули и не пометили
        assert E.status()["mode_return"]["waiting_other_mode"] == 1
        apply(env, "test", 2)
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


@pytest.mark.parametrize("age,expected", [(F.MANAGER_OUT_KEEP_S + 60, ("human", "mode_off_kept")), (F.MANAGER_OUT_KEEP_S - 600, ("ai", "dialog_returned"))],
                         ids=["older-than-the-log", "inside"])
def test_only_removals_not_older_than_the_manager_log(w, age, expected):
    env = w.env

    async def go():
        await w.led_off()
        set_since(DEAL, env.t - age, watch_from=env.t - 30 * 86400)
        w.on()
        await w.sweep()
        assert w.owner() == expected
        if expected[1] == "mode_off_kept":
            assert w.lead_calls == [] and "трёх суток" in w.note()

    run(go())


def test_removals_made_before_this_version_are_not_returned(w):
    env = w.env

    async def go():
        await w.led_off()
        with store._connect() as conn:
            conn.execute("DELETE FROM mode_return_meta")                           # прежний код: метки «с какого момента запоминаем» нет
        MR.reset_for_tests()
        env.t += 100
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "не запоминал" in w.note() and w.lead_calls == []

    run(go())


@pytest.mark.parametrize("pause_s,expected", [(MR.GAP_S + 60, ("human", "mode_off_kept")), (120, ("ai", "dialog_returned"))], ids=["long-stop", "deploy"])
def test_a_long_stop_of_the_engine_voids_the_proof_and_a_restart_does_not(w, pause_s, expected):
    env = w.env

    async def go():
        await w.led_off()
        MR.reset_for_tests()                                                       # новый процесс
        env.t += pause_s
        w.on()
        await w.sweep(1)
        assert w.owner() == expected

    run(go())


# =============================================================================================
# когда проход работает
# =============================================================================================

def test_the_pass_needs_work_hours_a_live_ai_service_and_no_switch(w):
    env = w.env

    async def go():
        await w.led_off()
        w.on(work_hours=[])
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and MR.blocked_by(env.t) == "сейчас вне рабочих часов"
        w.on()
        env.health_ok = False
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and w.lead_calls == []
        env.health_ok = True
        env.mp.setattr(cfg, "MODE_RETURN_DISABLED", True)
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and "AI_MODE_RETURN_DISABLED" in MR.blocked_by(env.t)
        assert E.status()["mode_return"]["enabled"] is False
        env.mp.setattr(cfg, "MODE_RETURN_DISABLED", False)
        env.mp.setattr(cfg, "ENABLED", False)
        assert MR.blocked_by(env.t) == "движок выключен"
        env.mp.setattr(cfg, "ENABLED", True)
        settings.set_document(settings.document(), fetched_at=time.time() - cfg.SETTINGS_STALE_S - 5)
        assert "устарели" in MR.blocked_by(env.t)
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


def test_the_pass_waits_until_the_stop_notes_have_been_read(w):
    env = w.env

    async def go():
        await w.led_off()

        async def no_tick(eng, now):
            return None

        async def no_note(deal_id, text):
            return None

        env.mp.setattr(P, "tick", no_tick)
        env.mp.setattr(P, "_amo_add_note", no_note)
        env.mp.setattr(cfg, "PAUSE_NOTES_ENABLED", True)
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off") and "стоп" in MR.blocked_by(env.t)
        env.mp.setitem(P._state, "last_poll_at", env.t)
        await w.sweep(1)
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


@pytest.mark.parametrize("what", ["manager", "stop", "off"])
def test_everything_is_checked_again_after_amo_has_answered(w, what):
    env = w.env

    async def go():
        await w.led_off()
        real = E.ai_engine_tasks._get_open

        async def slow(deal_id):
            if what == "manager":
                await env.say(human(env))
            elif what == "stop":
                await E.manager_stop(deal_id)
            else:
                env.set_settings(mode="off", pipelines=[])
            return await real(deal_id)

        env.mp.setattr(E.ai_engine_tasks, "_get_open", slow)
        env.t += 5
        w.on()
        await w.sweep()
        expected = {"manager": ("human", "mode_off_kept"), "stop": ("human", "paused"), "off": ("human", "mode_off")}[what]
        assert w.owner() == expected and store.get_owner(DEAL)["epoch"] == 2 and w.summaries() == []

    run(go())


# =============================================================================================
# нагрузка, итог, срез
# =============================================================================================

def test_a_few_deals_per_pass_and_one_summary_when_the_queue_is_done(w):
    env = w.env

    async def go():
        deals = w.many(7)
        await env.advance(1)
        await w.off()
        assert all(w.owner(d) == ("human", "mode_off") for d in deals)
        env.tasks_open.append({"id": 1, "text": "x", "responsible_user_id": 7})     # подставные задачи общие на все сделки: сперва без них
        env.tasks_open.clear()
        w.on()
        returned = []
        for _ in range(3):
            before = len(w.lead_calls)
            await w.sweep()
            assert len(w.lead_calls) - before <= MR.PER_RUN
            returned.append(sum(w.owner(d)[0] == "ai" for d in deals))
        assert returned == [3, 6, 7]
        assert w.summaries() == ["ИИ-менеджер (тест): после включения режима сделок возвращено ИИ: 7, оставлено менеджерам: 0."]
        assert "Тиана" not in w.summaries()[0] and "@" not in w.summaries()[0]
        await w.sweep()
        assert len(w.summaries()) == 1

    run(go())


def test_the_summary_counts_kept_deals_and_goes_out_if_the_window_closes_midway(w):
    env = w.env

    async def go():
        w.many(6)
        await env.advance(1)
        await w.off()
        F.note_manager_out(f"{CHAN}:5550000", env.t + 1)
        env.t += 5
        w.on()
        await w.sweep()
        assert w.summaries() == []                                                 # очередь ещё не разобрана
        w.on(work_hours=[])
        await w.sweep()
        assert w.summaries() == ["ИИ-менеджер (тест): после включения режима сделок возвращено ИИ: 3, оставлено менеджерам: 1."]

    run(go())


def test_two_amo_failures_in_a_row_pause_the_whole_pass(w):
    env = w.env

    async def go():
        deals = w.many(5)
        await env.advance(1)
        await w.off()
        w.lead_calls.clear()
        w.lead_down = True
        w.on()
        await w.sweep()
        assert len(w.lead_calls) == 2 and "две неудачи" in E.status()["mode_return"]["last_result"]
        for _ in range(3):
            await w.sweep()
        assert len(w.lead_calls) == 2 and all(w.owner(d) == ("human", "mode_off") for d in deals)
        w.lead_down = False
        for _ in range(3):
            await w.sweep(MR.RETRY_S if _ == 0 else 61)
        assert all(w.owner(d)[0] == "ai" for d in deals)

    run(go())


def test_the_pass_is_a_background_task_and_not_a_sixth_loop(w):
    env = w.env

    async def go():
        await w.led_off()
        w.on()
        loops = list(E._tasks)
        await env.advance(61)
        assert MR._task is not None and E._tasks == loops
        await drain()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


def test_the_status_block(w):
    async def go():
        await w.led_off()
        block = E.status()["mode_return"]
        assert block["enabled"] is True and block["waiting"] == 1 and (block["returned_24h"], block["kept_24h"]) == (0, 0)
        assert "Выключено" in block["blocked_by"] and block["watch_from"]
        w.on()
        await w.sweep()
        block = E.status()["mode_return"]
        assert (block["waiting"], block["returned_24h"], block["kept_24h"], block["blocked_by"]) == (0, 1, 0, None)
        assert "возвращено 1" in block["last_result"] and block["last_run_at"]

    run(go())
