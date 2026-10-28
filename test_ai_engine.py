"""Движок для ИИ-менеджера: сценарии контракта v2 (сторона `amo_fix_fields`).

Проверяем то, ошибка в чём означает сообщение живому клиенту от ИИ, который уже не вправе писать, или молчание
там, где клиента надо передать человеку:

  • таймеры: первое ожидание, debounce, верхний предел, один ход на сделку;
  • менеджер и медиа снимают диалог с ИИ, эхо автоматики и самого ИИ - нет;
  • режимы: чужая воронка, чужой контакт, режим off, протухшие настройки;
  • дедуп сделок (правила A, C, D), молчание amo не равно «сделок нет»;
  • `permit` и `result`: эпоха, владелец, «клиент написал ещё», повторы, три провала хода;
  • служебные события `payment_received` и `dialog_returned`;
  • очередь к сервису: повторы 1/5/25 с, backlog, отказ сервиса, падение сервиса надолго.

Сети нет: amo, сервис и Telegram подставные, время ручное. Запуск: python -m pytest test_ai_engine.py -q
"""
import asyncio
import json
import time

import httpx
import pytest

import ai_engine as E
import ai_engine_client as client
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service

CHAN = "12d3ccf1-8f15-4144-8b6e-5fffe3a2f647"
CHAT = "1920391385"
PHONE = "79260000001"
DEAL = 36587533
PIPE_TEST = 8642414
PIPE_RETAIL = 10593102
PIPE_OFFICE = 9421022
CONTACT = 555
CHAT_KEY = f"{CHAN}:{CHAT}"


class Env:
    """Подставной мир: ручное время, подставные amo / сервис / Telegram, помощники сценариев."""

    def __init__(self, tmp_path, monkeypatch):
        self.mp = monkeypatch
        self.t = time.time()
        self.alerts: list[str] = []
        self.notes: list[tuple[int, str]] = []
        self.leads: list[dict] = []
        self.amo_down = False
        self.service_turn_status = 202
        self.service_turn_error = False
        self.health_ok = True
        self.turn_requests: list[dict] = []
        self.cancels: list[int] = []

        mp = monkeypatch
        mp.setattr(cfg, "ENABLED", True)
        mp.setattr(cfg, "DB_PATH", str(tmp_path / "ai_engine.sqlite3"))
        mp.setattr(cfg, "SETTINGS_CACHE_PATH", str(tmp_path / "settings.json"))
        mp.setattr(cfg, "CHANNELS", {CHAN})
        mp.setattr(cfg, "SERVICE_URL", "http://ai.test")
        mp.setattr(cfg, "SERVICE_TOKEN", "svc-token")
        mp.setattr(cfg, "ENGINE_TOKEN", "engine-token")
        mp.setattr(cfg, "ADMIN_TOKEN", "admin-token")
        store.init()
        self.set_settings()

        mp.setattr(E, "_now", lambda: self.t)
        mp.setattr(E, "_ai_ok_at", self.t)
        mp.setattr(E, "_ai_down_since", None)
        mp.setattr(E, "_ai_down_alerted", False)
        mp.setattr(E, "_off_alerted", False)
        mp.setattr(E, "_lock", None)
        mp.setattr(E, "_last_off_sweep", 0.0)
        mp.setattr(E, "_last_expire", 0.0)
        mp.setattr(E, "_last_housekeeping", self.t)
        mp.setattr(E, "_alert_seen", {})
        mp.setattr(E, "_rule_c_seen", {})
        mp.setattr(E, "_autopilot_busy", lambda *a, **k: False)

        async def deliver(d):
            self.alerts.append(d.text)
            return True

        mp.setattr(E, "_deliver_alert", deliver)

        async def find(query, with_=(), page_limit=250, max_pages=5):
            return None if self.amo_down else list(self.leads)

        async def get_full(lead_id, with_=()):
            return next((l for l in self.leads if l["id"] == int(lead_id)), None)

        async def add_note(lead_id, text):
            self.notes.append((int(lead_id), text))
            return {}

        mp.setattr(amo_service, "find_leads_by_query_all", find)
        mp.setattr(amo_service, "get_lead_full", get_full)
        mp.setattr(amo_service, "add_note", add_note)
        mp.setattr(amo_service, "get_status_sort", lambda status, pipeline: None)

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/v1/health":
                return httpx.Response(200, json={"ok": True, "queue": 0}) if self.health_ok else httpx.Response(503)
            if path == "/v1/turn":
                if self.service_turn_error:
                    raise httpx.ConnectError("нет связи")
                self.turn_requests.append(json.loads(request.content))
                return httpx.Response(self.service_turn_status, json={"accepted": True})
            if path.endswith("/cancel"):
                self.cancels.append(int(path.split("/")[-2]))
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(404)

        mp.setattr(client, "_transport", httpx.MockTransport(handler))

    # ------------------------------------------------------------------ настройки и сделки
    def set_settings(self, **over):
        doc = {
            "mode": "test", "pipelines": [PIPE_TEST],
            "contacts": {"scope": "test", "restricted": True,
                         "items": [{"contact_id": CONTACT, "phone_norm": PHONE, "name": "Тест"}]},
            "timers": {"first_reply_wait_s": 60, "debounce_s": 10, "debounce_cap_s": 60,
                       "deal_resolve_timeout_s": 600, "ai_down_release_s": 120},
            "post_sale_window_days": 7,
            "work_hours": [{"start": "00:00", "end": "23:59"}],
            "updated_at": None,
        }
        doc.update(over)
        settings.set_document(doc, fetched_at=time.time())

    def lead(self, lead_id=DEAL, pipeline=PIPE_TEST, status=1000, price=0, contacts=(CONTACT,), fields=None):
        lead = {
            "id": lead_id, "pipeline_id": pipeline, "status_id": status, "price": price,
            "custom_fields_values": [{"field_id": k, "values": [{"value": v}]} for k, v in (fields or {}).items()],
            "_embedded": {"contacts": [{"id": c, "is_main": i == 0} for i, c in enumerate(contacts)]},
        }
        self.leads = [l for l in self.leads if l["id"] != lead_id] + [lead]
        return lead

    # ------------------------------------------------------------------ сообщения
    def msg(self, text="слово", *, mid=None, typ="text", echo=False, author=None, status=None):
        m = {
            "messageId": mid or f"m-{len(text)}-{time.time_ns()}", "channelId": CHAN, "chatId": CHAT,
            "chatType": "telegram", "type": typ, "text": text, "isEcho": echo,
            "dateTime": "2026-10-28T11:05:29.000Z",
            "contact": {"name": "Тиана", "phone": PHONE},
        }
        if echo:
            m["authorName"] = author if author is not None else ""
            m["status"] = status or "sent"
        else:
            m["status"] = "inbound"
        return m

    async def say(self, *messages):
        await E._handle_payload({"messages": list(messages)})

    async def advance(self, seconds):
        self.t += seconds
        await E.tick_once()
        await E.dispatch_once()

    async def start_turn(self, text="слово"):
        """Клиент написал, сделка есть, ждём первое окно: возвращает первый ход, который ушёл сервису."""
        if not self.leads:
            self.lead()
        await self.say(self.msg(text))
        await self.advance(61)
        assert self.turn_requests, "ход не ушёл"
        return self.turn_requests[-1]

    def result(self, turn, outcome="continue", reason=None, epoch=None):
        return asyncio.ensure_future(E.handle_result(
            turn["turn_id"], turn["deal_id"], epoch if epoch is not None else turn["epoch"], outcome, reason, None,
        ))


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


def run(coro):
    return asyncio.run(coro)


# =============================================================================================
# таймеры
# =============================================================================================

def test_next_turn_at_formulas():
    t = {"first_reply_wait_s": 60, "debounce_s": 10, "debounce_cap_s": 60}
    # первая пачка: не раньше T0+60, тишина после последнего не короче 10
    assert E.next_turn_at(first=True, batch_start=0, last=0, timers=t) == 60
    assert E.next_turn_at(first=True, batch_start=0, last=55, timers=t) == 65
    # верхний предел первой пачки: T0 + 60 + 60
    assert E.next_turn_at(first=True, batch_start=0, last=500, timers=t) == 120
    # следующие пачки: только тишина и свой предел
    assert E.next_turn_at(first=False, batch_start=100, last=100, timers=t) == 110
    assert E.next_turn_at(first=False, batch_start=100, last=500, timers=t) == 160


def test_first_message_turn_not_before_wait_and_has_contract_shape(env):
    async def go():
        env.lead()
        await env.say(env.msg("слово", mid="mid-1"))
        await env.advance(59)
        assert env.turn_requests == []
        await env.advance(2)
        assert len(env.turn_requests) == 1
        turn = env.turn_requests[0]
        assert turn["reason"] == "first_message" and turn["mode"] == "test" and turn["epoch"] == 1
        assert turn["deal_id"] == DEAL
        assert turn["chat"] == {"chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN}
        assert [m["message_id"] for m in turn["messages"]] == ["mid-1"]
        assert turn["up_to_message_id"] == "mid-1"
        assert turn["send_window"]["open_now"] is True
        assert turn["returning"] is None and turn["client_memory"] is None
        assert "event" not in turn
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("ai", 1)
    run(go())


def test_three_quick_messages_are_one_turn_and_pause_gives_two(env):
    async def go():
        turn1 = await env.start_turn()
        r = await E.handle_result(turn1["turn_id"], DEAL, turn1["epoch"], "continue", None, None)
        assert r[0] == 200
        env.turn_requests.clear()
        await env.say(env.msg("раз", mid="a"))
        await env.advance(5)
        await env.say(env.msg("два", mid="b"))
        await env.advance(5)
        await env.say(env.msg("три", mid="c"))
        await env.advance(9)
        assert env.turn_requests == []                      # тишина ещё не 10 секунд
        await env.advance(2)
        assert len(env.turn_requests) == 1
        second = env.turn_requests[0]
        assert [m["message_id"] for m in second["messages"]] == ["a", "b", "c"]
        assert second["reason"] == "client_message"
        await E.handle_result(second["turn_id"], DEAL, second["epoch"], "continue", None, None)
        await env.say(env.msg("четыре", mid="d"))
        await env.advance(11)                               # пауза 11 секунд: отдельный ход
        assert len(env.turn_requests) == 2
        assert [m["message_id"] for m in env.turn_requests[1]["messages"]] == ["d"]
    run(go())


def test_debounce_cap_fires_even_if_client_keeps_writing(env):
    async def go():
        turn1 = await env.start_turn()
        await E.handle_result(turn1["turn_id"], DEAL, turn1["epoch"], "continue", None, None)
        env.turn_requests.clear()
        for i in range(8):                                  # сообщение каждые 8 секунд: тишины в 10 с не бывает
            await env.say(env.msg(f"п{i}", mid=f"p{i}"))
            await env.advance(8)
            if env.t - first_at < 60:
                assert env.turn_requests == []
        # к 64-й секунде пачки предел в 60 с уже сработал, хотя клиент не замолкал
        assert len(env.turn_requests) == 1
        assert len(env.turn_requests[0]["messages"]) >= 7
    first_at = env.t
    run(go())


def test_one_turn_per_deal_while_turn_runs(env):
    async def go():
        turn1 = await env.start_turn()
        await env.say(env.msg("ещё", mid="x"))
        await env.advance(30)
        assert len(env.turn_requests) == 1                  # ход идёт, новые сообщения копятся
        await E.handle_result(turn1["turn_id"], DEAL, turn1["epoch"], "continue", None, None)
        await env.advance(11)
        assert len(env.turn_requests) == 2
        assert [m["message_id"] for m in env.turn_requests[1]["messages"]] == ["x"]
    run(go())


def test_duplicate_webhook_message_does_not_double(env):
    async def go():
        env.lead()
        m = env.msg("слово", mid="same")
        await env.say(m, m)
        await env.say(m)
        await env.advance(61)
        assert len(env.turn_requests) == 1 and len(env.turn_requests[0]["messages"]) == 1
    run(go())


def test_timers_survive_restart(env):
    """Состояние в sqlite: «перезапуск» (потеря памяти модуля) ничего не теряет."""
    async def go():
        env.lead()
        await env.say(env.msg("слово", mid="r1"))
        await env.advance(30)
        E._lock = None                                       # новый процесс: ни замков, ни кэшей
        E._alert_seen.clear()
        await env.advance(31)
        assert len(env.turn_requests) == 1 and env.turn_requests[0]["messages"][0]["message_id"] == "r1"
    run(go())


# =============================================================================================
# менеджер, эхо, медиа
# =============================================================================================

def test_manager_message_during_wait_cancels_timer(env):
    async def go():
        env.lead()
        await env.say(env.msg("слово"))
        await env.advance(30)
        await env.say(env.msg("Здравствуйте!", echo=True, author="Иванов Иван"))
        await env.advance(120)
        assert env.turn_requests == []
        assert store.get_session(CHAT_KEY)["state"] == "human"
    run(go())


def test_manager_message_takes_deal_from_ai_and_cancels_turn(env):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("Беру на себя", echo=True, author="Phone"))
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        await asyncio.sleep(0.05)
        assert env.cancels == [turn["turn_id"]]
        code, body = E.decide_permit(DEAL, 2, turn["turn_id"], turn["up_to_message_id"])
        assert body == {"allowed": False, "reason": "owner_changed"}
        code, body = E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert code == 409 and body["reason"] == "stale_epoch"
    run(go())


@pytest.mark.parametrize("author", ["Admin", "", "bot", "ADMIN"])
def test_echo_of_automation_and_ai_is_not_a_manager(env, author):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("Ответ ИИ", echo=True, author=author, mid="ai-echo"))
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("ai", 1)
        assert len(env.turn_requests) == 1
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        await env.say(env.msg("Ещё ответ", echo=True, author=author, mid="ai-echo-2"))
        await env.advance(120)
        assert len(env.turn_requests) == 1                  # эхо хода не запускает
    run(go())


def test_media_first_message_goes_to_human_without_turn(env):
    async def go():
        env.lead()
        await env.say(env.msg("", typ="image", mid="img-1"))
        await env.advance(1)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 1)
        assert env.turn_requests == []
        assert any("картинку" in a for a in env.alerts)
    run(go())


def test_media_in_pack_with_text_sends_whole_pack_to_human(env):
    async def go():
        env.lead()
        await env.say(env.msg("вот скрин", mid="t-1"), env.msg("", typ="image", mid="img-2"))
        await env.advance(1)
        await env.advance(120)
        assert store.get_owner(DEAL)["owner"] == "human"
        assert env.turn_requests == []
        assert store.pending_inbox(CHAT_KEY) == []
    run(go())


def test_media_while_ai_owns_is_immediate_and_cancels_turn(env):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("", typ="audio", mid="voice-1"))
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        await asyncio.sleep(0.05)
        assert env.cancels == [turn["turn_id"]]
        assert any("голосовое" in a for a in env.alerts)
        code, body = E.decide_permit(DEAL, 2, turn["turn_id"], turn["up_to_message_id"])
        assert body["allowed"] is False
    run(go())


def test_empty_text_message_is_treated_as_media(env):
    async def go():
        env.lead()
        await env.say(env.msg("   ", mid="blank"))
        await env.advance(1)
        assert store.get_owner(DEAL)["owner"] == "human" and env.turn_requests == []
    run(go())


# =============================================================================================
# режимы и списки
# =============================================================================================

def test_mode_off_ignores_everything(env):
    async def go():
        env.set_settings(mode="off", pipelines=[])
        E._queue = asyncio.Queue()
        E.on_wazzup({"messages": [env.msg("слово")]})
        assert E._queue.empty()
        E._queue = None
    run(go())


def test_stale_settings_mean_off(env):
    settings.set_document(settings.document(), fetched_at=time.time() - cfg.SETTINGS_STALE_S - 5)
    assert settings.effective_mode() == "off"


def test_pipeline_mismatch_means_off(env):
    env.set_settings(mode="test", pipelines=[PIPE_RETAIL])          # панель увела тест в боевую воронку
    assert settings.effective_mode() == "off"
    env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TEST])
    assert settings.effective_mode() == "off"
    env.set_settings(mode="prod", pipelines=[])
    assert settings.effective_mode() == "off"


def test_disabled_flag_means_off(env, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert settings.effective_mode() == "off"


def test_deal_outside_mode_pipeline_is_not_taken(env):
    async def go():
        env.lead(pipeline=PIPE_RETAIL)                      # режим test, сделка в рознице
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
    run(go())


def test_contact_outside_list_is_not_taken(env):
    async def go():
        # В списке запись без телефона: чат до amo доходит, а контакт сделки (999) в списке не значится.
        env.set_settings(contacts={"scope": "test", "restricted": True,
                                   "items": [{"contact_id": CONTACT, "phone_norm": "", "name": "Тест"}]})
        env.lead(contacts=(999,))
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
        assert store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_chat_whose_phone_is_not_in_list_never_reaches_amo(env):
    async def go():
        looked = []

        async def find(q, with_=(), limit=50):
            looked.append(q)
            return []

        env.mp.setattr(amo_service, "find_leads_by_query_all", find)
        m = env.msg("слово")
        m["contact"]["phone"] = "79990000000"
        m["chatId"] = "777"
        await env.say(m)
        await env.advance(120)
        assert looked == [] and store.get_session(f"{CHAN}:777") is None
    run(go())


def test_empty_restricted_list_means_nobody(env):
    env.set_settings(contacts={"scope": "test", "restricted": True, "items": []})
    assert settings.phone_may_match(PHONE) is False
    assert settings.contact_allowed([CONTACT], [PHONE]) is False
    env.set_settings(contacts={"scope": "pilot", "restricted": False, "items": []})
    assert settings.contact_allowed([CONTACT], [PHONE]) is True


def test_channel_outside_list_is_ignored(env):
    async def go():
        env.lead()
        m = env.msg("слово")
        m["channelId"] = "other-channel"
        await env.say(m)
        await env.advance(120)
        assert env.turn_requests == []
    run(go())


# =============================================================================================
# дедуп сделок
# =============================================================================================

def test_rule_D_several_empty_deals_go_to_human(env):
    async def go():
        env.lead(DEAL)
        env.lead(DEAL + 1)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
        assert store.get_session(CHAT_KEY)["state"] == "human"
        assert any("несколько подходящих сделок" in a for a in env.alerts)
    run(go())


def test_rule_A_parallel_deal_with_order_goes_to_human(env):
    async def go():
        env.lead(DEAL)
        env.lead(DEAL + 7, pipeline=PIPE_OFFICE, price=19980)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == []
        assert store.get_owner(DEAL)["owner"] == "human"
        assert any("другая сделка с заказом" in a for a in env.alerts)
    run(go())


def test_rule_A_order_number_field_counts_as_work(env):
    async def go():
        env.lead(DEAL)
        env.lead(DEAL + 7, pipeline=PIPE_OFFICE, fields={576697: "09058"})
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "human"
    run(go())


def test_closed_other_deal_does_not_block(env):
    async def go():
        env.lead(DEAL)
        env.lead(DEAL + 7, pipeline=PIPE_OFFICE, price=19980, status=142)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert len(env.turn_requests) == 1
    run(go())


def test_no_deal_waits_then_gives_up_with_alert_for_restricted_list(env):
    async def go():
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_session(CHAT_KEY)["state"] == "collecting"
        env.lead()                                            # сделка появилась через 3 минуты
        await env.advance(180)
        assert len(env.turn_requests) == 1
    run(go())


def test_deal_never_appears_gives_up_after_timeout(env):
    async def go():
        await env.say(env.msg("слово"))
        for _ in range(12):
            await env.advance(60)
        assert env.turn_requests == []
        assert store.get_session(CHAT_KEY)["state"] == "skip"
        assert any("сделка клиента так и не появилась" in a for a in env.alerts)
    run(go())


def test_amo_silence_is_not_no_deal_and_is_retried(env):
    async def go():
        env.lead()
        env.amo_down = True
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_session(CHAT_KEY)["state"] == "collecting"
        env.amo_down = False
        await env.advance(61)
        assert len(env.turn_requests) == 1
    run(go())


def test_rule_C_new_deal_with_order_stops_ai_and_notes_the_chat_deal(env):
    async def go():
        turn = await env.start_turn()
        env.lead(DEAL + 9, pipeline=PIPE_OFFICE, price=24990, contacts=(CONTACT,))
        ev = E.extract_lead_event(DEAL + 9, PIPE_OFFICE, 1000, {"leads": {"add": {"0": {"id": str(DEAL + 9)}}}})
        await E._handle_lead_event(ev)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        await asyncio.sleep(0.05)
        assert env.cancels == [turn["turn_id"]]
        assert env.notes and env.notes[0][0] == DEAL and "ИИ остановлен" in env.notes[0][1]
        assert any("ИИ остановлен" in a for a in env.alerts)
    run(go())


def test_rule_C_ignores_empty_new_deal_and_other_contacts(env):
    async def go():
        await env.start_turn()
        env.lead(DEAL + 9, pipeline=PIPE_OFFICE, price=0)
        await E._handle_lead_event(E.extract_lead_event(DEAL + 9, PIPE_OFFICE, 1000, {"leads": {"add": {"0": {"id": "1"}}}}))
        env.lead(DEAL + 10, pipeline=PIPE_OFFICE, price=500, contacts=(12345,))
        await E._handle_lead_event(E.extract_lead_event(DEAL + 10, PIPE_OFFICE, 1000, {"leads": {"add": {"0": {"id": "1"}}}}))
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_rule_C_not_checked_when_ai_owns_nothing(env):
    async def go():
        calls = []

        async def get_full(lead_id, with_=()):
            calls.append(lead_id)
            return None

        env.mp.setattr(amo_service, "get_lead_full", get_full)
        await E._handle_lead_event(E.extract_lead_event(5, PIPE_OFFICE, 1000, {"leads": {"add": {"0": {"id": "5"}}}}))
        assert calls == []                                    # вебхук приходит на любую сделку: без ИИ amo не трогаем
    run(go())


def test_autopilot_chat_is_left_alone(env):
    async def go():
        env.lead()
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: True)
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
    run(go())


def test_human_owned_deal_is_not_retaken(env):
    async def go():
        env.lead()
        store.set_owner(DEAL, "human", reason="manager")
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL)["owner"] == "human"
    run(go())


# =============================================================================================
# permit
# =============================================================================================

def test_permit_allowed_then_newer_client_message(env):
    async def go():
        turn = await env.start_turn()
        args = (DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert E.decide_permit(*args) == (200, {"allowed": True})
        await env.say(env.msg("ещё одно", mid="late"))
        assert E.decide_permit(*args) == (200, {"allowed": False, "reason": "newer_client_message"})
    run(go())


def test_permit_stale_epoch_closed_and_mode_off(env):
    async def go():
        turn = await env.start_turn()
        assert E.decide_permit(DEAL, 7, turn["turn_id"], "x")[0] == 409
        env.set_settings(mode="off", pipelines=[])
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "mode_off"
        env.set_settings()
        store.mark_closed(DEAL)
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[1]["reason"] == "deal_closed"
        assert E.decide_permit(777, 1, 1, "x")[1]["reason"] == "owner_changed"
    run(go())


# =============================================================================================
# result
# =============================================================================================

def test_result_continue_waits_for_next_client_message(env):
    async def go():
        turn = await env.start_turn()
        code, body = await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        assert (code, body) == (200, {"ok": True})
        assert store.get_session(CHAT_KEY)["state"] == "ai"
        code, body = await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        assert code == 200 and body.get("duplicate") is True        # повтор итога ничего не двигает
    run(go())


def test_result_with_old_epoch_is_rejected(env):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("Беру", echo=True, author="Иванов Иван"))
        code, body = await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        assert code == 409 and body["detail"] == "stale_epoch"
        assert store.get_owner(DEAL)["owner"] == "human"
    run(go())


def test_result_handoff_to_human(env):
    async def go():
        turn = await env.start_turn()
        code, _ = await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "просьба позвонить", None)
        assert code == 200
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        assert any("ИИ передал диалог менеджеру" in a for a in env.alerts)
        await env.say(env.msg("ещё", mid="after"))
        await env.advance(120)
        assert len(env.turn_requests) == 1                   # у человека ИИ молчит
    run(go())


def test_result_release_to_engine_after_sale_closes_the_chat_session(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "release_to_engine", "sale_completed", None)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["closed"]) == ("engine", 2, 1)
        assert store.get_session(CHAT_KEY) is None
        assert E.decide_permit(DEAL, 1, turn["turn_id"], "x")[1]["reason"] == "deal_closed"
    run(go())


def test_result_release_to_engine_plain_stays_released(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "release_to_engine", "back_to_scenario", None)
        assert store.get_owner(DEAL)["owner"] == "engine"
        assert store.get_session(CHAT_KEY)["state"] == "released"
        await env.say(env.msg("опять", mid="again"))
        await env.advance(120)
        assert len(env.turn_requests) == 1
    run(go())


def test_result_aborted_newer_message_replans_same_messages(env):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("ещё", mid="late"))
        await E.handle_result(turn["turn_id"], DEAL, 1, "aborted", "newer_client_message", None)
        await env.advance(11)
        assert len(env.turn_requests) == 2
        ids = [m["message_id"] for m in env.turn_requests[1]["messages"]]
        assert ids == [turn["messages"][0]["message_id"], "late"]
    run(go())


def test_result_aborted_mode_mismatch_goes_to_human(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "aborted", "mode_mismatch", None)
        assert store.get_owner(DEAL)["owner"] == "human"
        assert any("воронка не совпала" in a for a in env.alerts)
    run(go())


def test_result_failed_retries_after_30_and_120_seconds_then_human(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "failed", "model_error", None)
        await env.advance(29)
        assert len(env.turn_requests) == 1
        await env.advance(2)
        assert len(env.turn_requests) == 2 and env.turn_requests[1]["reason"] == "retry"
        assert env.turn_requests[1]["messages"] == turn["messages"]
        await E.handle_result(env.turn_requests[1]["turn_id"], DEAL, 1, "failed", "model_error", None)
        await env.advance(119)
        assert len(env.turn_requests) == 2
        await env.advance(2)
        assert len(env.turn_requests) == 3
        await E.handle_result(env.turn_requests[2]["turn_id"], DEAL, 1, "failed", "model_error", None)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        assert any("после трёх попыток" in a for a in env.alerts)
    run(go())


def test_turn_without_result_is_closed_without_retry_and_alerts(env):
    """Потерянный итог чаще всего значит, что ИИ уже ответил: повтор написал бы клиенту второй раз."""
    async def go():
        turn = await env.start_turn()
        env.t += cfg.TURN_TIMEOUT_S + 5
        await E._expire_turns()
        assert store.get_turn(turn["turn_id"])["outcome"] == "timeout"
        await env.advance(300)
        assert len(env.turn_requests) == 1                    # никакого повтора
        assert store.get_owner(DEAL)["owner"] == "ai" and store.get_session(CHAT_KEY)["state"] == "ai"
        assert any("не прислал итог" in a for a in env.alerts)
        await env.say(env.msg("новое", mid="n1"))
        await env.advance(11)
        assert len(env.turn_requests) == 2                    # а следующее сообщение клиента идёт в работу
    run(go())


def test_unknown_turn_result_is_409(env):
    async def go():
        env.lead()
        code, body = await E.handle_result(424242, DEAL, 1, "continue", None, None)
        assert code == 409
    run(go())


# =============================================================================================
# служебные события
# =============================================================================================

def _paid_event(lead_id=DEAL, pipeline=PIPE_TEST):
    nested = {"leads": {"update": {"0": {"id": str(lead_id), "custom_fields": {
        "0": {"id": "576669", "values": {"0": {"value": "Оплачен", "enum": "1040151"}}}}}}}}
    return E.extract_lead_event(lead_id, pipeline, 1000, nested)


def test_payment_received_event_in_test_pipeline_by_paystatus(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        await E._handle_lead_event(_paid_event())
        await env.advance(1)
        assert len(env.turn_requests) == 2
        ev_turn = env.turn_requests[1]
        assert ev_turn["reason"] == "event" and ev_turn["messages"] == []
        assert ev_turn["event"]["type"] == "payment_received" and ev_turn["event"]["at"].endswith("Z")
        assert ev_turn["up_to_message_id"] == turn["up_to_message_id"]
        # повтор того же вебхука события не плодит
        await E.handle_result(ev_turn["turn_id"], DEAL, 1, "continue", None, None)
        await E._handle_lead_event(_paid_event())
        await env.advance(5)
        assert len(env.turn_requests) == 2
    run(go())


def test_payment_received_event_in_retail_by_stage(env):
    async def go():
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, 9822330],
                         contacts={"scope": "pilot", "restricted": False, "items": []})
        env.lead(pipeline=PIPE_RETAIL)
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        ev = E.extract_lead_event(DEAL, PIPE_RETAIL, 83537874, {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        await E._handle_lead_event(ev)
        await env.advance(1)
        assert env.turn_requests[-1]["event"]["type"] == "payment_received"
        assert env.turn_requests[-1]["mode"] == "prod"
    run(go())


def test_paystatus_in_retail_pipeline_does_not_fire(env):
    async def go():
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, 9822330],
                         contacts={"scope": "pilot", "restricted": False, "items": []})
        env.lead(pipeline=PIPE_RETAIL)
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        await E._handle_lead_event(_paid_event(pipeline=PIPE_RETAIL))
        await env.advance(5)
        assert len(env.turn_requests) == 1                    # поле оплаты правит автоматика: событие там по этапу
    run(go())


def test_payment_event_needs_ai_owner_and_mode(env):
    async def go():
        env.lead()
        store.set_owner(DEAL, "human", reason="x", pipeline_id=PIPE_TEST)
        await E._handle_lead_event(_paid_event())
        await env.advance(5)
        assert env.turn_requests == []
        turn_owner = store.set_owner(DEAL, "ai", reason="x")
        env.set_settings(mode="off", pipelines=[])
        await E._handle_lead_event(_paid_event())
        assert store.claim_event(DEAL, "payment_received") is True   # режим off: событие даже не засчитано
    run(go())


def test_payment_event_waits_for_running_turn(env):
    async def go():
        turn = await env.start_turn()
        await E._handle_lead_event(_paid_event())
        await env.advance(30)
        assert len(env.turn_requests) == 1                    # ход идёт: событие ждёт итога
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        await env.advance(1)
        assert len(env.turn_requests) == 2 and env.turn_requests[1]["event"]["type"] == "payment_received"
    run(go())


def test_event_turn_survives_failure_and_is_retried_as_event(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        await E._handle_lead_event(_paid_event())
        await env.advance(1)
        ev_turn = env.turn_requests[-1]
        await E.handle_result(ev_turn["turn_id"], DEAL, 1, "failed", "model_error", None)
        await env.advance(31)
        assert len(env.turn_requests) == 3
        again = env.turn_requests[-1]
        assert again["reason"] == "event" and again["event"]["type"] == "payment_received"
        # и при «клиент написал ещё» событие тоже не теряется
        await E.handle_result(again["turn_id"], DEAL, 1, "aborted", "newer_client_message", None)
        await env.advance(2)
        assert env.turn_requests[-1]["event"]["type"] == "payment_received"
    run(go())


def test_dialog_returned_event(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "x", None)
        assert store.get_owner(DEAL)["owner"] == "human"
        code, body = await E.return_dialog(DEAL)
        assert code == 200 and body["epoch"] == 3
        await env.advance(1)
        ev_turn = env.turn_requests[-1]
        assert ev_turn["reason"] == "event" and ev_turn["event"]["type"] == "dialog_returned"
        assert ev_turn["epoch"] == 3 and ev_turn["messages"] == []
    run(go())


def test_dialog_return_refused_when_off_or_service_down(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "x", None)
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        code, body = await E.return_dialog(DEAL)
        assert code == 503
        env.health_ok = True
        env.set_settings(mode="off", pipelines=[])
        code, body = await E.return_dialog(DEAL)
        assert code == 409 and body["detail"] == "mode_off"
        env.set_settings()
        assert (await E.return_dialog(777))[0] == 409
    run(go())


def test_closed_deal_event_drops_ownership_and_cancels_turn(env):
    async def go():
        turn = await env.start_turn()
        ev = E.extract_lead_event(DEAL, PIPE_TEST, 143, {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        await E._handle_lead_event(ev)
        owner = store.get_owner(DEAL)
        assert owner["closed"] == 1 and owner["owner"] == "engine"
        await asyncio.sleep(0.05)
        assert env.cancels == [turn["turn_id"]]
        assert store.get_session(CHAT_KEY) is None
    run(go())


def test_deal_leaving_the_mode_pipeline_counts_as_closed(env):
    async def go():
        await env.start_turn()
        ev = E.extract_lead_event(DEAL, PIPE_OFFICE, 1000, {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        await E._handle_lead_event(ev)
        assert store.get_owner(DEAL)["closed"] == 1
    run(go())


# =============================================================================================
# очередь к сервису, здоровье
# =============================================================================================

def test_send_retries_1_5_25_then_backlog_then_resend_after_recovery(env):
    async def go():
        env.lead()
        env.service_turn_error = True
        await env.say(env.msg("слово"))
        await env.advance(61)                                # первая попытка, не вышло
        turn_id = store.active_turn_for_deal(DEAL)["turn_id"]
        assert store.get_turn(turn_id)["send_attempts"] == 1
        await env.advance(1.5)
        assert store.get_turn(turn_id)["send_attempts"] == 2
        await env.advance(5.5)
        assert store.get_turn(turn_id)["send_attempts"] == 3
        await env.advance(25.5)
        assert store.get_turn(turn_id)["state"] == "backlog"
        env.service_turn_error = False
        await E._probe_health()
        await E._on_health_state()
        await env.advance(1)
        assert store.get_turn(turn_id)["state"] == "accepted"
        assert [r["turn_id"] for r in env.turn_requests] == [turn_id]
    run(go())


def test_service_refusal_goes_to_human_and_alerts(env):
    async def go():
        env.lead()
        env.service_turn_status = 403
        await env.say(env.msg("слово"))
        await env.advance(61)
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "human"
        assert any("не принял ход" in a for a in env.alerts)
    run(go())


def test_service_duplicate_turn_counts_as_accepted(env):
    async def go():
        env.lead()
        env.service_turn_status = 200
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert store.active_turn_for_deal(DEAL)["state"] == "accepted"
    run(go())


def test_turn_for_changed_owner_is_not_sent(env):
    async def go():
        env.lead()
        env.service_turn_error = True
        await env.say(env.msg("слово"))
        await env.advance(61)
        await env.say(env.msg("Беру", echo=True, author="Иванов Иван"))
        env.service_turn_error = False
        await env.advance(60)
        assert env.turn_requests == []                        # запоздавший ход старой эпохи сервису не уходит
    run(go())


def test_entry_when_ai_service_is_down_leaves_dialog_to_managers(env):
    async def go():
        env.lead()
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        await env.say(env.msg("слово"))
        await env.advance(61)
        assert env.turn_requests == [] and store.get_owner(DEAL) is None
        assert any("ИИ-сервис не отвечает" in a for a in env.alerts)
    run(go())


def test_ai_down_longer_than_release_gives_deals_to_humans_once(env):
    async def go():
        turn = await env.start_turn()
        env.health_ok = False
        await E._probe_health()
        await E._on_health_state()
        assert store.get_owner(DEAL)["owner"] == "ai"        # молчит недолго: ждём
        env.t += 130
        await E._probe_health()
        await E._on_health_state()
        await E._on_health_state()
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"]) == ("human", 2)
        assert sum("ИИ-сервис не отвечает дольше" in a for a in env.alerts) == 1
        # очередь после подъёма отбрасывается по эпохе: запоздавшее разрешение отказывает
        assert E.decide_permit(DEAL, 1, turn["turn_id"], turn["up_to_message_id"])[0] == 409
    run(go())


def test_mode_switched_off_returns_ai_deals_to_humans(env):
    async def go():
        await env.start_turn()
        env.set_settings(mode="off", pipelines=[])
        await E._sweep_if_off()
        assert store.get_owner(DEAL)["owner"] == "human"
        assert any("ИИ-менеджер выключен" in a for a in env.alerts)
    run(go())


def test_unreachable_panel_at_boot_does_not_dump_deals(env):
    async def go():
        await env.start_turn()
        settings.set_document(None)
        await E._sweep_if_off()
        assert store.get_owner(DEAL)["owner"] == "ai"         # настроек ещё нет: это не «выключили»
    run(go())


# =============================================================================================
# алерт «сделка так и не появилась»: только для чатов из списка режима (боевой тест 28.10.2026)
# =============================================================================================

TEST_PHONE_ITEM = {"contact_id": 48595431, "phone_norm": "79956109902", "name": "Тиана"}
NO_PHONE_ITEM = {"contact_id": 555, "phone_norm": "", "name": "Без телефона"}


def _chat_msg(env, chat_id, phone, mid):
    m = env.msg("слово", mid=mid)
    name = "Тиана" if mid == "own" else "Чужой клиент"
    m["chatId"], m["contact"] = chat_id, {"name": name, "phone": phone}
    return m


async def _wait_out_resolving(env):
    for _ in range(12):
        await env.advance(60)


def test_deal_not_found_alert_goes_only_to_chats_from_the_list_by_phone(env):
    async def go():
        # в списке запись с телефоном и запись без телефона: до amo доходят ВСЕ чаты (телефон второй неизвестен)
        env.set_settings(contacts={"scope": "test", "restricted": True, "items": [TEST_PHONE_ITEM, NO_PHONE_ITEM]})
        await env.say(_chat_msg(env, "1920391385", "79956109902", "own"))          # телеграм из списка
        await env.say(_chat_msg(env, "5550001", "79261112233", "stranger"))        # живой клиент канала
        await _wait_out_resolving(env)
        texts = "\n".join(env.alerts)
        assert len(env.alerts) == 1 and "Тиана" in texts and "Чужой клиент" not in texts
        assert store.get_session(f"{CHAN}:1920391385")["state"] == "skip"
        assert store.get_session(f"{CHAN}:5550001")["state"] == "skip"             # ИИ по нему не работает, как и задумано
        assert env.turn_requests == []
    run(go())


def test_deal_not_found_alert_silent_for_stranger_even_when_no_list_item_has_a_phone(env):
    async def go():
        env.set_settings(contacts={"scope": "test", "restricted": True, "items": [NO_PHONE_ITEM]})
        await env.say(_chat_msg(env, "5550001", "79261112233", "stranger"))
        await _wait_out_resolving(env)
        assert env.alerts == []
    run(go())


def test_deal_not_found_alert_matches_whatsapp_chat_id_with_the_phone(env):
    async def go():
        env.set_settings(contacts={"scope": "test", "restricted": True, "items": [TEST_PHONE_ITEM, NO_PHONE_ITEM]})
        await env.say(_chat_msg(env, "79956109902", "", "wa"))                     # WhatsApp: chat_id и есть телефон
        await _wait_out_resolving(env)
        assert len(env.alerts) == 1
    run(go())


def test_deal_not_found_alert_pilot_list_works_the_same(env):
    async def go():
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, 9822330],
                         contacts={"scope": "pilot", "restricted": True, "items": [TEST_PHONE_ITEM, NO_PHONE_ITEM]})
        await env.say(_chat_msg(env, "1920391385", "79956109902", "own"))
        await env.say(_chat_msg(env, "5550001", "79261112233", "stranger"))
        await _wait_out_resolving(env)
        assert len(env.alerts) == 1
    run(go())


def test_deal_not_found_silent_in_open_prod(env):
    async def go():
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, 9822330],
                         contacts={"scope": "pilot", "restricted": False, "items": []})
        await env.say(_chat_msg(env, "1920391385", "79956109902", "own"))
        await _wait_out_resolving(env)
        assert env.alerts == []
    run(go())


def test_phone_helpers_on_the_real_numbers_from_the_live_test():
    env_doc = {"mode": "test", "pipelines": [PIPE_TEST],
               "contacts": {"scope": "test", "restricted": True, "items": [TEST_PHONE_ITEM]}}
    settings.set_document(env_doc)
    assert settings.phone_key("79956109902") == "79956109902"
    assert settings.phone_key("+7 (995) 610-99-02") == "79956109902"
    assert settings.phone_key("89956109902") == "79956109902"
    assert settings._same_phone("79956109902", "89956109902"[-11:].replace("8", "7", 1))
    assert settings.contact_allowed([], ["79956109902", "1920391385"]) is True
    assert settings.contact_allowed([], ["79261112233", "5550001"]) is False
    assert settings.contact_allowed([], ["", "1920391385"]) is False        # telegram chat_id 1920391385 - не телефон из списка
