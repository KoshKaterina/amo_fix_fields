"""Журнал приёма движка ИИ-менеджера (`intake`): тело вебхука ложится на диск ДО ответа 200 и разбирается после рестарта.

Проверяем то, ошибка в чём означает потерянное сообщение клиента при том, что Wazzup уже получил 200 и повторов не
пришлёт:

  • сообщение принято, процесс «упал» до разбора, после рестарта ход создан;
  • повторный разбор той же строки не плодит ход (и не теряет таймер, если прошлый разбор оборвался на полпути);
  • переполнение очереди в памяти строк не теряет; сбой записи в базу вебхук не ломает;
  • события сделки (оплата, закрытие) тоже переживают рестарт;
  • порядок воспроизведения, правило устаревших строк, выключенный флаг и режим `off`, чистка, срез `status()`.

«Падение» здесь - новый пустой `_queue` и новый `_started_at`, без запуска `_ingest_loop`: остаётся только база.
Сети нет (мир подставной, см. `Env` в test_ai_engine.py). Запуск: python -m pytest test_ai_engine_intake.py -q
"""
import asyncio
import json
import sqlite3
import time

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_return
import ai_engine_store as store
from test_ai_engine import CHAN, CHAT, CHAT_KEY, DEAL, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from waybill_config import STATUS_SUCCESS

HOUR = 3600


def boot(env, mp, maxsize=2000):
    """Новый «процесс»: пустая память, время запуска позже всего принятого. Остаётся только база."""
    env.t += 1
    mp.setattr(E, "_queue", asyncio.Queue(maxsize=maxsize))
    mp.setattr(E, "_started_at", env.t)
    mp.setattr(E, "_replayed", None)


def rows():
    with sqlite3.connect(cfg.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        out = [dict(r) for r in conn.execute("SELECT * FROM intake ORDER BY id")]
    return out


def reopen():
    """Как будто процесс упал между «разобрано» и «отмечено»."""
    with sqlite3.connect(cfg.DB_PATH) as conn:
        conn.execute("UPDATE intake SET done_at=NULL, note=''")


def paid_nested(lead_id=DEAL):
    return {"leads": {"update": {"0": {"id": str(lead_id), "custom_fields": {
        "0": {"id": "576669", "values": {"0": {"value": "Оплачен", "enum": "1040151"}}}}}}}}


async def until(cond, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        assert loop.time() < end, "не дождались"
        await asyncio.sleep(0.02)


# =============================================================================================
# сообщение клиента переживает рестарт
# =============================================================================================

def test_message_is_on_disk_before_on_wazzup_returns(env, monkeypatch):
    async def go():
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("привет", mid="m1")], "statuses": [{"x": 1}]})
        [row] = rows()
        assert row["kind"] == "wazzup" and row["done_at"] is None
        assert json.loads(row["payload"])["messages"][0]["messageId"] == "m1"
        assert "statuses" not in json.loads(row["payload"])        # храним только то, что разбирает движок
        assert E._queue.qsize() == 1                               # жетон «в журнале новое»
    run(go())


def test_crash_before_parsing_then_restart_creates_the_turn(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("помогите с заказом", mid="m-lost")]})
        assert store.get_session(CHAT_KEY) is None                 # разбор не начинался
        # процесс упал: память пропала, цикл приёма не запускался
        boot(env, monkeypatch)
        assert await E.drain_intake() is True
        assert store.get_session(CHAT_KEY)["state"] == "collecting"
        [row] = rows()
        assert row["done_at"] is not None and row["note"] == "ok"
        await env.advance(61)
        assert len(env.turn_requests) == 1
        turn = env.turn_requests[0]
        assert turn["reason"] == "first_message" and turn["deal_id"] == DEAL
        assert [m["message_id"] for m in turn["messages"]] == ["m-lost"]
    run(go())


def test_attachment_survives_restart_and_reaches_the_ai_as_a_note(env, monkeypatch):
    """Стык с правкой «вложения идут ИИ строкой-пометкой» (81a7ddd): картинка с подписью тоже переживает рестарт."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("вот чек", mid="m-img", typ="image")]})
        boot(env, monkeypatch)
        await E.drain_intake()
        await env.advance(61)
        [turn] = env.turn_requests
        assert [m["message_id"] for m in turn["messages"]] == ["m-img"]
        assert "Клиент прислал картинку" in turn["messages"][0]["text"] and "вот чек" in turn["messages"][0]["text"]
    run(go())


def test_timers_count_from_receipt_not_from_replay(env, monkeypatch):
    """Клиент уже ждал 100 секунд: первое ожидание (60 с) не начинается заново, ход идёт на первом же тике."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("алло", mid="m-wait")]})
        env.t += 100
        boot(env, monkeypatch)
        await E.drain_intake()
        await env.advance(0)
        assert len(env.turn_requests) == 1
        assert store.get_session(CHAT_KEY)["resolve_deadline"] > env.t      # срок поиска сделки - от разбора
    run(go())


def test_replaying_the_same_row_twice_makes_one_turn(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("раз", mid="m-once")]})
        await E.drain_intake()
        await env.advance(61)
        assert len(env.turn_requests) == 1
        # строку не успели отметить: после рестарта она разберётся ещё раз
        reopen()
        boot(env, monkeypatch)
        await E.drain_intake()
        await env.advance(200)
        assert len(env.turn_requests) == 1                         # сообщение уже в ходе: повтор ничего не делает
        assert len(store.pending_inbox(CHAT_KEY)) == 0
        assert store.get_turn(env.turn_requests[0]["turn_id"])["state"] in ("accepted", "queued")
        assert rows()[0]["note"] == "ok"
    run(go())


def test_replay_of_unfinished_parse_keeps_the_chat_timer(env, monkeypatch):
    """Прошлый запуск успел положить сообщение в inbox, но не двинул таймер чата: повтор строки его двигает."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        msg = env.msg("половина", mid="m-half")
        E.on_wazzup({"messages": [msg]})
        store.add_inbox("m-half", CHAT_KEY, "половина", "text", "", env.t)          # разбор оборвался после этого
        assert store.get_session(CHAT_KEY) is None
        boot(env, monkeypatch)
        await E.drain_intake()
        assert store.get_session(CHAT_KEY)["state"] == "collecting"
        await env.advance(61)
        assert [m["message_id"] for m in env.turn_requests[0]["messages"]] == ["m-half"]
    run(go())


def test_live_duplicate_of_unconsumed_message_does_not_move_the_timer(env, monkeypatch):
    """Обычный повтор вебхука (не рестарт) по-прежнему молча отбрасывается: дебаунс не растягивается."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        msg = env.msg("дубль", mid="m-dup")
        E.on_wazzup({"messages": [msg]})
        await E.drain_intake()
        before = store.get_session(CHAT_KEY)["next_at"]
        env.t += 30
        E.on_wazzup({"messages": [msg]})                           # Wazzup повторил тот же вебхук
        await E.drain_intake()
        assert store.get_session(CHAT_KEY)["next_at"] == before
        assert [r["note"] for r in rows()] == ["ok", "ok"]
    run(go())


# =============================================================================================
# переполнение, сбои записи, потолок тела
# =============================================================================================

def test_queue_overflow_does_not_lose_rows(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch, maxsize=1)
        for i in range(5):
            E.on_wazzup({"messages": [env.msg(f"слово {i}", mid=f"m-{i}")]})
        assert E._queue.qsize() == 1                               # жетон один, остальные не поместились
        assert store.intake_stats()["pending"] == 5                # а строки все на месте
        await E.drain_intake()
        assert store.intake_stats()["pending"] == 0
        assert [m["message_id"] for m in store.pending_inbox(CHAT_KEY)] == [f"m-{i}" for i in range(5)]
    run(go())


def test_write_failure_does_not_break_the_webhook_and_falls_back_to_memory(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)

        def broken(*a, **k):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(store, "add_intake", broken)
        E.on_wazzup({"messages": [env.msg("диск болен", mid="m-mem")]})      # не бросает
        assert rows() == []
        kind, body = E._queue.get_nowait()
        assert kind == "wazzup"
        await E._handle_memory_item(kind, body)                    # старый путь: разбор из памяти
        assert [m["message_id"] for m in store.pending_inbox(CHAT_KEY)] == ["m-mem"]
        # то же для события сделки
        E.on_lead_change(DEAL, PIPE_TEST, 1000, {"leads": {"add": {"0": {"id": str(DEAL)}}}})
        kind, ev = E._queue.get_nowait()
        assert kind == "lead" and ev["lead_id"] == DEAL
    run(go())


def test_locked_database_does_not_freeze_the_webhook(env, monkeypatch):
    """База занята чужой записью: вебхук ждёт недолго и идёт запасным путём, а не стоит 10 секунд вместе со всем приложением."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        monkeypatch.setattr(store, "INTAKE_WRITE_TIMEOUT_S", 0.2)
        blocker = sqlite3.connect(cfg.DB_PATH)
        blocker.execute("BEGIN EXCLUSIVE")
        try:
            started = time.monotonic()
            E.on_wazzup({"messages": [env.msg("база занята", mid="m-lock")]})
            E.on_lead_change(DEAL, PIPE_TEST, 1000, {"leads": {"update": {"0": {"id": str(DEAL)}}}})      # неизвестная сделка
            assert time.monotonic() - started < 2
        finally:
            blocker.rollback()
            blocker.close()
        assert rows() == []
        kinds = [E._queue.get_nowait()[0] for _ in range(E._queue.qsize())]
        assert kinds[0] == "wazzup"
    run(go())


def test_full_memory_queue_and_failed_write_only_log(env, monkeypatch):
    async def go():
        boot(env, monkeypatch, maxsize=1)
        E._queue.put_nowait(E._WAKE)
        monkeypatch.setattr(store, "add_intake", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("нет диска")))
        E.on_wazzup({"messages": [env.msg("потеряется", mid="m-drop")]})    # не бросает даже тут
    run(go())


def test_oversized_body_is_not_stored_but_still_processed(env, monkeypatch):
    async def go():
        boot(env, monkeypatch)
        monkeypatch.setattr(cfg, "INTAKE_MAX_BODY_BYTES", 200)
        E.on_wazzup({"messages": [env.msg("я" * 500, mid="m-big")]})
        assert rows() == []
        kind, body = E._queue.get_nowait()
        assert kind == "wazzup" and body["messages"][0]["messageId"] == "m-big"
    run(go())


def test_only_our_channels_and_relevant_deals_are_written(env, monkeypatch):
    async def go():
        boot(env, monkeypatch)
        foreign = {**env.msg("чужой", mid="m-foreign"), "channelId": "other-channel"}
        no_chat = {**env.msg("без чата", mid="m-nochat"), "chatId": ""}
        E.on_wazzup({"messages": [foreign, no_chat]})
        assert rows() == [] and E._queue.empty()
        E.on_wazzup({"messages": [foreign, env.msg("наш", mid="m-ours")]})
        assert [m["messageId"] for m in json.loads(rows()[0]["payload"])["messages"]] == ["m-ours"]
        # событие по сделке, которой мы не знаем и которая не новая: разбор ничего бы не сделал
        E.on_lead_change(777, PIPE_TEST, 1000, {"leads": {"update": {"0": {"id": "777"}}}})
        assert len(rows()) == 1
        E.on_lead_change(777, PIPE_TEST, 1000, {"leads": {"add": {"0": {"id": "777"}}}})      # новая: правило C
        assert [r["kind"] for r in rows()] == ["wazzup", "lead"]
    run(go())


# =============================================================================================
# события сделки
# =============================================================================================

def test_payment_received_survives_restart_and_is_not_duplicated(env, monkeypatch):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        boot(env, monkeypatch)
        E.on_lead_change(DEAL, PIPE_TEST, 1000, paid_nested())
        assert [r["kind"] for r in rows()] == ["lead"]
        boot(env, monkeypatch)                                     # упали до разбора
        await E.drain_intake()
        await env.advance(1)
        assert len(env.turn_requests) == 2
        assert env.turn_requests[1]["reason"] == "event" and env.turn_requests[1]["event"]["type"] == "payment_received"
        # строку не успели отметить: повтор события второго хода не даёт (`claim_event`)
        await E.handle_result(env.turn_requests[1]["turn_id"], DEAL, 1, "continue", None, None)
        reopen()
        boot(env, monkeypatch)
        await E.drain_intake()
        await env.advance(5)
        assert len(env.turn_requests) == 2
    run(go())


def test_deal_closed_survives_restart_and_replay_is_harmless(env, monkeypatch):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        boot(env, monkeypatch)
        E.on_lead_change(DEAL, PIPE_TEST, int(STATUS_SUCCESS), {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        boot(env, monkeypatch)
        await E.drain_intake()
        assert store.get_owner(DEAL)["closed"] == 1
        reopen()
        boot(env, monkeypatch)
        await E.drain_intake()                                     # второй раз: без ошибок, ничего нового
        assert store.get_owner(DEAL)["closed"] == 1 and rows()[0]["note"] == "ok"
    run(go())


# =============================================================================================
# порядок и устаревшие строки
# =============================================================================================

def test_old_rows_are_replayed_in_id_order_before_new_ones(env, monkeypatch):
    async def go():
        env.lead()
        seen: list[str] = []
        real = E._handle_payload

        async def spy(payload, **kw):
            seen.extend(m["messageId"] for m in payload["messages"])
            await real(payload, **kw)

        monkeypatch.setattr(E, "_handle_payload", spy)
        boot(env, monkeypatch)
        for mid in ("m-1", "m-2", "m-3"):
            E.on_wazzup({"messages": [env.msg(mid, mid=mid)]})
        boot(env, monkeypatch)                                     # рестарт
        E.on_wazzup({"messages": [env.msg("новое", mid="m-4")]})   # новый вебхук уже после рестарта
        await E.drain_intake()
        assert seen == ["m-1", "m-2", "m-3", "m-4"]
        assert [r["id"] for r in rows()] == sorted(r["id"] for r in rows())
    run(go())


def test_manager_reply_replayed_after_client_message_keeps_the_dialog_human(env, monkeypatch):
    """Порядок важен: менеджер ответил позже клиента, значит ИИ ход не получает."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("вопрос", mid="m-q")]})
        E.on_wazzup({"messages": [env.msg("Добрый день!", mid="m-a", echo=True, author="Иван Иванов")]})
        boot(env, monkeypatch)
        await E.drain_intake()
        assert store.get_session(CHAT_KEY)["state"] == "human"
        await env.advance(300)
        assert env.turn_requests == []
    run(go())


def test_tick_loop_waits_for_the_first_replay(env, monkeypatch):
    async def go():
        monkeypatch.setattr(E, "_replayed", asyncio.Event())
        monkeypatch.setattr(cfg, "TICK_S", 0.01)
        calls = []

        async def fake_tick():
            calls.append(1)

        monkeypatch.setattr(E, "tick_once", fake_tick)
        task = asyncio.create_task(E._tick_loop())
        await asyncio.sleep(0.1)
        assert calls == []                                         # журнал ещё не разобран
        E._replayed.set()
        await until(lambda: calls)
        task.cancel()
    run(go())


def test_stale_message_is_not_replayed_but_the_team_is_told(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("давно", mid="m-old")]})
        env.t += cfg.INTAKE_STALE_WAZZUP_S + 1
        boot(env, monkeypatch)
        await E.drain_intake()
        [row] = rows()
        assert row["note"] == "stale" and row["done_at"] is not None
        assert store.get_session(CHAT_KEY) is None and store.pending_inbox(CHAT_KEY) == []
        await env.advance(300)
        assert env.turn_requests == []
        assert any("необработанные входящие" in a for a in env.alerts)
        assert "давно" not in " ".join(env.alerts)                 # текстов клиентов в алерте нет
    run(go())


def test_message_just_inside_the_age_limit_is_replayed(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("почти давно", mid="m-edge")]})
        env.t += cfg.INTAKE_STALE_WAZZUP_S - 60
        boot(env, monkeypatch)
        await E.drain_intake()
        assert rows()[0]["note"] == "ok"
        await env.advance(0)
        assert len(env.turn_requests) == 1
        assert env.alerts == []
    run(go())


def test_deal_events_live_longer_than_messages(env, monkeypatch):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        boot(env, monkeypatch)
        E.on_lead_change(DEAL, PIPE_TEST, int(STATUS_SUCCESS), {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        env.t += 5 * HOUR                                          # для сообщения это уже поздно, для закрытия нет
        boot(env, monkeypatch)
        await E.drain_intake()
        assert rows()[0]["note"] == "ok" and store.get_owner(DEAL)["closed"] == 1
    run(go())


def test_very_old_deal_event_is_dropped(env, monkeypatch):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "continue", None, None)
        boot(env, monkeypatch)
        E.on_lead_change(DEAL, PIPE_TEST, int(STATUS_SUCCESS), {"leads": {"update": {"0": {"id": str(DEAL)}}}})
        env.t += cfg.INTAKE_STALE_LEAD_S + 1
        boot(env, monkeypatch)
        await E.drain_intake()
        assert rows()[0]["note"] == "stale" and store.get_owner(DEAL)["closed"] == 0
        assert env.alerts == []                                    # про события алерт не шлём: чата для проверки нет
    run(go())


# =============================================================================================
# выключено
# =============================================================================================

def test_flag_off_writes_nothing_and_creates_no_database(tmp_path, monkeypatch):
    path = tmp_path / "never.sqlite3"
    monkeypatch.setattr(cfg, "ENABLED", False)
    monkeypatch.setattr(cfg, "DB_PATH", str(path))
    monkeypatch.setattr(E, "_queue", asyncio.Queue())
    E.on_wazzup({"messages": [{"channelId": CHAN, "chatId": CHAT, "text": "привет"}]})
    E.on_lead_change(DEAL, PIPE_TEST, 1000, {"leads": {"add": {"0": {"id": "1"}}}})
    assert E._queue.empty() and not path.exists()


def test_mode_off_writes_nothing(env, monkeypatch):
    async def go():
        boot(env, monkeypatch)
        env.set_settings(mode="off", pipelines=[])
        E.on_wazzup({"messages": [env.msg("слово", mid="m-off")]})
        E.on_lead_change(DEAL, PIPE_TEST, 1000, {"leads": {"add": {"0": {"id": str(DEAL)}}}})
        assert rows() == [] and E._queue.empty()
    run(go())


def test_rows_wait_for_panel_settings_just_after_start_then_are_dropped_if_still_off(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("до панели", mid="m-early")]})
        boot(env, monkeypatch)
        env.set_settings(mode="off", pipelines=[])                 # панель ещё не ответила / режим off
        assert await E.drain_intake() is False                     # окно ожидания: строка не сбрасывается
        assert rows()[0]["done_at"] is None
        env.set_settings()                                         # панель ответила: режим test
        assert await E.drain_intake() is True
        assert rows()[0]["note"] == "ok" and store.get_session(CHAT_KEY) is not None
        # режим off остался после окна ожидания: строки закрываются, как раньше закрылся бы разбор в режиме off
        E.on_wazzup({"messages": [env.msg("ещё", mid="m-late")]})
        env.set_settings(mode="off", pipelines=[])
        env.t += cfg.INTAKE_SETTINGS_WAIT_S + 1
        assert await E.drain_intake() is True
        assert [r["note"] for r in rows()] == ["ok", "mode_off"]
        assert all(m["message_id"] != "m-late" for m in store.pending_inbox(CHAT_KEY))
    run(go())


# =============================================================================================
# уборка и срез состояния
# =============================================================================================

def test_done_rows_are_pruned_after_a_day_open_rows_are_kept(env):
    old = store.add_intake("wazzup", "{}", 1.0)
    fresh = store.add_intake("wazzup", "{}", 2.0)
    open_ = store.add_intake("wazzup", "{}", 3.0)
    store.finish_intake(old, "ok", env.t - 2 * 86400)
    store.finish_intake(fresh, "ok", env.t - 100)
    assert store.prune_intake(cfg.INTAKE_KEEP_DONE_S, env.t) == 1
    assert {r["id"] for r in rows()} == {fresh, open_}


def test_hourly_housekeeping_prunes_intake(env):
    async def go():
        old = store.add_intake("wazzup", "{}", env.t)
        store.finish_intake(old, "ok", env.t - 2 * 86400)
        await env.advance(HOUR + 1)
        assert rows() == []
    run(go())


def test_status_shows_pending_count_without_texts(env, monkeypatch):
    async def go():
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("секретный текст клиента", mid="m-s")]})
        env.t += 42
        st = E.status()
        assert st["counts"]["intake"] == {"pending": 1, "oldest_age_s": 42}
        assert "секретный" not in json.dumps(st, ensure_ascii=False)
    run(go())


# =============================================================================================
# настоящий init() и настоящий цикл приёма
# =============================================================================================

def test_init_replays_the_journal_before_anything_new_and_the_turn_reaches_the_service(env, monkeypatch):
    async def go():
        env.lead()
        # прошлый запуск: принял и упал
        monkeypatch.setattr(E, "_queue", asyncio.Queue())
        E.on_wazzup({"messages": [env.msg("помогите", mid="m-crash")]})
        env.t += 5
        # новый запуск: настоящий init с настоящими циклами
        for name, value in (("_queue", None), ("_tasks", []), ("_replayed", None), ("_started_at", 0.0)):
            monkeypatch.setattr(E, name, value)
        monkeypatch.setattr(ai_engine_return, "_return_fn", None)
        monkeypatch.setattr(cfg, "TICK_S", 0.02)
        monkeypatch.setattr(cfg, "INTAKE_POLL_S", 0.05)
        await E.init()
        try:
            E.on_wazzup({"messages": [env.msg("и ещё", mid="m-new")]})       # новый вебхук уже после старта
            await until(lambda: store.intake_stats()["pending"] == 0)
            assert [r["note"] for r in rows()] == ["ok", "ok"]
            env.t += 61
            await until(lambda: env.turn_requests)
        finally:
            await E.shutdown()
        assert [m["message_id"] for m in env.turn_requests[0]["messages"]] == ["m-crash", "m-new"]
    run(go())


def test_loop_without_a_wake_token_still_picks_up_rows(env, monkeypatch):
    """Очередь была полна и жетон не встал: цикл заглядывает в журнал сам."""
    async def go():
        env.lead()
        boot(env, monkeypatch, maxsize=1)
        monkeypatch.setattr(cfg, "INTAKE_POLL_S", 0.05)
        E._queue.put_nowait(E._WAKE)
        task = asyncio.create_task(E._ingest_loop())
        try:
            await asyncio.sleep(0.1)
            E._queue.put_nowait(E._WAKE)                           # забить очередь, чтобы жетон нового не встал
            E.on_wazzup({"messages": [env.msg("тихо", mid="m-poll")]})
            await until(lambda: store.intake_stats()["pending"] == 0)
        finally:
            task.cancel()
        assert [m["message_id"] for m in store.pending_inbox(CHAT_KEY)] == ["m-poll"]
    run(go())


def test_cancel_in_the_middle_leaves_the_row_open(env, monkeypatch):
    """SIGTERM посреди разбора: строка остаётся открытой и разберётся после рестарта."""
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("вот-вот", mid="m-cut")]})

        async def hang(payload, **kw):
            await asyncio.sleep(60)

        monkeypatch.setattr(E, "_handle_payload", hang)
        task = asyncio.create_task(E.drain_intake())
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert rows()[0]["done_at"] is None
    run(go())


def test_row_that_cannot_be_closed_does_not_spin_the_loop(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        E.on_wazzup({"messages": [env.msg("не закроется", mid="m-spin")]})
        monkeypatch.setattr(store, "finish_intake", lambda *a, **k: None)      # отметка в базе не ложится
        assert await asyncio.wait_for(E.drain_intake(), 5) is False            # остановились, а не крутимся вечно
        assert len(store.pending_inbox(CHAT_KEY)) == 1                         # и сообщение второй раз не добавлено
    run(go())


def test_broken_row_is_closed_with_error_and_does_not_block_the_rest(env, monkeypatch):
    async def go():
        env.lead()
        boot(env, monkeypatch)
        store.add_intake("wazzup", "{не json", env.t)
        E.on_wazzup({"messages": [env.msg("целое", mid="m-ok")]})
        await E.drain_intake()
        assert [r["note"] for r in rows()] == ["error", "ok"]
        assert [m["message_id"] for m in store.pending_inbox(CHAT_KEY)] == ["m-ok"]
    run(go())
