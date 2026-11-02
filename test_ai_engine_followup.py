"""Отложенные касания ИИ (ai_engine_followup): регистрация, срок, события и все отмены.

Решение Кати 02.11.2026: только по просьбе клиента, в названное время, примечание в сделке, без задачи менеджеру, до 7 дней;
если клиент или менеджер написали в окно ожидания, отправка отменяется, а ИИ получает пометку об этом.
Сети нет: подставной мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_followup.py -q
"""
import asyncio

import ai_engine as E
import ai_engine_followup as F
import ai_engine_store as store
from test_ai_engine import DEAL, env, run  # noqa: F401  (env - фикстура)

HOUR = 3600


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


async def scheduled(env, hours=24, reason="клиент попросил написать завтра, думает про Tangem", draft="Напомнить про Tangem 2.0"):
    """Ход ИИ, в котором он зарегистрировал касание (как это делает сервис до итога хода), затем итог `continue`."""
    turn = await env.start_turn()
    code, out = await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"],
                                          env.t + hours * HOUR, reason, draft)
    assert (code, out["allowed"]) == (200, True), out
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    await drain()
    return turn


def pending():
    return F.pending_for_deal(DEAL)


def test_schedule_writes_a_note_and_keeps_one_pending_row(env):
    async def go():
        await scheduled(env)
        row = pending()
        assert row["status"] == "pending" and row["reason"].startswith("клиент попросил") and row["epoch"] == 1
        assert any("по просьбе клиента напишу ему" in t and F.fmt_due(row["due_at"]) in t for _, t in env.notes)
        assert env.tasks_open == []                                    # задачу менеджеру касание не ставит
    run(go())


def test_new_schedule_replaces_the_old_one(env):
    async def go():
        turn = await scheduled(env, hours=24)
        first = pending()["id"]
        code, out = await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + 48 * HOUR, "другой срок", "")
        # ход уже закрыт итогом, но разрешение на касание считается по владельцу и сообщениям, а не по состоянию хода
        assert code == 200 and pending()["id"] != first and pending()["reason"] == "другой срок"
    run(go())


def test_refuses_too_soon_too_far_and_unknown_owner(env):
    async def go():
        turn = await env.start_turn()
        args = (DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert (await E.schedule_followup(*args, env.t + 60, "рано", ""))[1]["reason"] == "too_soon"
        assert (await E.schedule_followup(*args, env.t + 8 * 86400, "далеко", ""))[1]["reason"] == "too_far"
        assert (await E.schedule_followup(DEAL, turn["epoch"] + 1, turn["turn_id"], turn["up_to_message_id"], env.t + 24 * HOUR, "эпоха", ""))[0] == 409
        assert pending() is None
    run(go())


def test_refuses_when_the_client_wrote_during_the_turn(env):
    async def go():
        turn = await env.start_turn()
        await env.say(env.msg("ещё одно сообщение", mid="late-1"))
        code, out = await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + 24 * HOUR, "поздно", "")
        assert out["allowed"] is False and out["reason"] == "newer_client_message" and pending() is None
    run(go())


def test_due_time_gives_the_ai_a_followup_due_event_with_reason_and_draft(env):
    async def go():
        await scheduled(env)
        before = len(env.turn_requests)
        await env.advance(24 * HOUR - 60)
        assert len(env.turn_requests) == before and pending() is not None          # срок ещё не пришёл
        await env.advance(120)
        await env.advance(5)
        await env.advance(5)
        assert len(env.turn_requests) == before + 1
        req = env.turn_requests[-1]
        assert req["reason"] == "event" and req["messages"] == []
        ev = req["event"]
        assert ev["type"] == "followup_due" and ev["reason"].startswith("клиент попросил") and ev["draft"] == "Напомнить про Tangem 2.0"
        assert len(ev["due_at"]) == len("03.11 12:00")                                # человекочитаемое время срока
        assert pending() is None
    run(go())


def test_followup_waits_for_the_send_window(env):
    async def go():
        env.set_settings(work_hours=[])                              # пустые часы: окно закрыто
        await scheduled(env)
        before = len(env.turn_requests)
        await env.advance(25 * HOUR)
        assert len(env.turn_requests) == before and pending() is not None            # ждёт окна, не отменяется
        env.set_settings()                                           # окно открылось
        await env.advance(5)
        await env.advance(5)                                         # первый проход ставит событие, второй собирает ход
        assert len(env.turn_requests) == before + 1 and env.turn_requests[-1]["event"]["type"] == "followup_due"
    run(go())


def test_client_message_cancels_and_the_next_turn_knows_why(env):
    async def go():
        await scheduled(env)
        await env.say(env.msg("Здравствуйте, я уже готов", mid="c-2"))
        await drain()
        assert pending() is None
        assert any("отложенное касание" in t and "отменено: клиент написал раньше" in t for _, t in env.notes)
        await env.advance(61)
        req = env.turn_requests[-1]
        assert req["reason"] != "event" and req["followup_cancelled"][0]["why"] == "клиент написал раньше"
        assert req["followup_cancelled"][0]["draft"] == "Напомнить про Tangem 2.0"
        # пометка показывается один раз
        turn2 = req
        await E.handle_result(turn2["turn_id"], DEAL, turn2["epoch"], "continue", None, None)
        await env.say(env.msg("и ещё вопрос", mid="c-3"))
        await env.advance(61)
        assert "followup_cancelled" not in env.turn_requests[-1]
        # к сроку ничего не уходит
        before = len(env.turn_requests)
        await env.advance(25 * HOUR)
        assert not any(r.get("event", {}) and r["event"]["type"] == "followup_due" for r in env.turn_requests[before:])
    run(go())


def test_client_attachment_cancels_too(env):
    async def go():
        await scheduled(env)
        await env.say(env.msg("", typ="image", mid="img-1"))
        assert pending() is None
    run(go())


def test_manager_message_in_the_waiting_window_cancels_the_followup(env):
    async def go():
        await scheduled(env)
        await env.say(env.msg("Добрый день, это менеджер", echo=True, author="Иван Иванов", mid="mgr-1"))
        await drain()
        assert pending() is None
        assert store.get_owner(DEAL)["owner"] == "human"
        assert any("отменено: менеджер написал в чат" in t for _, t in env.notes)
        before = len(env.turn_requests)
        await env.advance(25 * HOUR)
        assert len(env.turn_requests) == before                       # к сроку ИИ ничего не получает и не пишет
        # менеджер вернул диалог ИИ: в ход приходит пометка, что касание отменено из-за менеджера
        code, _ = await E.return_dialog(DEAL)
        assert code == 200
        await env.advance(5)
        req = env.turn_requests[-1]
        assert req["event"]["type"] == "dialog_returned" and req["followup_cancelled"][0]["why"] == "менеджер написал в чат"
    run(go())


def test_robot_echo_does_not_cancel(env):
    async def go():
        await scheduled(env)
        await env.say(env.msg("Автоматика: шаблон", echo=True, author="Admin", mid="bot-1"))
        await env.say(env.msg("Ответ ИИ", echo=True, author="", mid="ai-1"))
        assert pending() is not None
    run(go())


def test_closed_deal_cancels(env):
    async def go():
        await scheduled(env)
        await E._deal_closed(DEAL, "closed")
        assert pending() is None
        assert any("отменено: сделка закрыта" in t for _, t in env.notes)
    run(go())


def test_ai_handoff_cancels(env):
    async def go():
        turn = await env.start_turn()
        await E.schedule_followup(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"], env.t + 24 * HOUR, "касание", "")
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "handoff_to_human", "просьба позвонить", None)
        assert pending() is None
    run(go())


def test_mode_off_at_due_time_fails_with_note_and_alert_but_no_task(env):
    async def go():
        await scheduled(env)
        env.set_settings(mode="off")
        await env.advance(25 * HOUR)
        await drain()
        assert pending() is None
        assert any("не вышло: режим ИИ выключен" in t for _, t in env.notes)
        assert env.tasks_open == []
    run(go())


def test_ai_service_down_gives_up_after_half_an_hour(env, monkeypatch):
    async def go():
        await scheduled(env)

        async def down():
            return False

        monkeypatch.setattr(E, "_ai_is_up", down)
        before = len(env.turn_requests)
        await env.advance(24 * HOUR + 60)
        assert pending() is not None and len(env.turn_requests) == before
        await env.advance(31 * 60)
        await drain()
        assert pending() is None and any("не вышло: ИИ-сервис был недоступен" in t for _, t in env.notes)
        assert env.tasks_open == []
    run(go())


def test_owner_change_before_due_time_cancels(env):
    async def go():
        await scheduled(env)
        await E._to_human(None, "test_reason", alert=None, deal_id=DEAL)
        assert pending() is None
    run(go())
