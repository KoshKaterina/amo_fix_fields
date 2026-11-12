"""Передача сделки ИИ вручную (`ai_engine.give_deal`, решение Кати 12.11.2026): заказы с сайта лежат на «Новом лиде», клиент уже написал,
менеджер не ответил; сделку отдают ИИ, и он продолжает переписку, как после возврата диалога.

Сети нет: подставной мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_give.py -q
"""
import asyncio

import ai_engine as E
import ai_engine_give as G
import ai_engine_store as store
from test_ai_engine import CHAN, CHAT, CONTACT, DEAL, PHONE, PIPE_RETAIL, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)

DEAL2 = DEAL + 1


async def give(**over):
    args = dict(deal_id=DEAL, chat_id=CHAT, chat_type="telegram", channel_id=CHAN, phone=PHONE, contact_name="Тиана")
    args.update(over)
    return await E.give_deal(**args)


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


def test_give_takes_the_deal_and_sends_a_deal_given_event(env):
    async def go():
        env.lead()
        code, out = await give()
        assert (code, out["ok"], out["epoch"]) == (200, True, 1)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["epoch"], owner["reason"], owner["chat_id"]) == ("ai", 1, "deal_given", CHAT)
        await env.advance(1)
        req = env.turn_requests[-1]
        assert req["reason"] == "event" and req["event"]["type"] == "deal_given" and req["messages"] == []
        assert req["chat"] == {"chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN}
        assert req["up_to_message_id"] == "" and req["epoch"] == 1 and req["deal_id"] == DEAL
        await drain()
        assert any("передана ИИ-менеджеру" in t for _, t in env.notes)
    run(go())


def test_give_marks_the_manager_cutoff_for_every_turn_of_this_deal(env):
    async def go():
        env.lead()
        await give()
        await env.advance(1)
        first = env.turn_requests[-1]
        cutoff = first["manager_cutoff"]
        assert cutoff and cutoff.endswith("Z")
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        env.turn_requests.clear()
        await env.say(env.msg("Да", mid="c-1"))
        await env.advance(11)
        later = env.turn_requests[-1]
        assert later["reason"] == "client_message" and later["manager_cutoff"] == cutoff and [m["message_id"] for m in later["messages"]] == ["c-1"]
    run(go())


def test_ordinary_entry_has_no_manager_cutoff(env):
    async def go():
        turn = await env.start_turn()
        assert "manager_cutoff" not in turn
        assert G.cutoff_iso(DEAL) is None
    run(go())


def test_client_message_after_give_continues_the_dialog_and_a_manager_message_still_stops_it(env):
    async def go():
        env.lead()
        await give()
        await env.advance(1)
        ev = env.turn_requests[-1]
        await E.handle_result(ev["turn_id"], DEAL, ev["epoch"], "continue", None, None)
        await env.say(env.msg("Здравствуйте", mid="c-2"))
        await env.advance(11)
        assert env.turn_requests[-1]["reason"] == "client_message"
        await env.say(env.msg("Добрый день, это менеджер", echo=True, author="Иван Иванов", mid="m-1"))
        assert store.get_owner(DEAL)["owner"] == "human"                 # после передачи менеджер снимает диалог, как обычно
    run(go())


def test_give_works_for_a_deal_held_by_a_human(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "x", None)
        assert store.get_owner(DEAL)["owner"] == "human"
        code, out = await give()
        assert code == 200 and out["epoch"] == 3
        await env.advance(1)
        assert env.turn_requests[-1]["event"]["type"] == "deal_given" and env.turn_requests[-1]["epoch"] == 3
    run(go())


def test_give_refused_when_the_ai_already_owns_the_deal_or_it_is_closed(env):
    async def go():
        await env.start_turn()
        code, out = await give()
        assert (code, out["detail"]) == (409, "already_ai")
        store.set_owner(DEAL, store.OWNER_ENGINE, reason="sale_completed", closed=True)
        code, out = await give()
        assert (code, out["detail"]) == (409, "deal_closed")
    run(go())


def test_give_refused_with_a_reason_and_nothing_changes(env):
    async def go():
        env.lead()
        # режим off
        env.set_settings(mode="off")
        assert (await give())[1]["detail"] == "mode_off"
        env.set_settings()
        # нет чата
        assert (await give(chat_id=""))[0] == 422
        # контакта нет в списке режима
        env.leads = []
        env.lead(contacts=(999,))
        env.set_settings(contacts={"scope": "test", "restricted": True, "items": [{"contact_id": CONTACT, "phone_norm": "79990000000", "name": "Другой"}]})
        code, out = await give()
        assert (code, out["detail"]) == (409, "contact_not_in_list")
        env.set_settings()
        # сделка в чужой воронке режима: подходящих нет
        env.leads = []
        env.lead(pipeline=PIPE_RETAIL)
        assert (await give())[0] == 404
        # сделки у клиента нет совсем
        env.leads = []
        assert (await give())[0] == 404
        # amo не отвечает
        env.lead()
        env.amo_down = True
        assert (await give())[0] == 503
        env.amo_down = False
        assert store.get_owner(DEAL) is None and env.turn_requests == []
    run(go())


def test_give_names_the_other_deal_and_the_duplicate_rules(env):
    async def go():
        env.lead()
        env.lead(lead_id=DEAL2)
        code, out = await give()                                       # две пустые сделки: правило D, решает человек
        assert (code, out["detail"], out["rule"]) == (409, "needs_human", "D")
        assert sorted(out["others"]) == [DEAL, DEAL2]
        env.leads = []
        env.lead(lead_id=DEAL2)
        code, out = await give()                                       # у клиента одна подходящая, но это другая сделка
        assert (code, out["detail"], out["deal_id"]) == (409, "other_deal_chosen", DEAL2)
        assert store.get_owner(DEAL) is None
    run(go())


def test_give_refused_when_the_ai_service_is_down(env):
    async def go():
        env.lead()
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        code, out = await give()
        assert (code, out["detail"]) == (503, "ai_unavailable")
        assert store.get_owner(DEAL) is None
    run(go())


def test_give_releases_the_intake_tasks(env):
    async def go():
        env.lead()
        env.tasks_open.append({"id": 50, "text": "Новый заказ с сайта", "task_type_id": 4232862, "created_by": 0, "responsible_user_id": 7, "deal_id": DEAL})
        await give()
        await drain()
        assert env.tasks_open[0].get("is_completed") is True            # первое вхождение ИИ закрывает задачи приёма, как при входе по сообщению
    run(go())


def test_every_ai_alert_pings_the_extra_tag_besides_the_shift(env):
    async def go():
        await E._alert("deal_not_found", ["Клиент написал, но сделки нет."])
        assert env.alerts and "@koshkatarina" in env.alerts[-1]
        assert "@offf1cer" in env.alerts[-1]                         # смена по-прежнему в алерте
        assert env.alerts[-1].count("@koshkatarina") == 1
    run(go())


def test_forced_give_overrides_the_duplicate_rules_now_and_for_later_messages(env):
    async def go():
        env.lead()
        env.lead(lead_id=DEAL2, price=5000)                          # вторая открытая сделка с признаком работы: правило A/D отказало бы
        code, out = await give()
        assert code == 409 and out["detail"] == "needs_human"
        code, out = await give(force=True)
        assert (code, out["ok"]) == (200, True) and G.is_forced(DEAL)
        await env.advance(1)
        first = env.turn_requests[-1]
        assert first["event"]["type"] == "deal_given"
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        env.turn_requests.clear()
        await env.say(env.msg("Здравствуйте", mid="c-9"))
        await env.advance(11)
        assert env.turn_requests and env.turn_requests[-1]["reason"] == "client_message"          # правило C не отдало сделку человеку
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_ordinary_give_is_not_forced(env):
    async def go():
        env.lead()
        await give()
        assert G.is_forced(DEAL) is False
    run(go())


def test_forced_give_marks_every_turn_with_given_forced(env):
    async def go():
        env.lead()
        env.lead(lead_id=DEAL2, price=5000)
        await give(force=True)
        await env.advance(1)
        assert env.turn_requests[-1]["given_forced"] is True
        env.leads = []
        env.lead(lead_id=DEAL + 5)
        code, _ = await give(deal_id=DEAL + 5)
        await env.advance(1)
        assert "given_forced" not in env.turn_requests[-1]
    run(go())
