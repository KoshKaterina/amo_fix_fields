"""Лестница тишины (`ai_engine_silence`, решение Кати 12.11.2026): час без ответа клиента на сообщение ИИ: ИИ пробует другой канал,
ещё час: алерт менеджерам. Ответ клиента, закрытие сделки и передача человеку снимают отсчёт.
Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_silence.py -q
"""
import ai_engine as E
import ai_engine_silence as S
from test_ai_engine import DEAL, env, run  # noqa: F401  (env - фикстура)

HOUR = 3600


async def ai_owns_and_wrote(env):
    """Клиент написал, ИИ взял сделку, отработал ход; затем ИИ получил разрешение написать клиенту (взводит отсчёт)."""
    turn = await env.start_turn()
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    env.turn_requests.clear()
    await E.silence_arm(DEAL)


def test_first_ai_message_arms_the_countdown_and_later_ones_do_not_move_it(env):
    async def go():
        await ai_owns_and_wrote(env)
        first = S.get(DEAL)["armed_at"]
        env.t += 600
        await E.silence_arm(DEAL)
        assert S.get(DEAL)["armed_at"] == first and S.get(DEAL)["stage"] == 0
    run(go())


def test_an_hour_of_silence_gives_the_ai_an_alt_channel_event_and_the_second_hour_an_alert(env):
    async def go():
        await ai_owns_and_wrote(env)
        await env.advance(HOUR - 60)
        assert env.turn_requests == [] and env.alerts == []                         # ещё не час
        await env.advance(120)
        await env.advance(5)
        await env.advance(5)
        ev = env.turn_requests[-1]
        assert ev["reason"] == "event" and ev["event"]["type"] == "silent_alt" and ev["event"]["reason"]
        assert S.get(DEAL)["stage"] == 1 and env.alerts == []
        await E.handle_result(ev["turn_id"], DEAL, ev["epoch"], "continue", None, None)
        await env.advance(HOUR - 60)
        assert env.alerts == []                                                     # ещё не второй час
        await env.advance(120)
        await env.advance(5)
        assert len(env.alerts) == 1 and "больше двух часов" in env.alerts[0] and "@koshkatarina" in env.alerts[0]
        assert S.get(DEAL)["stage"] == 2
        await env.advance(3 * HOUR)
        assert len(env.alerts) == 1                                                 # алерт один
    run(go())


def test_client_reply_resets_the_ladder(env):
    async def go():
        await ai_owns_and_wrote(env)
        await env.advance(HOUR + 120)
        await env.say(env.msg("Здравствуйте, да", mid="reply-1"))
        await env.advance(70)
        assert S.get(DEAL) is None
        env.alerts.clear()
        await env.advance(3 * HOUR)
        assert not any("больше двух часов" in a for a in env.alerts)          # лестница тишины не сработала
    run(go())


def test_closed_deal_clears_the_countdown(env):
    async def go():
        await ai_owns_and_wrote(env)
        await E._deal_closed(DEAL, "closed")
        assert S.get(DEAL) is None
    run(go())


def test_human_handover_clears_the_countdown(env):
    async def go():
        await ai_owns_and_wrote(env)
        await E._to_human(None, "test_reason", alert=None, deal_id=DEAL)
        assert S.get(DEAL) is None
    run(go())


def test_off_hours_wait_for_the_window(env):
    async def go():
        await ai_owns_and_wrote(env)
        env.set_settings(work_hours=[])                                              # окно закрыто
        await env.advance(5 * HOUR)
        assert env.turn_requests == [] and S.get(DEAL)["stage"] == 0
        env.set_settings()
        await env.advance(5)
        await env.advance(5)
        assert env.turn_requests and env.turn_requests[-1]["event"]["type"] == "silent_alt"
    run(go())


def test_manual_alert_has_the_standard_header_and_pings(env):
    async def go():
        await E.raise_alert("manual", ["Клиент не отвечает.", "ссылка"])
        text = env.alerts[-1]
        assert "ИИ-менеджер" in text and "@koshkatarina" in text and "Клиент не отвечает." in text
    run(go())
