"""Окна перевода в УР (`ai_engine_ur`, решение Кати 12.11.2026): 17:50 всем сделкам ИИ, а тем, кому ИИ писал в 17:00 и позже, 18:45.
Сети нет: подставный мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_ur.py -q
"""
import datetime

import ai_engine as E
import ai_engine_settings as settings
from test_ai_engine import DEAL, env, run  # noqa: F401  (env - фикстура)

MSK = datetime.timezone(datetime.timedelta(hours=3))


def clock(env, ts):
    """Перевести часы стенда (и связанные с ними отметки движка) на момент `ts`."""
    env.t = ts
    env.mp.setattr(E.cfg, "UR_WAVE_1", "17:50")
    env.mp.setattr(E, "_ai_ok_at", ts)
    env.mp.setattr(E, "_last_housekeeping", ts)
    env.mp.setattr(settings, "_now", lambda: env.t)               # настройки считаются свежими относительно часов стенда
    settings._fetched_at = ts


def at(h, m=0):
    """Момент `h:m` по Москве завтра: стенд не любит время раньше настоящего, а границы окон считаются по дню."""
    day = datetime.datetime.now(MSK).date() + datetime.timedelta(days=1)
    return datetime.datetime(day.year, day.month, day.day, h, m, tzinfo=MSK).timestamp()


async def owned_and_wrote(env, hour, minute=0):
    """Клиент написал в 14:00, ИИ взял сделку; затем ИИ написал клиенту в указанное время."""
    clock(env, at(14))
    turn = await env.start_turn()
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    env.turn_requests.clear()
    clock(env, at(hour, minute))
    await E.silence_arm(DEAL)                                    # разрешение на отправку: движок запоминает время сообщения ИИ


async def to(env, h, m=0):
    """Часы на `h:m`, затем несколько проходов; ходы, что уходят сервису по дороге (лестница тишины и др.), закрываем «continue»."""
    clock(env, at(h, m))
    answered = getattr(env, "_answered", None)
    if answered is None:
        answered = env._answered = set()
    for _ in range(4):
        await E.tick_once()
        await E.dispatch_once()
        await env.advance(5)
        for r in list(env.turn_requests):
            if r["turn_id"] not in answered and r.get("event", {}).get("type") != "ur_wave":
                answered.add(r["turn_id"])
                await E.handle_result(r["turn_id"], DEAL, r["epoch"], "continue", None, None)


def waves(env):
    return [r for r in env.turn_requests if r.get("event", {}).get("type") == "ur_wave"]


def test_first_window_at_1750_gives_every_ai_deal_the_event_once(env):
    async def go():
        await owned_and_wrote(env, 14)
        await to(env, 17, 49)
        assert waves(env) == []
        await to(env, 17, 51)
        assert len(waves(env)) == 1 and "волна 1" in waves(env)[0]["event"]["reason"]
        first = waves(env)[0]
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        await to(env, 18, 10)
        await to(env, 19, 30)
        assert len(waves(env)) == 1                                                  # одно окно на сделку в день
    run(go())


def test_ai_message_at_1700_or_later_moves_the_deal_to_the_second_window(env):
    async def go():
        await owned_and_wrote(env, 17, 10)
        await to(env, 17, 51)
        assert waves(env) == []                                                      # первое окно пропущено
        await to(env, 18, 44)
        assert waves(env) == []
        await to(env, 18, 46)
        assert len(waves(env)) == 1 and "волна 2" in waves(env)[0]["event"]["reason"]
        await E.handle_result(waves(env)[0]["turn_id"], DEAL, waves(env)[0]["epoch"], "continue", None, None)
        await to(env, 20, 0)
        assert len(waves(env)) == 1
    run(go())


def test_message_just_before_1700_is_not_late(env):
    async def go():
        await owned_and_wrote(env, 16, 59)
        await to(env, 17, 51)
        assert len(waves(env)) == 1 and "волна 1" in waves(env)[0]["event"]["reason"]
    run(go())


def test_closed_deal_and_mode_off_get_no_event(env):
    async def go():
        await owned_and_wrote(env, 14)
        await E._deal_closed(DEAL, "closed")
        await to(env, 17, 55)
        assert waves(env) == []
    run(go())


def test_mode_off_gives_no_event(env):
    async def go():
        await owned_and_wrote(env, 14)
        env.set_settings(mode="off")
        await to(env, 17, 55)
        assert waves(env) == []
    run(go())
