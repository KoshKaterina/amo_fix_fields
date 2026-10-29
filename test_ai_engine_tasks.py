"""Задачи «Связаться с клиентом, срочно» при передаче человеку, которую делает движок, и их закрытие при возврате диалога ИИ.

Сети нет: amo подставной (`Env.tasks_open` и подмены в test_ai_engine.py). Запуск: python -m pytest test_ai_engine_tasks.py -q
"""
import asyncio
import time

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_store as store
from test_ai_engine import DEAL, env, run  # noqa: F401  (env - фикстура)


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


async def media_handoff(env):
    env.lead()
    await env.say(env.msg("", typ="image", mid="img-1"))
    await env.advance(1)
    await drain()


def test_media_handoff_creates_an_urgent_contact_task_for_the_responsible(env):
    async def go():
        await media_handoff(env)
        assert len(env.tasks_open) == 1
        t = env.tasks_open[0]
        assert t["text"].startswith("[handoff:media] Связаться с клиентом, срочно: Клиент прислал картинку")
        assert t["responsible_user_id"] == 7 and t["deal_id"] == DEAL
        assert 3000 < t["complete_till"] - time.time() < 3700                           # срок около часа
    run(go())


def test_ai_own_handoff_does_not_get_a_second_task_from_the_engine(env):
    async def go():
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "просьба позвонить", None)   # задачу ставит тулза сервиса
        await drain()
        assert env.tasks_open == []
    run(go())


def test_no_second_task_for_the_same_reason_and_no_task_without_responsible_or_when_amo_is_silent(env):
    async def go():
        await media_handoff(env)
        await E.ai_engine_tasks.create_contact_task(DEAL, "media", "ещё раз")
        assert len(env.tasks_open) == 1                                              # такая уже есть
        env.tasks_open.clear()
        env.task_responsible = None
        await E.ai_engine_tasks.create_contact_task(DEAL, "media", "некому")
        assert env.tasks_open == []
        env.task_responsible = 7
        env.tasks_down = True
        await E.ai_engine_tasks.create_contact_task(DEAL, "media", "amo молчит")
        assert env.tasks_open == []                                                  # молчание amo не значит «задач нет»
    run(go())


def test_return_dialog_closes_handoff_tasks_and_spares_foreign_ones(env):
    async def go():
        await media_handoff(env)
        env.tasks_open.append({"id": 999, "text": "позвонить в банк", "responsible_user_id": 7})
        code, _ = await E.return_dialog(DEAL)
        assert code == 200
        await drain()
        done = {t["id"]: t.get("is_completed") for t in env.tasks_open}
        assert done[100] is True and not done[999]
        assert env.tasks_open[0]["result"] == "Диалог возвращён ИИ"
    run(go())


def test_feature_flag_off_makes_tasks_inert(env, monkeypatch):
    async def go():
        monkeypatch.setattr(cfg, "HANDOFF_TASKS", False)
        await media_handoff(env)
        assert env.tasks_open == []
    run(go())
