"""Возврат диалога ИИ реакцией менеджера на алерт передачи (ai_engine_return).

Сети нет: Telegram подставной (см. `Env` в test_ai_engine.py: `alert_ids`, `reactions`, `bot_messages`).
Запуск: python -m pytest test_ai_engine_return.py -q
"""
import asyncio

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_return as R
import ai_engine_store as store
from test_ai_engine import DEAL, env, run  # noqa: F401  (env - фикстура)

CHAT = -1003680811996
EMOJI = cfg.RETURN_EMOJI
TAGGED = "egorkonsss"          # в смене из tg_recipients.MANAGERS_ON_SHIFT


@pytest.fixture(autouse=True)
def _install(monkeypatch):
    monkeypatch.setattr(R, "_return_fn", E.return_dialog)    # как делает ai_engine.init


async def drain():
    while E._bg:
        await asyncio.gather(*list(E._bg))


def react(mid, username=TAGGED, new=(EMOJI,), old=(), is_bot=False):
    return {"chat_id": CHAT, "message_id": mid, "user_id": 1, "username": username, "is_bot": is_bot,
            "new": list(new), "old": list(old)}


async def handoff(env):
    """Ход, по итогу которого ИИ передал диалог человеку: алерт с тегом и метка бота."""
    turn = await env.start_turn()
    code, _ = await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "просьба позвонить", None)
    assert code == 200
    await drain()
    return env.alert_ids[-1][2]


def test_tags_of_takes_nicks_in_lower_case_without_repeats():
    assert R.tags_of("🤖 ИИ-менеджер\n@OFFf1cer @egorkonsss @offf1cer текст") == "offf1cer egorkonsss"
    assert R.tags_of("без тегов") == ""


def test_handoff_alert_gets_the_bot_marker_and_is_remembered(env):
    async def go():
        mid = await handoff(env)
        assert (CHAT, mid, EMOJI) in env.reactions
        row = store.get_alert_return(CHAT, mid)
        assert row["deal_id"] == DEAL and row["state"] == "waiting" and TAGGED in row["tags"].split()
        assert row["thread_id"] == 10479
    run(go())


def test_media_alert_is_returnable_too(env):
    async def go():
        env.lead()
        await env.say(env.msg("", typ="image", mid="img-1"))
        await env.advance(1)
        await drain()
        mid = env.alert_ids[-1][2]
        assert store.get_alert_return(CHAT, mid)["deal_id"] == DEAL
    run(go())


def test_tagged_manager_reaction_returns_the_dialog_to_ai(env):
    async def go():
        mid = await handoff(env)
        assert store.get_owner(DEAL)["owner"] == "human"
        await R.on_reaction(react(mid))
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai" and owner["epoch"] == 3
        assert store.get_alert_return(CHAT, mid)["state"] == "returned"
        assert (CHAT, mid, cfg.RETURNED_EMOJI) in env.reactions          # бот сменил метку
        assert any(d == DEAL and "@egorkonsss" in t for d, t in env.notes)  # след в сделке
        await env.advance(1)
        ev = env.turn_requests[-1]
        assert ev["reason"] == "event" and ev["event"]["type"] == "dialog_returned"
    run(go())


def test_reaction_is_ignored_unless_it_is_the_tagged_manager_setting_the_right_emoji(env):
    async def go():
        mid = await handoff(env)
        for ev in (
            react(mid, username="stranger"),                      # не тегнут
            react(mid, username=None),                            # ника нет
            react(mid, is_bot=True),                              # бот
            react(mid, new=("🔥",)),                              # другая реакция
            react(mid, new=(EMOJI,), old=(EMOJI,)),               # уже стояла
            react(mid, new=()),                                   # снял
            react(mid + 1),                                       # чужое сообщение
        ):
            await R.on_reaction(ev)
        assert store.get_owner(DEAL)["owner"] == "human"
        assert store.get_alert_return(CHAT, mid)["state"] == "waiting"
        assert env.bot_messages == []
    run(go())


def test_second_reaction_does_not_return_twice(env):
    async def go():
        mid = await handoff(env)
        await asyncio.gather(R.on_reaction(react(mid)), R.on_reaction(react(mid, username="offf1cer")))
        assert store.get_owner(DEAL)["epoch"] == 3
        await R.on_reaction(react(mid, username="kathrina_bistraya"))
        assert store.get_owner(DEAL)["epoch"] == 3
    run(go())


def test_return_closes_other_waiting_alerts_of_the_deal(env):
    async def go():
        mid1 = await handoff(env)
        store.put_alert_return(CHAT, 777, 10479, DEAL, "egorkonsss")
        await R.on_reaction(react(mid1))
        assert store.get_alert_return(CHAT, 777)["state"] == "closed"
    run(go())


def test_refusal_is_explained_in_the_thread_and_permanent_reasons_close_the_alert(env):
    async def go():
        mid = await handoff(env)
        env.set_settings(mode="off", pipelines=[])
        await R.on_reaction(react(mid))
        assert store.get_owner(DEAL)["owner"] == "human"
        msg = env.bot_messages[-1]
        assert "режим ИИ выключен" in msg["text"] and "@egorkonsss" in msg["text"]
        assert msg["chat_id"] == CHAT and msg["thread_id"] == 10479
        assert store.get_alert_return(CHAT, mid)["state"] == "closed"
    run(go())


def test_temporary_refusal_keeps_the_alert_working(env):
    async def go():
        mid = await handoff(env)
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        await R.on_reaction(react(mid))
        assert "недоступен" in env.bot_messages[-1]["text"] and "Снимите реакцию" in env.bot_messages[-1]["text"]
        assert store.get_alert_return(CHAT, mid)["state"] == "waiting"
        env.health_ok = True
        E._ai_ok_at = env.t
        await R.on_reaction(react(mid, old=(EMOJI,), new=()))           # снял
        await R.on_reaction(react(mid))                                 # поставил снова
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_feature_flag_off_makes_everything_inert(env, monkeypatch):
    async def go():
        monkeypatch.setattr(cfg, "RETURN_BY_REACTION", False)
        turn = await env.start_turn()
        await E.handle_result(turn["turn_id"], DEAL, 1, "handoff_to_human", "x", None)
        await drain()
        assert env.reactions == []
        assert store.get_owner(DEAL)["owner"] == "human"
    run(go())
