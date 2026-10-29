"""Возврат диалога ИИ реакцией менеджера на алерт передачи (сторона движка ИИ-менеджера).

Как это выглядит для людей: когда ИИ передал диалог человеку, алерт в чат ОП уходит с тегом менеджера, а бот сам ставит под
ним реакцию (по умолчанию 👍). Менеджер, которого тегнули, ставит такую же реакцию, и диалог возвращается ИИ: тот же путь,
что у служебной ручки `return_dialog` (`owner = ai`, `epoch + 1`, событие `dialog_returned`). Бот меняет свою метку на 👌.

Правила, чтобы реакция не была случайным нажатием:
  • считается только установка нужной реакции (раньше её у этого человека не было), снятие и чужие эмодзи игнорируются;
  • реагировать должен тегнутый в этом алерте (`@ник` из текста алерта сверяется с ником автора реакции) или человек из
    `AI_RETURN_ALSO` (например руководитель проекта, которого в алертах нет); без ника в Telegram или анонимный админ - игнор;
  • один алерт возвращает диалог один раз (`waiting` -> `busy` одним шагом); если ИИ вернуться не может (режим off, ИИ-сервис
    недоступен, сделка закрыта или уже у ИИ), бот отвечает в ту же тему причиной словами; при временной причине алерт остаётся
    рабочим, при постоянной закрывается;
  • после возврата остальные ждущие алерты этой сделки закрываются.

⚠️ Реакции приходят боту только если он администратор чата алертов. Без этого метка ставится, а возвращать нечем: в логе
будет одна строка при старте, в панели движка (`/health`) видно, что реакций не было.
Тесты: test_ai_engine_return.py.
"""
import asyncio
import html
import logging
import re
from typing import Awaitable, Callable

import ai_engine_config as cfg
import ai_engine_store as store
import amo_service
import telegram_bot
from api import BASE_URL

logger = logging.getLogger("uvicorn")

_TAG = re.compile(r"@([A-Za-z0-9_]{4,32})")

# Причины отказа `return_dialog` словами для менеджера. `retry` - можно попробовать позже, алерт остаётся рабочим.
_WHY = {
    "mode_off": ("режим ИИ выключен", False),
    "not_returnable": ("сделка уже закрыта или диалог уже у ИИ", False),
    "pipeline_not_allowed": ("эта воронка не входит в режим ИИ", False),
    "ai_unavailable": ("ИИ-сервис сейчас недоступен, попробуйте позже", True),
}

_return_fn: Callable[[int], Awaitable[tuple[int, dict]]] | None = None
_stats = {"reactions": 0, "returned": 0, "ignored": 0, "refused": 0}


def install(return_dialog: Callable[[int], Awaitable[tuple[int, dict]]]) -> None:
    """Подключить функцию возврата движка и обработчик реакций бота. Зовётся один раз из `ai_engine.init`."""
    global _return_fn
    _return_fn = return_dialog
    if cfg.RETURN_BY_REACTION:
        telegram_bot.set_reaction_handler(on_reaction)


def stats() -> dict:
    return {"enabled": cfg.RETURN_BY_REACTION, "emoji": cfg.RETURN_EMOJI, "also": len(cfg.RETURN_ALSO), **_stats}


def tags_of(text: str) -> str:
    """Ники из текста алерта через пробел, в нижнем регистре, без повторов."""
    return " ".join(dict.fromkeys(m.lower() for m in _TAG.findall(text or "")))


async def remember(chat_id: int, thread_id: int | None, message_id: int, deal_id: int, text: str) -> None:
    """Запомнить отправленный алерт передачи и поставить под ним метку бота. Не бросает: алерт уже ушёл."""
    if not cfg.RETURN_BY_REACTION or not message_id:
        return
    try:
        tags = tags_of(text)
        if not tags:
            return  # некого слушать: без тега реакция ничего не вернёт
        await asyncio.to_thread(store.put_alert_return, int(chat_id), int(message_id), thread_id, int(deal_id), tags)
        await asyncio.to_thread(store.purge_alert_return, cfg.RETENTION_DAYS * 86400)
        await telegram_bot.set_reaction(int(chat_id), int(message_id), cfg.RETURN_EMOJI)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_return: не запомнил алерт передачи по сделке %s", deal_id)


async def on_reaction(ev: dict) -> None:
    """Реакция человека на сообщение бота: если это нужная реакция тегнутого менеджера на алерт передачи, вернуть диалог ИИ."""
    if not cfg.RETURN_BY_REACTION or _return_fn is None or ev.get("is_bot"):
        return
    _stats["reactions"] += 1
    if cfg.RETURN_EMOJI not in ev.get("new", []) or cfg.RETURN_EMOJI in ev.get("old", []):
        return
    chat_id, message_id = int(ev["chat_id"]), int(ev["message_id"])
    row = await asyncio.to_thread(store.get_alert_return, chat_id, message_id)
    if row is None or row["state"] != store.ALERT_WAITING:
        return
    username = (ev.get("username") or "").lower()
    if not username or (username not in row["tags"].split() and username not in cfg.RETURN_ALSO):
        _stats["ignored"] += 1
        logger.info("ai_engine_return: реакция не от тегнутого и не из AI_RETURN_ALSO (%s), сделка %s",
                    username or "без ника", row["deal_id"])
        return
    if not await asyncio.to_thread(store.claim_alert_return, chat_id, message_id):
        return
    deal_id = int(row["deal_id"])
    try:
        code, out = await _return_fn(deal_id)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_return: возврат диалога по сделке %s упал", deal_id)
        code, out = 500, {"detail": "error"}
    if code == 200:
        _stats["returned"] += 1
        await asyncio.to_thread(store.set_alert_return_state, chat_id, message_id, store.ALERT_RETURNED)
        await asyncio.to_thread(store.close_alerts_of_deal, deal_id)
        await telegram_bot.set_reaction(chat_id, message_id, cfg.RETURNED_EMOJI)
        try:
            await asyncio.wait_for(amo_service.add_note(
                deal_id, f"Диалог возвращён ИИ: менеджер @{username} поставил реакцию на уведомление."), timeout=15)
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_return: не записал заметку о возврате в сделку %s", deal_id)
        return
    _stats["refused"] += 1
    why, retry = _WHY.get(str(out.get("detail")), ("не получилось, подробности в журнале движка", True))
    if retry:
        why += ". Снимите реакцию и поставьте снова"
    await asyncio.to_thread(store.set_alert_return_state, chat_id, message_id,
                            store.ALERT_WAITING if retry else store.ALERT_CLOSED)
    link = f'<a href="{BASE_URL}/leads/detail/{deal_id}">Открыть сделку</a>'
    await telegram_bot.send_alert(
        f"@{html.escape(username)} ИИ не вернулся к сделке: {why}.\n{link}",
        parse_mode="HTML", chat_id=chat_id, message_thread_id=row["thread_id"],
    )
