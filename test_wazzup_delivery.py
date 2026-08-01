"""Тесты контроля доставки Wazzup (wazzup_delivery).

Тела вебхуков — НЕ выдуманные: взяты из продовой БД панели (wazzup_message.raw,
срез 31.07.2026), включая три реальные ошибки того дня. Телефоны и имена в тестах
оставлены как в бою — это внутренний репозиторий, а подмена ломала бы смысл
проверки «на живых данных».

Запуск: python3 -m pytest test_wazzup_delivery.py -q
"""

import asyncio
import os
import sys
import types

# Модуль конфига требует переменных окружения — ставим до импорта.
os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Подменяем telegram_bot до импорта модуля: в тестах aiogram не нужен.
_sent: list[dict] = []


def _install_stubs():
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id, "thread": message_thread_id})
        return True

    tg.send_alert = send_alert
    sys.modules["telegram_bot"] = tg


_install_stubs()

import wazzup_delivery  # noqa: E402


# --- фикстуры-тела вебхуков (прод, 31.07.2026) ------------------------------

ERROR_TEMPLATE = {  # шаблон со ссылкой на оплату СБП, не дошёл
    "messages": [{
        "text": "*Заказ успешно сформирован!* ✅\n\n👉🏼 Ссылка на оплату: https://qr.nspk.ru/AD10102GGBV5PN519G0BKLFSHPH57CBL",
        "type": "wapi_template",
        "error": {"error": "UNKNOWN_ERROR", "description": "An unknown error has occured. Try again later."},
        "chatId": "79775703378",
        "isEcho": True,
        "status": "error",
        "contact": {"name": "Суров Никита Денисович", "phone": "79775703378"},
        "chatType": "whatsapp",
        "dateTime": "2026-07-31T15:27:44.832Z",
        "messageId": "ecfa0a08-654e-4900-a4e1-0febf33e9aff",
        "authorName": "Admin",
    }]
}

ERROR_BAD_CONTACT = {
    "messages": [{
        "text": "*Здравствуйте, Даниил!*\n\n🛍 *Ваш Заказ №18185*",
        "type": "wapi_template",
        "error": {"error": "BAD_CONTACT", "description": "Number may not be on WhatsApp or uses an old version."},
        "chatId": "79199277589",
        "isEcho": True,
        "status": "error",
        "contact": {"name": "Даниил Логинов", "phone": "79199277589"},
        "chatType": "whatsapp",
        "messageId": "cfaa7ed3-ef9a-47da-80e6-c2fa17ad060b",
        "authorName": "Admin",
    }]
}

OUT_SENT_WA = {  # исходящее менеджера, доставки пока нет
    "messages": [{
        "text": "Добрый день! Заказ собран",
        "type": "text",
        "chatId": "79001234567",
        "isEcho": True,
        "status": "sent",
        "contact": {"name": "Клиент", "phone": "79001234567"},
        "chatType": "whatsapp",
        "messageId": "wa-sent-1",
        "authorName": "Егор Константинов",
    }]
}

OUT_SENT_TG = {  # Telegram: delivered не приходит вообще — таймер не ставим
    "messages": [{
        "text": "Нет, это именно к определенной модели",
        "type": "text",
        "chatId": "932999556",
        "isEcho": True,
        "status": "sent",
        "contact": {"name": "Евгений"},
        "chatType": "telegram",
        "messageId": "tg-sent-1",
        "authorName": "Егор Константинов",
    }]
}

INBOUND = {
    "messages": [{
        "text": "Здравствуйте, а есть в наличии?",
        "type": "text",
        "chatId": "79001234567",
        "isEcho": False,
        "status": "inbound",
        "chatType": "whatsapp",
        "messageId": "in-1",
    }]
}


async def _fake_resolve_lead(query):
    """Сделку в тестах не ищем: живой amo тут не нужен, а без стаба модуль честно
    пошёл бы в API и тест бы просто ждал ретраев."""
    return (12345, 13929334) if query else (None, None)


wazzup_delivery._resolve_lead_safe = _fake_resolve_lead


def _reset():
    _sent.clear()
    wazzup_delivery._tracked.clear()
    wazzup_delivery._enabled = True
    wazzup_delivery._burst_window_start = 0.0
    wazzup_delivery._burst_count = 0
    wazzup_delivery._burst_suppressed = 0


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


async def _handle(payload):
    """handle_webhook внутри живого loop: алерты уходят фоновыми задачами,
    поэтому ждём именно их завершения, а не «сколько-то тиков»."""
    before = asyncio.all_tasks()
    wazzup_delivery.handle_webhook(payload)
    spawned = asyncio.all_tasks() - before - {asyncio.current_task()}
    if spawned:
        await asyncio.gather(*spawned, return_exceptions=True)


# --- ошибки доставки --------------------------------------------------------

def test_error_alerts_immediately():
    _reset()
    _run(_handle(ERROR_TEMPLATE))
    assert len(_sent) == 1, "ошибка доставки должна дать ровно один алерт"
    text = _sent[0]["text"]
    assert "НЕ доставлено" in text
    assert "UNKNOWN_ERROR" in text
    assert "Суров Никита Денисович" in text
    assert "79775703378" in text
    assert "автоматика amo" in text, "authorName=Admin — это бот, а не человек"
    assert "WABA-шаблон" in text


def test_error_hint_is_human():
    _reset()
    _run(_handle(ERROR_BAD_CONTACT))
    assert "номера нет в WhatsApp" in _sent[0]["text"]


def test_error_not_duplicated():
    _reset()
    _run(_handle(ERROR_TEMPLATE))
    _run(_handle(ERROR_TEMPLATE))
    assert len(_sent) == 1, "повторная доставка того же вебхука не должна дублировать алерт"


def test_status_error_without_message():
    """statuses[] пришёл, а самого сообщения мы не видели (рестарт) — всё равно алертим."""
    _reset()
    _run(_handle({"statuses": [{
        "messageId": "zzz-1", "status": "error", "chatId": "79001234567",
        "chatType": "whatsapp",
        "error": {"error": "NOT_ENOUGH_MONEY", "description": "no money"},
    }]}))
    assert len(_sent) == 1
    assert "закончились деньги" in _sent[0]["text"]


# --- «отправлено, но не доставлено» -----------------------------------------

def test_stuck_sent_alerts_after_threshold():
    _reset()
    _run(_handle(OUT_SENT_WA))
    assert "wa-sent-1" in wazzup_delivery._tracked
    # состарим запись и прогоним sweep
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))
    assert len(_sent) == 1
    assert "висит «отправлено»" in _sent[0]["text"]


def test_delivered_cancels_timer():
    _reset()
    _run(_handle(OUT_SENT_WA))
    _run(_handle({"statuses": [{"messageId": "wa-sent-1", "status": "delivered"}]}))
    assert "wa-sent-1" not in wazzup_delivery._tracked
    wazzup_delivery._tracked.clear()
    _run(wazzup_delivery._sweep(threshold_s=0))
    assert _sent == [], "доставленное сообщение алертить нельзя"


def test_telegram_sent_is_not_tracked():
    """У Telegram delivered не приходит вовсе — иначе алерт был бы ложным."""
    _reset()
    _run(_handle(OUT_SENT_TG))
    assert wazzup_delivery._tracked == {}


def test_telegram_error_still_alerts():
    _reset()
    _run(_handle({"messages": [dict(OUT_SENT_TG["messages"][0], status="error",
                                    error={"error": "CHANNEL_BLOCKED", "description": "blocked"})]}))
    assert len(_sent) == 1, "ошибки ловим на всех каналах, включая Telegram"


def test_inbound_ignored():
    _reset()
    _run(_handle(INBOUND))
    assert wazzup_delivery._tracked == {}
    assert _sent == []


# --- антиспам ---------------------------------------------------------------

def test_burst_limited():
    _reset()
    payloads = []
    for i in range(15):
        m = dict(ERROR_TEMPLATE["messages"][0])
        m["messageId"] = f"burst-{i}"
        payloads.append({"messages": [m]})

    async def run_all():
        for p in payloads:
            await _handle(p)

    _run(run_all())
    burst_max = wazzup_delivery.WAZZUP_DELIVERY_BURST_MAX
    assert len(_sent) == burst_max + 1, "после лимита — ровно одна строка «дальше молчу»"
    assert "похоже на массовый сбой" in _sent[-1]["text"]
    assert "молчу" in _sent[-1]["text"]


def test_ttl_cleans_up():
    _reset()
    _run(_handle(OUT_SENT_WA))
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= wazzup_delivery._TTL_SECONDS + 1
    _run(wazzup_delivery._sweep(threshold_s=60))
    assert wazzup_delivery._tracked == {}
    assert _sent == [], "протухшая запись чистится молча, без алерта"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
