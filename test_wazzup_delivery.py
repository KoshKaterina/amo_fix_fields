"""Тесты контроля доставки Wazzup (wazzup_delivery).

Тела вебхуков — НЕ выдуманные: взяты из продовой БД панели (wazzup_message.raw,
срез 31.07.2026), включая три реальные ошибки того дня. Телефоны и имена в тестах
оставлены как в бою — это внутренний репозиторий, а подмена ломала бы смысл
проверки «на живых данных».

Запуск: python3 -m pytest test_wazzup_delivery.py -q
"""

import asyncio
import datetime
import os
import sys
import types

# Модуль конфига требует переменных окружения — ставим до импорта.
os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Подменяем telegram_bot до импорта модуля: в тестах aiogram не нужен.
_sent: list[dict] = []
_notes: list[tuple] = []


def _install_stubs():
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id, "thread": message_thread_id})
        return True

    tg.send_alert = send_alert
    sys.modules["telegram_bot"] = tg


_install_stubs()

import wazzup_delivery  # noqa: E402
import amo_service  # noqa: E402


async def _fake_add_note(lead_id, text):
    """Примечания в живой amo из тестов не пишем — ловим вызов."""
    _notes.append((lead_id, text))
    return {}


amo_service.add_note = _fake_add_note


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

# Настоящие «часы» модуля: тесты дайджеста подменяют их на фиксированный час.
_real_now_msk = wazzup_delivery._now_msk


def _reset():
    _sent.clear()
    _notes.clear()
    wazzup_delivery._tracked.clear()
    wazzup_delivery._stuck_pending.clear()
    wazzup_delivery._stuck_overflow = 0
    wazzup_delivery._stuck_digest_sent_for = ""
    wazzup_delivery._now_msk = _real_now_msk
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
    assert "НЕ отправлено" in text
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


# --- «отправлено, но доставки нет» → вечерний дайджест ------------------

def test_stuck_sent_goes_to_digest_not_to_chat():
    """Главное правило (Катя 02.08.2026): висяк не звенит сразу."""
    _reset()
    _run(_handle(OUT_SENT_WA))
    assert "wa-sent-1" in wazzup_delivery._tracked
    # состарим запись и прогоним sweep
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))
    assert _sent == [], "висяк в чат сразу не шлём"
    assert "wa-sent-1" in wazzup_delivery._stuck_pending
    assert wazzup_delivery._stuck_pending["wa-sent-1"]["lead_id"] == 12345


def test_stuck_queued_once():
    _reset()
    _run(_handle(OUT_SENT_WA))
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))
    _run(wazzup_delivery._sweep(threshold_s=60))
    assert len(wazzup_delivery._stuck_pending) == 1


def test_late_delivery_removes_from_digest():
    """Ради этого висяки и ждут вечера: дошло позже — в список не попадает."""
    _reset()
    _run(_handle(OUT_SENT_WA))
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))
    _run(_handle({"statuses": [{"messageId": "wa-sent-1", "status": "delivered"}]}))
    assert wazzup_delivery._stuck_pending == {}
    assert wazzup_delivery._tracked == {}
    assert _sent == []


def test_delivered_cancels_timer():
    _reset()
    _run(_handle(OUT_SENT_WA))
    _run(_handle({"statuses": [{"messageId": "wa-sent-1", "status": "delivered"}]}))
    assert "wa-sent-1" not in wazzup_delivery._tracked
    _run(wazzup_delivery._sweep(threshold_s=0))
    assert _sent == [], "доставленное сообщение алертить нельзя"
    assert wazzup_delivery._stuck_pending == {}


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


# --- вечерний дайджест висяков ----------------------------------------------

class _FakeMsk:
    """Подменяем «сейчас» по МСК: дайджест смотрит на час."""

    def __init__(self, hour, day=2):
        self._dt = datetime.datetime(2026, 8, day, hour, 5, tzinfo=wazzup_delivery._MSK)

    def __call__(self):
        return self._dt


def _queue_stuck(message_id="wa-sent-1", payload=OUT_SENT_WA):
    """Кладёт исходящее в очередь дайджеста через настоящий sweep."""
    _run(_handle(payload))
    wazzup_delivery._tracked[message_id]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))


def test_digest_silent_before_its_hour():
    _reset()
    _queue_stuck()
    wazzup_delivery._now_msk = _FakeMsk(wazzup_delivery.WAZZUP_STUCK_DIGEST_HOUR_MSK - 1)
    _run(wazzup_delivery._maybe_stuck_digest())
    assert _sent == [], "до часа дайджеста молчим"
    assert len(wazzup_delivery._stuck_pending) == 1


def test_digest_sends_at_its_hour():
    _reset()
    _queue_stuck()
    wazzup_delivery._now_msk = _FakeMsk(wazzup_delivery.WAZZUP_STUCK_DIGEST_HOUR_MSK)
    _run(wazzup_delivery._maybe_stuck_digest())
    assert len(_sent) == 1
    text = _sent[0]["text"]
    assert "Отправлено, но в Wazzup не помечено как доставлено: 1" in text
    assert "Егор Константинов" in text
    assert "Заказ собран" in text
    assert "/leads/detail/12345" in text
    assert wazzup_delivery._stuck_pending == {}, "очередь чистится после отправки"


def test_digest_once_per_day():
    _reset()
    _queue_stuck()
    wazzup_delivery._now_msk = _FakeMsk(wazzup_delivery.WAZZUP_STUCK_DIGEST_HOUR_MSK)
    _run(wazzup_delivery._maybe_stuck_digest())
    # новый висяк в тот же час — ждёт завтрашнего дайджеста, а не второго сегодня
    wazzup_delivery._stuck_pending["wa-sent-2"] = {
        "sent_at_msk": "18:07", "contact_name": "Клиент", "chat_id": "79001234568",
        "author_name": "Егор Константинов", "text": "ещё одно",
    }
    _run(wazzup_delivery._maybe_stuck_digest())
    assert len(_sent) == 1, "второй раз за те же сутки дайджест не шлём"


def test_digest_silent_when_nothing_stuck():
    _reset()
    wazzup_delivery._now_msk = _FakeMsk(wazzup_delivery.WAZZUP_STUCK_DIGEST_HOUR_MSK)
    _run(wazzup_delivery._maybe_stuck_digest())
    assert _sent == [], "пустой день — без «всё хорошо» в чат"


def test_digest_trims_long_list():
    _reset()
    items = [
        {"sent_at_msk": "11:1%d" % (i % 10), "contact_name": f"Клиент {i}",
         "chat_id": f"7900000000{i}", "author_name": "Егор Константинов", "text": "тест"}
        for i in range(wazzup_delivery.WAZZUP_STUCK_DIGEST_MAX_LINES + 7)
    ]
    text = wazzup_delivery._build_stuck_digest(items, overflow=3)
    assert f"как доставлено: {len(items)}" in text
    assert "…и ещё 10 — смотреть в панели" in text, "7 обрезанных + 3 не влезших в очередь"


def test_digest_marks_automation():
    _reset()
    text = wazzup_delivery._build_stuck_digest([
        {"sent_at_msk": "09:30", "contact_name": "Клиент", "chat_id": "79001234567",
         "author_name": "Admin", "text": "Ваш заказ"}
    ])
    assert "автоматика amo" in text


# --- примечание в сделку ----------------------------------------------------

def test_error_writes_note_to_lead():
    _reset()
    _run(_handle(ERROR_TEMPLATE))
    assert len(_notes) == 1, "ошибка доставки должна лечь примечанием в сделку"
    lead_id, text = _notes[0]
    assert lead_id == 12345
    assert "НЕ доставлено клиенту" in text
    assert "UNKNOWN_ERROR" in text
    assert "Что делать" in text
    assert "<b>" not in text, "лента amo HTML не рендерит — примечание плейн-текстом"


def test_stuck_does_not_write_note():
    """Примечание — только про подтверждённую недоставку. «Висит sent» ещё может дойти."""
    _reset()
    _run(_handle(OUT_SENT_WA))
    wazzup_delivery._tracked["wa-sent-1"]["sent_mono"] -= 10_000
    _run(wazzup_delivery._sweep(threshold_s=60))
    assert _notes == []


def test_note_written_even_when_chat_muted():
    """Антиспам глушит ТГ, но не сделку: менеджер должен узнать про своего клиента."""
    _reset()

    async def run_all():
        for i in range(wazzup_delivery.WAZZUP_DELIVERY_BURST_MAX + 3):
            m = dict(ERROR_TEMPLATE["messages"][0])
            m["messageId"] = f"muted-{i}"
            await _handle({"messages": [m]})

    _run(run_all())
    assert len(_notes) == wazzup_delivery.WAZZUP_DELIVERY_BURST_MAX + 3
    assert len(_sent) == wazzup_delivery.WAZZUP_DELIVERY_BURST_MAX + 1


# --- вечерняя сводка --------------------------------------------------------

DAILY = {
    "hours": 24, "outbound": 49, "inbound": 47,
    "by_channel": {
        "whatsapp": {"read": 31, "delivered": 5, "error": 4, "inbound": 15},
        "telegram": {"read": 13, "sent": 2, "inbound": 32},
    },
    "errors": {"UNKNOWN_ERROR": 2, "BAD_CONTACT": 1, "24_HOURS_EXCEEDED": 1},
    "delivery_delay_median_s": 4.2,
}


def test_summary_text():
    text = wazzup_delivery._build_summary(DAILY)
    assert "Доставка Wazzup за сутки" in text
    assert "Исходящих 49" in text
    assert "ошибок 4" in text
    assert "UNKNOWN_ERROR — 2" in text
    assert "4 сек" in text


def test_summary_without_errors():
    text = wazzup_delivery._build_summary({**DAILY, "errors": {}, "delivery_delay_median_s": None})
    assert "Ошибок доставки не было" in text
    assert "пока не посчитать" in text


def test_human_delay():
    assert wazzup_delivery._human_delay(12) == "12 сек"
    assert wazzup_delivery._human_delay(150) == "2.5 мин"
    assert wazzup_delivery._human_delay(7200) == "2.0 ч"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
