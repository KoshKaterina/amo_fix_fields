"""Тесты алерта «новый заказ с самовывозом → записать в шоурум» (showroom_alert).

Проверяем то, ради чего фича: срабатывает на оба наших самовывоза, молчит на
доставке и на «CDEK: Самовывоз», не дублирует на эхо-вебхуках, тегает Катю и
даёт ссылку на сделку.

⚠️ Фон (asyncio.create_task) в этих тестах НЕ используется: соседние тест-модули
(test_dup_autoclose, test_unmiss_tag) подменяют asyncio.create_task глобально, и
при общем прогоне настоящие задачи не создаются. Поэтому гейт проверяем на
notify_bg со своей заглушкой, а отправку — вызовом _apply напрямую.

Запуск: python3 -m pytest test_showroom_alert.py -q
"""

import asyncio
import os
import sys
import types

import pytest

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

import showroom_alert  # noqa: E402
import tg_recipients  # noqa: E402

LEAD_ID = 36600001


async def _fake_lead(lead_id, with_=()):
    return {
        "id": lead_id, "price": 42000,
        "custom_fields_values": [
            {"field_id": 577313, "values": [{"value": "Keystone 3 Pro — 1 шт."}]},
            {"field_id": 577315, "values": [{"value": "Самовывоз из офиса Sunscrypt"}]},
        ],
        "_embedded": {"contacts": [{"id": 1, "name": "Пётр Иванов"}]},
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    _sent.clear()
    showroom_alert._seen_leads.clear()
    showroom_alert._seen_order.clear()
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_THREAD_ID", 777, raising=False)
    monkeypatch.setattr(showroom_alert.amo_service, "get_lead_full", _fake_lead)
    yield
    _sent.clear()
    showroom_alert._seen_leads.clear()
    showroom_alert._seen_order.clear()


@pytest.fixture
def scheduled(monkeypatch):
    """Считает, сколько раз notify_bg завёл фон (саму корутину не исполняем)."""
    fired: list = []

    class _Task:
        def add_done_callback(self, cb):
            pass

    def _fake_create_task(coro):
        coro.close()
        fired.append(True)
        return _Task()

    monkeypatch.setattr(showroom_alert.asyncio, "create_task", _fake_create_task)
    return fired


# ── гейт по типу доставки ────────────────────────────────────────────────────

@pytest.mark.parametrize("delivery", [
    "Самовывоз из офиса Sunscrypt",
    "самовывоз из офиса sunscrypt",
    "Самовывоз из Шоурума",
])
def test_nash_samovyvoz_triggerit(delivery):
    assert showroom_alert.is_pickup(delivery) is True


@pytest.mark.parametrize("delivery", [
    "CDEK: Самовывоз",
    "CDEK: Курьером до двери",
    "Курьером по Москве",
    "Почта России",
    "",
    None,
])
def test_dostavka_ne_triggerit(delivery):
    assert showroom_alert.is_pickup(delivery) is False


# ── гейт notify_bg ───────────────────────────────────────────────────────────

def test_samovyvoz_zavodit_fon(scheduled):
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert len(scheduled) == 1


def test_dostavka_fon_ne_zavodit(scheduled):
    showroom_alert.notify_bg("CDEK: Самовывоз", LEAD_ID)
    assert scheduled == []


def test_eho_vebhuka_ne_dublit(scheduled):
    """Поле корзины обновляется несколько раз — сообщение должно уйти одно."""
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert len(scheduled) == 1


def test_raznye_sdelki_obe_prohodyat(scheduled):
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", 1)
    showroom_alert.notify_bg("Самовывоз из Шоурума", 2)
    assert len(scheduled) == 2


def test_bez_topika_fon_ne_zavodim(scheduled, monkeypatch):
    """Топик не настроен → молчим и пишем в лог, а не сыпем в General супергруппы."""
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_THREAD_ID", None, raising=False)
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert scheduled == []


def test_bez_lead_id_fon_ne_zavodim(scheduled):
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", None)
    assert scheduled == []


# ── само сообщение ───────────────────────────────────────────────────────────

def test_alert_uhodit_v_topik_shourum_s_tegom_kati():
    asyncio.run(showroom_alert._apply(LEAD_ID, "Самовывоз из офиса Sunscrypt"))

    assert len(_sent) == 1
    msg = _sent[0]
    assert msg["chat_id"] == tg_recipients.NOTIFY_CHAT_ID
    assert msg["thread"] == 777
    assert tg_recipients.SHOWROOM_ALERT_TAG in msg["text"]
    assert "Пётр Иванов" in msg["text"]
    assert "Keystone 3 Pro" in msg["text"]
    assert f"leads/detail/{LEAD_ID}" in msg["text"]


def test_sdelka_ne_prochitalas_soobshenie_vsyo_ravno_uhodit(monkeypatch):
    """amo не отдал сделку → шлём короткий алерт со ссылкой, а не молчим."""
    async def no_lead(lead_id, with_=()):
        return None

    monkeypatch.setattr(showroom_alert.amo_service, "get_lead_full", no_lead)
    asyncio.run(showroom_alert._apply(LEAD_ID, "Самовывоз из Шоурума"))

    assert len(_sent) == 1
    assert tg_recipients.SHOWROOM_ALERT_TAG in _sent[0]["text"]
    assert "Самовывоз из Шоурума" in _sent[0]["text"]


def test_html_ekraniruetsya(monkeypatch):
    """parse_mode=HTML: угловые скобки в имени клиента не должны ломать разметку."""
    async def sharp_lead(lead_id, with_=()):
        return {"id": lead_id, "price": 0, "custom_fields_values": [],
                "_embedded": {"contacts": [{"id": 1, "name": "<b>Вася</b>"}]}}

    monkeypatch.setattr(showroom_alert.amo_service, "get_lead_full", sharp_lead)
    asyncio.run(showroom_alert._apply(LEAD_ID, "Самовывоз из офиса Sunscrypt"))

    assert "&lt;b&gt;Вася&lt;/b&gt;" in _sent[0]["text"]
