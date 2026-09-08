"""Тесты алерта «новый лид в Академии → Гладкову в топик УВЕДОМЛЕНИЯ» (academy_lead_alert).

Что тут проверяется в первую очередь — три места, где такой алерт обычно ломается в бою:
  • ЧУЖИЕ воронки и этапы не должны будить фон (вебхук ходит на каждое изменение сделки);
  • дедуп с окном: эхо вебхука молчит, а честный повторный заход через сутки доходит;
  • лид, уведённый с этапа за время паузы, молчит — и отметка дедупа при этом снимается,
    иначе следующий настоящий заход промолчал бы всё окно.

⚠️ Фон (asyncio.create_task) тут НЕ используется: соседние тест-модули
(test_dup_autoclose, test_unmiss_tag) подменяют asyncio.create_task глобально.
Гейт проверяем на notify_bg со своей заглушкой, отправку — вызовом _apply.

Запуск: python3 -m pytest test_academy_lead_alert.py -q
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
_send_ok = [True]


def _install_stubs():
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id, "thread": message_thread_id})
        return _send_ok[0]

    tg.send_alert = send_alert
    sys.modules["telegram_bot"] = tg


_install_stubs()

import academy_lead_alert  # noqa: E402
from waybill_config import (  # noqa: E402
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_ACADEMY_INBOUND_LEAD,
    STATUS_NEW_LEAD,
)

LEAD_ID = 41200001
STATUS_ACADEMY_COURSE = 70070974  # «Курс» — этап дальше по воронке Академии


def _lead(lead_id=LEAD_ID, pipeline=PIPELINE_ACADEMY, status=STATUS_ACADEMY_INBOUND_LEAD,
          name="Заявка на курс DeFi", contacts=({"id": 1, "is_main": True},)):
    return {
        "id": lead_id,
        "pipeline_id": pipeline,
        "status_id": status,
        "name": name,
        "_embedded": {"contacts": list(contacts)},
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    _sent.clear()
    _send_ok[0] = True
    academy_lead_alert._seen.clear()
    academy_lead_alert._sent_times.clear()
    # Дедуп пишется на диск (постоянный том контейнера) — в тестах во временную папку.
    monkeypatch.setattr(academy_lead_alert, "_SEEN_PATH", str(tmp_path / "seen.json"),
                        raising=False)
    monkeypatch.setattr(academy_lead_alert, "_seen_loaded", False, raising=False)
    monkeypatch.setattr(academy_lead_alert, "_burst_notified", False, raising=False)
    monkeypatch.setattr(academy_lead_alert, "ACADEMY_LEAD_ALERT_ENABLED", True, raising=False)
    monkeypatch.setattr(academy_lead_alert, "ACADEMY_LEAD_ALERT_DELAY_S", 0, raising=False)

    async def contact(contact_id, with_=()):
        return {
            "id": contact_id,
            "name": "Пётр Иванов",
            "custom_fields_values": [
                {"field_id": 413385, "values": [{"value": "+79991234567"}]},
            ],
        }

    monkeypatch.setattr(academy_lead_alert.amo_service, "get_contact_by_id", contact)
    yield
    _sent.clear()
    academy_lead_alert._seen.clear()
    academy_lead_alert._sent_times.clear()


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

    monkeypatch.setattr(academy_lead_alert.asyncio, "create_task", _fake_create_task)
    return fired


def _send(lead, monkeypatch, lead_id=LEAD_ID):
    async def get_lead(lid, with_=()):
        return lead

    monkeypatch.setattr(academy_lead_alert.amo_service, "get_lead_full", get_lead)
    asyncio.run(academy_lead_alert._apply(lead_id))


# ── гейт вебхука: кого будим, кого нет ───────────────────────────────────────

def test_vhodyashchiy_lid_akademii_zavodit_fon(scheduled):
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert len(scheduled) == 1


def test_stroki_iz_vebhuka_tozhe_prohodyat(scheduled):
    """amo шлёт значения формой, то есть строками — сравнение должно это переживать."""
    academy_lead_alert.notify_bg(str(LEAD_ID), str(PIPELINE_ACADEMY),
                                 str(STATUS_ACADEMY_INBOUND_LEAD))
    assert len(scheduled) == 1


def test_drugoy_etap_akademii_ne_zavodit_fon(scheduled):
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT)
    assert scheduled == []


def test_roznica_ne_zavodit_fon(scheduled):
    """Вебхук ходит на каждое изменение любой сделки — розница сюда не относится."""
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    assert scheduled == []


def test_master_flag_glushit(scheduled, monkeypatch):
    monkeypatch.setattr(academy_lead_alert, "ACADEMY_LEAD_ALERT_ENABLED", False)
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert scheduled == []


def test_pustoy_vebhuk_ne_padaet(scheduled):
    academy_lead_alert.notify_bg(None, None, None)
    assert scheduled == []


# ── дедуп с окном ────────────────────────────────────────────────────────────

def test_eho_vebhuka_molchit(scheduled):
    """Два вебхука подряд по одной сделке — фон заводится один раз."""
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert len(scheduled) == 1


def test_dedup_perezhivaet_restart(scheduled, monkeypatch):
    """Список поднимается с диска: пересборка контейнера не должна давать повтор."""
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    academy_lead_alert._seen.clear()
    monkeypatch.setattr(academy_lead_alert, "_seen_loaded", False)
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert len(scheduled) == 1


def test_posle_okna_uvedomlyaem_snova(scheduled):
    """Честный повторный заход на этап через сутки — это новый лид для Саши."""
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    stale = academy_lead_alert._seen[str(LEAD_ID)]
    academy_lead_alert._seen[str(LEAD_ID)] = stale - (
        academy_lead_alert.ACADEMY_LEAD_ALERT_DEDUP_H * 3600 + 60)
    academy_lead_alert.notify_bg(LEAD_ID, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert len(scheduled) == 2


# ── гейт по сделке: пауза между вебхуком и отправкой ─────────────────────────

def test_lid_na_etape_prohodit():
    assert academy_lead_alert.is_inbound_lead(_lead()) is True


def test_uehal_dalshe_po_voronke_ne_prohodit():
    assert academy_lead_alert.is_inbound_lead(_lead(status=STATUS_ACADEMY_COURSE)) is False


def test_chuzhaya_voronka_ne_prohodit():
    assert academy_lead_alert.is_inbound_lead(_lead(pipeline=PIPELINE_CLEVER_MAIN)) is False


def test_net_sdelki_ne_prohodit():
    assert academy_lead_alert.is_inbound_lead(None) is False


def test_uveli_za_pauzu_molchim_i_otmetka_snyata(monkeypatch):
    academy_lead_alert._seen[str(LEAD_ID)] = 1.0
    _send(_lead(status=STATUS_ACADEMY_COURSE), monkeypatch)
    assert _sent == []
    # Отметку сняли: следующий настоящий заход на этап должен дойти.
    assert str(LEAD_ID) not in academy_lead_alert._seen


def test_amo_molchit_otmetka_snyata(monkeypatch):
    """Сделка не прочиталась — не теряем событие насовсем."""
    async def boom(lid, with_=()):
        raise RuntimeError("amo 502")

    academy_lead_alert._seen[str(LEAD_ID)] = 1.0
    monkeypatch.setattr(academy_lead_alert.amo_service, "get_lead_full", boom)
    asyncio.run(academy_lead_alert._apply(LEAD_ID))
    assert _sent == []
    assert str(LEAD_ID) not in academy_lead_alert._seen


def test_telegram_ne_prinyal_otmetka_snyata(monkeypatch):
    academy_lead_alert._seen[str(LEAD_ID)] = 1.0
    _send_ok[0] = False
    _send(_lead(), monkeypatch)
    assert len(_sent) == 1
    assert str(LEAD_ID) not in academy_lead_alert._seen


# ── сообщение ────────────────────────────────────────────────────────────────

def test_soobshchenie_soderzhit_ssylku_i_teg(monkeypatch):
    _send(_lead(), monkeypatch)
    assert len(_sent) == 1
    text = _sent[0]["text"]
    assert "🎓 Новый лид в Академии" in text
    assert "@gladkov_369" in text
    assert f"/leads/detail/{LEAD_ID}" in text
    assert "Открыть сделку" in text
    assert "+79991234567" in text
    assert "Пётр Иванов" in text
    assert "Заявка на курс DeFi" in text
    # Правило Кати 26.08.2026: точки посередине в текстах для людей не ставим.
    assert "·" not in text


def test_uhodit_v_topik_uvedomleniya(monkeypatch):
    _send(_lead(), monkeypatch)
    assert _sent[0]["chat_id"] == -1003680811996
    assert _sent[0]["thread"] == 10479


def test_bez_kontakta_soobshchenie_vse_ravno_uhodit(monkeypatch):
    """Лид из рассылки может прийти без привязанного контакта — молчать нельзя."""
    _send(_lead(contacts=()), monkeypatch)
    assert len(_sent) == 1
    assert f"/leads/detail/{LEAD_ID}" in _sent[0]["text"]


def test_kontakt_ne_prochitalsya_soobshchenie_uhodit(monkeypatch):
    async def boom(contact_id, with_=()):
        raise RuntimeError("amo 502")

    monkeypatch.setattr(academy_lead_alert.amo_service, "get_contact_by_id", boom)
    _send(_lead(), monkeypatch)
    assert len(_sent) == 1


def test_uglovye_skobki_ekraniruyutsya(monkeypatch):
    """parse_mode=HTML: имя с «<» иначе сломает разметку и сообщение не дойдёт."""
    _send(_lead(name="Заявка <курс> DeFi"), monkeypatch)
    assert "&lt;курс&gt;" in _sent[0]["text"]


# ── часовой лимит вместо гейта по возрасту ───────────────────────────────────

def test_massovyy_prognoz_glushitsya_posle_limita(monkeypatch):
    """Перебор лимита: одно предупреждение в чат и тишина до конца часа."""
    monkeypatch.setattr(academy_lead_alert, "ACADEMY_LEAD_ALERT_HOUR_LIMIT", 3)
    for i in range(5):
        academy_lead_alert._seen[str(LEAD_ID + i)] = 1.0
        _send(_lead(lead_id=LEAD_ID + i), monkeypatch, lead_id=LEAD_ID + i)
    # 3 алерта + ОДНО предупреждение о переборе, повторов предупреждения нет.
    assert len(_sent) == 4
    assert "приглушены" in _sent[-1]["text"]
    # Приглушённые лиды не помечены отправленными — вернутся следующим вебхуком.
    assert str(LEAD_ID + 4) not in academy_lead_alert._seen
