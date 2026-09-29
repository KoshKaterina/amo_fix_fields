"""Действие клиента в Академии: одно сообщение на касание, дедуп, своя ветка.

Что держим:
  • на касание уходит ОДНО сообщение, сколько полей изменилось - столько строк внутри;
  • дедуп по паре «поле, значение» в рамках СДЕЛКИ (у склеенного человека контактов два);
  • адрес - ветка «Уведомления академии», а не общий топик отдела (29.09.2026);
  • тег - ответственному, а нет его в карте - Гладкову; фолбэк «вся смена розницы» неверен;
  • событие настраивается с экрана: выключили в панели - не шлём и отметку дедупа снимаем.

⚠️ Дедуп пишется на диск, и путь по умолчанию боевой (`/app/var/...`). Фикстура ниже
уводит его во временную папку. Без неё тесты писали в `C:\\app\\var` на Windows и травили
друг друга: первый прогон оставлял ключ, следующий получал «повтор в окне» вместо отправки.
Поймано 29.09.2026 - я сама на это села и почти записала свой мусор как чужой красный тест.

Запуск: python3 -m pytest test_academy_intent_alert.py -q
"""

import asyncio
import sys
import types

import pytest

tg = types.ModuleType("telegram_bot")


async def _send_alert(*args, **kwargs):
    return True


tg.send_alert = _send_alert
sys.modules.setdefault("telegram_bot", tg)

import academy_intent_alert as alert  # noqa: E402
import alert_settings_client as settings_client  # noqa: E402
import tg_recipients  # noqa: E402
import waybill_config  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_ACADEMY_EVENT_REGISTRATION,
    FIELD_ACADEMY_MANAGER_ACTION,
)

GLADKOV = 11513202
EGOR = 13929334


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    """Дедуп - во временный файл, состояние - с нуля на каждый тест."""
    monkeypatch.setattr(alert, "_SEEN_PATH", str(tmp_path / "seen.json"), raising=False)
    monkeypatch.setattr(alert, "_seen_loaded", False, raising=False)
    alert._seen.clear()
    monkeypatch.setattr(alert, "ACADEMY_CUTOVER_TS", 100, raising=False)
    monkeypatch.setattr(alert, "ACADEMY_INTENT_ALERT_ENABLED", True, raising=False)
    # По умолчанию панель не опрашиваем: сендер шлёт свой текст и свой адрес.
    monkeypatch.setattr(waybill_config, "ALERT_SETTINGS_FROM_PANEL", "off")
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "off")
    yield
    alert._seen.clear()
    settings_client.set_settings_for_tests(None)


def _contact(value="написать менеджеру", field_id=FIELD_ACADEMY_MANAGER_ACTION, name="Анна"):
    return {
        "id": 10,
        "name": name,
        "custom_fields_values": [{"field_id": field_id, "values": [{"value": value}]}],
        "_embedded": {"leads": [{"id": 20}]},
    }


def _lead(responsible=GLADKOV, created_at=101):
    return {"id": 20, "pipeline_id": alert.PIPELINE_ACADEMY,
            "responsible_user_id": responsible, "created_at": created_at}


def _wire(monkeypatch, contact, lead):
    sent = []

    async def get_contact(*a, **k):
        return contact

    async def get_lead(*a, **k):
        return lead

    async def send(text, **kwargs):
        sent.append((text, kwargs))
        return True

    monkeypatch.setattr(alert.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(alert.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(alert.telegram_bot, "send_alert", send)
    return sent


# ── кого будим ──────────────────────────────────────────────────────────────

def test_scheduler_ignores_unrelated_field(monkeypatch):
    called = []
    monkeypatch.setattr(alert.asyncio, "create_task", lambda coro: called.append(coro))
    alert.on_contact_change(1, {123})
    assert called == []


def test_historical_lead_sends_nothing(monkeypatch):
    sent = _wire(monkeypatch, _contact(), _lead(created_at=99))
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "no_academy_lead"
    assert sent == []


def test_empty_changed_value_sends_nothing(monkeypatch):
    """⚠️ Ожидание поправлено 29.09.2026: дедупная версия отвечает «empty», а тест держал
    прежнее «empty_or_failed» - оно осталось от версии до переписывания модуля."""
    contact = {"id": 10, "name": "", "_embedded": {"leads": [{"id": 20}]}}
    sent = _wire(monkeypatch, contact, _lead())
    assert run(alert.process(10, {FIELD_ACADEMY_EVENT_REGISTRATION})) == "empty"
    assert sent == []


# ── адрес и тег ─────────────────────────────────────────────────────────────

def test_uhodit_v_vetku_akademii(monkeypatch):
    """Ветка «Уведомления академии», а не общий топик отдела."""
    monkeypatch.setattr(alert, "ACADEMY_NOTIFY_THREAD", 20518, raising=False)
    sent = _wire(monkeypatch, _contact(), _lead())
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "sent"
    text, kw = sent[0]
    assert kw["chat_id"] == alert.NOTIFY_CHAT_ID
    assert kw["message_thread_id"] == 20518
    assert "написать менеджеру" in text
    assert text.count("<a href=") == 1


def test_bez_nomera_vetki_ostaemsya_v_obshchem_topike(monkeypatch):
    monkeypatch.setattr(alert, "ACADEMY_NOTIFY_THREAD", None, raising=False)
    sent = _wire(monkeypatch, _contact(), _lead())
    run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION}))
    assert sent[0][1]["message_thread_id"] == alert.NOTIFY_THREAD_ID


def test_teg_otvetstvennogo_inache_gladkov(monkeypatch):
    """Ответственный в карте - его ник; нет в карте - Гладков.
    ⚠️ Фолбэк «вся смена РОЗНИЦЫ» здесь неверен: это клиент Академии, розничные
    менеджеры к нему отношения не имеют."""
    sent = _wire(monkeypatch, _contact(), _lead(responsible=EGOR))
    run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION}))
    assert "@egorkonsss" in sent[0][0]
    assert tg_recipients.MANAGERS_ON_SHIFT not in sent[0][0]

    alert._seen.clear()
    sent2 = _wire(monkeypatch, _contact(), _lead(responsible=GLADKOV))
    run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION}))
    assert tg_recipients.ACADEMY_ALERT_TAG in sent2[0][0]


# ── одно сообщение на касание и дедуп ───────────────────────────────────────

def test_dva_polya_odno_soobshchenie(monkeypatch):
    contact = {
        "id": 10, "name": "Анна",
        "custom_fields_values": [
            {"field_id": FIELD_ACADEMY_EVENT_REGISTRATION, "values": [{"value": "Практикум"}]},
            {"field_id": FIELD_ACADEMY_MANAGER_ACTION, "values": [{"value": "написать менеджеру"}]},
        ],
        "_embedded": {"leads": [{"id": 20}]},
    }
    sent = _wire(monkeypatch, contact, _lead())
    run(alert.process(10, {FIELD_ACADEMY_EVENT_REGISTRATION, FIELD_ACADEMY_MANAGER_ACTION}))
    assert len(sent) == 1
    assert "Практикум" in sent[0][0] and "написать менеджеру" in sent[0][0]


def test_povtor_v_okne_molchit(monkeypatch):
    sent = _wire(monkeypatch, _contact(), _lead())
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "sent"
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "duplicate"
    assert len(sent) == 1


# ── управление с экрана панели ──────────────────────────────────────────────

def test_vyklyucheno_v_paneli_ne_shlem_i_otmetku_snimaem(monkeypatch):
    """Выключили событие на экране - молчим. И отметку дедупа СНИМАЕМ: включат обратно,
    следующее честное касание должно дойти, а не молчать до конца окна."""
    monkeypatch.setattr(waybill_config, "ALERT_SETTINGS_FROM_PANEL", "on")
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "on")
    settings_client.set_settings_for_tests(
        {"events": {"academy_intent": {"enabled": False}}, "people": []})
    sent = _wire(monkeypatch, _contact(), _lead())
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "disabled"
    assert sent == []
    assert alert._seen == {}


def test_tekst_i_adres_iz_paneli(monkeypatch):
    """Шаблон и канал из панели действуют, строки собираются по переменной на поле."""
    monkeypatch.setattr(waybill_config, "ALERT_SETTINGS_FROM_PANEL", "on")
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "on")
    monkeypatch.setattr(tg_recipients, "ACADEMY_NOTIFY_THREAD", 20518, raising=False)
    settings_client.set_settings_for_tests({"events": {"academy_intent": {
        "enabled": True, "channel": "op_academy", "recipients_mode": "responsible",
        "template": "🎓 Действие\n{{теги}}\n👤 {{клиент}}\n"
                    "🔹 Запись: {{запись_на_мероприятие}}\n"
                    "🔹 Действие: {{действие_клиента}}\n🔗 {{ссылка_на_сделку}}",
    }}, "people": []})
    sent = _wire(monkeypatch, _contact(), _lead(responsible=EGOR))
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "sent"
    text, kw = sent[0]
    assert kw["chat_id"] == tg_recipients.NOTIFY_CHAT_ID
    assert kw["message_thread_id"] == 20518
    assert "🔹 Действие: написать менеджеру" in text
    # Строка про запись на мероприятие выпала: поле не менялось, значение пустое.
    assert "Запись:" not in text
    assert "@egorkonsss" in text
