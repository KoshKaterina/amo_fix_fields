"""Тесты алерта «новый заказ с самовывозом → записать в шоурум» (showroom_alert).

Половина тестов написана по РЕАЛЬНОМУ разбору 07.08.2026: фича ушла в бой и
насыпала в топик пачку сообщений по чужим сделкам. Что тогда прошло сквозь гейт
и теперь ловится тестами:
  • закрытые сделки из воронки «Тест» (10.07-29.07), которым чужой прогон
    переписал корзину;
  • сделки в Офисе и в работе, куда алерт вообще не относится;
  • сообщение без товара — состав ещё не успел записаться;
  • пустое имя клиента: во вложенных контактах amo отдаёт только id и is_main.

⚠️ Фон (asyncio.create_task) тут НЕ используется: соседние тест-модули
(test_dup_autoclose, test_unmiss_tag) подменяют asyncio.create_task глобально.
Гейт проверяем на notify_bg со своей заглушкой, отправку — вызовом _apply.

Запуск: python3 -m pytest test_showroom_alert.py -q
"""

import asyncio
import os
import sys
import time
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
from waybill_config import (  # noqa: E402
    PIPELINE_CLEVER_MAIN,
    STATUS_NEW_LEAD,
    STATUS_NEW_LEAD_BUFFERS,
)

LEAD_ID = 36600001
PIPELINE_OFFICE_ANY = 10593103
STATUS_IN_WORK = 83537858
STATUS_CLOSED_LOST = 143


def _lead(
    lead_id=LEAD_ID,
    pipeline=PIPELINE_CLEVER_MAIN,
    status=STATUS_NEW_LEAD,
    age_min=1,
    delivery="Самовывоз из офиса Sunscrypt, 1 шт, 0.00 рублей",
    composition="Keystone 3 Pro, 1 шт, 15 990.00 рублей",
    price=15990,
    contacts=({"id": 1, "is_main": True},),
):
    fields = []
    if composition is not None:
        fields.append({"field_id": 577313, "values": [{"value": composition}]})
    if delivery is not None:
        fields.append({"field_id": 577315, "values": [{"value": delivery}]})
    return {
        "id": lead_id,
        "pipeline_id": pipeline,
        "status_id": status,
        "created_at": int(time.time()) - age_min * 60,
        "price": price,
        "custom_fields_values": fields,
        "_embedded": {"contacts": list(contacts)},
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    _sent.clear()
    showroom_alert._seen_leads.clear()
    showroom_alert._seen_order.clear()
    # Дедуп пишется на диск (постоянный том контейнера) — в тестах во временную папку.
    monkeypatch.setattr(showroom_alert, "_SEEN_PATH", str(tmp_path / "seen.json"),
                        raising=False)
    monkeypatch.setattr(showroom_alert, "_seen_loaded", False, raising=False)
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_THREAD_ID", 4083, raising=False)
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_ENABLED", True, raising=False)
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_DELAY_S", 0, raising=False)

    async def contact(contact_id):
        return {"id": contact_id, "name": "Пётр Иванов"}

    monkeypatch.setattr(showroom_alert.amo_service, "get_contact_by_id", contact)
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


def _send(lead, monkeypatch, delivery="Самовывоз из офиса Sunscrypt"):
    async def get_lead(lead_id, with_=()):
        return lead

    monkeypatch.setattr(showroom_alert.amo_service, "get_lead_full", get_lead)
    asyncio.run(showroom_alert._apply(lead.get("id") if lead else LEAD_ID, delivery))


# ── гейт по типу доставки ────────────────────────────────────────────────────

@pytest.mark.parametrize("delivery", [
    "Самовывоз из офиса Sunscrypt",
    "самовывоз из офиса sunscrypt",
    "Самовывоз из Шоурума",
])
def test_nash_samovyvoz_triggerit(delivery):
    assert showroom_alert.is_pickup(delivery) is True


@pytest.mark.parametrize("delivery", [
    "CDEK: Самовывоз, (1-2 дней), 1 шт, 219.00 рублей",
    "CDEK: Курьером до двери",
    "Курьером по Москве",
    "Почта России",
    "",
    None,
])
def test_dostavka_ne_triggerit(delivery):
    assert showroom_alert.is_pickup(delivery) is False


# ── гейт «свежая заявка» (то, чего не хватило в бою 07.08) ───────────────────

def test_svezhaya_zayavka_prohodit():
    assert showroom_alert.is_fresh_new_lead(_lead()) is True


@pytest.mark.parametrize("buffer_status", STATUS_NEW_LEAD_BUFFERS)
def test_bufernye_etapy_tozhe_prohodyat(buffer_status):
    assert showroom_alert.is_fresh_new_lead(_lead(status=buffer_status)) is True


def test_chuzhaya_voronka_ne_prohodit():
    """Сделка из Офиса/Теста - не наш случай, даже если самовывоз."""
    assert showroom_alert.is_fresh_new_lead(_lead(pipeline=PIPELINE_OFFICE_ANY)) is False


def test_sdelka_v_rabote_ne_prohodit():
    assert showroom_alert.is_fresh_new_lead(_lead(status=STATUS_IN_WORK)) is False


def test_zakrytaya_sdelka_ne_prohodit():
    assert showroom_alert.is_fresh_new_lead(_lead(status=STATUS_CLOSED_LOST)) is False


def test_staraya_sdelka_ne_prohodit():
    """Бой 07.08: чужой прогон переписал корзину сделкам от 10-29.07."""
    assert showroom_alert.is_fresh_new_lead(_lead(age_min=60 * 24 * 28)) is False


def test_net_sdelki_ne_prohodit():
    assert showroom_alert.is_fresh_new_lead(None) is False


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


def test_dedup_perezhivaet_restart(scheduled, monkeypatch):
    """Бой 07.08: пересборка контейнера обнулила память, и по заказу 45-минутной
    давности ушёл второй алерт. Список уведомлённых лежит на диске."""
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert len(scheduled) == 1

    # «Рестарт»: память процесса чистая, файл на месте.
    showroom_alert._seen_leads.clear()
    showroom_alert._seen_order.clear()
    monkeypatch.setattr(showroom_alert, "_seen_loaded", False, raising=False)

    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert len(scheduled) == 1


def test_bitiy_fayl_dedupa_ne_lomaet_alert(scheduled, monkeypatch, tmp_path):
    """Файл повреждён → начинаем дедуп с нуля, но алерты идут."""
    bad = tmp_path / "broken.json"
    bad.write_text("{не json", encoding="utf-8")
    monkeypatch.setattr(showroom_alert, "_SEEN_PATH", str(bad), raising=False)
    monkeypatch.setattr(showroom_alert, "_seen_loaded", False, raising=False)

    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert len(scheduled) == 1


def test_master_flag_gasit(scheduled, monkeypatch):
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_ENABLED", False, raising=False)
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert scheduled == []


def test_bez_topika_fon_ne_zavodim(scheduled, monkeypatch):
    """Топик не настроен → молчим и пишем в лог, а не сыпем в General супергруппы."""
    monkeypatch.setattr(showroom_alert, "SHOWROOM_ALERT_THREAD_ID", None, raising=False)
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", LEAD_ID)
    assert scheduled == []


def test_bez_lead_id_fon_ne_zavodim(scheduled):
    showroom_alert.notify_bg("Самовывоз из офиса Sunscrypt", None)
    assert scheduled == []


# ── само сообщение ───────────────────────────────────────────────────────────

def test_alert_uhodit_v_topik_shourum_s_tegom_kati(monkeypatch):
    _send(_lead(), monkeypatch)

    assert len(_sent) == 1
    msg = _sent[0]
    assert msg["chat_id"] == tg_recipients.NOTIFY_CHAT_ID
    assert msg["thread"] == 4083
    assert tg_recipients.SHOWROOM_ALERT_TAG in msg["text"]
    assert "Пётр Иванов" in msg["text"]
    assert "Keystone 3 Pro" in msg["text"]
    assert f"leads/detail/{LEAD_ID}" in msg["text"]


def test_imya_klienta_dochityvaetsya_otdelnym_zaprosom(monkeypatch):
    """Бой 07.08: имени не было, потому что во вложенном контакте только id."""
    asked: list = []

    async def contact(contact_id):
        asked.append(contact_id)
        return {"id": contact_id, "name": "Мария Кузнецова"}

    monkeypatch.setattr(showroom_alert.amo_service, "get_contact_by_id", contact)
    _send(_lead(contacts=({"id": 777, "is_main": True},)), monkeypatch)

    assert asked == [777]
    assert "Мария Кузнецова" in _sent[0]["text"]


def test_starye_sdelki_v_topik_ne_letyat(monkeypatch):
    """Тот самый спам: закрытая сделка из воронки «Тест» от 10.07."""
    _send(_lead(pipeline=PIPELINE_OFFICE_ANY, status=STATUS_CLOSED_LOST,
                age_min=60 * 24 * 28), monkeypatch)
    assert _sent == []


def test_dostavka_pomenyalas_poka_zhdali(monkeypatch):
    """За время паузы клиент/менеджер сменил доставку на СДЭК → не шлём."""
    _send(_lead(delivery="CDEK: Самовывоз, (1-2 дней), 1 шт, 219.00 рублей"), monkeypatch)
    assert _sent == []


def test_sdelka_ne_prochitalas_molchim(monkeypatch):
    """amo не отдал сделку → проверить гейты нечем, лучше промолчать."""
    _send(None, monkeypatch)
    assert _sent == []


def test_bez_sostava_stroka_ne_vyvoditsya(monkeypatch):
    _send(_lead(composition=None), monkeypatch)
    assert len(_sent) == 1
    assert "📦" not in _sent[0]["text"]


def test_html_ekraniruetsya(monkeypatch):
    """parse_mode=HTML: угловые скобки в имени клиента не должны ломать разметку."""
    async def contact(contact_id):
        return {"id": contact_id, "name": "<b>Вася</b>"}

    monkeypatch.setattr(showroom_alert.amo_service, "get_contact_by_id", contact)
    _send(_lead(), monkeypatch)

    assert "&lt;b&gt;Вася&lt;/b&gt;" in _sent[0]["text"]
