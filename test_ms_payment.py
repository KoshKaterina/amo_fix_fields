"""Запись оплаты Ozon в заказ МойСклад (ms_payment.py + вызов из ozon_invoice).

Проверяем без сети и без прода: ms_client, amo_service и telegram_bot подменяются
фикстурой с возвратом (monkeypatch), чужие модули остаются настоящими.

Ключевые инварианты:
  • сухой прогон читает МС, но НЕ пишет: ни платежа, ни примечаний, ни алертов;
  • боевой режим создаёт ровно один paymentin на сумму Ozon с operations/linkedSum;
  • решение идёт по payedSum: оплачен целиком или сумма больше остатка - не пишем;
  • «МС не ответил» (None) не читается как «платежа нет»: ничего не пишем;
  • вебхук и сверка одного платежа дают один и тот же ключ - дубль опознаётся;
  • сбой МС не мешает переводу сделки в «Оплата получена».
"""
import asyncio
import hashlib
import sys
import types

import pytest

# Стаб aiogram-зависимого telegram_bot ставим только на время импорта кода под тестом
# (приём 2 из AGENTS.md): код держит ссылку на заглушку, соседям возвращаем настоящее.
_tb = types.ModuleType("telegram_bot")
_tb.send_alert = None
_prev_tb = sys.modules.get("telegram_bot")
sys.modules["telegram_bot"] = _tb
import amo_service  # noqa: E402
import ms_client  # noqa: E402
import ms_payment  # noqa: E402
import ozon_invoice  # noqa: E402
if _prev_tb is None:
    sys.modules.pop("telegram_bot", None)
else:
    sys.modules["telegram_bot"] = _prev_tb

from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER_MAIN,
    STATUS_LINK_SENT,
    STATUS_PAYMENT_RECEIVED,
)

UU = "0e5a2b05-aaaa-bbbb-cccc-0123456789ab"
LEAD_ID = 777001
EXT = "amo-777001-123"
KEY = "777001-123"
ORG = {"href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/o1", "type": "organization"}
AGENT = {"href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/a1", "type": "counterparty"}
ORDER_META = {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/customerorder/{UU}", "type": "customerorder"}


def run(coro):
    return asyncio.run(coro)


class FakeMS:
    """МойСклад в памяти: один заказ и список платежей. Параметры включают сбои."""

    def __init__(self, total=1164900, payed=1014900, shape_ok=True):
        self.order = {"id": UU, "name": "09386", "sum": total, "payedSum": payed, "meta": ORDER_META}
        if shape_ok:
            self.order["organization"] = {"meta": ORG}
            self.order["agent"] = {"meta": AGENT}
        self.payments: list[dict] = []
        self.posts: list[tuple[str, dict]] = []
        self.gets: list[str] = []
        self.read_fails = False        # get заказа -> None
        self.lookup_fails = False      # get paymentin -> None
        self.post_returns_none = False
        self.post_stores_when_none = False   # «таймаут после исполнения»
        self.post_raises_payed = True
        self.yield_in_get = False

    async def get(self, path, params=None, **kw):
        self.gets.append(path)
        if self.yield_in_get:
            await asyncio.sleep(0)
        if path == f"entity/customerorder/{UU}":
            return None if self.read_fails else dict(self.order)
        if path == "entity/paymentin":
            if self.lookup_fails:
                return None
            code = (params or {}).get("filter", "").split("=", 1)[-1]
            return {"rows": [p for p in self.payments if p["externalCode"] == code]}
        return None

    async def post(self, path, body, **kw):
        self.posts.append((path, body))
        if self.post_returns_none:
            if self.post_stores_when_none:
                self.payments.append(body)
            return None
        self.payments.append(body)
        if self.post_raises_payed:
            self.order["payedSum"] += body["sum"]
        return {"id": "pay-1", **body}


@pytest.fixture
def ms(monkeypatch):
    fake = FakeMS()
    monkeypatch.setattr(ms_client, "get", fake.get)
    monkeypatch.setattr(ms_client, "post", fake.post)
    ms_payment._inflight.clear()
    return fake


# ── чистое решение по суммам ─────────────────────────────────────────────────

def test_decide_po_payedsum():
    assert ms_payment.decide(1164900, 1014900, 150000) == "create"          # ровно остаток (сделка 36591997)
    assert ms_payment.decide(1164900, 1014900, 100000) == "create"          # меньше остатка
    assert ms_payment.decide(1164900, 1014900, 150001) == "skipped-overpay"
    assert ms_payment.decide(1164900, 1164900, 150000) == "skipped-fully-paid"
    assert ms_payment.decide(1164900, 1200000, 1) == "skipped-fully-paid"
    assert ms_payment.decide(0, 0, 100) == "skipped-zero-sum"
    assert ms_payment.decide(1000, 0, 1000) == "create"


def test_kod_platezha_ustoychiv_dlya_vebhuka_i_sverki():
    assert ms_payment.payment_code("777001-123") == "ozon-777001-123"
    # платёж «amo-…», заказ «ord-…» и заказ «<номер МС>_amo-…» - один ключ
    keys = {ozon_invoice._ms_payment_key(*args) for args in (
        ("amo-777001-123",), ("", "ord-777001-123"), ("", "05740_amo-777001-123", "pay-9"),
    )}
    assert keys == {KEY}
    assert ozon_invoice._ms_payment_key("site-order-1") == "site-order-1"
    assert ozon_invoice._ms_payment_key("", "") == ""


# ── ms_payment.record_ozon_payment ───────────────────────────────────────────

def test_suhoy_progon_chitaet_no_ne_pishet(ms):
    outcome, detail = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=True))
    assert outcome == "dry-run", (outcome, detail)
    assert not ms.posts
    assert f"entity/customerorder/{UU}" in ms.gets and "entity/paymentin" in ms.gets
    assert "10149.00 -> 11649.00" in detail, detail


def test_boevoy_rezhim_odin_platezh_na_summu_ozon(ms):
    outcome, detail = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "created", (outcome, detail)
    assert len(ms.posts) == 1
    path, body = ms.posts[0]
    assert path == "entity/paymentin"
    assert body["sum"] == 150000
    assert body["externalCode"] == "ozon-777001-123"
    assert body["organization"] == {"meta": ORG} and body["agent"] == {"meta": AGENT}
    assert body["operations"] == [{"meta": ORDER_META, "linkedSum": 150000}]
    assert "ВНИМАНИЕ" not in detail and "11649.00" in detail
    assert ms.order["payedSum"] == ms.order["sum"]


def test_povtor_togo_zhe_platezha_ne_sozdaet_vtoroy(ms):
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))[0] == "created"
    outcome, _ = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "already-recorded"
    assert len(ms.posts) == 1


def test_zakaz_uzhe_oplachen_celikom_ne_pishem(ms):
    ms.order["payedSum"] = ms.order["sum"]
    outcome, _ = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "skipped-fully-paid" and not ms.posts


def test_summa_bolshe_ostatka_ne_pishem(ms):
    outcome, detail = run(ms_payment.record_ozon_payment(UU, 150001, KEY, dry_run=False))
    assert outcome == "skipped-overpay" and not ms.posts, (outcome, detail)
    assert outcome in ms_payment.ATTENTION


def test_ms_ne_otdal_zakaz_ili_poisk_none_ne_znachit_net_platezha(ms):
    ms.read_fails = True
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))[0] == "failed-ms-read"
    ms.read_fails = False
    ms.lookup_fails = True
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))[0] == "failed-ms-lookup"
    assert not ms.posts


def test_post_bez_otveta_no_dokument_sozdalsya_vtoroy_ne_shlyom(ms):
    ms.post_returns_none = True
    ms.post_stores_when_none = True
    outcome, _ = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "created" and len(ms.posts) == 1


def test_post_bez_otveta_i_dokumenta_net_oshibka(ms):
    ms.post_returns_none = True
    outcome, _ = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "failed-ms-post" and len(ms.posts) == 1


def test_post_bez_otveta_a_proverit_nelzya_oshibka_proverki(ms, monkeypatch):
    ms.post_returns_none = True

    real_get = ms.get
    calls = {"n": 0}

    async def flaky_get(path, params=None, **kw):
        if path == "entity/paymentin":
            calls["n"] += 1
            if calls["n"] > 1:        # первый поиск (до POST) проходит, повторный - нет
                return None
        return await real_get(path, params, **kw)

    monkeypatch.setattr(ms_client, "get", flaky_get)
    outcome, _ = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "failed-ms-lookup"


def test_net_zakaza_net_summy_net_organizacii(ms):
    assert run(ms_payment.record_ozon_payment("", 150000, KEY))[0] == "skipped-no-order"
    assert run(ms_payment.record_ozon_payment("не-uuid", 150000, KEY))[0] == "skipped-no-order"
    assert run(ms_payment.record_ozon_payment(UU, 0, KEY))[0] == "skipped-no-amount"
    assert run(ms_payment.record_ozon_payment(UU, None, KEY))[0] == "skipped-no-amount"
    assert not ms.gets and not ms.posts      # МС вообще не трогали
    ms.order.pop("organization")
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))[0] == "failed-order-shape"
    assert not ms.posts


def test_payedsum_ne_soshelsya_pomechaetsya_no_platezh_ostaetsya(ms):
    ms.post_raises_payed = False
    outcome, detail = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert outcome == "created" and "ВНИМАНИЕ" in detail, (outcome, detail)


def test_parallelnye_vebhuk_i_sverka_odin_post(ms):
    ms.yield_in_get = True

    async def both():
        return await asyncio.gather(
            ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False),
            ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False),
        )

    outcomes = sorted(o for o, _ in run(both()))
    assert outcomes == ["created", "skipped-inflight"], outcomes
    assert len(ms.posts) == 1
    assert not ms_payment._inflight


# ── вызов из ozon_invoice ────────────────────────────────────────────────────

class Amo:
    def __init__(self):
        self.patches, self.notes, self.alerts = [], [], []


@pytest.fixture
def env(monkeypatch, ms):
    """Сделка на «Ссылка отправлена» с заказом МС; амо и телеграм - в память."""
    amo = Amo()
    lead = {
        "id": LEAD_ID, "name": "Заказ №4242", "price": 11649,
        "status_id": STATUS_LINK_SENT, "pipeline_id": PIPELINE_CLEVER_MAIN,
        "responsible_user_id": 11513202,
        "custom_fields_values": [{"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": UU}]}],
    }

    async def get_lead_full(lead_id, with_=()):
        return lead

    async def patch_lead(lead_id, **kw):
        amo.patches.append(kw)
        return {"ok": True, "status_code": 200}

    async def add_note(lead_id, text):
        amo.notes.append(text)
        return {"ok": True}

    async def send_alert(text, **kw):
        amo.alerts.append(text)
        return True

    monkeypatch.setattr(amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(amo_service, "add_note", add_note)
    monkeypatch.setattr(ozon_invoice.telegram_bot, "send_alert", send_alert)
    monkeypatch.setattr(ozon_invoice, "OZON_PAY_ACCESS_KEY", "AK-test")
    monkeypatch.setattr(ozon_invoice, "OZON_PAY_NOTIFICATION_SECRET_KEY", "NS-test")
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_ENABLED", True)
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", True)
    ozon_invoice._paid_recent.clear()
    ozon_invoice._failed_recent.clear()
    amo.lead = lead
    return amo


def notif(ext=EXT, amount="150000"):
    d = {"extTransactionID": ext, "status": "Completed", "amount": amount,
         "currencyCode": "643", "operationType": "Payment", "paymentMethod": "SBP"}
    d["requestSign"] = hashlib.sha256(f"AK-test|||{ext}|{amount}|643|NS-test".encode()).hexdigest()
    return d


def moved(amo):
    return [p for p in amo.patches if p.get("status_id") == STATUS_PAYMENT_RECEIVED]


def test_vebhuk_suhoy_progon_sdelka_edet_ms_ne_pishetsya(env, ms):
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(moved(env)) == 1
    assert not ms.posts and not ms.payments
    assert f"entity/customerorder/{UU}" in ms.gets
    assert len(env.notes) == 1 and not env.alerts       # только штатное «Оплата подтверждена Ozon»


def test_vebhuk_boevoy_rezhim_odin_platezh_s_kluchom_scheta(env, ms, monkeypatch):
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", False)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(ms.posts) == 1
    body = ms.posts[0][1]
    assert body["sum"] == 150000 and body["externalCode"] == "ozon-777001-123"
    assert not env.alerts and len(env.notes) == 1


def test_sverka_togo_zhe_platezha_posle_vebhuka_dubl_ne_sozdaet(env, ms, monkeypatch):
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", False)
    run(ozon_invoice._handle_notification(notif()))
    # сверка видит тот же счёт под другими идентификаторами; сделка уже дальше
    env.lead["status_id"] = STATUS_PAYMENT_RECEIVED
    run(ozon_invoice._mark_paid(
        env.lead, "1500", "extId amo-777001-123", "сверка",
        kopecks=150000, ext_key=ozon_invoice._ms_payment_key(EXT, "ord-777001-123", "pay-9"),
    ))
    assert len(ms.posts) == 1, ms.posts


def test_flag_vyklyuchen_ms_ne_trogaem_sovsem(env, ms, monkeypatch):
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_ENABLED", False)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert not ms.gets and not ms.posts


def test_staryy_vyzov_bez_summy_ms_ne_trogaem(env, ms):
    assert run(ozon_invoice._mark_paid(env.lead, "1500", "extId x", "вебхук")) == "moved"
    assert not ms.gets and not ms.posts


def test_boevoy_sboy_ms_sdelka_vse_ravno_edet_chelovek_uvedomlen_odin_raz(env, ms, monkeypatch):
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", False)
    ms.read_fails = True
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(moved(env)) == 1 and not ms.posts
    assert any("не записана" in n for n in env.notes), env.notes
    assert len(env.alerts) == 1, env.alerts
    # тот же сбой по той же сделке в течение часа - без повторных примечания и алерта
    ozon_invoice._paid_recent.clear()
    run(ozon_invoice._handle_notification(notif(ext="amo-777001-124")))
    assert len([n for n in env.notes if "не записана" in n]) == 1
    assert len(env.alerts) == 1


def test_suhoy_progon_sboy_ms_tolko_log_bez_primechaniy_i_alertov(env, ms):
    ms.read_fails = True
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(env.notes) == 1 and not env.alerts


def test_pereplata_boevoy_rezhim_zovyom_cheloveka_platezh_ne_pishem(env, ms, monkeypatch):
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", False)
    ms.order["payedSum"] = 1164900 - 100000      # остаток 1000 ₽, Ozon заплатил 1500 ₽
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert not ms.posts
    assert any("skipped-overpay" in n for n in env.notes) and len(env.alerts) == 1


def test_isklyuchenie_v_ms_payment_ne_lomaet_perevod_sdelki(env, ms, monkeypatch):
    async def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(ms_payment, "record_ozon_payment", boom)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(moved(env)) == 1


def test_sdelka_uzhe_dalshe_platezh_vse_ravno_pishem(env, ms, monkeypatch):
    """Деньги получены независимо от этапа сделки: ветка «noted» не должна терять платёж."""
    monkeypatch.setattr(ozon_invoice, "OZON_MS_PAYMENT_DRY_RUN", False)
    env.lead["status_id"] = 142
    assert run(ozon_invoice._handle_notification(notif())) == "noted"
    assert len(ms.posts) == 1
