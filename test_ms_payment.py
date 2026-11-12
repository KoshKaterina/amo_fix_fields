"""Запись оплаты в заказ МойСклад: ms_payment.py (логика) и вызов из ozon_invoice.

Без сети и прода: ms_client, amo_service и telegram_bot подменяются фикстурой с возвратом
(monkeypatch), чужие модули остаются настоящими. Уведомления, очередь повторов и триггер
«Успешно реализовано» Офиса - в test_ms_payment_flow.py.

Ключевые инварианты:
  • сухой прогон читает МС, но НЕ пишет: ни платежа, ни примечаний, ни алертов;
  • Ozon: пишем строго ту сумму, что оплатил клиент, при ЛЮБОМ соотношении с суммой заказа,
    кроме «заказ уже оплачен целиком» (там не пишем);
  • «МС не ответил» (None) не читается как «платежа нет»: ничего не пишем;
  • вебхук и сверка одного платежа дают один и тот же ключ - дубль опознаётся;
  • сбой МС не мешает переводу сделки в «Оплата получена»;
  • оплата при выдаче пишет остаток до суммы, а не всю сумму (онлайн-оплаченный заказ - молча).
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
import ms_payment_flow  # noqa: E402
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

def test_decide_po_nakoplennomu_itogu():
    d = ms_payment.decide
    assert d(1164900, 1014900, 150000) == "created"            # итог ровно сумма заказа (сделка 36591997)
    assert d(1164900, 0, 1164900) == "created"                 # заказ без предоплаты, оплачен целиком Ozon
    assert d(1164900, 1014900, 100000) == "created-short"      # недоплата
    assert d(1164900, 0, 100) == "created-short"
    assert d(1164900, 1014900, 150001) == "created-over"       # переплата
    assert d(1164900, 1164900, 150000) == "skipped-fully-paid"  # заказ уже оплачен целиком
    assert d(1164900, 1200000, 1) == "skipped-fully-paid"
    assert d(0, 0, 100) == "skipped-zero-sum"
    assert d(-5, 0, 100) == "skipped-zero-sum"


def test_kod_platezha_ustoychiv_dlya_vebhuka_i_sverki():
    assert ms_payment.payment_code("777001-123") == "ozon-777001-123"
    assert ms_payment.office_code(36590681) == "office-36590681"
    # платёж «amo-…», заказ «ord-…» и заказ «<номер МС>_amo-…» - один ключ
    keys = {ozon_invoice._ms_payment_key(*args) for args in (
        ("amo-777001-123",), ("", "ord-777001-123"), ("", "05740_amo-777001-123", "pay-9"),
    )}
    assert keys == {KEY}
    assert ozon_invoice._ms_payment_key("site-order-1") == "site-order-1"
    assert ozon_invoice._ms_payment_key("", "") == ""


# ── ms_payment.record_ozon_payment ───────────────────────────────────────────

def test_suhoy_progon_chitaet_no_ne_pishet(ms):
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=True))
    assert res.outcome == "created" and res.dry_run is True, res
    assert not ms.posts
    assert f"entity/customerorder/{UU}" in ms.gets and "entity/paymentin" in ms.gets
    assert "итог 11649.00" in res.detail, res.detail


def test_boevoy_rezhim_odin_platezh_na_summu_ozon(ms):
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "created" and res.dry_run is False, res
    assert len(ms.posts) == 1
    path, body = ms.posts[0]
    assert path == "entity/paymentin"
    assert body["sum"] == 150000
    assert body["externalCode"] == "ozon-777001-123"
    assert body["organization"] == {"meta": ORG} and body["agent"] == {"meta": AGENT}
    assert body["operations"] == [{"meta": ORDER_META, "linkedSum": 150000}]
    assert "ВНИМАНИЕ" not in res.detail and "итог 11649.00" in res.detail
    assert ms.order["payedSum"] == ms.order["sum"]
    assert res.facts == {"order": "09386", "sum": 11649.0, "payed": 10149.0, "paid": 1500.0, "after": 11649.0}


def test_nedoplata_platezh_zapisan_iskhod_short(ms):
    res = run(ms_payment.record_ozon_payment(UU, 100000, KEY, dry_run=False))
    assert res.outcome == "created-short" and len(ms.posts) == 1
    assert ms.posts[0][1]["sum"] == 100000                      # строго то, что оплатил клиент
    assert ms.order["payedSum"] < ms.order["sum"]


def test_pereplata_platezh_zapisan_iskhod_over_strogo_summa_ozon(ms):
    res = run(ms_payment.record_ozon_payment(UU, 200000, KEY, dry_run=False))
    assert res.outcome == "created-over" and len(ms.posts) == 1
    assert ms.posts[0][1]["sum"] == 200000 and ms.posts[0][1]["operations"][0]["linkedSum"] == 200000


def test_povtor_togo_zhe_platezha_ne_sozdaet_vtoroy(ms):
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "created"
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "already-recorded"
    assert len(ms.posts) == 1


def test_zakaz_uzhe_oplachen_celikom_ne_pishem(ms):
    ms.order["payedSum"] = ms.order["sum"]
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "skipped-fully-paid" and not ms.posts


def test_summa_zakaza_nol_ne_pishem(ms):
    ms.order["sum"] = 0
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "skipped-zero-sum" and not ms.posts


def test_ms_ne_otdal_zakaz_ili_poisk_none_ne_znachit_net_platezha(ms):
    ms.read_fails = True
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "failed-ms-read"
    ms.read_fails = False
    ms.lookup_fails = True
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "failed-ms-lookup"
    assert not ms.posts
    assert {"failed-ms-read", "failed-ms-lookup", "failed-ms-post"} <= ms_payment.RETRIABLE


def test_post_bez_otveta_no_dokument_sozdalsya_vtoroy_ne_shlyom(ms):
    ms.post_returns_none = True
    ms.post_stores_when_none = True
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "created" and len(ms.posts) == 1


def test_post_bez_otveta_i_dokumenta_net_oshibka(ms):
    ms.post_returns_none = True
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "failed-ms-post" and len(ms.posts) == 1


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
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "failed-ms-lookup"


def test_net_zakaza_net_summy_net_organizacii(ms):
    assert run(ms_payment.record_ozon_payment("", 150000, KEY)).outcome == "skipped-no-order"
    assert run(ms_payment.record_ozon_payment("не-uuid", 150000, KEY)).outcome == "skipped-no-order"
    assert run(ms_payment.record_ozon_payment(UU, 0, KEY)).outcome == "skipped-no-amount"
    assert run(ms_payment.record_ozon_payment(UU, None, KEY)).outcome == "skipped-no-amount"
    assert not ms.gets and not ms.posts      # МС вообще не трогали
    ms.order.pop("organization")
    assert run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False)).outcome == "failed-order-shape"
    assert not ms.posts
    assert "failed-order-shape" not in ms_payment.RETRIABLE      # данные не починятся повтором


def test_payedsum_ne_soshelsya_pomechaetsya_no_platezh_ostaetsya(ms):
    ms.post_raises_payed = False
    res = run(ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False))
    assert res.outcome == "created" and "ВНИМАНИЕ" in res.detail, res


def test_parallelnye_vebhuk_i_sverka_odin_post(ms):
    ms.yield_in_get = True

    async def both():
        return await asyncio.gather(
            ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False),
            ms_payment.record_ozon_payment(UU, 150000, KEY, dry_run=False),
        )

    outcomes = sorted(r.outcome for r in run(both()))
    assert outcomes == ["created", "skipped-inflight"], outcomes
    assert len(ms.posts) == 1
    assert not ms_payment._inflight


# ── оплата при выдаче: остаток до суммы ──────────────────────────────────────

def test_vydacha_bez_predoplaty_pishem_vsyu_summu(ms):
    ms.order["payedSum"] = 0
    res = run(ms_payment.record_office_payment(UU, 36590681, label="наложка", dry_run=False))
    assert res.outcome == "created", res
    body = ms.posts[0][1]
    assert body["sum"] == 1164900 and body["externalCode"] == "office-36590681"
    assert body["operations"] == [{"meta": ORDER_META, "linkedSum": 1164900}]
    assert "наложка" in body["description"] and "36590681" in body["description"]
    assert ms.order["payedSum"] == ms.order["sum"]


def test_vydacha_s_chastichnoy_predoplatoy_pishem_tolko_ostatok(ms):
    res = run(ms_payment.record_office_payment(UU, 1, dry_run=False))     # payedSum 10149 из 11649
    assert res.outcome == "created" and ms.posts[0][1]["sum"] == 150000


def test_vydacha_zakaz_uzhe_oplachen_onlayn_molcha_bez_zapisi(ms):
    ms.order["payedSum"] = ms.order["sum"]
    res = run(ms_payment.record_office_payment(UU, 1, dry_run=False))
    assert res.outcome == "skipped-fully-paid" and not ms.posts


def test_vydacha_suhoy_progon_i_povtor(ms):
    ms.order["payedSum"] = 0
    res = run(ms_payment.record_office_payment(UU, 1, dry_run=True))
    assert res.outcome == "created" and res.dry_run and not ms.posts
    assert run(ms_payment.record_office_payment(UU, 1, dry_run=False)).outcome == "created"
    assert run(ms_payment.record_office_payment(UU, 1, dry_run=False)).outcome == "already-recorded"
    assert len(ms.posts) == 1


def test_vydacha_net_zakaza_nulevaya_summa_sboy_ms(ms):
    assert run(ms_payment.record_office_payment("", 1)).outcome == "skipped-no-order"
    ms.order["sum"] = 0
    assert run(ms_payment.record_office_payment(UU, 1, dry_run=False)).outcome == "skipped-zero-sum"
    ms.order["sum"] = 1164900
    ms.read_fails = True
    assert run(ms_payment.record_office_payment(UU, 1, dry_run=False)).outcome == "failed-ms-read"
    assert not ms.posts


# ── вызов из ozon_invoice ────────────────────────────────────────────────────

class Amo:
    def __init__(self):
        self.patches, self.notes, self.alerts = [], [], []


@pytest.fixture
def env(monkeypatch, ms, tmp_path):
    """Сделка на «Ссылка отправлена» с заказом МС; амо и телеграм - в память."""
    amo = Amo()
    lead = {
        "id": LEAD_ID, "name": "Заказ №4242", "price": 11649,
        "status_id": STATUS_LINK_SENT, "pipeline_id": PIPELINE_CLEVER_MAIN,
        "responsible_user_id": 11513202,
        "custom_fields_values": [{"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": UU}]}],
    }
    amo.invoice_note = "Счёт СБП создан автоматически: 1500 ₽ (поле «Другая сумма»), действителен 24 ч.\nextId amo-777001-123"

    async def get_lead_full(lead_id, with_=()):
        return lead

    async def patch_lead(lead_id, **kw):
        amo.patches.append(kw)
        return {"ok": True, "status_code": 200}

    async def add_note(lead_id, text):
        amo.notes.append(text)
        return {"ok": True}

    async def get_lead_notes(lead_id, limit=100):
        return [{"params": {"text": amo.invoice_note}, "created_at": 1}]

    async def send_alert(text, **kw):
        amo.alerts.append((text, kw))
        return True

    monkeypatch.setattr(amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(amo_service, "add_note", add_note)
    monkeypatch.setattr(amo_service, "get_lead_notes", get_lead_notes)
    monkeypatch.setattr(ms_payment_flow.telegram_bot, "send_alert", send_alert)
    monkeypatch.setattr(ozon_invoice.telegram_bot, "send_alert", send_alert)
    monkeypatch.setattr(ozon_invoice, "OZON_PAY_ACCESS_KEY", "AK-test")
    monkeypatch.setattr(ozon_invoice, "OZON_PAY_NOTIFICATION_SECRET_KEY", "NS-test")
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_ENABLED", True)
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_DRY_RUN", True)
    monkeypatch.setattr(ms_payment_flow, "MS_PAYMENT_PENDING_PATH", str(tmp_path / "pending.json"))
    ozon_invoice._paid_recent.clear()
    ozon_invoice._failed_recent.clear()
    ms_payment_flow._alerted_recent.clear()
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


def test_vebhuk_boevoy_rezhim_idealnyy_sluchay_tiho(env, ms, monkeypatch):
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_DRY_RUN", False)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(ms.posts) == 1
    body = ms.posts[0][1]
    assert body["sum"] == 150000 and body["externalCode"] == "ozon-777001-123"
    assert not env.alerts and len(env.notes) == 1       # итог равен сумме заказа - тихая оплата


def test_sverka_togo_zhe_platezha_posle_vebhuka_dubl_ne_sozdaet(env, ms, monkeypatch):
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_DRY_RUN", False)
    run(ozon_invoice._handle_notification(notif()))
    # сверка видит тот же счёт под другими идентификаторами; сделка уже дальше
    env.lead["status_id"] = STATUS_PAYMENT_RECEIVED
    run(ozon_invoice._mark_paid(
        env.lead, "1500", "extId amo-777001-123", "сверка",
        kopecks=150000, ext_key=ozon_invoice._ms_payment_key(EXT, "ord-777001-123", "pay-9"),
    ))
    assert len(ms.posts) == 1, ms.posts


def test_flag_vyklyuchen_ms_ne_trogaem_sovsem(env, ms, monkeypatch):
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_ENABLED", False)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert not ms.gets and not ms.posts


def test_staryy_vyzov_bez_summy_ms_ne_trogaem(env, ms):
    assert run(ozon_invoice._mark_paid(env.lead, "1500", "extId x", "вебхук")) == "moved"
    assert not ms.gets and not ms.posts


def test_isklyuchenie_v_ms_payment_ne_lomaet_perevod_sdelki(env, ms, monkeypatch):
    async def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(ms_payment_flow, "on_ozon_paid", boom)
    assert run(ozon_invoice._handle_notification(notif())) == "moved"
    assert len(moved(env)) == 1


def test_sdelka_uzhe_dalshe_platezh_vse_ravno_pishem(env, ms, monkeypatch):
    """Деньги получены независимо от этапа сделки: ветка «noted» не должна терять платёж."""
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_DRY_RUN", False)
    env.lead["status_id"] = 142
    assert run(ozon_invoice._handle_notification(notif())) == "noted"
    assert len(ms.posts) == 1


def test_summa_oplaty_vsegda_izvestna_vebhuk_bez_summy_beryot_summu_scheta(env, ms, monkeypatch):
    """Ozon прислал пустую сумму - платёж не теряем, берём сумму счёта из примечания."""
    monkeypatch.setattr(ms_payment_flow, "OZON_MS_PAYMENT_DRY_RUN", False)
    assert run(ozon_invoice._handle_notification(notif(amount=""))) == "moved"
    assert len(ms.posts) == 1 and ms.posts[0][1]["sum"] == 150000
    assert any("1500 ₽" in n for n in env.notes), env.notes


def test_summa_scheta_iz_primechaniya_berotsya_imenno_etogo_scheta(env):
    env.invoice_note = "Счёт СБП создан автоматически: 7590.5 ₽ (заказ МС 05740)\nextId amo-777001-999"
    assert run(ozon_invoice._invoice_kopecks(LEAD_ID, "777001-999")) == 759050
    assert run(ozon_invoice._invoice_kopecks(LEAD_ID, "777001-123")) is None     # чужой счёт
    assert run(ozon_invoice._invoice_kopecks(LEAD_ID)) == 759050
