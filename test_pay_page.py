"""Своя страница оплаты (pay_page.py): хранение, статус из Ozon, маршруты.

Запуск: python -m pytest test_pay_page.py
"""
import asyncio
import re
import sys
import time
import types

import pytest

_tb = types.ModuleType("telegram_bot")
_tb.send_alert = None
sys.modules.setdefault("telegram_bot", _tb)

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import ozon_invoice  # noqa: E402
import pay_page  # noqa: E402

PAYLOAD = "https://qr.nspk.ru/AD10107AGA7BIIL69PAA6FVRC08GR4LE?type=02&sum=159600"
CHECKOUT = "https://checkout.ozon.ru/order/01a216ce-002f-742c-9255-a37d847301e3"
ORDER_EXT = "10414_amo-36607307-1795678987"
DESKTOP = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
IPHONE = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1"
ANDROID = "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Mobile Safari/537.36"


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr(pay_page, "_DB", tmp_path / "pay_page.sqlite3")


def _make(**over):
    args = dict(lead_id=36607307, ext_id="amo-36607307-1795678987", order_ext_id=ORDER_EXT,
                pipeline_id=10593102, amount_kopecks=159600, order_name="10414",
                sbp_payload=PAYLOAD, checkout_url=CHECKOUT, ttl_s=86400)
    args.update(over)
    return pay_page.create_link(**args)


class _OrderClient:
    """Ozon getOrderStatus: отдаёт заданный заказ и считает обращения."""

    def __init__(self, status="STATUS_PAYMENT_PENDING", value="159600", code=200):
        self.status, self.value, self.code, self.calls = status, value, code, 0

    async def post(self, url, json=None):
        self.calls += 1
        assert url.endswith("/v1/getOrderStatus"), url
        outer = self

        class R:
            status_code = outer.code
            text = '{"code":7}'

            def json(self):
                return {"id": "01a216ce", "extId": json["extId"], "status": outer.status, "isTestMode": False,
                        "originalAmount": {"currencyCode": "643", "value": outer.value}}
        return R()


@pytest.fixture
def ozon(monkeypatch):
    client = _OrderClient()
    monkeypatch.setattr(ozon_invoice, "_client", client)
    return client


@pytest.fixture
def http(ozon):
    app = FastAPI()
    app.include_router(pay_page.router)
    return TestClient(app, base_url="https://pay.sunscrypt.ru", follow_redirects=False)


# --- хранение ------------------------------------------------------------------

def test_nomer_ssylki_sluchaynyy_16_simvolov():
    tokens = {_make() for _ in range(20)}
    assert len(tokens) == 20
    for t in tokens:
        assert re.fullmatch(r"[A-Za-z0-9_-]{16}", t), t
        assert pay_page.get_link(t)["amount"] == 159600


@pytest.mark.parametrize("over", [
    {"sbp_payload": ""}, {"sbp_payload": "https://evil.example/qr"},
    {"checkout_url": "https://evil.example/pay"}, {"checkout_url": ""},
    {"order_ext_id": ""}, {"amount_kopecks": 0},
])
def test_nepolnye_dannye_ssylku_ne_sozdayut(over):
    with pytest.raises(ValueError):
        _make(**over)


def test_chuzhoy_nomer_ne_ishetsya():
    _make()
    for bad in ("", "short", "../../etc/passwd", "x" * 65, "a' OR '1'='1aaaaaa"):
        assert pay_page.get_link(bad) is None


def test_sostoyaniya_oplacheno_silnee_sroka():
    link = pay_page.get_link(_make())
    now = time.time()
    assert pay_page.effective_state(link, now) == "pending"
    assert pay_page.effective_state(link, link["expires_at"] + 1) == "expired"
    assert pay_page.effective_state({**link, "state": "closed"}, now) == "expired"
    assert pay_page.effective_state({**link, "state": "paid"}, link["expires_at"] + 1) == "paid"


# --- статус из Ozon -------------------------------------------------------------

def test_oplachen_v_ozon_stanovitsya_paid_i_bolshe_ne_sprashivaem(ozon):
    ozon.status = "STATUS_PAID"
    token = _make()
    assert asyncio.run(pay_page.refresh_state(pay_page.get_link(token))) == "paid"
    assert pay_page.get_link(token)["state"] == "paid"
    assert asyncio.run(pay_page.refresh_state(pay_page.get_link(token))) == "paid"
    assert ozon.calls == 1


def test_drugaya_summa_ne_oplata(ozon):
    ozon.status, ozon.value = "STATUS_PAID", "100"
    token = _make()
    assert asyncio.run(pay_page.refresh_state(pay_page.get_link(token))) == "pending"
    assert pay_page.get_link(token)["state"] == "pending"


def test_otmenyonnyy_zakaz_istyok(ozon):
    ozon.status = "STATUS_EXPIRED"
    token = _make()
    assert asyncio.run(pay_page.refresh_state(pay_page.get_link(token))) == "expired"


def test_oshibka_ozon_ne_lomaet_stranicu(ozon):
    ozon.code = 500
    token = _make()
    assert asyncio.run(pay_page.refresh_state(pay_page.get_link(token))) == "pending"


def test_ozon_sprashivaem_ne_chashche_raza_v_10_sekund(ozon):
    token = _make()
    for _ in range(5):
        asyncio.run(pay_page.refresh_state(pay_page.get_link(token)))
    assert ozon.calls == 1


# --- маршруты -------------------------------------------------------------------

def test_kompyuter_vidit_qr_bez_slova_sbp_i_bez_lichnyh_dannyh(http):
    token = _make()
    r = http.get(f"/o/{token}", headers={"user-agent": DESKTOP})
    assert r.status_code == 200
    body = r.text
    assert "Оплатите заказ онлайн" in body and "<svg" in body and 'aria-label="QR-код для оплаты"' in body
    assert "Заказ №10414" in body and "1 596 ₽" in body
    assert "СБП" not in body
    for secret in ("36607307", ORDER_EXT, PAYLOAD):
        assert secret not in body, secret
    assert r.headers["referrer-policy"] == "no-referrer"
    assert r.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]


@pytest.mark.parametrize("ua", [IPHONE, ANDROID])
def test_telefon_srazu_na_stranicu_ozon(http, ua):
    token = _make()
    r = http.get(f"/o/{token}", headers={"user-agent": ua})
    assert r.status_code == 302
    assert r.headers["location"] == CHECKOUT
    assert r.headers["referrer-policy"] == "no-referrer"


def test_oplachennyy_s_telefona_ne_uvodit_v_ozon(http, ozon):
    ozon.status = "STATUS_PAID"
    token = _make()
    r = http.get(f"/o/{token}", headers={"user-agent": IPHONE})
    assert r.status_code == 200 and "Оплата получена" in r.text and "<script>" not in r.text


def test_istyokshaya_ssylka(http):
    token = _make(ttl_s=-1)
    r = http.get(f"/o/{token}", headers={"user-agent": IPHONE})
    assert r.status_code == 200 and "Срок ссылки на оплату истёк" in r.text


def test_neizvestnyy_nomer_404(http):
    r = http.get("/o/AAAAAAAAAAAAAAAA", headers={"user-agent": DESKTOP})
    assert r.status_code == 404 and "Ссылка на оплату не найдена" in r.text
    assert http.get("/o/AAAAAAAAAAAAAAAA/status").status_code == 404


def test_tolko_na_imeni_pay_sunscrypt_ru(ozon):
    app = FastAPI()
    app.include_router(pay_page.router)
    token = _make()
    other = TestClient(app, base_url="https://team.sunscrypt.ru", follow_redirects=False)
    assert other.get(f"/o/{token}").status_code == 404
    assert other.get(f"/o/{token}/status").status_code == 404


def test_status_json(http, ozon):
    token = _make()
    assert http.get(f"/o/{token}/status").json() == {"state": "pending"}
    ozon.status = "STATUS_PAID"
    pay_page._update(token, checked_at=0)
    assert http.get(f"/o/{token}/status").json() == {"state": "paid"}


def test_mobilnyy_user_agent():
    assert pay_page.is_mobile(IPHONE) and pay_page.is_mobile(ANDROID)
    assert not pay_page.is_mobile(DESKTOP) and not pay_page.is_mobile("")
