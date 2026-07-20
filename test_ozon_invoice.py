"""Юнит-тест счёта СБП (process_invoice_lead) без сети/прода. Схема «тех-этап».

Мокаем amo_service (get_lead_full/patch_lead/add_note/add_tag), ms_client.get,
telegram_bot.send_alert и _create_payment внутри модуля; чистые функции
(подпись, get_custom_field_value, looks_like_uuid) — реальные.

Проверяем ключевые инварианты:
  • успех = ОДИН атомарный PATCH: ссылка в 577617 + перевод в «Ссылка
    отправлена» (боты этапа получают уже заполненное поле);
  • «Нет заказа в МС» → тег+примечание+ТГ, Ozon не дёргается, сделка не едет;
  • сделка уехала с тех-этапа, пока ждала в очереди → полный скип;
  • PATCH упал → сделка остаётся на тех-этапе, ссылка менеджеру в примечание;
  • TTL-дедуп двойного вебхука;
  • формула подписи createPayment (extId+accessKey+secretKey, sha256 hex).
"""
import asyncio
import hashlib
import sys
import types

# --- стаб aiogram-зависимого telegram_bot (как в test_wazzup_sla) ---
_tb = types.ModuleType("telegram_bot")
_tb.send_alert = None
sys.modules.setdefault("telegram_bot", _tb)

import amo_service  # noqa: E402
import ms_client  # noqa: E402
import ozon_invoice  # noqa: E402
import telegram_bot  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_LINK,
    PIPELINE_CLEVER_MAIN,
    STATUS_LINK_SENT,
    STATUS_PAYMENT_REQUESTED,
    TAG_INVOICE_ERROR,
)

UU = "0e5a2b05-aaaa-bbbb-cccc-0123456789ab"
LEAD_ID = 777001

_patches: list = []
_notes: list = []
_tags: list = []
_alerts: list = []
_ozon_calls: list = []


def _lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_CLEVER_MAIN, uuid=UU):
    cf = []
    if uuid is not None:
        cf.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": uuid}]})
    return {
        "id": LEAD_ID,
        "name": "Заказ №4242",
        "status_id": status,
        "pipeline_id": pipeline,
        "responsible_user_id": 11513202,
        "custom_fields_values": cf,
    }


def _install_mocks(lead, *, order_sum=1234500, ozon_ok=True, patch_ok=True):
    async def fake_get_lead_full(lead_id, with_=()):
        return lead

    async def fake_patch_lead(lead_id, **kw):
        _patches.append({"lead_id": lead_id, **kw})
        return {"ok": patch_ok, "status_code": 200 if patch_ok else 500}

    async def fake_add_note(lead_id, text):
        _notes.append((lead_id, text))
        return {"ok": True}

    async def fake_add_tag(lead_id, tag):
        _tags.append((lead_id, tag))
        return {"ok": True}

    async def fake_ms_get(path, params=None, **kw):
        if path == f"entity/customerorder/{UU}":
            return {"id": UU, "name": "01234", "sum": order_sum}
        return None

    async def fake_send_alert(text, **kw):
        _alerts.append(text)
        return True

    async def fake_create_payment(ext_id, kopecks):
        _ozon_calls.append((ext_id, kopecks))
        if ozon_ok:
            return f"https://payment.ozon.ru/link/{ext_id}", "pay-id-1", ""
        return None, "", "Ozon HTTP 400: bad"

    amo_service.get_lead_full = fake_get_lead_full
    amo_service.patch_lead = fake_patch_lead
    amo_service.add_note = fake_add_note
    amo_service.add_tag = fake_add_tag
    ms_client.get = fake_ms_get
    telegram_bot.send_alert = fake_send_alert
    ozon_invoice._create_payment = fake_create_payment


def _reset():
    for coll in (_patches, _notes, _tags, _alerts, _ozon_calls):
        coll.clear()
    ozon_invoice._recent.clear()


def run(coro):
    return asyncio.run(coro)


# ── 0a) _create_payment: платёж «без заказа» → ссылка из sbp.payload ────────
# Боевой ответ Ozon 20.07.2026: order=None, ссылка в paymentDetails.sbp.payload.
class _FakeResp:
    status_code = 200
    text = ""
    def json(self):
        return {"order": None, "paymentDetails": {
            "paymentId": "pid-1", "type": "SBP", "status": "PAYMENT_NEW",
            "sbp": {"payload": "https://qr.nspk.ru/TEST123"}}}

class _FakeClient:
    async def post(self, url, json=None):
        return _FakeResp()

ozon_invoice._client = _FakeClient()
link, pid, err = asyncio.run(ozon_invoice._create_payment("ext-x", 1000))
assert link == "https://qr.nspk.ru/TEST123" and pid == "pid-1" and err == "", (link, pid, err)
ozon_invoice._client = None
print("✓ платёж без заказа: ссылка берётся из paymentDetails.sbp.payload")

# ── 0) подпись createPayment: формула из боевого плагина ────────────────────
sig = ozon_invoice._sign_create_payment("ext1", "AK", "SK")
assert sig == hashlib.sha256(b"ext1AKSK").hexdigest(), sig
assert sig == sig.lower() and len(sig) == 64
print("✓ подпись: sha256(extId+accessKey+secretKey), hex lower")

# ── 1) успех: сумма МС → Ozon → ОДИН PATCH (577617 + «Ссылка отправлена») ───
_reset()
_install_mocks(_lead())
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "created", res
assert len(_ozon_calls) == 1 and _ozon_calls[0][1] == 1234500, _ozon_calls
assert _ozon_calls[0][0].startswith(f"amo-{LEAD_ID}-"), _ozon_calls
assert len(_patches) == 1, _patches
p = _patches[0]
assert p["custom_fields"][FIELD_PAYMENT_LINK].startswith("https://payment.ozon.ru/"), p
assert p.get("status_id") == STATUS_LINK_SENT and p.get("pipeline_id") == PIPELINE_CLEVER_MAIN, p
assert len(_notes) == 1 and "12345 ₽" in _notes[0][1] and "01234" in _notes[0][1], _notes
assert not _tags and not _alerts, "успех не должен алертить"
print("✓ успех: один атомарный PATCH — ссылка в 577617 + перевод в «Ссылка отправлена»")

# ── 2) нет заказа МС → тег+примечание+ТГ, Ozon не дёргается, сделка стоит ───
_reset()
_install_mocks(_lead(uuid=None))
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "failed-no-ms-order", res
assert not _ozon_calls and not _patches
assert _tags == [(LEAD_ID, TAG_INVOICE_ERROR)], _tags
assert len(_alerts) == 1 and "Нет заказа в МС" in _alerts[0], _alerts
assert "@gladkov_369" in _alerts[0], _alerts
print("✓ нет заказа МС: ошибка менеджеру (тег+примечание+ТГ с @), сделка осталась на тех-этапе")

# ── 3) сделка уехала с тех-этапа, пока ждала в очереди → полный скип ────────
_reset()
_install_mocks(_lead(status=STATUS_LINK_SENT))
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "skipped-moved", res
assert not _ozon_calls and not _patches and not _alerts and not _notes
print("✓ сделка уехала с тех-этапа: ничего не делаем")

# ── 4) PATCH упал → сделка остаётся на тех-этапе, ссылка менеджеру ──────────
_reset()
_install_mocks(_lead(), patch_ok=False)
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "failed-patch", res
assert len(_ozon_calls) == 1
assert any("вручную" in a for a in _alerts), _alerts
assert any("https://payment.ozon.ru/" in n[1] for n in _notes), "ссылка должна уйти менеджеру в примечание"
print("✓ PATCH упал: сделка не переведена, ссылка отдана менеджеру")

# ── 5) TTL-дедуп: второй вебхук той же смены этапа не создаёт второй счёт ───
_reset()
_install_mocks(_lead())
res1 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
res2 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res1 == "created" and res2 == "skipped-recent", (res1, res2)
assert len(_ozon_calls) == 1 and len(_patches) == 1
print("✓ дедуп: двойной вебхук = один счёт и один перевод")

# ── 6) сумма 0 → ошибка менеджеру, Ozon не дёргаем ──────────────────────────
_reset()
_install_mocks(_lead(), order_sum=0)
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "failed-zero-sum", res
assert not _ozon_calls and not _patches
assert any("Сумма заказа МС = 0" in a for a in _alerts), _alerts
print("✓ сумма 0: счёт не создаём, менеджеру ошибка")

print("\nozon_invoice: все тесты прошли")
