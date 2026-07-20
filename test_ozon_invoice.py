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


def _lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_CLEVER_MAIN, uuid=UU, link=None, other=None):
    cf = []
    if uuid is not None:
        cf.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": uuid}]})
    if link is not None:
        cf.append({"field_id": FIELD_PAYMENT_LINK, "values": [{"value": link}]})
    if other is not None:
        cf.append({"field_id": ozon_invoice.FIELD_INVOICE_OTHER_AMOUNT, "values": [{"value": other}]})
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

# ── 8) «Другая сумма»: парсер рублей → копейки ──────────────────────────────
for raw, want in [("20000", 2000000), ("15 398,50", 1539850), ("15398.5 ₽", 1539850), ("", None), (None, None)]:
    got, err = ozon_invoice._parse_other_amount(raw)
    assert got == want and err == "", (raw, got, err)
for raw in ["тыща", "0", "0,5"]:
    got, err = ozon_invoice._parse_other_amount(raw)
    assert got is None and err, (raw, got, err)
print("✓ парсер «Другой суммы»: рубли→копейки, пробелы/запятые/₽, мусор и <1 ₽ = ошибка")

# ── 9) «Другая сумма» задана → счёт на неё, не на сумму заказа ──────────────
_reset()
_install_mocks(_lead(other="2 500"))
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "created", res
assert _ozon_calls[0][1] == 250000, _ozon_calls
assert "Другая сумма" in _notes[0][1] and "2500 ₽" in _notes[0][1], _notes
print("✓ «Другая сумма» 2 500 → счёт на 250000 коп., источник в примечании")

# ── 10) «Другая сумма» мусор → ошибка менеджеру, счёт не создаём ────────────
_reset()
_install_mocks(_lead(other="約тыща"))
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "failed-other-amount", res
assert not _ozon_calls and not _patches
assert any("Другая сумма" in a for a in _alerts), _alerts
print("✓ мусор в «Другой сумме»: счёт не создан, менеджеру понятная ошибка")

# ── 7) 577617 уже заполнено → скип (update_lead приходит на ЛЮБУЮ правку) ───
# Кейс 20.07: менеджер вписал ссылку руками, сделка стоит на тех-этапе —
# следующий вебхук не должен плодить второй платёж.
_reset()
_install_mocks(_lead(link="https://qr.nspk.ru/MANUAL"))
res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
assert res == "skipped-link-present", res
assert not _ozon_calls and not _patches and not _alerts and not _tags
print("✓ ссылка уже в поле: второй платёж не создаём, ручную ссылку уважаем")

# ═══ Этап 2: вебхук факта оплаты ════════════════════════════════════════════

def _notif(ext="amo-777001-123", status="Completed", amount="759000", sign=True):
    d = {"extTransactionID": ext, "status": status, "amount": amount,
         "currencyCode": "643", "operationType": "Payment", "paymentMethod": "SBP"}
    if sign:
        import hashlib as _h
        d["requestSign"] = _h.sha256(
            f"{ozon_invoice.OZON_PAY_ACCESS_KEY}|||{ext}|{amount}|643|{ozon_invoice.OZON_PAY_NOTIFICATION_SECRET_KEY}".encode()
        ).hexdigest()
    return d

ozon_invoice.OZON_PAY_ACCESS_KEY = "AK-test"
ozon_invoice.OZON_PAY_NOTIFICATION_SECRET_KEY = "NS-test"

# ── 11) подпись: валидная self-формула проходит, битая — нет ────────────────
assert ozon_invoice.verify_notification(_notif()) is True
bad = _notif(); bad["requestSign"] = "0" * 64
assert ozon_invoice.verify_notification(bad) is False
assert ozon_invoice.verify_notification({"requestSign": ""}) is False
print("✓ вебхук: подпись self-формулы проверяется, битая отклоняется")

# ── 12) Completed + сделка на «Ссылка отправлена» → перевод в «Оплата получена» ─
_reset(); ozon_invoice._paid_recent.clear()
_install_mocks(_lead(status=STATUS_LINK_SENT))
res = run(ozon_invoice._handle_notification(_notif()))
assert res == "moved", res
assert _patches and _patches[0].get("status_id") == ozon_invoice.STATUS_PAYMENT_RECEIVED, _patches
assert any("Оплата подтверждена" in n[1] and "7590 ₽" in n[1] for n in _notes), _notes
print("✓ Completed: сделка со «Ссылка отправлена» уехала в «Оплата получена» + примечание")

# ── 13) Completed, но менеджер уже перевёл сам → только примечание ──────────
_reset(); ozon_invoice._paid_recent.clear()
_install_mocks(_lead(status=142))
res = run(ozon_invoice._handle_notification(_notif()))
assert res == "noted", res
assert not [p for p in _patches if p.get("status_id")], _patches
assert any("не двигаю" in n[1] for n in _notes), _notes
print("✓ Completed по уже переведённой сделке: примечание, этап не трогаем")

# ── 14) повторный вебхук того же extId → дедуп ──────────────────────────────
_reset(); ozon_invoice._paid_recent.clear()
_install_mocks(_lead(status=STATUS_LINK_SENT))
r1 = run(ozon_invoice._handle_notification(_notif()))
r2 = run(ozon_invoice._handle_notification(_notif()))
assert r1 == "moved" and r2 == "skipped-duplicate", (r1, r2)
assert len([p for p in _patches if p.get("status_id")]) == 1
print("✓ ретрай вебхука: один перевод, дубль скипнут")

# ── 15) не наш extId / не Completed / битая подпись → игнор без действий ────
_reset(); ozon_invoice._paid_recent.clear()
_install_mocks(_lead(status=STATUS_LINK_SENT))
assert run(ozon_invoice._handle_notification(_notif(ext="site-order-1"))) == "ignored-foreign"
assert run(ozon_invoice._handle_notification(_notif(status="Rejected"))) == "ignored-status"
assert run(ozon_invoice._handle_notification(bad)) == "ignored-bad-sign"
assert not _patches and not _notes and not _alerts
print("✓ чужой extId, Rejected и битая подпись: полный игнор")

print("\nozon_invoice: все тесты прошли")
