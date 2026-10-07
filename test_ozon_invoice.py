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
import time
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
    STATUS_PAYMENT_RECEIVED,
    STATUS_PAYMENT_REQUESTED,
    TAG_INVOICE_ERROR,
)

UU = "0e5a2b05-aaaa-bbbb-cccc-0123456789ab"
LEAD_ID = 777001

# Оригиналы функций, которые тесты выше подменяют моками: нижние тесты
# проверяют НАСТОЯЩЕЕ поведение (сборка тела запроса в Ozon), а не мок.
_REAL_CREATE_PAYMENT = ozon_invoice._create_payment
_REAL_GET_PAYMENT_STATUS = ozon_invoice.get_payment_status

_patches: list = []
_notes: list = []
_tags: list = []
_alerts: list = []
_ozon_calls: list = []
_reads: list = []          # сколько раз модуль читал сделку — это и есть «попытка»


# Способ оплаты и бюджет у фабрики ЗАПОЛНЕНЫ по умолчанию: с 28.09.2026 незаполненная
# сделка счёт не создаёт вовсе, а большинству тестов нужна именно нормальная сделка.
def _lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_CLEVER_MAIN, uuid=UU, link=None,
          other=None, by_card=None, method="Онлайн-оплата", price=12345):
    cf = []
    if uuid is not None:
        cf.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": uuid}]})
    if link is not None:
        cf.append({"field_id": FIELD_PAYMENT_LINK, "values": [{"value": link}]})
    if other is not None:
        cf.append({"field_id": ozon_invoice.FIELD_INVOICE_OTHER_AMOUNT, "values": [{"value": other}]})
    if by_card is not None:
        cf.append({"field_id": ozon_invoice.FIELD_INVOICE_BY_CARD, "values": [{"value": by_card}]})
    if method is not None:
        cf.append({"field_id": ozon_invoice.FIELD_PAYMENT_METHOD, "values": [{"value": method}]})
    return {
        "id": LEAD_ID,
        "name": "Заказ №4242",
        "price": price,
        "status_id": status,
        "pipeline_id": pipeline,
        "responsible_user_id": 11513202,
        "custom_fields_values": cf,
    }


def _install_mocks(lead, *, order_sum=1234500, ozon_ok=True, patch_ok=True):
    async def fake_get_lead_full(lead_id, with_=()):
        _reads.append(lead_id)
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

    async def fake_create_payment(ext_id, kopecks, by_card=False, ms_order_name=""):
        _ozon_calls.append((ext_id, kopecks, by_card, ms_order_name))
        if ozon_ok:
            base = "https://checkout.ozon.ru/order/" if by_card else "https://qr.nspk.ru/"
            return f"{base}{ext_id}", "pay-id-1", ""
        return None, "", "Ozon HTTP 400: bad"

    amo_service.get_lead_full = fake_get_lead_full
    amo_service.patch_lead = fake_patch_lead
    amo_service.add_note = fake_add_note
    amo_service.add_tag = fake_add_tag
    ms_client.get = fake_ms_get
    telegram_bot.send_alert = fake_send_alert
    ozon_invoice._create_payment = fake_create_payment


def _reset():
    for coll in (_patches, _notes, _tags, _alerts, _ozon_calls, _reads):
        coll.clear()
    ozon_invoice._recent.clear()
    ozon_invoice._blocked_noted.clear()
    ozon_invoice._failed_recent.clear()


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

# ── 0a-card: by_card → заказ в теле, ссылка из order.item.payLink ───────────
_card_bodies = []
class _FakeRespCard:
    status_code = 200
    text = ""
    def json(self):
        return {"order": {"item": {"payLink": "https://checkout.ozon.ru/order/xyz"}},
                "paymentDetails": {"paymentId": "pid-2", "sbp": {"payload": "https://qr.nspk.ru/IGNORED"}}}
class _FakeClientCard:
    async def post(self, url, json=None):
        _card_bodies.append(json)
        return _FakeRespCard()

# ── 10-2) «Другая сумма» + МС молчит → счёт создаём, просто без номера ──────
async def _ms_silent(path, params=None, **kw):
    return None

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

# ── 17) lead_id вытаскивается из обоих форматов extId ───────────────────────
cases = {
    "amo-36523057-1785242820": 36523057,          # extId платежа
    "05740_amo-36523057-1785242820": 36523057,    # новый extId заказа
    "ord-36522883-1785232770": 36522883,          # старый extId заказа
}

class _RespDetails:
    status_code = 200
    text = ""
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload

class _ClientDetails:
    def __init__(self, payload):
        self._payload = payload
        self.bodies = []
    async def post(self, url, json=None):
        self.bodies.append((url, json))
        return _RespDetails(self._payload)

async def fake_by_status(status_id, with_=("contacts",), page_limit=50):
    return [_stuck] if status_id == STATUS_LINK_SENT else []

async def fake_notes(lead_id, limit=100):
    return [{"created_at": int(time.time()) - 300, "params": {"text":
             "Счёт (оплата картой, страница выбора Ozon) создан автоматически: 10 ₽\n"
             "https://checkout.ozon.ru/order/abc\n"
             "extId amo-777001-1785242820\npaymentId 019fa8c3-9ed2-7eca-99d0-b9a314ad5563\n"
             "orderExtId 05748_amo-777001-1785242820"}}]

async def fake_status_completed(payment_id, ext_id="", order_ext_id=""):
    assert payment_id == "019fa8c3-9ed2-7eca-99d0-b9a314ad5563", payment_id
    # сверка обязана прокинуть оба extId — без них карта не ищется
    assert ext_id == "amo-777001-1785242820", ext_id
    assert order_ext_id == "05748_amo-777001-1785242820", order_ext_id
    return "PAYMENT_COMPLETED", 1000, ""


async def fake_notes_old(lead_id, limit=100):
    return [{"created_at": int(time.time()) - 3 * 3600, "params": {"text":
             "Счёт СБП создан автоматически: 10 ₽\nextId amo-777001-1\npaymentId pay-old"}}]


async def fake_status_pending(payment_id, ext_id="", order_ext_id=""):
    return "PAYMENT_NEW", None, ""


async def fake_status_rejected(payment_id, ext_id="", order_ext_id=""):
    return "PAYMENT_REJECTED", None, ""

async def fake_by_status_nolink(status_id, with_=("contacts",), page_limit=50):
    return [_no_link] if status_id == STATUS_LINK_SENT else []

async def fake_status_spy(payment_id, ext_id="", order_ext_id=""):
    _asked.append(payment_id)
    return "PAYMENT_COMPLETED", 1000, ""

# ═══════════════════════════════════════════════════════════════════════════
# Итерация 2 (по логам прода 28.07): боевая форма ответа + поиск по extId.
# ═══════════════════════════════════════════════════════════════════════════

# ── 23) боевая форма ответа: статус лежит в items[0] ───────────────────────
# Дословно из лога прода: {'items': [{'status': 'PAYMENT_REJECTED', ...}]}.
# Прежний парсер искал статус в paymentDetails и отдавал «не распознан».
real = {"items": [{"transactionUid": "019f941d-cd5a-760d-9f53-f9b1b8bb0c8f",
                   "extId": "amo-36519747-1784896408",
                   "status": "PAYMENT_REJECTED",
                   "amount": {"currencyCode": "643", "value": "1297900"}}]}

# ── 25) карта: по paymentId пусто → спрашиваем по extId заказа ─────────────
_asked_bodies = []

class _ClientChain:
    async def post(self, url, json=None):
        _asked_bodies.append(json)
        # по id и по extId платежа Ozon отвечает пустотой (как на проде),
        # находится только по extId ЗАКАЗА
        if json.get("extId") == "05748_amo-777001-1785242820":
            return _RespDetails({"items": [{"status": "PAYMENT_COMPLETED",
                                            "amount": {"value": "1000"}}]})
        return _RespDetails({"items": []})

# ── 26) нигде не нашли → честная ошибка, сделку не трогаем ─────────────────
class _ClientEmpty:
    async def post(self, url, json=None):
        return _RespDetails({"items": []})

# ── 26б) отказ по СРЕДНЕМУ ключу не обрывает каскад (боевой случай 29.08.2026)
class _RespDenied:
    status_code = 403
    text = '{"code":7, "message":"недостаточно прав"}'
    def json(self):
        return {}

class _ClientDeniedMiddle:
    """Как боевой Ozon 29.08: по paymentId пусто (карта), по extId платежа —
    «нет прав», статус лежит только под extId ЗАКАЗА."""
    def __init__(self):
        self.asked = []
    async def post(self, url, json=None):
        value = json.get("id") or json.get("extId")
        self.asked.append(value)
        if value == "pay-403":
            return _RespDetails({"items": []})
        if value == "amo-36544371-1788004604":
            return _RespDenied()
        return _RespDetails({"items": [{"status": "PAYMENT_COMPLETED",
                                        "amount": {"value": "1036300"}}]})

# ── 26в) отказ по ВСЕМ ключам → возвращаем отказ, а не «не найден» ──────────
class _ClientAllDenied:
    async def post(self, url, json=None):
        return _RespDenied()

async def notes_with_order(lead_id, limit=100):
    return [{"created_at": 1785242820, "params": {"text":
             "Счёт (оплата картой, страница выбора Ozon) создан автоматически: 10 ₽\n"
             "extId amo-777001-1785242820\npaymentId pid-1\n"
             "orderExtId 05748_amo-777001-1785242820"}}]

async def notes_old_format(lead_id, limit=100):
    return [{"created_at": 1785242820, "params": {"text":
             "Счёт СБП создан автоматически: 10 ₽\nextId amo-777001-1785242820\npaymentId pid-2"}}]

# --- частота напоминаний: не чаще раза в сутки (правило Кати, 31.07.2026) -----
import datetime as _dt


def _at(day, hour, minute=0):
    return _dt.datetime(2026, 8, day, hour, minute, tzinfo=_MSK)


def _ts(day, hour, minute=0):
    return int(_at(day, hour, minute).timestamp())

# ══════════ картотека «Работа с базой»: та же цепочка во второй воронке ══════════
# Этапы скопированы Катей 07.09.2026. Автоматика в amo своя, код — общий:
# воронка и этапы берутся из карты OZON_PAYMENT_STAGES, а не из констант розницы.
from waybill_config import (  # noqa: E402
    PIPELINE_DB_WORK,
    STATUS_DB_LINK_SENT,
    STATUS_DB_PAYMENT_RECEIVED,
    STATUS_DB_PAYMENT_REQUESTED,
)

async def _fake_by_status(status_id, with_=()):
    _asked.append(status_id)
    return []

# ══════════ воронка «Академия»: третья воронка на той же карте ══════════
# Обучение продаётся по той же схеме (постановка Кати 09.09.2026). Проверяем,
# что третья воронка добавляется данными, а не новой веткой логики.
from waybill_config import (  # noqa: E402
    PIPELINE_ACADEMY,
    STATUS_ACADEMY_LINK_SENT,
    STATUS_ACADEMY_PAYMENT_RECEIVED,
    STATUS_ACADEMY_PAYMENT_REQUESTED,
)

# ── е) сверка обходит все три воронки, по два этапа на каждую ──────────────
_asked_all: list = []

async def _fake_by_status_all(status_id, with_=()):
    _asked_all.append(status_id)
    return []

# ══════════════════════ TangemShop: четвёртая воронка ══════════════════════
# ТЗ 29.09.2026: оплата заказов магазина tangemshop.ru идёт по той же схеме,
# что розничная. Этапы у воронки свои, механика общая.
from waybill_config import (  # noqa: E402
    PIPELINE_TANGEMSHOP,
    STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED,
    STATUS_TANGEM_LINK_SENT,
    STATUS_TANGEM_PAYMENT_REQUESTED,
)

# сверка: выключенный флаг не стоит ни одного лишнего запроса
_asked_tg: list = []


async def _fake_by_status_tg(status_id, with_=()):
    _asked_tg.append(status_id)
    return []





def test_platezh_bez_zakaza_ssylka_beretsya_iz_paymentdetails_sbp_p():
    """платёж без заказа: ссылка берётся из paymentDetails.sbp.payload"""
    assert link == "https://qr.nspk.ru/TEST123" and pid == "pid-1" and err == "", (link, pid, err)
    ozon_invoice._client = None


def test_by_card_zakaz_mode_shortened_v_tele_ssylka_iz_order_item_p():
    """by_card: заказ MODE_SHORTENED в теле, ссылка из order.item.payLink (не sbp)"""
    ozon_invoice._client = _FakeClientCard()
    link, pid, err = asyncio.run(ozon_invoice._create_payment("amo-9-1", 1000, by_card=True))
    assert link == "https://checkout.ozon.ru/order/xyz" and pid == "pid-2" and err == "", (link, pid, err)
    assert "order" in _card_bodies[0] and _card_bodies[0]["order"]["mode"] == "MODE_SHORTENED", _card_bodies
    assert _card_bodies[0]["order"]["extId"] == "ord-9-1", _card_bodies  # amo-→ord- в extId заказа
    ozon_invoice._client = None


def test_podpis_sha256_extid_accesskey_secretkey_hex_lower():
    """подпись: sha256(extId+accessKey+secretKey), hex lower"""
    # ── 0) подпись createPayment: формула из боевого плагина ────────────────────
    sig = ozon_invoice._sign_create_payment("ext1", "AK", "SK")
    assert sig == hashlib.sha256(b"ext1AKSK").hexdigest(), sig
    assert sig == sig.lower() and len(sig) == 64


def test_uspeh_odin_atomarnyy_patch_ssylka_v_577617_perevod_v_ssylk():
    """успех: один атомарный PATCH — ссылка в 577617 + перевод в «Ссылка отправлена»"""
    # ── 1) успех: сумма МС → Ozon → ОДИН PATCH (577617 + «Ссылка отправлена») ───
    _reset()
    _install_mocks(_lead())
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert len(_ozon_calls) == 1 and _ozon_calls[0][1] == 1234500, _ozon_calls
    assert _ozon_calls[0][0].startswith(f"amo-{LEAD_ID}-"), _ozon_calls
    assert len(_patches) == 1, _patches
    p = _patches[0]
    assert p["custom_fields"][FIELD_PAYMENT_LINK].startswith("https://qr.nspk.ru/"), p
    assert p.get("status_id") == STATUS_LINK_SENT and p.get("pipeline_id") == PIPELINE_CLEVER_MAIN, p
    assert len(_notes) == 1 and "12345 ₽" in _notes[0][1] and "01234" in _notes[0][1], _notes
    assert not _tags and not _alerts, "успех не должен алертить"


def test_net_zakaza_ms_oshibka_menedzheru_teg_primechanie_tg_s_sdel():
    """нет заказа МС: ошибка менеджеру (тег+примечание+ТГ с @), сделка осталась на тех-этапе"""
    # ── 2) нет заказа МС → тег+примечание+ТГ, Ozon не дёргается, сделка стоит ───
    _reset()
    _install_mocks(_lead(uuid=None))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "failed-no-ms-order", res
    assert not _ozon_calls and not _patches
    assert _tags == [(LEAD_ID, TAG_INVOICE_ERROR)], _tags
    assert len(_alerts) == 1 and "Нет заказа в МС" in _alerts[0], _alerts
    assert "@" in _alerts[0] and "@gladkov_369" not in _alerts[0], _alerts


def test_sdelka_uehala_s_teh_etapa_nichego_ne_delaem():
    """сделка уехала с тех-этапа: ничего не делаем"""
    # ── 3) сделка уехала с тех-этапа, пока ждала в очереди → полный скип ────────
    _reset()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-moved", res
    assert not _ozon_calls and not _patches and not _alerts and not _notes


def test_patch_upal_sdelka_ne_perevedena_ssylka_otdana_menedzheru():
    """PATCH упал: сделка не переведена, ссылка отдана менеджеру"""
    # ── 4) PATCH упал → сделка остаётся на тех-этапе, ссылка менеджеру ──────────
    _reset()
    _install_mocks(_lead(), patch_ok=False)
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "failed-patch", res
    assert len(_ozon_calls) == 1
    assert any("вручную" in a for a in _alerts), _alerts
    assert any("https://qr.nspk.ru/" in n[1] for n in _notes), "ссылка должна уйти менеджеру в примечание"


def test_dedup_dvoynoy_vebhuk_odin_schet_i_odin_perevod():
    """дедуп: двойной вебхук = один счёт и один перевод"""
    # ── 5) TTL-дедуп: второй вебхук той же смены этапа не создаёт второй счёт ───
    _reset()
    _install_mocks(_lead())
    res1 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    res2 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res1 == "created" and res2 == "skipped-recent", (res1, res2)
    assert len(_ozon_calls) == 1 and len(_patches) == 1


def test_summa_0_schet_ne_sozdaem_menedzheru_oshibka():
    """сумма 0: счёт не создаём, менеджеру ошибка"""
    # ── 6) сумма 0 → ошибка менеджеру, Ozon не дёргаем ──────────────────────────
    _reset()
    _install_mocks(_lead(), order_sum=0)
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "failed-zero-sum", res
    assert not _ozon_calls and not _patches
    assert any("Сумма заказа МС = 0" in a for a in _alerts), _alerts


def test_parser_drugoy_summy_rubli_kopeyki_probely_zapyatye_musor_i():
    """парсер «Другой суммы»: рубли→копейки, пробелы/запятые/₽, мусор и <1 ₽ = ошибка"""
    # ── 8) «Другая сумма»: парсер рублей → копейки ──────────────────────────────
    for raw, want in [("20000", 2000000), ("15 398,50", 1539850), ("15398.5 ₽", 1539850), ("", None), (None, None)]:
        got, err = ozon_invoice._parse_other_amount(raw)
        assert got == want and err == "", (raw, got, err)
    for raw in ["тыща", "0", "0,5"]:
        got, err = ozon_invoice._parse_other_amount(raw)
        assert got is None and err, (raw, got, err)


def test_drugaya_summa_2_500_schet_na_250000_kop_istochnik_v_primec():
    """«Другая сумма» 2 500 → счёт на 250000 коп., источник в примечании"""
    # ── 9) «Другая сумма» задана → счёт на неё, не на сумму заказа ──────────────
    _reset()
    _install_mocks(_lead(other="2 500"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][1] == 250000, _ozon_calls
    assert "Другая сумма" in _notes[0][1] and "2500 ₽" in _notes[0][1], _notes


def test_musor_v_drugoy_summe_schet_ne_sozdan_menedzheru_ponyatnaya():
    """мусор в «Другой сумме»: счёт не создан, менеджеру понятная ошибка"""
    # ── 10) «Другая сумма» мусор → ошибка менеджеру, счёт не создаём ────────────
    _reset()
    _install_mocks(_lead(other="約тыща"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "failed-other-amount", res
    assert not _ozon_calls and not _patches
    assert any("Другая сумма" in a for a in _alerts), _alerts


def test_drugaya_summa_bez_zakaza_ms_schet_sozdan_oshibki_menedzher():
    """«Другая сумма» без заказа МС: счёт создан, ошибки менеджеру нет"""
    # ── 10-1) «Другая сумма» без заказа МС → счёт всё равно создаём ─────────
    # Катя 28.09.2026: заказ МС нужен только как ИСТОЧНИК суммы. Менеджер вписал
    # сумму сам — требовать заказ не за что. Путь один на все воронки.
    _reset()
    _install_mocks(_lead(uuid=None, other="2 500"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][1] == 250000, _ozon_calls
    assert _ozon_calls[0][3] == "", _ozon_calls  # номера заказа нет — extId без префикса
    assert not _tags and not _alerts, (_tags, _alerts)
    assert "Другая сумма" in _notes[0][1], _notes


def test_drugaya_summa_molchaschiy_moysklad_schet_sozdan_nomer_zaka():
    """«Другая сумма» + молчащий МойСклад: счёт создан, номер заказа пуст"""
    _reset()
    _install_mocks(_lead(other="2 500"))
    ms_client.get = _ms_silent
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][1] == 250000 and _ozon_calls[0][3] == "", _ozon_calls


def test_bez_drugoy_summy_molchaschiy_moysklad_schet_ne_sozdaem():
    """без «Другой суммы»: молчащий МойСклад — счёт не создаём"""
    # ── 10-3) без «Другой суммы» заказ МС по-прежнему обязателен (регресс) ──────
    _reset()
    _install_mocks(_lead())
    ms_client.get = _ms_silent
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "failed-ms-fetch", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)


def test_stop_spisok_kripta_usdt_ustd_trc_wallet_v_lyubom_registre():
    """стоп-список: крипта/USDT/USTD/TRC/wallet в любом регистре, обычные способы живы"""
    # ── 11) стоп-список способов оплаты: крипта и прочее ───────────────────
    # Катя 28.09.2026: по таким способам оплата идёт мимо эквайринга, ссылка Ozon Pay
    # там не нужна. Сначала чистая функция — все написания разом.
    for raw in ("Крипта", "криптой", "ОПЛАТА КРИПТОЙ", "Crypto USDT", "usdt", "USDT TRC20",
                "ustd", "Trust Wallet", "wallet", "TRC-20"):
        assert ozon_invoice.blocked_invoice_payment_token(raw), raw
    for raw in ("Онлайн-оплата", "Картой на сайте", "При получении", "Безналичный перевод",
                "Наличные", "СберПей", "", None):
        assert ozon_invoice.blocked_invoice_payment_token(raw) == "", raw


def test_kripta_ssylki_net_ozon_ne_dernut_v_kartochke_obyasnenie_be():
    """«Крипта»: ссылки нет, Ozon не дёрнут, в карточке объяснение без тега и алерта"""
    # ── 11a) способ оплаты «Крипта» → ссылку не создаём, Ozon не дёргаем ────────
    _reset()
    _install_mocks(_lead(method="Крипта"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-payment-method", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    assert not _tags and not _alerts, "это не ошибка менеджера: ни тега, ни алерта в ТГ"
    assert len(_notes) == 1 and "Крипта" in _notes[0][1], _notes


def test_stop_spisok_silnee_drugoy_summy_schet_ne_sozdaetsya_voobsc():
    """стоп-список сильнее «Другой суммы»: счёт не создаётся вообще"""
    # ── 11b) «Другая сумма» стоп-список не отменяет ─────────────────────────
    # Запрет стоит ВЫШЕ развилки про сумму: крипта не пускает счёт ни при какой сумме.
    _reset()
    _install_mocks(_lead(method="криптой", uuid=None, other="2 500"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-payment-method", res
    assert not _ozon_calls, _ozon_calls


def test_stop_spisok_obyasnenie_v_kartochke_odno_skolko_by_vebhukov():
    """стоп-список: объяснение в карточке одно, сколько бы вебхуков ни пришло"""
    # ── 11c) повторные вебхуки той же сделки не сыплют примечаниями ─────────
    _reset()
    _install_mocks(_lead(method="USDT TRC20"))
    run(ozon_invoice.process_invoice_lead(LEAD_ID))
    ozon_invoice._recent.clear()          # следующий вебхук вне дедуп-окна
    run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert len(_notes) == 1, _notes


def test_sposob_oplaty_pust_schet_ne_sozdaem_i_ne_shumim_ni_tega_ni():
    """способ оплаты пуст: счёт не создаём и НЕ шумим (ни тега, ни примечания, ни алерта)"""
    # ── 12) незаполненная сделка: выставлять нечего, молчим ────────────────────
    # Разбор сделки 36389097 (Катя 28.09.2026): способ оплаты пуст, бюджет 0, заказа
    # МС нет. Раньше это был _fail с тегом и алертом, а наша же запись двигала
    # updated_at → сторож пробовал снова → восемь одинаковых примечаний за два часа.
    _reset()
    _install_mocks(_lead(method=None, uuid=None, price=0))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-not-ready", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    assert not _tags and not _alerts and not _notes, "незаполненная сделка — не повод шуметь"


def test_byudzhet_0_drugaya_summa_pusta_zakaza_ms_net_molcha_propus():
    """бюджет 0, «Другая сумма» пуста, заказа МС нет: молча пропускаем"""
    _reset()
    _install_mocks(_lead(uuid=None, price=0))         # способ есть, а суммы взять неоткуда
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-not-ready", res
    assert not _ozon_calls and not _notes and not _tags, (_ozon_calls, _notes, _tags)


def test_byudzhet_0_no_zakaz_ms_est_schet_sozdaetsya_po_summe_zakaz():
    """бюджет 0, но заказ МС есть — счёт создаётся по сумме заказа"""
    # ── 12a) но бюджет 0 при живом заказе МС счёт НЕ блокирует ─────────────────
    # Сумма берётся из заказа, а не из бюджета: блокировать такую сделку нельзя.
    _reset()
    _install_mocks(_lead(price=0))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][1] == 1234500, _ozon_calls


def test_byudzhet_0_no_drugaya_summa_zapolnena_schet_sozdaetsya():
    """бюджет 0, но «Другая сумма» заполнена — счёт создаётся"""
    _reset()
    _install_mocks(_lead(uuid=None, price=0, other="2 500"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created" and _ozon_calls[0][1] == 250000, (res, _ozon_calls)


def test_povtor_toy_zhe_prichiny_odno_primechanie_odin_alert_odin_t():
    """повтор той же причины: одно примечание, один алерт, один тег — без спама"""
    # ── 12b) одна и та же причина отказа не дублируется в течение часа ─────────
    _reset()
    _install_mocks(_lead(uuid=""))                    # заказа МС нет, но сделка заполнена
    res1 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    ozon_invoice._recent.clear()
    res2 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res1 == res2 == "failed-no-ms-order", (res1, res2)
    assert len(_notes) == 1, _notes
    assert len(_alerts) == 1, _alerts
    assert len(_tags) == 1, _tags


def test_is_checked_true_1_on_true_da_false_pusto_0_off_net():
    """_is_checked: True/1/on/true — да; False/пусто/0/off — нет"""
    # ── 10a) _is_checked: форматы amo checkbox ─────────────────────────────────
    for raw in (True, "1", "on", "true", "YES"):
        assert ozon_invoice._is_checked(raw) is True, raw
    for raw in (False, "", "0", None, "off"):
        assert ozon_invoice._is_checked(raw) is False, raw


def test_oplata_kartoy_schet_s_zakazom_ssylka_checkout_ozon_ru_prim():
    """«Оплата картой»: счёт с заказом, ссылка checkout.ozon.ru, примечание про карту"""
    # ── 10b) галочка «Оплата картой» → счёт с заказом, ссылка checkout ──────────
    _reset()
    _install_mocks(_lead(by_card="1"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][2] is True, _ozon_calls  # by_card проброшен
    assert _patches[0]["custom_fields"][FIELD_PAYMENT_LINK].startswith("https://checkout.ozon.ru/"), _patches
    assert any("оплата картой" in n[1] for n in _notes), _notes


def test_bez_galochki_chistyy_sbp_pryamaya_ssylka_qr_nspk_ru():
    """без галочки: чистый СБП, прямая ссылка qr.nspk.ru"""
    # ── 10c) галочка не стоит → чистый СБП (by_card=False, ссылка qr) ───────────
    _reset()
    _install_mocks(_lead())
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created" and _ozon_calls[0][2] is False, _ozon_calls
    assert _patches[0]["custom_fields"][FIELD_PAYMENT_LINK].startswith("https://qr.nspk.ru/"), _patches


def test_ssylka_uzhe_v_pole_vtoroy_platezh_ne_sozdaem_ruchnuyu_ssyl():
    """ссылка уже в поле: второй платёж не создаём, ручную ссылку уважаем"""
    # ── 7) 577617 уже заполнено → скип (update_lead приходит на ЛЮБУЮ правку) ───
    # Кейс 20.07: менеджер вписал ссылку руками, сделка стоит на тех-этапе —
    # следующий вебхук не должен плодить второй платёж.
    _reset()
    _install_mocks(_lead(link="https://qr.nspk.ru/MANUAL"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-link-present", res
    assert not _ozon_calls and not _patches and not _alerts and not _tags


def test_otkaz_ssylka_uzhe_est_ne_blokiruet_sleduyuschuyu_popytku():
    """отказ «ссылка уже есть» не блокирует следующую попытку"""
    # ── отказной заход НЕ занимает дедуп-окно (23.09.2026, сделка 36553383) ─────
    # Раньше окно занимал любой проход, включая отказной. Менеджер очищал поле,
    # чтобы получить новую ссылку, дёргал этап — и попадал в окно, занятое
    # предыдущим отказом. Чем настойчивее дёргал, тем дольше не создавалось.
    _reset()
    _install_mocks(_lead(link="https://qr.nspk.ru/OLD"))
    res1 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res1 == "skipped-link-present", res1
    assert not _ozon_calls, "при заполненном поле в Ozon не ходим"
    _install_mocks(_lead())          # менеджер очистил поле и дёрнул сделку снова
    res2 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res2 == "created", res2
    assert len(_ozon_calls) == 1, _ozon_calls


def test_otkaz_uehala_s_etapa_tozhe_ne_blokiruet_sleduyuschuyu_popy():
    """отказ «уехала с этапа» тоже не блокирует следующую попытку"""
    _reset()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    res1 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res1 == "skipped-moved", res1
    _install_mocks(_lead())
    res2 = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res2 == "created", res2


def test_storozh_ne_nastupaet_na_pyatki_svezhuyu_sdelku_ne_peresozd():
    """сторож не наступает на пятки: свежую сделку не пересоздаёт"""
    # ── сторож: сделка висит на «Оплата запрошена» без ссылки ───────────────────
    _reset()
    _install_mocks(_lead())
    fresh = _lead()
    fresh["updated_at"] = int(time.time())            # только что трогали
    assert run(ozon_invoice._retry_missing_link(fresh)) == 0
    assert not _ozon_calls, "свежую сделку не трогаем — её вебхук ещё в очереди"


def test_storozh_zavisshaya_bez_ssylki_sdelka_poluchaet_schet_povto():
    """сторож: зависшая без ссылки сделка получает счёт повторной попыткой"""
    _reset()
    _install_mocks(_lead())
    stale = _lead()
    stale["updated_at"] = int(time.time() - 10 * 60)  # тишина десять минут
    assert run(ozon_invoice._retry_missing_link(stale)) == 1
    assert len(_ozon_calls) == 1, _ozon_calls
    assert _patches and _patches[0]["status_id"] == STATUS_LINK_SENT


def test_storozh_odna_popytka_na_versiyu_sdelki_povtor_tolko_posle():
    """сторож: одна попытка на версию сделки, повтор только после изменения"""
    # ── сторож не долбит одну и ту же сделку (Катя 23.09.2026: «плохо для сервера») ──
    # Проход сверки идёт каждые три минуты. Без этой защиты сторож ходил в amo и
    # МойСклад по одним и тем же сделкам бесконечно: за сорок минут 18 заходов по
    # двум сделкам, у которых счёт в принципе не мог создаться (нет заказа МС).
    _reset()
    _install_mocks(_lead(uuid=""))                    # заказа МС нет — счёт невозможен
    broken = _lead(uuid="")
    broken["updated_at"] = int(time.time() - 10 * 60)
    assert run(ozon_invoice._retry_missing_link(broken)) == 0
    first_calls = len(_reads)
    assert run(ozon_invoice._retry_missing_link(broken)) == 0
    assert len(_reads) == first_calls, "вторая попытка на той же версии сделки не нужна"
    # сделку тронули — версия сменилась, пробуем снова. Считаем именно чтения сделки,
    # а не примечания: с 28.09.2026 одна и та же причина отказа пишется раз в час.
    broken["updated_at"] = int(time.time() - 9 * 60)
    assert run(ozon_invoice._retry_missing_link(broken)) == 0
    assert len(_reads) > first_calls, "после изменения сделки попытка обязана повториться"


def test_storozh_molchit_o_sdelkah_kotorye_prosto_dolgo_stoyat_na_e():
    """сторож молчит о сделках, которые просто долго стоят на этапе оплаты"""
    _reset()
    _install_mocks(_lead())
    old = _lead()
    old["updated_at"] = int(time.time() - 8 * 3600)    # висит восемь часов
    assert run(ozon_invoice._retry_missing_link(old)) == 0
    assert not _ozon_calls, "давно стоящая сделка — это работа менеджера, не наш сбой"


def test_storozh_ne_lezet_v_akademiyu_tam_schet_vystavlyayut_inache():
    """сторож не лезет в Академию: там счёт выставляют иначе"""
    # Константы Академии импортируются ниже по файлу — здесь берём их из модуля.
    _reset()
    _install_mocks(_lead())
    academy = _lead(status=ozon_invoice.OZON_PAYMENT_STAGES[ozon_invoice.PIPELINE_ACADEMY][0],
                    pipeline=ozon_invoice.PIPELINE_ACADEMY, uuid="")
    academy["updated_at"] = int(time.time() - 10 * 60)
    assert run(ozon_invoice._retry_missing_link(academy)) == 0
    assert not _ozon_calls and not _notes and not _tags, "Академию сторож не трогает вовсе"


def test_storozh_u_sdelki_so_stop_sposobom_ssylki_net_zakonno_chelo():
    """сторож: у сделки со стоп-способом ссылки нет законно — человека не зовём"""
    # ── сторож молчит о сделках со стоп-способом оплаты ─────────────────────────
    # Ссылки нет ПО ЗАМЫСЛУ. Без этого гейта сторож звал бы человека каждые сутки
    # по каждой сделке, где клиент платит криптой.
    _reset()
    _REAL_IN_WINDOW_CRYPTO = ozon_invoice._in_alert_window
    ozon_invoice._in_alert_window = lambda now=None: True
    _install_mocks(_lead(method="Оплата криптой"))
    crypto = _lead(method="Оплата криптой")
    crypto["updated_at"] = int(time.time() - 30 * 60)     # висит полчаса, порог алерта 15 мин
    assert run(ozon_invoice._retry_missing_link(crypto)) == 0
    assert not _alerts and not _tags, (_alerts, _tags)
    assert not _ozon_calls, _ozon_calls
    ozon_invoice._in_alert_window = _REAL_IN_WINDOW_CRYPTO


def test_vebhuk_podpis_self_formuly_proveryaetsya_bitaya_otklonyaet():
    """вебхук: подпись self-формулы проверяется, битая отклоняется"""
    ozon_invoice.OZON_PAY_ACCESS_KEY = "AK-test"
    ozon_invoice.OZON_PAY_NOTIFICATION_SECRET_KEY = "NS-test"
    # ── 11) подпись: валидная self-формула проходит, битая — нет ────────────────
    assert ozon_invoice.verify_notification(_notif()) is True
    bad = _notif(); bad["requestSign"] = "0" * 64
    bad = _notif(); bad["requestSign"] = "0" * 64
    assert ozon_invoice.verify_notification(bad) is False
    assert ozon_invoice.verify_notification({"requestSign": ""}) is False
    # ── 12) Completed + сделка на «Ссылка отправлена» → перевод в «Оплата получена» ─
    _reset(); ozon_invoice._paid_recent.clear()
    # ── 12) Completed + сделка на «Ссылка отправлена» → перевод в «Оплата получена» ─
    _reset(); ozon_invoice._paid_recent.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    res = run(ozon_invoice._handle_notification(_notif()))
    assert res == "moved", res
    assert _patches and _patches[0].get("status_id") == STATUS_PAYMENT_RECEIVED, _patches
    assert any("Оплата подтверждена" in n[1] and "7590 ₽" in n[1] for n in _notes), _notes
    # ── 13) Completed, но менеджер уже перевёл сам → только примечание ──────────
    _reset(); ozon_invoice._paid_recent.clear()
    # ── 13) Completed, но менеджер уже перевёл сам → только примечание ──────────
    _reset(); ozon_invoice._paid_recent.clear()
    _install_mocks(_lead(status=142))
    res = run(ozon_invoice._handle_notification(_notif()))
    assert res == "noted", res
    assert not [p for p in _patches if p.get("status_id")], _patches
    assert any("не двигаю" in n[1] for n in _notes), _notes
    # ── 14) повторный вебхук того же extId → дедуп ──────────────────────────────
    _reset(); ozon_invoice._paid_recent.clear()
    # ── 14) повторный вебхук того же extId → дедуп ──────────────────────────────
    _reset(); ozon_invoice._paid_recent.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    r1 = run(ozon_invoice._handle_notification(_notif()))
    r2 = run(ozon_invoice._handle_notification(_notif()))
    assert r1 == "moved" and r2 == "skipped-duplicate", (r1, r2)
    assert len([p for p in _patches if p.get("status_id")]) == 1
    # ── 15) не наш extId / не Completed / битая подпись → игнор без действий ────
    _reset(); ozon_invoice._paid_recent.clear()
    # ── 15) не наш extId / не Completed / битая подпись → игнор без действий ────
    _reset(); ozon_invoice._paid_recent.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    assert run(ozon_invoice._handle_notification(_notif(ext="site-order-1"))) == "ignored-foreign"
    assert run(ozon_invoice._handle_notification(_notif(status="Rejected"))) == "ignored-status"
    assert run(ozon_invoice._handle_notification(bad)) == "ignored-bad-sign"
    assert not _patches and not _notes and not _alerts


def test_nomer_zakaza_05740_amo_9_1_dlya_ofisa_bez_nomera_ms_prezhn():
    """номер заказа: «05740_amo-9-1» для офиса, без номера МС — прежний ord-"""
    # ── 16) order.extId начинается с номера заказа МС (глаза офиса) ─────────────
    # Прежний «ord-36522883-…» нёс id сделки amo — офис не мог сматчить поступление.
    _card_bodies.clear()
    ozon_invoice._create_payment = _REAL_CREATE_PAYMENT  # тесты выше подменили её моком
    ozon_invoice._client = _FakeClientCard()
    run(ozon_invoice._create_payment("amo-9-1", 1000, by_card=True, ms_order_name="05740"))
    assert _card_bodies[0]["order"]["extId"] == "05740_amo-9-1", _card_bodies
    # extId САМОГО платежа не трогаем — на нём держится дедуп и разбор вебхука.
    assert _card_bodies[0]["extId"] == "amo-9-1", _card_bodies
    _card_bodies.clear()
    run(ozon_invoice._create_payment("amo-9-2", 1000, by_card=True))  # номера МС нет
    assert _card_bodies[0]["order"]["extId"] == "ord-9-2", _card_bodies  # старое поведение
    ozon_invoice._client = None


def test_lead_id_iz_extid_platezh_novyy_zakaz_staryy_ord_schet_sayt():
    """lead_id из extId: платёж, новый заказ, старый ord-; счёт сайта не наш"""
    for raw, expected in cases.items():
        got, ts = ozon_invoice._lead_and_ts_from_ext(raw)
        assert got == expected, (raw, got)
        assert ts and ts > 1_000_000_000, (raw, ts)
    assert ozon_invoice._lead_and_ts_from_ext("18134_cde25f9d") == (None, None)  # счёт сайта — чужой
    assert ozon_invoice._lead_and_ts_from_ext("", None) == (None, None)


def test_vebhuk_kartoy_sdelka_naydena_po_extorderid_a_ne_tolko_po_p():
    """вебхук картой: сделка найдена по extOrderID, а не только по платежу"""
    # ── 18) вебхук картой: наш id только в extOrderID → сделку находим ──────────
    _reset(); ozon_invoice._paid_recent.clear()
    # ── 18) вебхук картой: наш id только в extOrderID → сделку находим ──────────
    _reset(); ozon_invoice._paid_recent.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    card_notif = dict(_notif())
    card_notif["extTransactionID"] = "ozon-internal-xyz"   # не наш платёж
    card_notif["extOrderID"] = "05740_amo-777001-1785242820"
    card_notif["requestSign"] = hashlib.sha256(
        f"{ozon_invoice.OZON_PAY_ACCESS_KEY}|||ozon-internal-xyz|"
        f"{card_notif['amount']}|{card_notif['currencyCode']}|"
        f"{ozon_invoice.OZON_PAY_NOTIFICATION_SECRET_KEY}".encode()
    ).hexdigest()
    res = run(ozon_invoice._handle_notification(card_notif))
    assert res == "moved", res
    assert [p for p in _patches if p.get("status_id") == STATUS_PAYMENT_RECEIVED], _patches


def test_getpaymentdetails_podpis_id_klyuchi_status_iz_paymentdetai():
    """getPaymentDetails: подпись id+ключи, статус из paymentDetails, пустой ответ = ошибка"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global kopecks
    # ── 19) getPaymentDetails: подпись и разбор статуса ─────────────────────────
    sig = ozon_invoice._sign_get_details("pay-1")
    assert sig == hashlib.sha256(
        f"pay-1{ozon_invoice.OZON_PAY_ACCESS_KEY}{ozon_invoice.OZON_PAY_SECRET_KEY}".encode()
    ).hexdigest(), sig
    # статус лежит в paymentDetails — как у боевого ответа Ozon
    ozon_invoice._client = _ClientDetails({"paymentDetails": {"status": "Completed", "amount": {"value": "1000"}}})
    status, kopecks, err = run(ozon_invoice.get_payment_status("pay-1"))
    assert (status, kopecks, err) == ("Completed", 1000, ""), (status, kopecks, err)
    # статуса нет вовсе → честная ошибка, а не «молча не оплачено»
    ozon_invoice._client = _ClientDetails({"whatever": 1})
    status, _, err = run(ozon_invoice.get_payment_status("pay-1"))
    assert status == "" and err, (status, err)
    ozon_invoice._client = None


def test_sverka_oplachennyy_schet_uvodit_sdelku_v_oplata_poluchena():
    """сверка: оплаченный счёт уводит сделку в «Оплата получена», повтор не дублирует"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global _stuck
    # ── 20) сверка: оплаченный счёт двигает сделку без всякого вебхука ──────────
    # Ровно случай Яны и теста 28.07: деньги прошли, уведомление не пришло.
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 20) сверка: оплаченный счёт двигает сделку без всякого вебхука ──────────
    # Ровно случай Яны и теста 28.07: деньги прошли, уведомление не пришло.
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 20) сверка: оплаченный счёт двигает сделку без всякого вебхука ──────────
    # Ровно случай Яны и теста 28.07: деньги прошли, уведомление не пришло.
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    _stuck = _lead(status=STATUS_LINK_SENT)
    _stuck["custom_fields_values"].append(
        {"field_id": FIELD_PAYMENT_LINK, "values": [{"value": "https://checkout.ozon.ru/order/abc"}]}
    )
    amo_service.get_leads_by_status = fake_by_status
    amo_service.get_lead_notes = fake_notes
    ozon_invoice.get_payment_status = fake_status_completed
    res = run(ozon_invoice._reconcile_once())
    assert "moved=1" in res, res
    assert [p for p in _patches if p.get("status_id") == STATUS_PAYMENT_RECEIVED], _patches
    assert any("сверка" in n[1] for n in _notes), _notes
    # второй проход не должен двигать повторно
    _patches.clear()
    run(ozon_invoice._reconcile_once())
    assert not [p for p in _patches if p.get("status_id")], _patches


def test_sverka_zavisshiy_schet_odno_napominanie_vozrast_cheloveche():
    """сверка: зависший счёт — одно напоминание, возраст человеческий, сделку не трогаем"""
    # ── 21) сверка: висит без оплаты дольше порога → одно напоминание ───────────
    # Окно алертов подменяем: тест не должен зависеть от времени суток, когда его
    # запустили (до фикса 31.07.2026 окна не было вовсе и алерты уходили ночью).
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21) сверка: висит без оплаты дольше порога → одно напоминание ───────────
    # Окно алертов подменяем: тест не должен зависеть от времени суток, когда его
    # запустили (до фикса 31.07.2026 окна не было вовсе и алерты уходили ночью).
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21) сверка: висит без оплаты дольше порога → одно напоминание ───────────
    # Окно алертов подменяем: тест не должен зависеть от времени суток, когда его
    # запустили (до фикса 31.07.2026 окна не было вовсе и алерты уходили ночью).
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    _REAL_IN_WINDOW = ozon_invoice._in_alert_window
    ozon_invoice._in_alert_window = lambda now=None: True
    amo_service.get_lead_notes = fake_notes_old
    ozon_invoice.get_payment_status = fake_status_pending
    run(ozon_invoice._reconcile_once())
    assert not [p for p in _patches if p.get("status_id")], _patches
    assert len(_alerts) == 1 and "без оплаты" in _alerts[0], _alerts
    assert "3 ч" in _alerts[0], f"возраст должен быть человеческим: {_alerts[0]!r}"
    assert "180 мин" not in _alerts[0], f"сырые минуты вернулись: {_alerts[0]!r}"
    run(ozon_invoice._reconcile_once())          # второй проход — молчим
    assert len(_alerts) == 1, _alerts
    # ── 21б) ночью не пишем вообще ──────────────────────────────────────────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21б) ночью не пишем вообще ──────────────────────────────────────────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21б) ночью не пишем вообще ──────────────────────────────────────────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    amo_service.get_lead_notes = fake_notes_old
    ozon_invoice.get_payment_status = fake_status_pending
    ozon_invoice._in_alert_window = lambda now=None: False
    run(ozon_invoice._reconcile_once())
    assert _alerts == [], f"вне рабочего окна сообщений быть не должно: {_alerts}"
    ozon_invoice._in_alert_window = _REAL_IN_WINDOW
    # ── 21в) отклонённая оплата подписана иначе, чем «просто не платят» ─────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21в) отклонённая оплата подписана иначе, чем «просто не платят» ─────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    # ── 21в) отклонённая оплата подписана иначе, чем «просто не платят» ─────────
    _reset(); ozon_invoice._paid_recent.clear(); ozon_invoice._stale_alerted.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    amo_service.get_lead_notes = fake_notes_old
    ozon_invoice._in_alert_window = lambda now=None: True
    ozon_invoice.get_payment_status = fake_status_rejected
    run(ozon_invoice._reconcile_once())
    assert len(_alerts) == 1, _alerts
    assert "Оплата отклонена" in _alerts[0], _alerts
    ozon_invoice._in_alert_window = _REAL_IN_WINDOW


def test_sverka_sdelka_bez_vystavlennogo_scheta_propuskaetsya_ozon():
    """сверка: сделка без выставленного счёта пропускается, Ozon не дёргаем"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global _asked, _no_link
    # ── 22) сверка: счёта нет (ссылка пустая) → Ozon не дёргаем ────────────────
    _reset(); ozon_invoice._stale_alerted.clear()
    # ── 22) сверка: счёта нет (ссылка пустая) → Ozon не дёргаем ────────────────
    _reset(); ozon_invoice._stale_alerted.clear()
    _install_mocks(_lead(status=STATUS_LINK_SENT))
    _no_link = _lead(status=STATUS_LINK_SENT)
    _asked = []
    amo_service.get_leads_by_status = fake_by_status_nolink
    ozon_invoice.get_payment_status = fake_status_spy
    res = run(ozon_invoice._reconcile_once())
    assert "checked=0" in res and not _asked, (res, _asked)


def test_razbor_otveta_status_iz_items_0_pustoy_items_platezha_net():
    """разбор ответа: статус из items[0], пустой items = платежа нет, старые формы живы"""
    assert ozon_invoice._extract_status(real) == ("PAYMENT_REJECTED", 1297900)
    assert ozon_invoice._extract_status({"items": []}) == ("", None)   # платежа Ozon не знает
    # старые формы всё ещё понимаем
    assert ozon_invoice._extract_status({"paymentDetails": {"status": "Completed"}})[0] == "Completed"


def test_statusy_completed_i_payment_completed_oplachen_rejected_ne():
    """статусы: Completed и PAYMENT_COMPLETED = оплачен, REJECTED/NEW/пусто = нет"""
    # ── 24) словарь статусов: PAYMENT_* и Completed ────────────────────────────
    for ok in ("Completed", "PAYMENT_COMPLETED", "payment_completed", " PAID "):
        assert ozon_invoice.is_paid_status(ok), ok
    for bad in ("PAYMENT_REJECTED", "PAYMENT_NEW", "", "Pending", "PAYMENT_CANCELLED"):
        assert not ozon_invoice.is_paid_status(bad), bad


def test_karta_pusto_po_platezhu_ischem_po_extid_zakaza_podpis_pod():
    """карта: пусто по платежу → ищем по extId заказа, подпись под каждый id"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global kopecks
    ozon_invoice.get_payment_status = _REAL_GET_PAYMENT_STATUS  # тесты 20-22 подменили её моком
    ozon_invoice._client = _ClientChain()
    status, kopecks, err = run(ozon_invoice.get_payment_status(
        "019fa8c3-pay", "amo-777001-1785242820", "05748_amo-777001-1785242820"))
    assert (status, kopecks, err) == ("PAYMENT_COMPLETED", 1000, ""), (status, kopecks, err)
    assert [b.get("id") or b.get("extId") for b in _asked_bodies] == [
        "019fa8c3-pay", "amo-777001-1785242820", "05748_amo-777001-1785242820"], _asked_bodies
    # подпись пересчитывается под КАЖДЫЙ искомый идентификатор
    assert _asked_bodies[-1]["requestSign"] == hashlib.sha256(
        f"05748_amo-777001-1785242820{ozon_invoice.OZON_PAY_ACCESS_KEY}"
        f"{ozon_invoice.OZON_PAY_SECRET_KEY}".encode()).hexdigest()
    ozon_invoice._client = None


def test_platezh_ne_nayden_ni_po_odnomu_id_oshibka_a_ne_ne_oplachen():
    """платёж не найден ни по одному id: ошибка, а не «не оплачен»"""
    ozon_invoice._client = _ClientEmpty()
    status, _, err = run(ozon_invoice.get_payment_status("p", "e", "o"))
    assert status == "" and "не найден" in err, (status, err)
    ozon_invoice._client = None


def test_otkaz_po_odnomu_klyuchu_ne_meshaet_nayti_platezh_po_extid():
    """отказ по одному ключу не мешает найти платёж по extId заказа"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global kopecks
    _denied = _ClientDeniedMiddle()
    ozon_invoice._client = _denied
    status, kopecks, err = run(ozon_invoice.get_payment_status(
        "pay-403", "amo-36544371-1788004604", "06930_amo-36544371-1788004604"))
    assert (status, kopecks, err) == ("PAYMENT_COMPLETED", 1036300, ""), (status, kopecks, err)
    assert _denied.asked == ["pay-403", "amo-36544371-1788004604",
                             "06930_amo-36544371-1788004604"], _denied.asked
    ozon_invoice._client = None


def test_otkaz_po_vsem_klyucham_v_oshibke_vidno_403_a_ne_platezh_ne():
    """отказ по всем ключам: в ошибке видно 403, а не «платёж не найден»"""
    ozon_invoice._client = _ClientAllDenied()
    status, _, err = run(ozon_invoice.get_payment_status("p", "e", "o"))
    assert status == "" and "403" in err, (status, err)
    ozon_invoice._client = None


def test_orderextid_pishetsya_dlya_karty_extid_platezha_ne_podmenya():
    """orderExtId: пишется для карты, extId платежа не подменяется, старые счета восстанавливаются"""
    # ── 27) orderExtId живёт в примечании и читается сверкой ───────────────────
    assert ozon_invoice.build_order_ext_id("amo-9-1", "05748") == "05748_amo-9-1"
    assert ozon_invoice.build_order_ext_id("amo-9-1", "") == "ord-9-1"
    amo_service.get_lead_notes = notes_with_order
    pid, ext, order_ext, _, _ = run(ozon_invoice._payment_ref(777001))
    # extId платежа НЕ должен подмениться номером заказа из orderExtId
    assert (pid, ext, order_ext) == ("pid-1", "amo-777001-1785242820",
                                     "05748_amo-777001-1785242820"), (pid, ext, order_ext)
    amo_service.get_lead_notes = notes_old_format
    pid, ext, order_ext, _, _ = run(ozon_invoice._payment_ref(777001))
    # у старых счетов orderExtId в примечании нет — восстанавливаем прежнюю форму
    assert order_ext == "ord-777001-1785242820", order_ext


def test_scenario():
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global _MSK
    _MSK = ozon_invoice._MSK
    _H = ozon_invoice.OZON_STALE_EVENING_H
    due = ozon_invoice._stale_due
    порог = ozon_invoice.OZON_STALE_ALERT_MIN
    # счёт моложе порога — молчим
    assert due(порог - 1, None, _at(3, 13)) == ""
    # перевисел порог, ещё не писали — первое напоминание
    assert due(порог + 1, None, _at(3, 13)) == "first"
    # писали сегодня утром — вечером того же дня ВТОРОЕ НЕ уходит
    assert due(600, _ts(3, 12), _at(3, _H)) == ""
    assert due(600, _ts(3, 12), _at(3, 23)) == ""
    # на следующий день вечером — одно напоминание
    assert due(2000, _ts(3, 12), _at(4, _H)) == "evening"
    # но днём следующего дня ещё рано
    assert due(2000, _ts(3, 12), _at(4, _H - 1)) == ""
    # и второе за те же сутки не уходит
    assert due(2000, _ts(4, _H), _at(4, _H + 1)) == ""


def test_kartoteka_flag_vyklyuchen_po_umolchaniyu_voronka_odna_rozn():
    """картотека: флаг выключен по умолчанию, воронка одна — розница"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global _asked
    _FLAG_WAS = ozon_invoice.OZON_INVOICE_DB_WORK
    # ── к) флаг выключен по умолчанию: код едет на прод, ничего не делая ────────
    assert _FLAG_WAS is False, "OZON_INVOICE_DB_WORK должен быть выключен по умолчанию"
    assert ozon_invoice._invoice_pipelines() == (PIPELINE_CLEVER_MAIN,)
    # ── к1) флаг выключен + сделка на тех-этапе картотеки → полный скип ─────────
    _reset()
    _install_mocks(_lead(status=STATUS_DB_PAYMENT_REQUESTED, pipeline=PIPELINE_DB_WORK))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-moved", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    # ── к2) флаг включён: счёт создан, PATCH несёт ЭТАПЫ И ВОРОНКУ КАРТОТЕКИ ────
    # Главный тест задачи: ловит «зашили розничный этап в картотечный PATCH».
    ozon_invoice.OZON_INVOICE_DB_WORK = True
    _reset()
    _install_mocks(_lead(status=STATUS_DB_PAYMENT_REQUESTED, pipeline=PIPELINE_DB_WORK))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["status_id"] == STATUS_DB_LINK_SENT, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_DB_WORK, _patches[0]
    assert _patches[0]["custom_fields"][FIELD_PAYMENT_LINK].startswith("https://qr.nspk.ru/")
    # ── к3) розница при включённом флаге не сломалась ───────────────────────────
    _reset()
    _install_mocks(_lead())
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _patches[0]["status_id"] == STATUS_LINK_SENT, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_CLEVER_MAIN, _patches[0]
    # ── к3a) картотека: «Другая сумма» без заказа МС тоже создаёт счёт ──────────
    # Требование Кати «во всех воронках»: развилка живёт в общем коде, не в розничной ветке.
    _reset()
    _install_mocks(_lead(status=STATUS_DB_PAYMENT_REQUESTED, pipeline=PIPELINE_DB_WORK,
                         uuid=None, other="2 500"))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert _ozon_calls[0][1] == 250000, _ozon_calls
    assert _patches[0]["pipeline_id"] == PIPELINE_DB_WORK, _patches[0]
    assert not _alerts, _alerts
    # ── к4) перекрёстный негатив: картотека на РОЗНИЧНОМ этапе → скип ───────────
    # Ловит гейт, который сверяет только этап и не смотрит, из какой он воронки.
    _reset()
    _install_mocks(_lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_DB_WORK))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-moved", res
    assert not _ozon_calls, _ozon_calls
    # ── к5) is_invoice_entry: вебхук без воронки, воронка строкой, мусор ────────
    assert ozon_invoice.is_invoice_entry(None, STATUS_DB_PAYMENT_REQUESTED) is True
    assert ozon_invoice.is_invoice_entry("11166334", str(STATUS_DB_PAYMENT_REQUESTED)) is True
    assert ozon_invoice.is_invoice_entry(PIPELINE_DB_WORK, STATUS_DB_LINK_SENT) is False
    assert ozon_invoice.is_invoice_entry("мусор", STATUS_DB_PAYMENT_REQUESTED) is False
    assert ozon_invoice.is_invoice_entry(PIPELINE_DB_WORK, None) is False
    # ── к6) сверка ходит только по включённым воронкам ──────────────────────────
    _asked = []
    _real_by_status = amo_service.get_leads_by_status
    amo_service.get_leads_by_status = _fake_by_status
    _asked.clear()
    run(ozon_invoice._reconcile_once())
    assert set(_asked) == {STATUS_LINK_SENT, STATUS_PAYMENT_REQUESTED,
                           STATUS_DB_LINK_SENT, STATUS_DB_PAYMENT_REQUESTED}, _asked
    ozon_invoice.OZON_INVOICE_DB_WORK = False
    _asked.clear()
    run(ozon_invoice._reconcile_once())
    assert set(_asked) == {STATUS_LINK_SENT, STATUS_PAYMENT_REQUESTED}, _asked
    # ── к7) оплата доводится до конца даже при ВЫКЛЮЧЕННОМ флаге ────────────────
    # Деньги списаны: оставить сделку на «ссылка отправлена» с одним примечанием
    # нельзя. Контракт отката, без теста развалится при первом рефакторинге.
    assert ozon_invoice.OZON_INVOICE_DB_WORK is False
    _reset()
    _install_mocks(_lead(status=STATUS_DB_LINK_SENT, pipeline=PIPELINE_DB_WORK))
    res = run(ozon_invoice._mark_paid(
        _lead(status=STATUS_DB_LINK_SENT, pipeline=PIPELINE_DB_WORK), "1000", "extId x", "вебхук"))
    assert res == "moved", res
    assert _patches[0]["status_id"] == STATUS_DB_PAYMENT_RECEIVED, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_DB_WORK, _patches[0]
    # ── к8) чужая воронка в _mark_paid → только примечание, PATCH нет ───────────
    _reset()
    _install_mocks(_lead())
    res = run(ozon_invoice._mark_paid(
        _lead(status=142, pipeline=9421022), "1000", "extId y", "сверка"))
    assert res == "noted", res
    assert not _patches, _patches
    amo_service.get_leads_by_status = _real_by_status
    ozon_invoice.OZON_INVOICE_DB_WORK = _FLAG_WAS
    _ACADEMY_FLAG_WAS = ozon_invoice.OZON_INVOICE_ACADEMY
    # ── а) флаг выключен по умолчанию ──────────────────────────────────────────
    assert _ACADEMY_FLAG_WAS is False, "OZON_INVOICE_ACADEMY должен быть выключен по умолчанию"
    assert PIPELINE_ACADEMY not in ozon_invoice._invoice_pipelines()
    # ── б) флаг выключен + сделка на тех-этапе Академии → полный скип ──────────
    _reset()
    _install_mocks(_lead(status=STATUS_ACADEMY_PAYMENT_REQUESTED, pipeline=PIPELINE_ACADEMY))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-moved", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    # ── в) флаг включён: счёт создан, PATCH несёт ЭТАПЫ И ВОРОНКУ АКАДЕМИИ ─────
    ozon_invoice.OZON_INVOICE_ACADEMY = True
    _reset()
    _install_mocks(_lead(status=STATUS_ACADEMY_PAYMENT_REQUESTED, pipeline=PIPELINE_ACADEMY))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["status_id"] == STATUS_ACADEMY_LINK_SENT, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_ACADEMY, _patches[0]
    # ── г) три воронки одновременно не мешают друг другу ───────────────────────
    ozon_invoice.OZON_INVOICE_DB_WORK = True
    assert ozon_invoice._invoice_pipelines() == (PIPELINE_CLEVER_MAIN, PIPELINE_DB_WORK, PIPELINE_ACADEMY)
    _reset()
    _install_mocks(_lead())
    assert run(ozon_invoice.process_invoice_lead(LEAD_ID)) == "created"
    assert _patches[0]["pipeline_id"] == PIPELINE_CLEVER_MAIN, _patches[0]
    # ── д) перекрёстный негатив: чужой тех-этап в Академии → скип ──────────────
    _reset()
    _install_mocks(_lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_ACADEMY))
    assert run(ozon_invoice.process_invoice_lead(LEAD_ID)) == "skipped-moved"
    assert not _ozon_calls, _ozon_calls
    amo_service.get_leads_by_status = _fake_by_status_all
    run(ozon_invoice._reconcile_once())
    assert set(_asked_all) == {
        STATUS_LINK_SENT, STATUS_PAYMENT_REQUESTED,
        STATUS_DB_LINK_SENT, STATUS_DB_PAYMENT_REQUESTED,
        STATUS_ACADEMY_LINK_SENT, STATUS_ACADEMY_PAYMENT_REQUESTED,
    }, _asked_all
    assert len(_asked_all) == 6, _asked_all
    amo_service.get_leads_by_status = _real_by_status
    # ── ж) оплата доводится до конца и с опущенным флагом ──────────────────────
    ozon_invoice.OZON_INVOICE_ACADEMY = False
    _reset()
    _install_mocks(_lead(status=STATUS_ACADEMY_LINK_SENT, pipeline=PIPELINE_ACADEMY))
    res = run(ozon_invoice._mark_paid(
        _lead(status=STATUS_ACADEMY_LINK_SENT, pipeline=PIPELINE_ACADEMY), "1000", "extId z", "вебхук"))
    assert res == "moved", res
    assert _patches[0]["status_id"] == STATUS_ACADEMY_PAYMENT_RECEIVED, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_ACADEMY, _patches[0]
    ozon_invoice.OZON_INVOICE_DB_WORK = _FLAG_WAS
    ozon_invoice.OZON_INVOICE_ACADEMY = _ACADEMY_FLAG_WAS
    _TG_FLAG_WAS = ozon_invoice.OZON_INVOICE_TANGEMSHOP
    assert _TG_FLAG_WAS is False, "OZON_INVOICE_TANGEMSHOP должен быть выключен по умолчанию"
    assert PIPELINE_TANGEMSHOP not in ozon_invoice._invoice_pipelines()
    # флаг выключен + сделка на тех-этапе Tangemshop → полный скип
    _reset()
    _install_mocks(_lead(status=STATUS_TANGEM_PAYMENT_REQUESTED, pipeline=PIPELINE_TANGEMSHOP))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "skipped-moved", res
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    # флаг включён: счёт создан, PATCH несёт этапы и воронку Tangemshop
    ozon_invoice.OZON_INVOICE_TANGEMSHOP = True
    _reset()
    _install_mocks(_lead(status=STATUS_TANGEM_PAYMENT_REQUESTED, pipeline=PIPELINE_TANGEMSHOP))
    res = run(ozon_invoice.process_invoice_lead(LEAD_ID))
    assert res == "created", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["status_id"] == STATUS_TANGEM_LINK_SENT, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_TANGEMSHOP, _patches[0]
    # перекрёстный негатив: розничный тех-этап внутри воронки Tangemshop → скип
    _reset()
    _install_mocks(_lead(status=STATUS_PAYMENT_REQUESTED, pipeline=PIPELINE_TANGEMSHOP))
    assert run(ozon_invoice.process_invoice_lead(LEAD_ID)) == "skipped-moved"
    assert not _ozon_calls and not _patches, (_ozon_calls, _patches)
    # оплата доводится до конца и с опущенным флагом: деньги клиента уже списаны
    ozon_invoice.OZON_INVOICE_TANGEMSHOP = False
    _reset()
    _install_mocks(_lead(status=STATUS_TANGEM_LINK_SENT, pipeline=PIPELINE_TANGEMSHOP))
    res = run(ozon_invoice._mark_paid(
        _lead(status=STATUS_TANGEM_LINK_SENT, pipeline=PIPELINE_TANGEMSHOP),
        "1000", "extId tg", "вебхук"))
    assert res == "moved", res
    assert _patches[0]["status_id"] == STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED, _patches[0]
    assert _patches[0]["pipeline_id"] == PIPELINE_TANGEMSHOP, _patches[0]
    amo_service.get_leads_by_status = _fake_by_status_tg
    run(ozon_invoice._reconcile_once())
    assert STATUS_TANGEM_LINK_SENT not in _asked_tg, _asked_tg
    assert STATUS_TANGEM_PAYMENT_REQUESTED not in _asked_tg, _asked_tg
    amo_service.get_leads_by_status = _real_by_status
    ozon_invoice.OZON_INVOICE_TANGEMSHOP = _TG_FLAG_WAS

