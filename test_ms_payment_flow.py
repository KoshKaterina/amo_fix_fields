"""Оркестровка записи оплаты в МС (ms_payment_flow): уведомления, автоповтор, триггер Офиса.

Без сети и прода: ms_client, amo_service и telegram_bot подменяются фикстурой с возвратом.

Ключевые инварианты:
  • недоплата - логический алерт в «ОП Уведомления» с тегом ответственного, платёж записан;
  • переплата и «заказ уже оплачен» - предупреждение в технический чат (адрес не задан);
  • сумма сошлась - тишина;
  • сбой МС: платёж в очереди на диске, через 30 минут ОДИН технический алерт, через сутки
    финальный; восстановился - платёж записан, очередь пуста;
  • сухой прогон не заводит очередь, не пишет в амо и не зовёт людей;
  • «Успешно реализовано» Офиса: только самовывоз из офиса и наложка, только свежие сделки,
    только остаток до суммы.
"""
import asyncio
import sys
import time
import types

import pytest

_tb = types.ModuleType("telegram_bot")
_tb.send_alert = None
_prev_tb = sys.modules.get("telegram_bot")
sys.modules["telegram_bot"] = _tb
import amo_service  # noqa: E402
import ms_client  # noqa: E402
import ms_payment  # noqa: E402
import ms_payment_flow as flow  # noqa: E402
import tg_recipients  # noqa: E402
if _prev_tb is None:
    sys.modules.pop("telegram_bot", None)
else:
    sys.modules["telegram_bot"] = _prev_tb

from waybill_config import (  # noqa: E402
    FIELD_DELIVERY_TYPE,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_METHOD,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OFFICE,
    STATUS_SUCCESS,
)

UU = "0e5a2b05-aaaa-bbbb-cccc-0123456789ab"
LEAD_ID = 36590681
ORG = {"href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/o1", "type": "organization"}
AGENT = {"href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/a1", "type": "counterparty"}
ORDER_META = {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/customerorder/{UU}", "type": "customerorder"}
PICKUP = "Самовывоз из офиса Sunscrypt, 0.00 рублей"
T0 = 1_800_000_000.0


def run(coro):
    return asyncio.run(coro)


class FakeMS:
    def __init__(self, total=1164900, payed=1014900):
        self.order = {"id": UU, "name": "09386", "sum": total, "payedSum": payed, "meta": ORDER_META,
                      "organization": {"meta": ORG}, "agent": {"meta": AGENT}}
        self.payments: list[dict] = []
        self.posts: list[dict] = []
        self.down = False          # МС недоступен целиком

    async def get(self, path, params=None, **kw):
        if self.down:
            return None
        if path == f"entity/customerorder/{UU}":
            return dict(self.order)
        if path == "entity/paymentin":
            code = (params or {}).get("filter", "").split("=", 1)[-1]
            return {"rows": [p for p in self.payments if p["externalCode"] == code]}
        return None

    async def post(self, path, body, **kw):
        if self.down:
            return None
        self.posts.append(body)
        self.payments.append(body)
        self.order["payedSum"] += body["sum"]
        return {"id": "pay-1", **body}


class Sink:
    def __init__(self):
        self.notes, self.alerts = [], []


def lead_dict(*, pipeline=PIPELINE_OFFICE, status=STATUS_SUCCESS, delivery=PICKUP, method="Наличные менеджер",
              closed_ago=3600, uuid=UU):
    cf = []
    if uuid is not None:
        cf.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": uuid}]})
    if delivery is not None:
        cf.append({"field_id": FIELD_DELIVERY_TYPE, "values": [{"value": delivery}]})
    if method is not None:
        cf.append({"field_id": FIELD_PAYMENT_METHOD, "values": [{"value": method}]})
    return {"id": LEAD_ID, "name": "Заказ №9276", "pipeline_id": pipeline, "status_id": status,
            "responsible_user_id": 11513202, "closed_at": int(time.time() - closed_ago) if closed_ago is not None else None,
            "custom_fields_values": cf}


@pytest.fixture
def ms(monkeypatch):
    fake = FakeMS()
    monkeypatch.setattr(ms_client, "get", fake.get)
    monkeypatch.setattr(ms_client, "post", fake.post)
    ms_payment._inflight.clear()
    return fake


@pytest.fixture
def env(monkeypatch, ms, tmp_path):
    sink = Sink()
    sink.lead = lead_dict()

    async def get_lead_full(lead_id, with_=()):
        return sink.lead

    async def add_note(lead_id, text):
        sink.notes.append(text)
        return {"ok": True}

    async def send_alert(text, **kw):
        sink.alerts.append((text, kw))
        return True

    monkeypatch.setattr(amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(amo_service, "add_note", add_note)
    monkeypatch.setattr(flow.telegram_bot, "send_alert", send_alert)
    monkeypatch.setattr(flow, "OZON_MS_PAYMENT_ENABLED", True)
    monkeypatch.setattr(flow, "OZON_MS_PAYMENT_DRY_RUN", False)
    monkeypatch.setattr(flow, "OFFICE_MS_PAYMENT_ENABLED", True)
    monkeypatch.setattr(flow, "OFFICE_MS_PAYMENT_DRY_RUN", False)
    monkeypatch.setattr(flow, "MS_PAYMENT_PENDING_PATH", str(tmp_path / "pending.json"))
    flow._alerted_recent.clear()
    flow._office_recent.clear()
    return sink


def ozon_lead():
    return lead_dict(pipeline=PIPELINE_CLEVER_MAIN, status=83537874, delivery=None, method=None)


def is_ops(alert):       # ОП Уведомления: чат и топик заданы
    return alert[1]["chat_id"] == tg_recipients.NOTIFY_CHAT_ID and alert[1]["message_thread_id"] == tg_recipients.NOTIFY_THREAD_ID


def is_tech(alert):      # технический чат: адрес не задан, send_alert возьмёт чат по умолчанию
    return alert[1]["chat_id"] is None and alert[1]["message_thread_id"] is None


# ── Ozon: уведомления по исходу ──────────────────────────────────────────────

def test_summa_soshlas_tiho_platezh_zapisan(env, ms):
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1")) == "created"
    assert len(ms.posts) == 1 and not env.alerts and not env.notes


def test_nedoplata_logicheskiy_alert_v_op_s_tegom_platezh_zapisan(env, ms):
    assert run(flow.on_ozon_paid(ozon_lead(), 100000, "1-1")) == "created-short"
    assert len(ms.posts) == 1 and ms.posts[0]["sum"] == 100000
    assert len(env.alerts) == 1 and is_ops(env.alerts[0]), env.alerts
    assert "меньше суммы заказа" in env.alerts[0][0] and "итог 11149.00" in env.alerts[0][0]
    assert tg_recipients.mentions_for(11513202) in env.alerts[0][0]
    assert len(env.notes) == 1


def test_pereplata_preduprezhdenie_v_tehchat_platezh_zapisan(env, ms):
    assert run(flow.on_ozon_paid(ozon_lead(), 200000, "1-1")) == "created-over"
    assert len(ms.posts) == 1 and ms.posts[0]["sum"] == 200000
    assert len(env.alerts) == 1 and is_tech(env.alerts[0]), env.alerts
    assert "больше суммы заказа" in env.alerts[0][0]


def test_zakaz_uzhe_oplachen_preduprezhdenie_v_tehchat_platezh_ne_pishem(env, ms):
    ms.order["payedSum"] = ms.order["sum"]
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1")) == "skipped-fully-paid"
    assert not ms.posts and len(env.alerts) == 1 and is_tech(env.alerts[0])
    assert "НЕ записан" in env.alerts[0][0]


def test_dannye_ne_shodyatsya_logicheskiy_alert_v_op(env, ms):
    ms.order["sum"] = 0
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1")) == "skipped-zero-sum"
    assert len(env.alerts) == 1 and is_ops(env.alerts[0])
    ms.order["sum"] = 1164900
    ms.order.pop("agent")
    flow._alerted_recent.clear()          # тот же заголовок по той же сделке в течение часа гасится
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-2")) == "failed-order-shape"
    assert len(env.alerts) == 2 and is_ops(env.alerts[1])
    assert flow.pending_count() == 0          # данные повтором не чинятся


def test_odin_i_tot_zhe_alert_po_sdelke_ne_chashche_raza_v_chas(env, ms):
    ms.order["sum"] = 0
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1"))
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-2"))
    assert len(env.alerts) == 1


def test_suhoy_progon_ne_pishet_ne_zovyot_ne_zavodit_ochered(env, ms, monkeypatch):
    monkeypatch.setattr(flow, "OZON_MS_PAYMENT_DRY_RUN", True)
    assert run(flow.on_ozon_paid(ozon_lead(), 100000, "1-1")) == "created-short"
    ms.down = True
    assert run(flow.on_ozon_paid(ozon_lead(), 100000, "1-2")) == "failed-ms-read"
    assert not ms.posts and not env.alerts and not env.notes and flow.pending_count() == 0


def test_flag_vyklyuchen(env, ms, monkeypatch):
    monkeypatch.setattr(flow, "OZON_MS_PAYMENT_ENABLED", False)
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1")) == "disabled"
    assert not ms.posts


# ── автоповтор ───────────────────────────────────────────────────────────────

def test_sboy_ms_platezh_v_ocheredi_alerta_net(env, ms):
    ms.down = True
    assert run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1")) == "queued"
    assert flow.pending_count() == 1 and not env.alerts and not ms.posts


def test_povtory_do_30_minut_molchat_potom_odin_tehnicheskiy_alert(env, ms):
    ms.down = True
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1"))
    entry = next(iter(flow._load_pending().values()))
    first = entry["first_at"]
    # тик раньше срока ничего не делает
    assert run(flow.retry_once(now=first + 10))["due"] == 0
    # повторы в пределах получаса - молчим, пауза растёт: 1, 2, 4, 8, 16 минут
    for minutes in (1.1, 3.2, 7.4, 15.6):
        stats = run(flow.retry_once(now=first + minutes * 60))
        assert stats["due"] == 1 and stats["failed"] == 1 and not env.alerts, (minutes, stats)
    assert flow._retry_delay(0) == 60 and flow._retry_delay(3) == 480 and flow._retry_delay(9) == 3600
    # перевалили за 30 минут - один технический алерт, не два
    entry = next(iter(flow._load_pending().values()))
    stats = run(flow.retry_once(now=max(entry["next_at"], first + 1801)))
    assert stats["alerted"] == 1 and len(env.alerts) == 1 and is_tech(env.alerts[0]), env.alerts
    assert "МС недоступен" in env.alerts[0][0]
    entry = next(iter(flow._load_pending().values()))
    run(flow.retry_once(now=entry["next_at"] + 1))
    assert len(env.alerts) == 1 and flow.pending_count() == 1


def test_ms_vosstanovilsya_platezh_zapisan_ochered_pusta_bez_alerta(env, ms):
    ms.down = True
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1"))
    entry = next(iter(flow._load_pending().values()))
    ms.down = False
    stats = run(flow.retry_once(now=entry["next_at"] + 1))
    assert stats["done"] == 1 and flow.pending_count() == 0
    assert len(ms.posts) == 1 and ms.posts[0]["sum"] == 150000 and not env.alerts


def test_posle_vosstanovleniya_nedoplata_vse_ravno_zovyot_op(env, ms):
    ms.down = True
    run(flow.on_ozon_paid(ozon_lead(), 100000, "1-1"))
    entry = next(iter(flow._load_pending().values()))
    ms.down = False
    run(flow.retry_once(now=entry["next_at"] + 1))
    assert len(env.alerts) == 1 and is_ops(env.alerts[0])


def test_sutki_povtorov_finalnyy_alert_i_snyatie_s_ocheredi(env, ms):
    ms.down = True
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1"))
    entry = next(iter(flow._load_pending().values()))
    stats = run(flow.retry_once(now=entry["first_at"] + 86400 + 1000))
    assert stats["gave_up"] == 1 and flow.pending_count() == 0
    assert len(env.alerts) == 1 and "внесите вручную" in env.alerts[0][0] and is_tech(env.alerts[0])


def test_ochered_perezhivaet_perezapusk_ona_na_diske(env, ms):
    ms.down = True
    run(flow.on_ozon_paid(ozon_lead(), 150000, "1-1"))
    import json
    with open(flow.MS_PAYMENT_PENDING_PATH, encoding="utf-8") as f:
        data = json.load(f)
    (qkey, e), = data.items()
    assert qkey == "ozon:1-1" and e["kopecks"] == 150000 and e["order_uuid"] == UU and e["lead_id"] == LEAD_ID


def test_povrezhdyonnyy_fayl_ocheredi_ne_valit_sistemu(env, ms):
    with open(flow.MS_PAYMENT_PENDING_PATH, "w", encoding="utf-8") as f:
        f.write("{not json")
    assert flow.pending_count() == 0
    assert run(flow.retry_once())["due"] == 0


# ── «Успешно реализовано» Офиса ──────────────────────────────────────────────

def test_gate_vebhuka_ofis_i_uspeshno_realizovano():
    assert flow.is_office_entry(PIPELINE_OFFICE, STATUS_SUCCESS)
    assert flow.is_office_entry(None, 142)                 # воронки в вебхуке нет - решит обработчик
    assert not flow.is_office_entry(PIPELINE_CLEVER_MAIN, 142)
    assert not flow.is_office_entry(PIPELINE_OFFICE, 75426862)
    assert not flow.is_office_entry(PIPELINE_OFFICE, None)
    assert not flow.is_office_entry(PIPELINE_OFFICE, "мусор")


def test_samovyvoz_iz_ofisa_pishet_ostatok(env, ms):
    ms.order["payedSum"] = 0
    env.lead = lead_dict(method="Онлайн оплата")           # способ не наложка: самовывоз сам по себе
    assert run(flow.process_office_payment(LEAD_ID)) == "created"
    assert ms.posts[0]["sum"] == 1164900 and ms.posts[0]["externalCode"] == f"office-{LEAD_ID}"
    assert "самовывоз из офиса" in ms.posts[0]["description"]
    assert not env.alerts and not env.notes                # успех - тихо


def test_nalozhka_pishet_ostatok(env, ms):
    ms.order["payedSum"] = 0
    env.lead = lead_dict(delivery="Доставка курьером по Москве, 1 000.00 рублей", method="Оплата при получении")
    assert run(flow.process_office_payment(LEAD_ID)) == "created"
    assert "наложка" in ms.posts[0]["description"] and "самовывоз" not in ms.posts[0]["description"]


def test_samovyvoz_i_nalozhka_srazu_oba_priznaka(env, ms):
    env.lead = lead_dict(method="Эвотор")
    assert run(flow.process_office_payment(LEAD_ID)) == "created"
    assert "самовывоз из офиса, наложка" in ms.posts[0]["description"]
    assert ms.posts[0]["sum"] == 150000                     # остаток, а не вся сумма заказа


def test_onlayn_oplaty_cdek_kurer_ne_nashi_sluchai(env, ms):
    env.lead = lead_dict(delivery="CDEK: Самовывоз, (3-4 дней), 305.00 рублей", method="Онлайн-оплата")
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-not-applicable"
    flow._office_recent.clear()
    env.lead = lead_dict(delivery="Самовывоз из шоурума Sunscrypt, 0.00 рублей", method="Онлайн-оплата")
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-not-applicable"
    assert not ms.posts and not ms.payments


def test_zakaz_uzhe_oplachen_onlayn_molchim(env, ms):
    ms.order["payedSum"] = ms.order["sum"]
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-fully-paid"
    assert not ms.posts and not env.alerts


def test_staraya_sdelka_bez_retroaktivnosti(env, ms):
    ms.order["payedSum"] = 0
    env.lead = lead_dict(closed_ago=3 * 86400)             # правка давно закрытой сделки
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-old"
    flow._office_recent.clear()
    env.lead = lead_dict(closed_ago=None)                  # closed_at нет - не рискуем
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-old"
    assert not ms.posts


def test_ne_ofis_ne_uspeshno_realizovano_uehala(env, ms):
    env.lead = lead_dict(pipeline=PIPELINE_CLEVER_MAIN)
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-moved"
    flow._office_recent.clear()
    env.lead = lead_dict(status=75426862)
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-moved"
    assert not ms.posts


def test_povtornyy_vebhuk_odin_platezh(env, ms):
    ms.order["payedSum"] = 0
    assert run(flow.process_office_payment(LEAD_ID)) == "created"
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-recent"
    flow._office_recent.clear()                            # и спустя окно - дубль опознан в МС
    assert run(flow.process_office_payment(LEAD_ID)) == "already-recorded"
    assert len(ms.posts) == 1


def test_ofis_suhoy_progon_ne_pishet_i_ne_zovyot(env, ms, monkeypatch):
    monkeypatch.setattr(flow, "OFFICE_MS_PAYMENT_DRY_RUN", True)
    ms.order["payedSum"] = 0
    assert run(flow.process_office_payment(LEAD_ID)) == "created"
    assert not ms.posts and not env.alerts and not env.notes and flow.pending_count() == 0


def test_ofis_flag_vyklyuchen_i_sdelka_bez_zakaza(env, ms, monkeypatch):
    env.lead = lead_dict(uuid=None)
    assert run(flow.process_office_payment(LEAD_ID)) == "skipped-no-order"
    assert not env.alerts                                  # 71 из 750 сделок без заказа - это не тревога
    monkeypatch.setattr(flow, "OFFICE_MS_PAYMENT_ENABLED", False)
    flow._office_recent.clear()
    assert run(flow.process_office_payment(LEAD_ID)) == "disabled"


def test_ofis_sboy_ms_ochered_i_povtor_pishet_ostatok(env, ms):
    ms.order["payedSum"] = 0
    ms.down = True
    assert run(flow.process_office_payment(LEAD_ID)) == "queued"
    (qkey, entry), = flow._load_pending().items()
    assert qkey == f"office:{LEAD_ID}" and entry["kind"] == "office"
    ms.down = False
    run(flow.retry_once(now=entry["next_at"] + 1))
    assert flow.pending_count() == 0 and ms.posts[0]["sum"] == 1164900


def test_ofis_lead_ne_prochitalsya(env, ms, monkeypatch):
    async def none(*a, **kw):
        return None

    monkeypatch.setattr(amo_service, "get_lead_full", none)
    assert run(flow.process_office_payment(LEAD_ID)) == "failed-lead-read"
