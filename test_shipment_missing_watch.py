"""Сторож «отгрузка не создалась» и причины отказа дублёра (пункт 3).

  • сделка на этапе отгрузки Офиса без «ID Отгрузки» → одно уведомление с тегом и причиной;
  • отгрузка есть, сделку увели с этапа, не Офис → молчим;
  • одна сделка - одно уведомление, повторный проход молчит;
  • режим отчёта ничего не шлёт и отметок не жжёт;
  • причина: записанная дублёром (текст МойСклада из ответа с ошибкой), нет заказа,
    этап, который дублёр не обслуживает;
  • дублёр записывает причину на отказе МойСклада и снимает её при успехе.

Запуск: python3 -m pytest test_shipment_missing_watch.py -q
"""
import asyncio
import json
import os
import sys
import tempfile
import types

_SAVED = {}


def _stub(name, **attrs):
    _SAVED.setdefault(name, sys.modules.get(name))
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


_tmp = tempfile.mkdtemp()
os.environ["SHIPMENT_WATCH_STATE"] = os.path.join(_tmp, "marks.json")
os.environ["AMGROUP_SHIPMENT_STATE_PATH"] = os.path.join(_tmp, "created.json")

OFF, RET = 9421022, 10593102
F_SHIP, F_ORDER = 576691, 576689
sent, leads_db, entered = [], {}, set()


def _cfv(lead, fid):
    for f in lead.get("custom_fields_values") or []:
        if f["field_id"] == fid:
            return f["values"][0]["value"]
    return None


async def _get_leads_by_ids(ids):
    return [leads_db[i] for i in ids if i in leads_db]


async def _send_alert(text, **kw):
    sent.append({"text": text, **kw})
    return True


class _D:
    def __init__(self, t, c, th, p):
        self.text, self.c, self.th, self.p = t, c, th, p

    def send_kwargs(self):
        return {"chat_id": self.c, "message_thread_id": self.th, "parse_mode": self.p}


ms_stub = _stub("ms_client", last_error=None, put=None, post=None, MS_API_URL="https://ms")
_stub("amo_service", get_custom_field_value=_cfv, get_leads_by_ids=_get_leads_by_ids,
      _do_patch=None, get_lead_full=None)
_stub("telegram_bot", send_alert=_send_alert)
_stub("alerts", lead_link=lambda i: f"<a>{i}</a>",
      decide=lambda key, legacy_text, values, chat_id=None, thread_id=None, parse_mode=None, lead=None:
      _D(legacy_text, chat_id, thread_id, parse_mode))
_stub("tg_recipients", NOTIFY_CHAT_ID=-100, NOTIFY_THREAD_ID=10479)
_stub("waybill_config", PIPELINE_OFFICE=OFF, AMGROUP_SHIPMENT_DRY_RUN=False, AMGROUP_SHIPMENT_ENABLED=True,
      AMGROUP_SHIPMENT_GRACE_SEC=0, FIELD_MOYSKLAD_ORDER_UUID=F_ORDER, MS_API_URL="https://ms",
      STATUS_OFFICE_COURIER_OWN=75426870, STATUS_SUCCESS=142, STATUS_WAYBILL_READY=75426874)
for _m in ("amgroup_shipment", "shipment_missing_watch"):
    _SAVED.setdefault(_m, sys.modules.get(_m))
    sys.modules.pop(_m, None)

import amgroup_shipment as A  # noqa: E402
import shipment_missing_watch as W  # noqa: E402

for _n, _o in _SAVED.items():
    if _o is not None:
        sys.modules[_n] = _o
    else:
        sys.modules.pop(_n, None)


async def _entered(ts_from, ts_to):
    return set(entered)

W._entered_leads = _entered


def _lead(lid, status=75426874, pipeline=OFF, ship=None, order="uuid-1"):
    cfs = []
    if ship:
        cfs.append({"field_id": F_SHIP, "values": [{"value": ship}]})
    if order:
        cfs.append({"field_id": F_ORDER, "values": [{"value": order}]})
    return {"id": lid, "name": f"Заказ {lid}", "pipeline_id": pipeline, "status_id": status,
            "custom_fields_values": cfs}


def _reset(dry=False):
    W.SHIPMENT_WATCH_DRY_RUN = dry
    sent.clear(); leads_db.clear(); entered.clear(); A._failures.clear()
    if os.path.exists(os.environ["SHIPMENT_WATCH_STATE"]):
        os.remove(os.environ["SHIPMENT_WATCH_STATE"])


def run(c):
    return asyncio.run(c)


def test_alerts_once_with_tag_and_reason():
    _reset()
    leads_db[1] = _lead(1)
    entered.add(1)
    r = run(W.sweep_once())
    assert r["missing"] == 1 and len(sent) == 1
    t = sent[0]["text"]
    assert "@kathrina_bistraya" in t and "Готова накладная" in t and "Причина:" in t
    assert sent[0]["message_thread_id"] == 10479
    run(W.sweep_once())
    assert len(sent) == 1


def test_silent_when_shipment_exists_or_moved_or_not_office():
    _reset()
    leads_db[2] = _lead(2, ship="abc")
    leads_db[3] = _lead(3, status=143)
    leads_db[4] = _lead(4, pipeline=RET)
    entered.update({2, 3, 4})
    run(W.sweep_once())
    assert sent == []


def test_dry_run_sends_nothing_and_keeps_marks():
    _reset(dry=True)
    leads_db[5] = _lead(5)
    entered.add(5)
    run(W.sweep_once())
    assert sent == []
    W.SHIPMENT_WATCH_DRY_RUN = False
    run(W.sweep_once())
    assert len(sent) == 1


def test_reasons():
    _reset()
    A._note_failure(6, "post_failed", json.dumps({"errors": [
        {"error": "Нельзя отгрузить товар, которого нет на складе", "code": 3007}]}))
    assert "нет на складе" in W.reason_for(_lead(6))
    assert "не привязан заказ" in W.reason_for(_lead(7, order=None))
    assert "этап не обслуживает" in W.reason_for(_lead(8, status=75426866))
    assert "создайте её в МойСкладе" in W.reason_for(_lead(9))
    assert A.human_reason({"kind": "post_failed", "detail": "<html>502</html>"}).startswith("МойСклад отказал")


def test_shipment_failure_recorded_and_cleared():
    _reset()

    async def find_none(order_uuid):
        return []

    async def put_ok(path, body):
        return {"positions": {"rows": []}}

    async def post_fail(path, body):
        ms_stub.last_error = json.dumps({"errors": [{"error": "Нет остатка: Tangem 2.0 ×1"}]})
        return None

    async def post_ok(path, body):
        return {"id": "d1", "name": "00042", "store": None}

    async def store_name(store):
        return None

    async def write_fields(lid, res):
        return True

    A._find_existing_demand = find_none
    A._resolve_store_name = store_name
    A.write_shipment_fields_to_lead = write_fields
    ms_stub.put = put_ok
    ms_stub.post = post_fail
    lead = _lead(10)
    assert run(A.create_shipment_for_lead(lead)) is None
    assert "Нет остатка" in A.human_reason(A.last_failure(10))
    ms_stub.post = post_ok
    assert run(A.create_shipment_for_lead(lead))["shipment_id"] == "d1"
    assert A.last_failure(10) is None
