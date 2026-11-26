"""Пересоздание отгрузки при правке заказа (пункт 1) и свежесть перехода у дублёра.

  • заказ совпадает с отгрузкой (услуга доставки не в счёт) → ничего не делаем;
  • разошлись → удалить старую, создать новую, сообщение «до → после» с автором и тегом;
  • режим отчёта ничего не удаляет и не пишет;
  • удаление не прошло → новую не создаём, «отгрузка не изменена»;
  • удалили, а новая не создалась → громкое сообщение и очистка «ID Отгрузки»;
  • несколько отгрузок → только сообщение, и один раз на один состав;
  • сделки в Офисе нет → не трогаем;
  • дублёр на новых этапах (Отгружена, В пути, ПВЗ, Достависта) создаёт отгрузку только при
    свежем переходе; на прежних трёх - как раньше.

Запуск: python3 -m pytest test_shipment_resync.py -q
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
os.environ["SHIPMENT_RESYNC_CURSOR"] = os.path.join(_tmp, "cursor.json")
os.environ["AMGROUP_SHIPMENT_STATE_PATH"] = os.path.join(_tmp, "created.json")

OFF, RET = 9421022, 10593102
F_SHIP, F_ORDER = 576691, 576689
sent, calls, patched_amo = [], [], []
ms = {}          # путь → ответ GET
amo_events = []  # ответ /api/v4/events


def _cfv(lead, fid):
    for f in lead.get("custom_fields_values") or []:
        if f["field_id"] == fid:
            return f["values"][0]["value"]
    return None


async def _ms_get(path, params=None):
    calls.append(("GET", path))
    return ms.get(path)


async def _ms_delete(path):
    calls.append(("DELETE", path))
    if ms.get("_delete_fail"):
        ms_mod.last_error = json.dumps({"errors": [{"error": "Документ проведён и закрыт"}]})
        return None
    return True


async def _ms_put(path, body):
    calls.append(("PUT", path))
    return {"positions": {"rows": []}, "store": None}


async def _ms_post(path, body):
    calls.append(("POST", path))
    if ms.get("_post_fail"):
        ms_mod.last_error = json.dumps({"errors": [{"error": "Нельзя отгрузить товар, которого нет на складе"}]})
        return None
    return {"id": "new-d", "name": "00099", "store": None}


amo_notes: list = []


async def _amo_get(path, params):
    if path.endswith("/notes"):
        return {"_embedded": {"notes": list(amo_notes)}}
    return {"_embedded": {"events": list(amo_events)}}


async def _amo_patch(path, body):
    patched_amo.append((path, body))
    return {"ok": True}


leads_by_query = {}


async def _find(q, limit=50, with_=()):
    return leads_by_query.get(q, [])


async def _send_alert(text, **kw):
    sent.append({"text": text, **kw})
    return True


class _D:
    def __init__(self, t, c, th, p):
        self.text, self.c, self.th, self.p = t, c, th, p

    def send_kwargs(self):
        return {"chat_id": self.c, "message_thread_id": self.th, "parse_mode": self.p}


ms_mod = _stub("ms_client", last_error=None, get=_ms_get, put=_ms_put, post=_ms_post, delete=_ms_delete)
_stub("amo_service", get_custom_field_value=_cfv, find_leads_by_query=_find, _do_get=_amo_get,
      _do_patch=_amo_patch, get_lead_full=None)
_stub("telegram_bot", send_alert=_send_alert)
_stub("alerts", lead_link=lambda i: f"<a>{i}</a>",
      decide=lambda key, legacy_text, values, chat_id=None, thread_id=None, parse_mode=None, lead=None:
      _D(legacy_text, chat_id, thread_id, parse_mode))
_stub("tg_recipients", NOTIFY_CHAT_ID=-100, NOTIFY_THREAD_ID=10479)
_stub("waybill_config", PIPELINE_OFFICE=OFF, AMGROUP_SHIPMENT_DRY_RUN=False, AMGROUP_SHIPMENT_ENABLED=True,
      AMGROUP_SHIPMENT_GRACE_SEC=0, FIELD_MOYSKLAD_ORDER_UUID=F_ORDER, MS_API_URL="https://ms",
      STATUS_OFFICE_COURIER_OWN=75426870, STATUS_SUCCESS=142, STATUS_WAYBILL_READY=75426874,
      STATUS_OFFICE_COURIER_MSK=75426866, STATUS_OFFICE_SHIPPED=75426878, STATUS_OFFICE_IN_TRANSIT=75426882,
      STATUS_OFFICE_AWAITING_PICKUP=75426886)
for _m in ("amgroup_shipment", "shipment_resync"):
    _SAVED.setdefault(_m, sys.modules.get(_m))
    sys.modules.pop(_m, None)

import amgroup_shipment as A  # noqa: E402
import shipment_resync as S  # noqa: E402

for _n, _o in _SAVED.items():
    if _o is not None:
        sys.modules[_n] = _o
    else:
        sys.modules.pop(_n, None)


async def _no_demand(order_uuid):
    return []


async def _no_store(store):
    return None


async def _fields_ok(lid, res):
    return True


A._find_existing_demand = _no_demand
A._resolve_store_name = _no_store
A.write_shipment_fields_to_lead = _fields_ok


def run(c):
    return asyncio.run(c)


def pos(aid, qty, price, kind="product", name=None):
    return {"assortment": {"meta": {"href": f"https://ms/entity/{kind}/{aid}", "type": kind},
                           "name": name or aid}, "quantity": qty, "price": price}


def _lead(lid=5, pipeline=OFF, status=75426874, order="ord-1"):
    return {"id": lid, "pipeline_id": pipeline, "status_id": status, "name": "Заказ",
            "custom_fields_values": [{"field_id": F_ORDER, "values": [{"value": order}]}]}


def _setup(order_rows, demand_rows, demands=("dem-1",), dry=False, lead=None, notify=False):
    S.SHIPMENT_RESYNC_DRY_RUN = dry
    S.SHIPMENT_RESYNC_NOTIFY_ONLY = notify
    sent.clear(); calls.clear(); patched_amo.clear(); ms.clear(); S._multi_notified.clear()
    S._changed_notified.clear()
    A._created.clear(); A._failures.clear()
    ms_mod.last_error = None
    ms["entity/customerorder/ord-1/positions"] = {"rows": order_rows}
    for d in demands:
        ms[f"entity/demand/{d}/positions"] = {"rows": demand_rows}
    ms["entity/customerorder/ord-1/audit"] = {"rows": [
        {"uid": "kirillp@aperfilovvv", "eventType": "update", "moment": "2026-11-13 12:00:00"},
        {"uid": "admin@aperfilovvv", "eventType": "update", "moment": "2026-11-13 11:00:00"}]}
    amo_notes.clear()
    leads_by_query.clear()
    leads_by_query["ord-1"] = [lead or _lead()]
    A._state_loaded = True
    A._created["5"] = {"order_uuid": "ord-1", "shipment_id": "dem-1"}
    recent = "2026-09-01 00:00:00.000"
    return {"id": "ord-1", "name": "09100", "created": recent, "moment": recent, "demands": [{"id": d, "name": f"D-{d}"} for d in demands]}


TANGEM, LEDGER, DELIVERY = "t2", "ln", "dlv"


def test_in_sync_ignores_delivery_service():
    o = _setup([pos(TANGEM, 1, 499000)], [pos(TANGEM, 1, 499000), pos(DELIVERY, 1, 50000, kind="service")])
    assert run(S.handle_order(o)) == "in-sync"
    assert sent == [] and not any(c[0] in ("DELETE", "POST") for c in calls)


def test_changed_order_recreated_with_diff():
    o = _setup([pos(TANGEM, 2, 499000, name="Tangem 2.0"), pos(LEDGER, 1, 900000, name="Ledger")],
               [pos(TANGEM, 1, 499000, name="Tangem 2.0")])
    assert run(S.handle_order(o)) == "recreated"
    assert ("DELETE", "entity/demand/dem-1") in calls and ("POST", "entity/demand") in calls
    assert calls.index(("DELETE", "entity/demand/dem-1")) < calls.index(("PUT", "entity/demand/new"))
    t = sent[0]["text"]
    assert "№09100" in t and "kirillp" in t and "пересоздана" in t and "@kathrina_bistraya" in t
    assert "Tangem 2.0: ×1 → ×2" in t and "+ Ledger ×1" in t
    assert sent[0]["message_thread_id"] == 10479


def test_recent_update_does_not_touch_historical_order():
    for field in ("created", "moment"):
        o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)])
        o[field] = "2026-08-31 23:59:59.999"
        o["updated"] = "2026-11-26 19:01:00.000"
        assert run(S.handle_order(o)) == "historical-order"
        assert calls == [] and sent == [] and patched_amo == []


def test_missing_or_invalid_order_date_does_not_trigger_resync():
    for value in (None, "unknown"):
        o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)])
        o["created"] = value
        assert run(S.handle_order(o)) == "historical-order"
        assert calls == []


def test_manual_shipment_on_recent_order_is_preserved():
    for previous in ({}, {"order_uuid": "ord-1", "shipment_id": "another-demand"},
                     {"order_uuid": "another-order", "shipment_id": "dem-1"}):
        o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)])
        A._created["5"] = previous
        assert run(S.handle_order(o)) == "protected"
        assert run(S.handle_order(o)) == "notify-known"
        assert len(sent) == 1 and "отгрузка сохранена" in sent[0]["text"]
        assert not any(c[0] in ("DELETE", "POST", "PUT") for c in calls)
        assert patched_amo == []


def test_price_change_counts_and_removed_item():
    o = _setup([pos(TANGEM, 1, 450000, name="Tangem")], [pos(TANGEM, 1, 499000, name="Tangem"),
                                                          pos(LEDGER, 1, 900000, name="Ledger")])
    assert run(S.handle_order(o)) == "recreated"
    assert "цена 4990 → 4500 ₽" in sent[0]["text"] and "− Ledger ×1" in sent[0]["text"]


def test_dry_run_does_nothing():
    o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)], dry=True)
    assert run(S.handle_order(o)) == "dry"
    assert sent == [] and not any(c[0] in ("DELETE", "POST") for c in calls)


def test_delete_failed_keeps_old_shipment():
    o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)])
    ms["_delete_fail"] = True
    assert run(S.handle_order(o)) == "delete_failed"
    assert not any(c[0] == "POST" for c in calls)
    assert "НЕ изменена" in sent[0]["text"] and "проведён и закрыт" in sent[0]["text"]


def test_create_failed_after_delete_is_loud_and_clears_field():
    o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)])
    ms["_post_fail"] = True
    assert run(S.handle_order(o)) == "create_failed"
    t = sent[0]["text"]
    assert "товар вернулся на склад" in t and "нет на складе" in t
    assert patched_amo and patched_amo[-1][1]["custom_fields_values"][0] == {"field_id": F_SHIP, "values": None}
    assert A.last_failure(5) is not None


def test_multiple_demands_notify_once():
    o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)], demands=("a", "b"))
    assert run(S.handle_order(o)) == "multi"
    assert run(S.handle_order(o)) == "multi-known"
    assert len(sent) == 1 and "несколько отгрузок" in sent[0]["text"]
    assert not any(c[0] in ("DELETE", "POST") for c in calls)


def test_no_office_lead_untouched():
    o = _setup([pos(TANGEM, 2, 1)], [pos(TANGEM, 1, 1)], lead=_lead(pipeline=RET))
    assert run(S.handle_order(o)) == "no-office-lead"
    assert sent == []


def test_sweep_cursor_and_outcomes():
    o = _setup([pos(TANGEM, 1, 1)], [pos(TANGEM, 1, 1)])
    ms["entity/customerorder"] = {"rows": [o, {"id": "x", "demands": []}]}
    r = run(S.sweep_once())
    assert r["outcomes"] == {"in-sync": 1}
    assert os.path.exists(os.environ["SHIPMENT_RESYNC_CURSOR"])


# ─────────────────────── свежесть перехода у дублёра ───────────────────────


def _ev(status):
    return {"value_after": [{"lead_status": {"id": status, "pipeline_id": OFF}}]}


def test_new_stage_needs_fresh_entry():
    _setup([], [])
    amo_events.clear()
    assert run(A.create_shipment_for_lead(_lead(lid=11, status=75426882))) is None
    assert not any(c[0] == "POST" for c in calls)
    amo_events.append(_ev(75426882))
    assert run(A.create_shipment_for_lead(_lead(lid=11, status=75426882)))["shipment_id"] == "new-d"


def test_old_stages_unchanged():
    _setup([], [])
    amo_events.clear()
    assert run(A.create_shipment_for_lead(_lead(lid=12, status=75426874)))["shipment_id"] == "new-d"


def test_notify_only_sends_diff_once_and_touches_nothing():
    o = _setup([pos(TANGEM, 2, 1, name="Tangem")], [pos(TANGEM, 1, 1, name="Tangem")], dry=True, notify=True)
    assert run(S.handle_order(o)) == "notified"
    assert run(S.handle_order(o)) == "notify-known"
    assert len(sent) == 1 and "НЕ пересоздана" in sent[0]["text"] and "Tangem: ×1 → ×2" in sent[0]["text"]
    assert not any(c[0] in ("DELETE", "POST", "PUT") for c in calls)


def test_integration_login_replaced_by_amgroup_saver():
    """Правка из карточки amo: в МойСкладе автор admin, имя - из примечания amgroup (17.11)."""
    import time as _t
    o = _setup([pos(TANGEM, 1, 499000, name="Tangem 2.0")], [pos(LEDGER, 1, 900000, name="Ledger")])
    ms["entity/customerorder/ord-1/audit"] = {"rows": [
        {"uid": "admin@aperfilovvv", "eventType": "update", "moment": "2026-11-17 18:19:00"}]}
    amo_notes[:] = [
        {"note_type": "service_message", "created_at": int(_t.time()) - 60,
         "params": {"text": "Заказ сохранен пользователем Егор Константинов", "service": "МойСклад интеграция №1 от amgroup"}},
        {"note_type": "service_message", "created_at": int(_t.time()) - 600,
         "params": {"text": "Заказ сохранен пользователем Игорь Оанча", "service": "МойСклад интеграция №1 от amgroup"}}]
    assert run(S.handle_order(o)) == "recreated"
    assert "изменён пользователем Егор Константинов" in sent[0]["text"] and "admin" not in sent[0]["text"]


def test_stale_amgroup_note_keeps_login():
    import time as _t
    o = _setup([pos(TANGEM, 1, 499000, name="Tangem 2.0")], [pos(LEDGER, 1, 900000, name="Ledger")])
    ms["entity/customerorder/ord-1/audit"] = {"rows": [
        {"uid": "admin@aperfilovvv", "eventType": "update", "moment": "2026-11-17 18:19:00"}]}
    amo_notes[:] = [{"note_type": "service_message", "created_at": int(_t.time()) - 2 * 3600,
                     "params": {"text": "Заказ сохранен пользователем Егор Константинов"}}]
    assert run(S.handle_order(o)) == "recreated"
    assert "изменён пользователем admin" in sent[0]["text"]
