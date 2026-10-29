"""Sipuni рядом с UIS: события АТС, решение «пропущенный», перезвон, гейт распределения.

⚠️ ВСЕ СОБЫТИЯ В ЭТОМ ФАЙЛЕ СИНТЕТИЧЕСКИЕ. Они собраны по описанию «События на АТС» из
кабинета Sipuni (поля event, call_id, src_num, dst_num, src_type, dst_type, status,
call_start_timestamp, call_answer_timestamp, is_inner_call, treeName). Реальных образцов у нас
нет: интеграция не подключена, звонков не было. Тест зелёный = код делает то, что написано в
заметке с принятыми решениями, а не то, что делает настоящий Sipuni. Первый тестовый звонок
обязан сверить фикстуры с журналом режима log и поправить этот файл.

Что защищаем:
  * пропущенный = ВЕСЬ звонок без ответа (все ноги), а не одна нога очереди; один алерт на call_id;
  * исходящий, внутренний, мусор и повторы доставки в алерты не попадают и ничего не роняют;
  * режим log (по умолчанию) не трогает ни Telegram, ни amo; live шлёт тот же алерт, что UIS;
  * секрет маршрута пуст или неверен - 403, а настоящий секрет в журнал не попадает;
  * «перезвонили» по потоку звонков; гейт распределения для источника Sipuni;
  * UIS-пути (теги, UIS_SOURCE_ID, /uis/) не изменились.

Запуск: python -m pytest test_sipuni_events.py -q
"""
import asyncio
import logging
import time

import pytest
from fastapi.testclient import TestClient

import lead_distribution as ld
import sipuni_events as S
import test_lead_distribution as tld
import uis_missed_call as U
import webhooks
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME

PHONE_RAW = "89555555555"          # синтетика: как Sipuni, по описанию, пишет внешний номер
PHONE_NORM = "+79555555555"
CALLER_DIGITS = "79555555555"      # вид, в котором телефон приходит от UIS


# ═══════════════════════ синтетические события ═══════════════════════


def e(kind, call_id="1700000000.1", *, src=PHONE_RAW, src_type=1, dst="012345101", dst_type=2,
      status=None, ts=1700000000, answer_ts=None, tree="000-4221010", **extra):
    """Одно СИНТЕТИЧЕСКОЕ событие в том виде, в каком оно приходит в параметрах запроса."""
    d = {"event": str(kind), "call_id": call_id, "src_num": src, "src_type": str(src_type),
         "dst_num": dst, "dst_type": str(dst_type), "timestamp": str(ts), "treeNumber": tree}
    if kind in (2, 4):
        d["status"] = status or "NOANSWER"
        d["call_start_timestamp"] = str(ts - 10)
        d["call_answer_timestamp"] = str(answer_ts if answer_ts is not None else 0)
    d.update(extra)
    return d


def leg(call_id, dst, ts, result="NOANSWER", **kw):
    """Нога входящего: событие 1, (ответ - событие 3), событие 2 с итогом."""
    out = [e(1, call_id, dst=dst, ts=ts, **kw)]
    if result == "ANSWER":
        out.append(e(3, call_id, dst=dst, ts=ts + 2, **kw))
        out.append(e(2, call_id, dst=dst, ts=ts + 30, status="ANSWER", answer_ts=ts + 2, **kw))
    else:
        out.append(e(2, call_id, dst=dst, ts=ts + 20, status=result, **kw))
    return out


def feed(tracker, events):
    for raw in events:
        tracker.ingest(S.parse_event(raw))


async def idle(tr, timeout=3.0):
    """Ждёт, пока у трекера не останется таймеров и задач обработчика."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        busy = bool(tr._bg) or any(st.timer is not None for st in tr._calls.values())
        if not busy:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("трекер не успокоился: таймер или обработчик завис")


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def tr():
    got = []
    tracker = S.SipuniCallTracker(quiet_s=0.05, stuck_s=0.6, on_missed=got.append)
    tracker.got = got
    yield tracker
    tracker.reset()


# ═══════════════════════ телефон и разбор ═══════════════════════


@pytest.mark.parametrize("raw, expected", [
    ("89555555555", PHONE_NORM),
    ("79555555555", PHONE_NORM),
    ("+79555555555", PHONE_NORM),
    ("+7 (955) 555-55-55", PHONE_NORM),
    ("8 955 555 55 55", PHONE_NORM),
    ("9555555555", PHONE_NORM),
    (79555555555, PHONE_NORM),
    ("+380501234567", "+380501234567"),
    ("012345101", ""),        # внутренний логин Sipuni
    ("101", ""),              # короткий номер
    ("", ""),
    (None, ""),
    ("anonymous", ""),
    ("12345678901234567890", ""),
])
def test_phone_formats(raw, expected):
    assert S.normalize_phone(raw) == expected


def test_internal_side_is_never_read_as_phone():
    """Внутренний логин из десяти цифр выглядел бы как телефон, поэтому у стороны с
    типом 2 номер не нормализуется вовсе."""
    ev = S.parse_event(e(1, src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1))
    assert ev.src_phone == "" and ev.dst_phone == PHONE_NORM


def test_collect_payload_query_form_json_and_wrappers():
    base = e(1)
    # адрес (GET)
    assert S.collect_payload(base) == [base]
    # форма: тело разбирается, адрес дополняет
    form = "event=1&call_id=1700000000.1&src_num=89555555555&src_type=1&dst_num=012345101&dst_type=2"
    got = S.collect_payload({"x": "1"}, form.encode())
    assert got[0]["call_id"] == "1700000000.1" and got[0]["x"] == "1"
    # JSON: одно событие, список, обёртка
    import json as _json
    assert S.collect_payload({}, _json.dumps(base).encode()) == [base]
    two = [e(1, "a.1"), e(2, "a.1")]
    assert [x["event"] for x in S.collect_payload({}, _json.dumps(two))] == ["1", "2"]
    wrapped = {"data": {"events": two}}
    assert len(S.collect_payload({}, _json.dumps(wrapped))) == 2
    # пустота и мусор: не падаем, событий нет
    assert S.collect_payload({}, b"") == []
    assert S.collect_payload({}, None) == []
    assert S.collect_payload(None, "") == []
    for junk in (b"\xff\xfe not json {", b"[1, 2, 3]", b'"just a string"', b"{}"):
        for item in S.collect_payload({}, junk):
            assert S.parse_event(item).kind is None


@pytest.mark.parametrize("raw", [
    None, {}, {"event": "abc"}, {"event": "9"}, {"call_id": "1.1"}, {"event": "1"},
    {"event": None, "call_id": None}, {"EVENT": "1", "CALL_ID": "x.1"},
    {"event": ["1"], "call_id": ["z"]}, {"timestamp": "не число", "event": "2", "call_id": "q"},
    "строка вместо словаря", 12345,
])
def test_garbage_event_does_not_raise(raw, tr):
    ev = S.parse_event(raw)
    st, dup = tr.ingest(ev)
    assert dup is False
    S.log_line(ev, st)  # и строка журнала тоже не падает


def test_event_keys_are_case_insensitive_and_numbers_may_be_json_numbers():
    ev = S.parse_event({"EVENT": 2, "Call_ID": "1.9", "SRC_TYPE": 1, "dst_type": 2.0,
                        "status": "answer", "call_answer_timestamp": 1700000005})
    assert ev.kind == 2 and ev.call_id == "1.9" and ev.src_type == 1 and ev.dst_type == 2
    assert ev.answered is True


@pytest.mark.parametrize("raw, answered", [
    (e(3), True),
    (e(2, status="ANSWER"), True),
    (e(2, status="NOANSWER", answer_ts=1700000009), True),
    (e(4, status="ANSWER"), True),
    (e(2, status="NOANSWER"), False),
    (e(2, status="BUSY"), False),
    (e(1), False),
])
def test_answer_evidence(raw, answered):
    assert S.parse_event(raw).answered is answered


# ═══════════════════════ решение «пропущенный» ═══════════════════════


def test_whole_call_unanswered_gives_exactly_one_missed(tr):
    """Все ноги NOANSWER: один алерт, номер звонящего и схема из treeName/treeNumber."""
    cid = "1700000001.1"

    async def go():
        feed(tr, leg(cid, "012345101", 100) + leg(cid, "012345102", 110, "BUSY"))
        await idle(tr)
        feed(tr, [e(2, cid, dst="012345103", ts=130, status="CANCEL")])  # запоздалое событие
        await idle(tr)

    run(go())
    assert len(tr.got) == 1
    m = tr.got[0]
    assert m.call_id == cid and m.phone == PHONE_NORM and m.legs == 2
    assert m.tree_number == "000-4221010"
    assert set(m.statuses) >= {"NOANSWER", "BUSY"}


def test_first_leg_not_answered_second_answered_is_not_missed(tr):
    cid = "1700000002.1"

    async def go():
        feed(tr, leg(cid, "012345101", 100) + leg(cid, "012345102", 110, "ANSWER"))
        await idle(tr)

    run(go())
    assert tr.got == []
    assert tr.get_call(cid).answered is True


def test_answer_event_3_alone_is_enough(tr):
    cid = "1700000003.1"

    async def go():
        feed(tr, [e(1, cid), e(3, cid, ts=1700000003), e(2, cid, ts=1700000050, status="NOANSWER")])
        await idle(tr)

    run(go())
    assert tr.got == []


def test_transfer_between_queues_with_event_4_unanswered_gives_one_missed(tr):
    """Перевод между очередями: нога первой очереди закрывается событием 4 и сразу открывается
    нога второй. Событие 4 НЕ значит, что звонок закончился: пока вторая нога открыта, алерта
    нет, а когда и она кончилась без ответа - ровно один."""
    cid = "1700000004.1"

    async def go():
        feed(tr, [e(1, cid, dst="012345101", ts=100),
                  e(4, cid, dst="012345101", ts=120, status="NOANSWER", transfer_from="012345101"),
                  e(1, cid, dst="012345201", ts=121)])
        await asyncio.sleep(0.25)          # больше тихого окна, меньше STUCK
        assert tr.got == [], "вторая очередь ещё звонит, раннего алерта быть не должно"
        closed = time.monotonic()
        feed(tr, [e(2, cid, dst="012345201", ts=150, status="NOANSWER")])
        await idle(tr)
        # алерт приходит по ТИХОМУ окну после закрытия последней ноги, а не по таймеру
        # «нога зависла» (он 0.6 с): событие 4 закрыло ногу первой очереди
        assert time.monotonic() - closed < 0.5

    run(go())
    assert len(tr.got) == 1 and tr.got[0].legs == 2


def test_transfer_between_queues_answered_in_second_queue_is_not_missed(tr):
    cid = "1700000005.1"

    async def go():
        feed(tr, [e(1, cid, dst="012345101", ts=100),
                  e(4, cid, dst="012345101", ts=120, status="NOANSWER"),
                  e(1, cid, dst="012345201", ts=121), e(3, cid, dst="012345201", ts=125),
                  e(2, cid, dst="012345201", ts=200, status="ANSWER", answer_ts=125)])
        await idle(tr)

    run(go())
    assert tr.got == []


def test_leg_that_never_closes_is_finalized_after_stuck_limit(tr):
    """Потеряли событие 2 - звонок не должен висеть вечно: через STUCK считаем законченным."""
    cid = "1700000006.1"

    async def go():
        feed(tr, [e(1, cid)])
        await idle(tr)

    run(go())
    assert len(tr.got) == 1


def test_repeated_delivery_of_same_events_gives_no_duplicate(tr):
    cid = "1700000007.1"
    events = leg(cid, "012345101", 100)

    async def go():
        feed(tr, events)
        feed(tr, events)                   # Sipuni повторил доставку
        await idle(tr)
        assert tr.get_call(cid).starts == 1 and tr.get_call(cid).ends == 1
        _st, dup = tr.ingest(S.parse_event(events[0]))
        assert dup is True

    run(go())
    assert len(tr.got) == 1


def test_same_call_id_after_alert_gives_no_second_alert(tr):
    cid = "1700000008.1"

    async def go():
        feed(tr, leg(cid, "012345101", 100))
        await idle(tr)
        feed(tr, leg(cid, "012345102", 400))                       # новые ноги того же call_id
        await idle(tr)
        tr._calls.clear()                                          # состояние вытеснили из памяти
        feed(tr, leg(cid, "012345103", 800))
        await idle(tr)

    run(go())
    assert len(tr.got) == 1


def test_outgoing_call_is_not_an_alert(tr):
    cid = "1700000009.1"

    async def go():
        feed(tr, [e(1, cid, src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1, ts=100),
                  e(2, cid, src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1, ts=130,
                    status="NOANSWER")])
        await idle(tr)

    run(go())
    assert tr.got == []
    assert tr.get_call(cid).direction == S.DIR_OUT and tr.get_call(cid).client_phone == PHONE_NORM


def test_internal_call_is_not_an_alert(tr):
    cid = "1700000010.1"

    async def go():
        feed(tr, [e(1, cid, src="012345102", src_type=2, dst="012345101", dst_type=2, is_inner_call="1"),
                  e(2, cid, src="012345102", src_type=2, dst="012345101", dst_type=2, status="NOANSWER")])
        feed(tr, [e(1, "1700000010.2", src="012345102", src_type=2, dst="012345101", dst_type=2),
                  e(2, "1700000010.2", src="012345102", src_type=2, dst="012345101", dst_type=2)])
        await idle(tr)

    run(go())
    assert tr.got == []


def test_missing_types_fall_back_to_numbers(tr):
    """ПРЕДПОЛОЖЕНИЕ: типов в событии может не быть - тогда судим по номерам."""
    cid = "1700000011.1"
    raw1 = {"event": "1", "call_id": cid, "src_num": PHONE_RAW, "dst_num": "101", "timestamp": "100"}
    raw2 = {"event": "2", "call_id": cid, "src_num": PHONE_RAW, "dst_num": "101", "timestamp": "120",
            "status": "NOANSWER"}

    async def go():
        feed(tr, [raw1, raw2])
        await idle(tr)

    run(go())
    assert len(tr.got) == 1 and tr.got[0].phone == PHONE_NORM


def test_forward_leg_to_external_number_does_not_replace_caller(tr):
    """Нога переадресации на мобильный (внешний на внешний) не делает мобильный клиентом."""
    cid = "1700000012.1"

    async def go():
        feed(tr, [e(1, cid, dst="012345101", ts=100),
                  e(1, cid, src=PHONE_RAW, src_type=1, dst="89161112233", dst_type=1, ts=101),
                  e(2, cid, dst="012345101", ts=120), e(2, cid, src=PHONE_RAW, dst="89161112233",
                                                         dst_type=1, ts=121)])
        await idle(tr)

    run(go())
    assert len(tr.got) == 1 and tr.got[0].phone == PHONE_NORM


def test_events_without_call_id_or_garbage_do_not_crash(tr):
    async def go():
        for raw in (e(1, call_id=""), {"event": "zzz"}, None, {}, e(1, "ok.1")):
            tr.ingest(S.parse_event(raw))
        await idle(tr)

    run(go())
    assert len(tr) == 1


def test_quiet_window_is_rearmed_by_every_event():
    got = []
    tracker = S.SipuniCallTracker(quiet_s=0.3, stuck_s=5, on_missed=got.append)
    cid = "1700000013.1"

    async def go():
        feed(tracker, [e(1, cid, dst="012345101", ts=1), e(2, cid, dst="012345101", ts=2)])
        await asyncio.sleep(0.15)
        feed(tracker, [e(1, cid, dst="012345102", ts=3), e(2, cid, dst="012345102", ts=4)])
        await asyncio.sleep(0.15)
        feed(tracker, [e(1, cid, dst="012345103", ts=5), e(2, cid, dst="012345103", ts=6)])
        await asyncio.sleep(0.15)
        assert got == [], "окно перевзводится на каждом событии, звонок ещё не признан пропущенным"
        await idle(tracker)

    try:
        run(go())
    finally:
        tracker.reset()
    assert len(got) == 1 and got[0].legs == 3


def test_sweep_decides_without_event_loop():
    """Без цикла событий таймеров нет - решение принимает sweep() по часам."""
    clock = {"t": 1000.0}
    got = []
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"], on_missed=got.append)
    feed(tracker, leg("1700000014.1", "012345101", 100))
    clock["t"] += 5
    assert tracker.sweep() == [] and got == []
    clock["t"] += 20
    assert len(tracker.sweep()) == 1 and len(got) == 1
    assert tracker.sweep() == [], "второй раз тот же звонок не решается"


def test_memory_is_bounded_by_ttl_and_size():
    clock = {"t": 1000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, ttl_s=100, max_calls=3, clock=lambda: clock["t"])
    for i in range(5):
        feed(tracker, [e(1, f"1800000000.{i}")])
    assert len(tracker) == 3 and tracker.get_call("1800000000.0") is None
    clock["t"] += 200
    feed(tracker, [e(1, "1800000000.99")])
    assert len(tracker) == 1, "просроченные звонки выброшены"


# ═══════════════════════ состояние номера и «перезвонили» ═══════════════════════


def test_phone_status_connected_active_missed_unknown():
    clock = {"t": 5000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    assert tracker.phone_status(PHONE_RAW) == S.PHONE_UNKNOWN
    assert tracker.phone_status("") == S.PHONE_UNKNOWN
    feed(tracker, [e(1, "s.1")])
    assert tracker.phone_status(PHONE_RAW) == S.PHONE_ACTIVE
    assert tracker.phone_status("+7 (955) 555-55-55") == S.PHONE_ACTIVE
    feed(tracker, [e(2, "s.1", status="NOANSWER")])
    clock["t"] += 30
    tracker.sweep()
    assert tracker.phone_status(PHONE_RAW) == S.PHONE_MISSED
    feed(tracker, [e(1, "s.2", dst="012345102"), e(3, "s.2", dst="012345102")])
    assert tracker.phone_status(PHONE_RAW) == S.PHONE_CONNECTED


def test_phone_status_ignores_calls_older_than_since():
    clock = {"t": 5000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    feed(tracker, [e(1, "o.1"), e(3, "o.1")])
    clock["t"] += 3600
    assert tracker.phone_status(PHONE_RAW) == S.PHONE_CONNECTED
    assert tracker.phone_status(PHONE_RAW, since=clock["t"] - 600) == S.PHONE_UNKNOWN


def _missed_call(tracker, clock, call_id="m.1"):
    feed(tracker, leg(call_id, "012345101", 100))
    clock["t"] += 30
    assert len(tracker.sweep()) == 1


def test_callback_by_outgoing_answered_call_is_seen_in_event_flow():
    clock = {"t": 9000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    _missed_call(tracker, clock)
    record = tracker.missed_record(CALLER_DIGITS)
    assert record is not None and record["called_back_at"] is None
    clock["t"] += 600
    feed(tracker, [e(1, "cb.1", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1),
                   e(3, "cb.1", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1)])
    assert tracker.missed_record(CALLER_DIGITS)["called_back_at"] is not None


def test_callback_by_incoming_answered_call_from_same_phone():
    clock = {"t": 9000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    _missed_call(tracker, clock)
    clock["t"] += 300
    feed(tracker, leg("cb.2", "012345102", 500, "ANSWER"))
    assert tracker.missed_record(PHONE_NORM)["called_back_at"] is not None


def test_unanswered_outgoing_and_other_phone_are_not_a_callback():
    clock = {"t": 9000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    _missed_call(tracker, clock)
    feed(tracker, [e(1, "nb.1", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1),
                   e(2, "nb.1", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1,
                     status="NOANSWER")])
    feed(tracker, [e(1, "nb.2", src="0123451010", src_type=2, dst="89160000000", dst_type=1),
                   e(3, "nb.2", src="0123451010", src_type=2, dst="89160000000", dst_type=1)])
    assert tracker.missed_record(PHONE_NORM)["called_back_at"] is None


def test_answered_call_before_the_missed_one_is_not_a_callback():
    clock = {"t": 9000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    feed(tracker, leg("old.1", "012345102", 10, "ANSWER"))
    clock["t"] += 3600
    _missed_call(tracker, clock)
    assert tracker.missed_record(PHONE_NORM)["called_back_at"] is None


def test_second_missed_call_does_not_replace_waiting_record():
    clock = {"t": 9000.0}
    tracker = S.SipuniCallTracker(quiet_s=20, clock=lambda: clock["t"])
    _missed_call(tracker, clock, "m.1")
    first = tracker.missed_record(PHONE_NORM)["missed_at"]
    clock["t"] += 100
    _missed_call(tracker, clock, "m.2")
    assert tracker.missed_record(PHONE_NORM)["missed_at"] == first


# ═══════════════════════ вход Sipuni в uis_missed_call ═══════════════════════


@pytest.fixture
def glue(monkeypatch):
    """Общий трекер процесса с короткими окнами; Telegram и amo подменены и записываются."""
    sipuni_tracker = S.tracker
    sipuni_tracker.reset()
    monkeypatch.setattr(sipuni_tracker, "_quiet_override", 0.05)
    monkeypatch.setattr(sipuni_tracker, "stuck_s", 0.6)
    monkeypatch.setattr(sipuni_tracker, "on_missed", None)
    monkeypatch.setattr(U, "_callback_pending", {})
    monkeypatch.setattr(U, "_seen_ids", set())
    monkeypatch.setattr(U, "_seen_order", __import__("collections").deque())
    monkeypatch.setattr(U, "ROP_CHAT_ID", -5358037627)
    monkeypatch.setattr(U, "_in_window", lambda now=None: True)

    class Spy:
        sends: list = []
        amo_queries: list = []
        decide_keys: list = []

    spy = Spy()
    spy.sends, spy.amo_queries, spy.decide_keys = [], [], []

    async def fake_send(text, **kw):
        spy.sends.append((text, kw))
        return True

    async def fake_find(query, *a, **k):
        spy.amo_queries.append(query)
        return [{"id": 36500001, "status_id": 1, "responsible_user_id": 13929334, "updated_at": 5}]

    real_decide = U.alerts.decide

    def spy_decide(event_key, **kw):
        spy.decide_keys.append(event_key)
        return real_decide(event_key, **kw)

    monkeypatch.setattr(U.telegram_bot, "send_alert", fake_send)
    monkeypatch.setattr(U.amo_service, "find_leads_by_query", fake_find)
    monkeypatch.setattr(U.alerts, "decide", spy_decide)
    yield spy
    sipuni_tracker.reset()


MISSED_CALL = leg("1700000100.1", "012345101", 100) + leg("1700000100.1", "012345102", 110)


def test_log_mode_makes_no_telegram_or_amo_calls(glue, monkeypatch, caplog):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "log")
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    run(go())
    assert glue.sends == [] and glue.amo_queries == [] and glue.decide_keys == []
    assert U._callback_pending == {}
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "Sipuni событие: ev=1 call=1700000100.1" in text, "строка журнала по каждому событию"
    assert text.count("Sipuni событие:") == len(MISSED_CALL)
    assert "режим log, алерт НЕ шлём" in text, "решение видно в журнале, действий нет"


_SIPUNI_ENV = ("SIPUNI_WEBHOOK_SECRET", "SIPUNI_EVENTS_MODE", "SIPUNI_MISSED_QUIET_S",
               "SIPUNI_SOURCE_IDS", "SIPUNI_CONNECT_WAIT_S", "SIPUNI_CONNECT_POLL_S")


def _probe_config(monkeypatch, **env):
    """Выполняет waybill_config.py под ДРУГИМ именем модуля при заданном окружении: настоящий
    модуль и ссылки на него в остальных файлах не трогаем."""
    import importlib.util
    import pathlib
    for name in _SIPUNI_ENV:
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    path = pathlib.Path(webhooks.__file__).with_name("waybill_config.py")
    spec = importlib.util.spec_from_file_location("waybill_config_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_defaults_are_safe_and_everything_is_off(monkeypatch):
    cfg = _probe_config(monkeypatch)
    assert cfg.SIPUNI_WEBHOOK_SECRET == ""          # пусто = маршрут отвечает 403
    assert cfg.SIPUNI_EVENTS_MODE == "log"          # только журнал
    assert cfg.SIPUNI_SOURCE_IDS == set()           # гейт распределения выключен
    assert cfg.SIPUNI_MISSED_QUIET_S == 20.0
    assert cfg.SIPUNI_CONNECT_WAIT_S == 180 and cfg.SIPUNI_CONNECT_POLL_S == 5.0


@pytest.mark.parametrize("raw, expected", [
    ("live", "live"), (" LIVE ", "live"), ("log", "log"), ("", "log"),
    ("loud", "log"), ("1", "log"), ("on", "log"),
])
def test_config_mode_only_exact_live_enables_actions(monkeypatch, raw, expected):
    """Опечатка в режиме не должна включать боевые действия."""
    assert _probe_config(monkeypatch, SIPUNI_EVENTS_MODE=raw).SIPUNI_EVENTS_MODE == expected


def test_config_source_ids_and_numbers(monkeypatch):
    cfg = _probe_config(monkeypatch, SIPUNI_SOURCE_IDS="13311299, 13311300,x,,13311301",
                        SIPUNI_MISSED_QUIET_S="7.5", SIPUNI_CONNECT_WAIT_S="60",
                        SIPUNI_CONNECT_POLL_S="2", SIPUNI_WEBHOOK_SECRET=" abc ")
    assert cfg.SIPUNI_SOURCE_IDS == {13311299, 13311300, 13311301}
    assert cfg.SIPUNI_MISSED_QUIET_S == 7.5 and cfg.SIPUNI_CONNECT_WAIT_S == 60
    assert cfg.SIPUNI_CONNECT_POLL_S == 2.0 and cfg.SIPUNI_WEBHOOK_SECRET == "abc"


def test_live_mode_sends_the_same_alert_as_uis(glue, monkeypatch):
    """Тот же текст, чат, топик и событие настроек панели, что у UIS."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def go():
        await U._apply({"contact_phone_number": CALLER_DIGITS, "contact_full_name": "",
                        "call_session_id": "uis-1"})
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 2
    (uis_text, uis_kw), (sip_text, sip_kw) = glue.sends
    assert sip_text == uis_text
    assert sip_kw == uis_kw
    assert sip_kw["chat_id"] == U.NOTIFY_CHAT_ID and sip_kw["message_thread_id"] == U.NOTIFY_THREAD_ID
    assert glue.decide_keys == ["missed_call", "missed_call"]
    assert glue.amo_queries == [CALLER_DIGITS, CALLER_DIGITS], "поиск сделки по телефону одинаков"
    assert "ПРОПУЩЕННЫЙ звонок" in sip_text and "·" not in sip_text


def test_live_mode_one_alert_per_call_id_even_with_repeats(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def go():
        await U._sipuni_apply(MISSED_CALL)
        await U._sipuni_apply(MISSED_CALL)              # повтор доставки
        await idle(S.tracker)
        await U._sipuni_apply(leg("1700000100.1", "012345104", 300))  # новые ноги того же звонка
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1


def test_live_mode_hidden_number_still_alerts_without_phone(glue, monkeypatch):
    """Скрытый номер: как и у UIS, алерт уходит без телефона и без ссылки, счётчик перезвона
    не ставится (по сделке проверять нечего)."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    cid = "1700000102.1"

    async def go():
        await U._sipuni_apply([e(1, cid, src="", src_type=1), e(2, cid, src="", src_type=1)])
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and "📞" not in glue.sends[0][0]
    assert glue.amo_queries == [] and U._callback_pending == {}


def test_live_mode_answered_call_sends_nothing(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def go():
        await U._sipuni_apply(leg("1700000101.1", "012345101", 100)
                              + leg("1700000101.1", "012345102", 110, "ANSWER"))
        await idle(S.tracker)

    run(go())
    assert glue.sends == [] and U._callback_pending == {}


def test_live_mode_watches_callback_with_sipuni_provider(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def go():
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    run(go())
    st = U._callback_pending[36500001]
    assert st["provider"] == "sipuni" and st["phone"] == CALLER_DIGITS


def _escalate(glue, st_phone=CALLER_DIGITS):
    st = U._callback_pending[36500001]
    st["since"] -= 61 * 60
    glue.sends.clear()
    run(U._sweep_callbacks())


def test_no_callback_in_event_flow_escalates_to_leadership(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    run(_scenario_missed(glue))
    _escalate(glue)
    assert len(glue.sends) == 1
    text, kw = glue.sends[0]
    assert "не перезвонили" in text and kw["chat_id"] == U.ROP_CHAT_ID
    assert glue.decide_keys[-1] == "missed_call_no_callback"
    assert U._callback_pending == {}


async def _scenario_missed(glue):
    await U._sipuni_apply(MISSED_CALL)
    await idle(S.tracker)


def test_callback_in_event_flow_cancels_escalation(glue, monkeypatch):
    """Менеджер перезвонил и клиент ответил: руководство молчит, теги amo не нужны."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    run(_scenario_missed(glue))
    run(U._sipuni_apply([e(1, "cb.9", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1, ts=500),
                         e(3, "cb.9", src="0123451010", src_type=2, dst=PHONE_RAW, dst_type=1, ts=505)]))
    _escalate(glue)
    assert glue.sends == []
    assert U._callback_pending == {}


def test_callback_unknown_after_state_loss_is_not_a_verdict(glue, monkeypatch):
    """Записи о пропущенном нет (рестарт, срок): вердикта нет, ожидание остаётся."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    run(_scenario_missed(glue))
    S.tracker._missed.clear()
    _escalate(glue)
    assert glue.sends == []
    assert 36500001 in U._callback_pending


def test_uis_path_is_unchanged(glue, monkeypatch):
    """UIS: тот же вход, дедуп по call_session_id, перезвон по ТЕГАМ сделки, а не по звонкам."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    params = {"contact_phone_number": CALLER_DIGITS, "contact_full_name": "Иван",
              "call_session_id": "uis-77"}

    async def go():
        await U._apply(params)
        await U._apply(params)

    run(go())
    assert len(glue.sends) == 1
    assert "Иван" in glue.sends[0][0]
    st = U._callback_pending[36500001]
    assert st["provider"] == "uis"

    async def lead_tags(lead_id, with_=()):
        return {"id": lead_id, "_tags": {TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME}}

    monkeypatch.setattr(U.amo_service, "get_lead_full", lead_tags)
    monkeypatch.setattr(U.amo_service, "has_tag", lambda ent, name: name in ent["_tags"])
    glue.sends.clear()
    st["since"] -= 61 * 60
    run(U._sweep_callbacks())
    assert glue.sends == [], "по тегу «Успешный звонок» руководство молчит, поток звонков Sipuni не нужен"


# ═══════════════════════ маршрут /sipuni/{secret} ═══════════════════════

SECRET = "spSECRETvalue123"


@pytest.fixture
def web(monkeypatch):
    monkeypatch.setattr(webhooks, "SIPUNI_WEBHOOK_SECRET", SECRET)
    received = []
    monkeypatch.setattr(U, "sipuni_notify_bg", lambda payload: received.append(payload))
    client = TestClient(webhooks.app)   # без with: фоновые циклы приложения не поднимаем
    client.received = received
    return client


def test_empty_secret_config_gives_403(web, monkeypatch):
    """SIPUNI_WEBHOOK_SECRET пуст - маршрут выключен: никакой сегмент пути не пройдёт."""
    monkeypatch.setattr(webhooks, "SIPUNI_WEBHOOK_SECRET", "")
    assert web.get("/sipuni/anything?event=1&call_id=1.1").status_code == 403
    assert web.post("/sipuni/anything", json=e(1)).status_code == 403
    assert web.get("/sipuni/x").status_code == 403
    assert web.received == []


def test_wrong_secret_gives_403_and_does_not_leak_secrets_in_log(web, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")
    resp = web.get("/sipuni/WRONGsecret?event=1&call_id=1.1")
    assert resp.status_code == 403
    assert SECRET not in resp.text
    assert web.post("/sipuni/WRONGsecret", json=e(1)).status_code == 403
    assert web.received == []
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "WRONGsecret" not in text and SECRET not in text


def test_get_query_is_accepted_and_answer_is_success_json(web, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")
    resp = web.get(f"/sipuni/{SECRET}", params=e(1))
    assert resp.status_code == 200 and resp.json() == {"success": True}
    assert web.received[-1][0]["call_id"] == "1700000000.1"
    assert SECRET not in "\n".join(r.getMessage() for r in caplog.records)


def test_post_json_and_form_and_list_are_accepted(web):
    resp = web.post(f"/sipuni/{SECRET}", json=e(2, status="ANSWER"))
    assert resp.status_code == 200 and resp.json() == {"success": True}
    assert web.received[-1][0]["status"] == "ANSWER"
    resp = web.post(f"/sipuni/{SECRET}", data=e(1))
    assert resp.status_code == 200 and resp.json() == {"success": True}
    assert web.received[-1][0]["event"] == "1"
    resp = web.post(f"/sipuni/{SECRET}", json=[e(1), e(2)])
    assert resp.json() == {"success": True} and len(web.received[-1]) == 2


def test_answer_is_success_even_for_garbage_and_for_failures(web, monkeypatch):
    """Без {"success": true} Sipuni приостановит отправку событий - отвечаем всегда."""
    for body in (b"", b"{", b"\xff\xfe", b"not a query at all"):
        resp = web.post(f"/sipuni/{SECRET}", content=body)
        assert resp.status_code == 200 and resp.json() == {"success": True}

    def boom(payload):
        raise RuntimeError("сбой постановки в фон")

    monkeypatch.setattr(U, "sipuni_notify_bg", boom)
    resp = web.get(f"/sipuni/{SECRET}", params=e(1))
    assert resp.status_code == 200 and resp.json() == {"success": True}


def test_oversized_body_is_dropped_but_answered(web):
    resp = web.post(f"/sipuni/{SECRET}", content=b"x" * (webhooks._SIPUNI_MAX_BODY + 1))
    assert resp.status_code == 200 and resp.json() == {"success": True}
    assert web.received[-1] == [], "тело выброшено, событий нет"


def test_uis_route_is_still_there_and_untouched(monkeypatch):
    monkeypatch.setattr(webhooks, "UIS_WEBHOOK_SECRET", "uisSECRET")
    seen = []
    monkeypatch.setattr(U, "notify_bg", lambda params: seen.append(params))
    client = TestClient(webhooks.app)
    assert client.get("/uis/uisSECRET", params={"call_session_id": "7"}).json() == {"ok": True}
    assert client.get("/uis/bad").status_code == 403
    assert seen == [{"call_session_id": "7"}]


# ═══════════════════════ секрет Sipuni в журнале ═══════════════════════


@pytest.mark.parametrize("path, expected", [
    (f"/sipuni/{SECRET}", "/sipuni/HIDDEN"),
    (f"/sipuni/{SECRET}?event=1&call_id=1.1", "/sipuni/HIDDEN"),
])
def test_sipuni_secret_is_masked_in_path(path, expected):
    assert webhooks.mask_secret_path(path) == expected
    assert "/sipuni/" in webhooks.SECRET_PATH_PREFIXES


def test_sipuni_secret_does_not_reach_the_access_log():
    f = webhooks.MaskSecretsInAccessLog()
    for path in (f"/sipuni/{SECRET}", f"/sipuni/{SECRET}?event=1&call_record_link=http%3A//a/b",
                 f"/sipuni/{SECRET}/extra"):
        rec = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1,
                                '%s - "%s %s HTTP/%s" %d', ("1.2.3.4:5", "GET", path, "1.1", 200), None)
        assert f.filter(rec) is True
        assert SECRET not in rec.getMessage()
        assert "HIDDEN" in rec.getMessage()


# ═══════════════════════ гейт распределения для источника Sipuni ═══════════════════════

SIPUNI_SRC = 777000777


@pytest.fixture
def dist(monkeypatch):
    """Стенд test_lead_distribution (фейки amo/telegram/журнала, временные файлы) плюс чистый
    трекер Sipuni. Режим live и источник Sipuni включены, как их включат на боевом сервере."""
    tld.setup_function()
    tld._reset_fakes()
    saved = tld.install_fakes()
    ld._uis_tag_wait_pending.clear()
    ld._sipuni_wait_pending.clear()
    S.tracker.reset()
    monkeypatch.setattr(S.tracker, "on_missed", None)
    monkeypatch.setattr(S.tracker, "_quiet_override", 0.05)
    monkeypatch.setattr(S.tracker, "stuck_s", 5.0)
    monkeypatch.setattr(ld, "SIPUNI_EVENTS_MODE", "live")
    monkeypatch.setattr(ld, "SIPUNI_SOURCE_IDS", {SIPUNI_SRC})
    monkeypatch.setattr(ld, "SIPUNI_CONNECT_WAIT_S", 0.3)
    monkeypatch.setattr(ld, "SIPUNI_CONNECT_POLL_S", 0.02)
    tld._seed_profile(name="SipuniCalls", source_ids=[SIPUNI_SRC, ld.UIS_SOURCE_ID],
                      participant_ids=[1, 2])
    yield
    S.tracker.reset()
    tld.restore_fakes(saved)
    tld.teardown_function()


def _sipuni_lead(lead_id, source_id=SIPUNI_SRC, tags=None):
    lead = tld._lead(lead_id=lead_id, source_id=source_id, tags=tags, contacts=[{"id": 500}])
    lead["created_at"] = int(time.time())
    tld._lead_by_id[lead_id] = lead
    tld._contact_by_id[500] = tld._contact(500, other_leads=[], name="Клиент", phone="+7 (955) 555-55-55")
    return lead


async def drain():
    """Ждёт все фоновые задачи распределения, в том числе порождённые по ходу ожидания."""
    for _ in range(200):
        pending = [t for t in ld._bg_tasks if not t.done()]
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)
    raise AssertionError("фоновые задачи распределения не закончились")


def _outcomes():
    return [c["outcome"] for c in tld._log_calls]


def test_sipuni_gate_answered_call_keeps_responsible_with_same_outcome(dist):
    """Ответ по телефону клиента уже был: ответственного не трогаем, исход тот же uis_connected."""
    _sipuni_lead(900)
    feed(S.tracker, leg("g.1", "012345101", 100, "ANSWER"))

    async def go():
        out = await ld.process_lead_distribution(900)
        await drain()
        return out

    assert run(go()) == "skipped-sipuni-connected"
    assert tld._patch_calls == [], "ответственного на такой сделке не меняем"
    assert _outcomes() == ["uis_connected"]
    assert tld._log_calls[0]["detail"]["call_provider"] == "sipuni"
    assert not ld.routed_store.is_routed(900), "сделка распределённой не считается"


def test_sipuni_gate_active_call_waits_then_connected(dist):
    _sipuni_lead(901)
    feed(S.tracker, [e(1, "g.2", dst="012345101", ts=100)])      # звонок идёт, ответа ещё нет

    async def go():
        out = await ld.process_lead_distribution(901)
        assert out == "waiting-for-sipuni-call"
        assert 901 in ld._sipuni_wait_pending
        assert tld._patch_calls == [] and tld._log_calls == []
        await asyncio.sleep(0.08)
        feed(S.tracker, [e(3, "g.2", dst="012345101", ts=105)])  # менеджер ответил
        await drain()
        return out

    run(go())
    assert tld._patch_calls == [], "дозвонились сами - пул мимо"
    assert _outcomes() == ["uis_connected"]
    assert 901 not in ld._sipuni_wait_pending


def test_sipuni_gate_active_call_ended_unanswered_distributes(dist):
    _sipuni_lead(902)
    feed(S.tracker, [e(1, "g.3", dst="012345101", ts=100)])

    async def go():
        assert await ld.process_lead_distribution(902) == "waiting-for-sipuni-call"
        await asyncio.sleep(0.05)
        feed(S.tracker, [e(2, "g.3", dst="012345101", ts=130, status="NOANSWER")])
        S.tracker.sweep(time.time() + 60)         # тихое окно истекло: звонок закончился пропущенным
        await drain()

    run(go())
    assert len(tld._patch_calls) == 1, "звонок не взяли - распределяем как обычно"
    assert ld.routed_store.is_routed(902)


def test_sipuni_gate_wait_budget_expires_and_distributes(dist):
    _sipuni_lead(903)
    feed(S.tracker, [e(1, "g.4", dst="012345101", ts=100)])     # звонок так и не заканчивается

    async def go():
        assert await ld.process_lead_distribution(903) == "waiting-for-sipuni-call"
        await drain()

    run(go())
    assert len(tld._patch_calls) == 1, "не дождались - то же решение, что у UIS: распределяем"
    assert 903 not in ld._sipuni_wait_pending


@pytest.mark.parametrize("events", [
    [],                                                              # событий по номеру нет
    leg("g.5", "012345101", 100),                                    # был и закончился без ответа
])
def test_sipuni_gate_missed_or_unknown_distributes_immediately(dist, events):
    _sipuni_lead(904)
    feed(S.tracker, events)
    S.tracker.sweep(time.time() + 120)

    async def go():
        out = await ld.process_lead_distribution(904)
        await drain()
        return out

    assert run(go()) == "routed"
    assert len(tld._patch_calls) == 1


def test_sipuni_gate_is_off_when_source_is_not_listed(dist, monkeypatch):
    """SIPUNI_SOURCE_IDS пуст (по умолчанию) - ничего не меняется, даже при отвеченном звонке."""
    monkeypatch.setattr(ld, "SIPUNI_SOURCE_IDS", set())
    _sipuni_lead(905)
    feed(S.tracker, leg("g.6", "012345101", 100, "ANSWER"))

    async def go():
        out = await ld.process_lead_distribution(905)
        await drain()
        return out

    # профиль у источника есть, гейта нет: распределяется как обычно
    assert run(go()) == "routed"


def test_sipuni_gate_is_off_in_log_mode(dist, monkeypatch):
    monkeypatch.setattr(ld, "SIPUNI_EVENTS_MODE", "log")
    _sipuni_lead(906)
    feed(S.tracker, leg("g.7", "012345101", 100, "ANSWER"))

    async def go():
        out = await ld.process_lead_distribution(906)
        await drain()
        return out

    assert run(go()) == "routed"


def test_sipuni_gate_breakdown_does_not_block_distribution(dist, monkeypatch):
    _sipuni_lead(907)

    def boom(*a, **k):
        raise RuntimeError("сбой трекера")

    monkeypatch.setattr(S.tracker, "phone_status", boom)

    async def go():
        out = await ld.process_lead_distribution(907)
        await drain()
        return out

    assert run(go()) == "routed"


def test_sipuni_gate_old_answer_before_lead_is_not_a_connect(dist):
    """Ответ вчерашнего звонка не считается дозвоном до сегодняшней сделки."""
    lead = _sipuni_lead(908)
    lead["created_at"] = int(time.time()) + 7200          # сделка «из будущего» относительно звонка
    feed(S.tracker, leg("g.8", "012345101", 100, "ANSWER"))

    async def go():
        out = await ld.process_lead_distribution(908)
        await drain()
        return out

    assert run(go()) == "routed"


# ═══════════════════════ UIS-гейт распределения не изменился ═══════════════════════


def test_uis_gate_behaviour_is_unchanged(dist):
    assert ld.UIS_SOURCE_ID == 13311214
    # тега нет: ждём тег
    _sipuni_lead(910, source_id=ld.UIS_SOURCE_ID)

    async def waiting():
        out = await ld.process_lead_distribution(910)
        task_set = set(ld._uis_tag_wait_pending)
        for t in list(ld._bg_tasks):
            t.cancel()
        await asyncio.gather(*ld._bg_tasks, return_exceptions=True)
        return out, task_set

    out, pending = run(waiting())
    assert out == "waiting-for-uis-tag" and 910 in pending
    assert ld._sipuni_wait_pending == set(), "UIS-ожидание не путается с ожиданием Sipuni"
    ld._uis_tag_wait_pending.clear()
    tld._reset_fakes()

    # «Успешный звонок»: дозвонились сами, исход тот же, что и раньше
    _sipuni_lead(911, source_id=ld.UIS_SOURCE_ID, tags=[{"name": TAG_SUCCESS_CALL_NAME}])

    async def connected():
        out = await ld.process_lead_distribution(911)
        await drain()
        return out

    assert run(connected()) == "skipped-uis-connected"
    assert _outcomes() == ["uis_connected"] and tld._patch_calls == []
    assert "call_provider" not in tld._log_calls[0]["detail"]
    tld._reset_fakes()

    # «пропущенный»: распределяем сразу
    _sipuni_lead(912, source_id=ld.UIS_SOURCE_ID, tags=[{"name": TAG_MISSED_NAME}])

    async def missed():
        out = await ld.process_lead_distribution(912)
        await drain()
        return out

    assert run(missed()) == "routed"


def test_uis_source_with_sipuni_events_present_ignores_the_tracker(dist):
    """У источника UIS решает тег, даже если трекер Sipuni знает отвеченный звонок с номера."""
    _sipuni_lead(913, source_id=ld.UIS_SOURCE_ID, tags=[{"name": TAG_MISSED_NAME}])
    feed(S.tracker, leg("g.9", "012345101", 100, "ANSWER"))

    async def go():
        out = await ld.process_lead_distribution(913)
        await drain()
        return out

    assert run(go()) == "routed"
