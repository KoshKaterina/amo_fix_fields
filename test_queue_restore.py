"""Восстановление очереди задач из журнала после перезапуска (queue_manager).

Проверяется сам стык: строки журнала → задачи в правильных дорожках с
правильными приоритетами, дедуп-пометки возвращены, прерванный счёт не
повторён, а зовёт человека алертом.

⚠️ Путь к базе — ДО импорта и через setdefault (грабля 2 из
knowledge/amo-fix-fields-testy-grabli.md). Воркеров не поднимаем: очереди
наполняем и читаем сами, иначе задачи уехали бы в реальные обработчики.

Запуск: python test_queue_restore.py  или  python -m pytest test_queue_restore.py -q
"""
import asyncio
import os
import tempfile

os.environ.setdefault(
    "TASK_QUEUE_DB_PATH",
    os.path.join(tempfile.mkdtemp(), "queue_restore_test.sqlite3"),
)

import queue_manager as qm  # noqa: E402
import task_queue_store as st  # noqa: E402

st.init()

_alerts: list = []


def _fake_alert_bg(key, text, event=None, values=None):
    _alerts.append({"key": key, "text": text, "event": event, "values": values})


qm._alert_bg = _fake_alert_bg


def run(coro):
    return asyncio.run(coro)


def _setup():
    """Чистый стенд: пустые очереди без воркеров, пустой журнал, пустые пометки."""
    _alerts.clear()
    st.take_for_restore()
    qm._queues.clear()
    for lane in qm.LANES:
        qm._queues[lane] = asyncio.PriorityQueue()
        qm._lane_stats[lane] = {"last_waited": 0.0, "processed": 0}
    qm._pending_leads.clear()
    qm._pending_waybills.clear()
    qm._pending_office_transfer.clear()
    qm._pending_lead_distribution.clear()
    qm._pending_invoice.clear()
    qm._pending_cdek_sync.clear()
    qm._pending_metrika_sync.clear()
    qm._pending_jivo.clear()


def _drain(lane):
    out = []
    q = qm._queues[lane]
    while not q.empty():
        item = q.get_nowait()
        out.append(item)
    return out


def test_enqueue_writes_journal_row():
    """Постановка задачи обязана оставить след на диске — иначе пересборка её
    снова съест."""
    _setup()
    qm.enqueue_office_transfer(4242, source="webhook")
    rows = st.take_for_restore()["resume"]
    assert len(rows) == 1
    assert rows[0]["kind"] == "office_transfer"
    assert rows[0]["lead_id"] == "4242"
    # и id строки доехал в payload, чтобы воркер мог её закрыть
    item = _drain(qm.LANE_AMO)[0]
    assert item.payload.get("_row_id") is not None


def test_restore_puts_tasks_back_into_right_lane_and_priority():
    _setup()
    st.add("amo", qm.PRIORITY_INVOICE, 1, "ozon_invoice", 10, {"lead_id": 10, "_kind": "ozon_invoice"})
    st.add("amo", qm.PRIORITY_NEW, 2, "office_transfer", 11, {"lead_id": 11, "_kind": "office_transfer"})
    st.add("sync", qm.PRIORITY_METRIKA_SYNC, 3, "metrika_sync", 12, {"lead_id": 12, "_kind": "metrika_sync"})
    st.add("cdek", qm.PRIORITY_CDEK_SYNC, 4, "cdek_sync", 13, {"lead_id": 13, "_kind": "cdek_sync", "_key": "k13"})

    res = run(qm.restore_queue())
    assert res["restored"] == 4, res

    amo = _drain(qm.LANE_AMO)
    # счёт должен выйти ПЕРВЫМ: у него приоритет выше (-10 против 0)
    assert [i.payload["lead_id"] for i in amo] == [10, 11], [i.payload for i in amo]
    assert [i.payload["lead_id"] for i in _drain(qm.LANE_SYNC)] == [12]
    assert [i.payload["lead_id"] for i in _drain(qm.LANE_CDEK)] == [13]


def test_restore_repopulates_dedup_marks():
    """Иначе повторная доставка вебхука сразу после рестарта завела бы ВТОРУЮ
    задачу по той же сделке."""
    _setup()
    st.add("amo", qm.PRIORITY_NEW, 1, "office_transfer", 21, {"lead_id": 21, "_kind": "office_transfer"})
    st.add("amo", qm.PRIORITY_INVOICE, 2, "ozon_invoice", 22, {"lead_id": 22, "_kind": "ozon_invoice"})
    st.add("amo", qm.PRIORITY_LEAD_DISTRIBUTION, 3, "lead_distribution", 23, {"lead_id": 23, "_kind": "lead_distribution"})
    st.add("amo", qm.PRIORITY_WAYBILL, 4, "waybill", 24, {"lead_id": 24, "_kind": "waybill"})
    run(qm.restore_queue())

    assert "21" in qm._pending_office_transfer
    assert "22" in qm._pending_invoice
    assert "23" in qm._pending_lead_distribution
    assert "24" in qm._pending_waybills

    # и дубль действительно отбивается
    before = qm._queues[qm.LANE_AMO].qsize()
    qm.enqueue_office_transfer(21, source="webhook")
    assert qm._queues[qm.LANE_AMO].qsize() == before, "дубль не должен был встать в очередь"


def test_interrupted_invoice_is_not_requeued_but_alerts():
    """⚠️ Повтор прерванного счёта создал бы клиенту вторую платёжку, поэтому
    вместо автоповтора зовём человека."""
    _setup()
    rid = st.add("amo", qm.PRIORITY_INVOICE, 1, "ozon_invoice", 31, {"lead_id": 31, "_kind": "ozon_invoice"})
    st.mark_running(rid)
    res = run(qm.restore_queue())

    assert res["restored"] == 0
    assert res["skipped"] == 1
    assert qm._queues[qm.LANE_AMO].qsize() == 0
    assert len(_alerts) == 1, _alerts
    a = _alerts[0]
    assert a["event"] == "queue_restore_skipped"
    assert "31" in a["text"]
    assert "ozon_invoice" in a["text"]


def test_interrupted_safe_kind_is_requeued_without_alert():
    _setup()
    rid = st.add("amo", qm.PRIORITY_NEW, 1, "office_transfer", 41, {"lead_id": 41, "_kind": "office_transfer"})
    st.mark_running(rid)
    res = run(qm.restore_queue())
    assert res["restored"] == 1
    assert res["skipped"] == 0
    assert _alerts == []


def test_restore_is_not_repeated_on_second_start():
    """Журнал чистится при разборе, а возвращённые задачи пишут НОВЫЕ строки.
    Значит второй рестарт подряд поднимет их один раз, а не два."""
    _setup()
    st.add("amo", qm.PRIORITY_NEW, 1, "office_transfer", 51, {"lead_id": 51, "_kind": "office_transfer"})
    assert run(qm.restore_queue())["restored"] == 1
    # имитируем второй рестарт: очереди пересоздаются, журнал остался от первого
    for lane in qm.LANES:
        qm._queues[lane] = asyncio.PriorityQueue()
    assert run(qm.restore_queue())["restored"] == 1
    # а если задачу доработали — строки нет, и поднимать нечего
    item = _drain(qm.LANE_AMO)[0]
    st.drop(item.payload["_row_id"])
    for lane in qm.LANES:
        qm._queues[lane] = asyncio.PriorityQueue()
    assert run(qm.restore_queue())["restored"] == 0


def test_restore_survives_unknown_lane():
    _setup()
    st.add("несуществующая", qm.PRIORITY_NEW, 1, "office_transfer", 61, {"lead_id": 61, "_kind": "office_transfer"})
    res = run(qm.restore_queue())
    assert res["restored"] == 0   # пропустили, но не упали


def test_restore_on_empty_journal_is_quiet():
    _setup()
    res = run(qm.restore_queue())
    assert res == {"restored": 0, "skipped": 0, "too_old": 0}
    assert _alerts == []


def test_coalesce_updates_journal_row():
    """Свежий вебхук по сделке, уже стоящей в очереди, правит payload на месте —
    после рестарта должно примениться СВЕЖЕЕ состояние, не старое."""
    _setup()
    qm.enqueue_new({"lead_id": 71, "goods": "старое"})
    qm.enqueue_new({"lead_id": 71, "goods": "свежее", "delivery_type": "СДЭК"})
    rows = st.take_for_restore()["resume"]
    assert len(rows) == 1, "коалесинг не должен плодить строки"
    assert rows[0]["payload"]["goods"] == "свежее"
    assert rows[0]["payload"]["delivery_type"] == "СДЭК"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    ok = 0
    for fn in fns:
        try:
            fn()
            print(f"OK {fn.__name__}")
            ok += 1
        except Exception as e:
            print(f"ПАДЕНИЕ {fn.__name__}: {e!r}")
    print(f"\n{ok}/{len(fns)} прошли")
