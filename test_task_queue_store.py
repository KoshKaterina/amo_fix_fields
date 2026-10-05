"""Юнит-тест журнала очереди задач на диске.

Главное, что здесь проверяется, — не «записалось ли», а **политика
восстановления**. Семантика журнала «хотя бы один раз», и разница между
«повторили лишний раз» и «клиент получил вторую платёжку» живёт ровно в этих
проверках. Если тест про ozon_invoice когда-нибудь покраснеет, значит кто-то
внёс счёт в RESUME_RUNNING — читать докстринг task_queue_store перед тем, как
«починить» тест.

⚠️ Путь к базе ставим ДО импорта и через setdefault — грабля 2 из
knowledge/amo-fix-fields-testy-grabli.md.

Запуск: python test_task_queue_store.py  или  python -m pytest test_task_queue_store.py -q
"""
import json
import os
import tempfile

os.environ.setdefault(
    "TASK_QUEUE_DB_PATH",
    os.path.join(tempfile.mkdtemp(), "task_queue_test.sqlite3"),
)

import task_queue_store as S  # noqa: E402

S.init()

_seq = [0]


def _next_seq() -> int:
    _seq[0] += 1
    return _seq[0]


def _clean():
    """Между тестами журнал пустой: take_for_restore сам его чистит."""
    S.take_for_restore()


def _add(kind, lead_id, lane="amo", priority=0, payload=None):
    p = payload if payload is not None else {"lead_id": lead_id, "_kind": kind}
    return S.add(lane, priority, _next_seq(), kind, lead_id, p)


# ── запись и удаление ───────────────────────────────────────────────────────

def test_added_task_is_restored():
    _clean()
    rid = _add("office_transfer", 111)
    assert rid is not None
    data = S.take_for_restore()
    assert len(data["resume"]) == 1
    row = data["resume"][0]
    assert row["kind"] == "office_transfer"
    assert row["lead_id"] == "111"
    assert row["payload"]["lead_id"] == 111


def test_dropped_task_is_not_restored():
    """Доработанная задача не должна подниматься на старте."""
    _clean()
    rid = _add("office_transfer", 222)
    S.drop(rid)
    assert S.take_for_restore()["resume"] == []


def test_journal_is_emptied_by_restore():
    """Иначе после двух рестартов подряд в таблице были бы дубли."""
    _clean()
    _add("lead_update", 333)
    assert len(S.take_for_restore()["resume"]) == 1
    assert S.take_for_restore()["resume"] == []


def test_update_payload_keeps_coalesced_state():
    """Коалесинг правит payload в очереди на месте — журнал обязан увидеть то
    же самое, иначе после рестарта применится устаревшее состояние."""
    _clean()
    payload = {"lead_id": 444, "_kind": "lead_update", "goods": "старое"}
    rid = _add("lead_update", 444, payload=payload)
    payload["goods"] = "свежее"
    payload["delivery_type"] = "СДЭК"
    S.update_payload(rid, payload)
    row = S.take_for_restore()["resume"][0]
    assert row["payload"]["goods"] == "свежее"
    assert row["payload"]["delivery_type"] == "СДЭК"


def test_row_id_is_not_persisted_into_payload():
    """`_row_id` живёт в payload в памяти, но в журнал его писать незачем — на
    восстановлении он выдаётся заново."""
    _clean()
    payload = {"lead_id": 555, "_kind": "lead_update", "_row_id": 999}
    _add("lead_update", 555, payload=payload)
    row = S.take_for_restore()["resume"][0]
    assert "_row_id" not in row["payload"]


# ── ПОЛИТИКА восстановления прерванных задач ────────────────────────────────

def test_interrupted_invoice_is_NOT_resumed():
    """⚠️ Ядро всей правки. ext_id платежа = amo-<lead>-<unixtime>, значит
    повторный заход создаст в Ozon ВТОРОЙ платёж, а не наткнётся на
    существующий. Если процесс умер между «платёж создан» и «ссылка записана»,
    автоповтор выдал бы клиенту две живые платёжки."""
    _clean()
    rid = _add("ozon_invoice", 666)
    S.mark_running(rid)
    data = S.take_for_restore()
    assert data["resume"] == [], "прерванный счёт повторять нельзя"
    assert len(data["skipped_running"]) == 1
    assert data["skipped_running"][0]["kind"] == "ozon_invoice"
    assert data["skipped_running"][0]["lead_id"] == "666"


def test_pending_invoice_IS_resumed():
    """А вот счёт, который в очереди только стоял и обработчик не начинался,
    возвращать обязательно: к сделке никто не прикасался, терять нечего."""
    _clean()
    _add("ozon_invoice", 777)          # статус pending, mark_running НЕ зовём
    data = S.take_for_restore()
    assert len(data["resume"]) == 1
    assert data["resume"][0]["kind"] == "ozon_invoice"
    assert data["skipped_running"] == []


def test_interrupted_waybill_is_resumed():
    """У накладных своё хранилище незавершённых заказов и resume_pending_orders
    на старте — повторный заход увидит отданный в СДЭК заказ и второго не
    создаст."""
    _clean()
    rid = _add("waybill", 888)
    S.mark_running(rid)
    data = S.take_for_restore()
    assert len(data["resume"]) == 1
    assert data["skipped_running"] == []


def test_interrupted_idempotent_kinds_are_resumed():
    _clean()
    for kind in ("lead_update", "office_transfer", "lead_distribution",
                 "cdek_sync", "metrika_sync", "jivo"):
        rid = _add(kind, 900)
        S.mark_running(rid)
    data = S.take_for_restore()
    assert len(data["resume"]) == 6, [r["kind"] for r in data["resume"]]
    assert data["skipped_running"] == []


def test_resume_running_list_excludes_invoice():
    """Защита от правки списка «не подумав»."""
    assert "ozon_invoice" not in S.RESUME_RUNNING
    assert "waybill" in S.RESUME_RUNNING


# ── возраст и порядок ───────────────────────────────────────────────────────

def test_too_old_tasks_are_dropped_not_resumed():
    """Задача, провисевшая сутки, почти наверняка уже неактуальна: сделку
    доработали руками, и поднимать её — значит дёрнуть клиента по мёртвому
    поводу."""
    _clean()
    rid = _add("office_transfer", 1001)
    with S._conn() as conn:
        conn.execute("UPDATE tasks SET enqueued_at = 1 WHERE id = ?", (rid,))
    data = S.take_for_restore()
    assert data["resume"] == []
    assert data["too_old"] == 1


def test_restore_order_is_priority_then_sequence():
    _clean()
    _add("office_transfer", 1, priority=5)
    _add("office_transfer", 2, priority=-10)   # счёт-подобный, выше всех
    _add("office_transfer", 3, priority=0)
    _add("office_transfer", 4, priority=-10)   # тот же приоритет, позже по seq
    order = [(r["priority"], r["lead_id"]) for r in S.take_for_restore()["resume"]]
    assert order == [(-10, "2"), (-10, "4"), (0, "3"), (5, "1")], order


def test_lanes_are_kept():
    _clean()
    _add("metrika_sync", 11, lane="sync")
    _add("cdek_sync", 12, lane="cdek")
    _add("lead_update", 13, lane="amo")
    lanes = {r["lead_id"]: r["lane"] for r in S.take_for_restore()["resume"]}
    assert lanes == {"11": "sync", "12": "cdek", "13": "amo"}


# ── отказоустойчивость ──────────────────────────────────────────────────────

def test_broken_store_does_not_raise():
    """Сломанный журнал не должен ломать очередь: задача всё равно ставится и
    обрабатывается, просто без долговечности."""
    _clean()
    real = S.DB_PATH
    S.DB_PATH = os.path.join(real, "nope", "task_queue.sqlite3")
    try:
        assert _add("lead_update", 1234) is None
        S.mark_running(None)
        S.drop(None)
        S.update_payload(None, {})
        assert S.take_for_restore()["resume"] == []
    finally:
        S.DB_PATH = real


def test_disabled_store_is_a_noop():
    _clean()
    S._enabled = False
    try:
        assert S.add("amo", 0, 1, "lead_update", 1, {}) is None
        assert S.is_enabled() is False
        assert S.stats() == {"enabled": False}
    finally:
        S._enabled = True


def test_broken_payload_row_is_skipped_not_fatal():
    _clean()
    rid = _add("lead_update", 1313)
    with S._conn() as conn:
        conn.execute("UPDATE tasks SET payload = ? WHERE id = ?", ("{не json", rid))
    data = S.take_for_restore()
    assert data["resume"] == []


def test_stats_count_rows_and_running():
    _clean()
    a = _add("lead_update", 1)
    _add("lead_update", 2)
    S.mark_running(a)
    st = S.stats()
    assert st["enabled"] is True
    assert st["rows"] == 2
    assert st["running"] == 1
    _clean()


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
