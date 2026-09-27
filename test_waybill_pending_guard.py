"""Юнит-тест гейта незавершённого заказа СДЭК (без сети и прода).

Разбор 27.09.2026, сделки 36565053 и 36553383. СДЭК держал заявки в
state=ACCEPTED без cdek_number больше часа. Когда фоновое дожидание сдавалось,
`_fail` ставил тег «ошибка накладной» через patch_lead, amo отвечал на это
lead_change с тем же статусом «Сделать накладную», webhooks.py смотрел на
ЗНАЧЕНИЕ статуса (а не на факт перехода) и звал enqueue_waybill снова. В логах
прода секунда между отказом и повторной постановкой в очередь, и как результат -
ВТОРОЕ настоящее отправление на одну посылку. Гейт «поле 571657 заполнено» тут
не спасал: номера СДЭК не дал, поле пустое.

Проверяем ровно это:
  1. эхо после отказа НЕ создаёт второй заказ, пока первый висит без номера;
  2. номер появился позже - гейт сам его коммитит, нового заказа не создаёт;
  3. СДЭК отклонил - блокировку снимаем, честный /retry снова работает;
  4. заказа в СДЭК нет вовсе (404) - блокировку снимаем, это выход для менеджера;
  5. СДЭК недоступен - второй заказ НЕ создаём (лучше не создать, чем удвоить);
  6. на возобновлении после рестарта теги сделки читаются заново, а не берутся
     пустым списком - иначе commit_waybill снёс бы сделке все теги.

Все три файла (test_waybill_pending_order.py, test_queue_manager_waybill_dedup.py
и этот) проверяют РАЗНЫЕ куски одной истории: те два - поведение внутри процесса,
этот - что круг через amo больше не приводит ко второму заказу.
"""

import asyncio
import os
import tempfile

import cdek_client
import waybill_pending_store
import waybill_service

# База - во временный файл, боевую /app/var не трогаем.
_TMP = tempfile.mkdtemp(prefix="waybill_pending_test_")
waybill_pending_store.DB_PATH = os.path.join(_TMP, "pending.sqlite3")
waybill_pending_store.init()

waybill_service.WAYBILL_BACKGROUND_POLL_SECONDS = 0.05
waybill_service.WAYBILL_BACKGROUND_POLL_INTERVAL_S = 0.01

LEAD = 36565053
UUID_FIRST = "8054037b-817a-4774-baee-b4cd24123dde"

_created: list = []
_commits: list = []
_notes: list = []
_patched: list = []


def _pending_response():
    """Ровно то, что СДЭК отдавал 27.09: заявка принята, номера нет, ошибок нет."""
    return {"entity": {"uuid": UUID_FIRST, "statuses": [{"code": "ACCEPTED"}]},
            "requests": [{"type": "CREATE", "state": "ACCEPTED"}]}


def _numbered_response(number="10326999999"):
    return {"entity": {"uuid": UUID_FIRST, "cdek_number": number},
            "requests": [{"type": "CREATE", "state": "SUCCESSFUL"}]}


def _rejected_response():
    return {"entity": {"uuid": UUID_FIRST},
            "requests": [{"type": "CREATE", "state": "INVALID",
                          "errors": [{"code": "v2_bad", "message": "неверный ПВЗ"}]}]}


def _reset(get_order, lead_tags=None):
    _created.clear()
    _commits.clear()
    _notes.clear()
    _patched.clear()
    waybill_pending_store.drop(LEAD)
    waybill_service._pending_pollers.clear()

    tags = lead_tags if lead_tags is not None else [{"id": 1, "name": "Горячий"}]

    async def _fake_get_order(uuid):
        r = get_order(uuid)
        if isinstance(r, Exception):
            raise r
        return r

    async def _fake_create_order(order):
        _created.append(order)
        return {"entity": {"uuid": "новый-заказ-которого-быть-не-должно"}}

    async def _fake_get_lead_full(lead_id, **kw):
        return {"id": lead_id, "_embedded": {"tags": list(tags)},
                "custom_fields_values": []}

    async def _fake_commit_waybill(lead_id, cdek_value, current_tags, **kw):
        _commits.append((lead_id, cdek_value, [t.get("name") for t in current_tags]))
        return {"ok": True}

    async def _fake_add_note(lead_id, text):
        _notes.append(text)
        return {"ok": True}

    async def _fake_patch_lead(lead_id, **kw):
        _patched.append(kw)
        return {"ok": True}

    async def _noop(*a, **kw):
        return None

    cdek_client.get_order = _fake_get_order
    cdek_client.create_order = _fake_create_order
    waybill_service.amo_service.get_lead_full = _fake_get_lead_full
    waybill_service.amo_service.commit_waybill = _fake_commit_waybill
    waybill_service.amo_service.add_note = _fake_add_note
    waybill_service.amo_service.patch_lead = _fake_patch_lead
    waybill_service.amo_service.move_to_ready_and_clear_error = _noop
    waybill_service._verify_trek_after_delay = _noop
    waybill_service._alert = _noop


def test_echo_does_not_create_second_order():
    """Главный случай: заказ висит без номера, прилетело эхо — второго заказа нет."""
    _reset(lambda u: _pending_response())
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    res = asyncio.run(waybill_service._resume_or_block_pending(LEAD, [], "webhook"))
    assert res is not None, "гейт обязан заблокировать повторное создание"
    assert res["reason"] == "pending", res
    assert _created == [], f"создан ВТОРОЙ заказ СДЭК: {_created}"
    assert waybill_pending_store.get(LEAD)["order_uuid"] == UUID_FIRST
    print("ok  эхо не создало второй заказ")


def test_number_arrived_later_is_committed_by_guard():
    _reset(lambda u: _numbered_response())
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    res = asyncio.run(waybill_service._resume_or_block_pending(
        LEAD, [{"id": 1, "name": "Горячий"}], "webhook"))
    assert res is not None and res["ok"] is True, res
    assert res["cdek_number"] == "10326999999", res
    assert _created == [], "нового заказа быть не должно"
    assert _commits and _commits[0][1] == "10326999999", _commits
    assert waybill_pending_store.get(LEAD) is None, "строку надо снять после коммита"
    print("ok  номер пришёл позже — гейт закоммитил без нового заказа")


def test_rejected_order_unblocks():
    _reset(lambda u: _rejected_response())
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    res = asyncio.run(waybill_service._resume_or_block_pending(LEAD, [], "retry"))
    assert res is None, "отказ = отправления нет, путь должен быть свободен"
    assert waybill_pending_store.get(LEAD) is None
    print("ok  отказ СДЭК снимает блокировку")


def test_missing_order_unblocks():
    err = cdek_client.CdekError("CDEK GET /orders/x: 404", status=404)
    _reset(lambda u: err)
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    # Заявка должна быть «не свежей», иначе 404 намеренно не считается честным.
    waybill_service.PENDING_404_TRUST_AFTER_S = 0.0
    try:
        res = asyncio.run(waybill_service._resume_or_block_pending(LEAD, [], "retry"))
    finally:
        waybill_service.PENDING_404_TRUST_AFTER_S = 120.0
    assert res is None, "заказа нет — создавать новый можно, это выход для менеджера"
    assert waybill_pending_store.get(LEAD) is None
    print("ok  отсутствующий заказ (404) снимает блокировку")


def test_fresh_404_is_not_trusted():
    """404 по только что созданной заявке — не доказательство, что заказа нет.
    Поверить ему значит создать второе реальное отправление."""
    err = cdek_client.CdekError("CDEK GET /orders/x: 404", status=404)
    _reset(lambda u: err)
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    res = asyncio.run(waybill_service._resume_or_block_pending(LEAD, [], "webhook"))
    assert res is not None and res["reason"] == "pending-unverified", res
    assert _created == [], "по свежему 404 второй заказ создавать нельзя"
    assert waybill_pending_store.get(LEAD) is not None, "блокировку снимать рано"
    print("ok  свежий 404 не снимает блокировку")


def test_cdek_unreachable_blocks():
    err = cdek_client.CdekError("CDEK GET /orders/x: 500", status=500)
    _reset(lambda u: err)
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")
    res = asyncio.run(waybill_service._resume_or_block_pending(LEAD, [], "webhook"))
    assert res is not None and res["reason"] == "pending-unverified", res
    assert _created == [], "при недоступном СДЭК второй заказ создавать нельзя"
    assert waybill_pending_store.get(LEAD) is not None, "блокировку снимать нельзя"
    print("ok  недоступный СДЭК блокирует создание")


def test_resume_reads_tags_fresh_and_does_not_wipe_them():
    """На возобновлении снимка тегов нет. Пустой список ушёл бы в commit_waybill и
    снёс сделке все теги — проверяем, что теги перечитываются из amo."""
    _reset(lambda u: _numbered_response(), lead_tags=[{"id": 7, "name": "Горячий"},
                                                     {"id": 8, "name": "ошибка накладной"}])
    waybill_pending_store.put(LEAD, UUID_FIRST, "webhook")

    async def _run():
        # Подъём и ожидание обязаны жить в ОДНОМ event loop: resume_pending_orders
        # создаёт задачу, и на выходе из asyncio.run она бы просто умерла.
        started = await waybill_service.resume_pending_orders()
        assert started == 1, started
        for _ in range(300):
            if _commits:
                return
            await asyncio.sleep(0.01)

    asyncio.run(_run())
    assert _commits, "возобновлённый опрос должен был закоммитить номер"
    names = _commits[0][2]
    assert "Горячий" in names, f"теги сделки затёрты на возобновлении: {names}"
    print("ok  возобновление читает теги заново, а не затирает их")


if __name__ == "__main__":
    test_echo_does_not_create_second_order()
    test_number_arrived_later_is_committed_by_guard()
    test_rejected_order_unblocks()
    test_missing_order_unblocks()
    test_fresh_404_is_not_trusted()
    test_cdek_unreachable_blocks()
    test_resume_reads_tags_fresh_and_does_not_wipe_them()
    print("\nвсе проверки гейта пройдены")
