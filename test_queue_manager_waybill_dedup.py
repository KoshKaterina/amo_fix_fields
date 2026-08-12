"""Юнит-тест дедупа waybill в очереди (queue_manager) — без сети/прода.

Разбор 12.08.2026 (сделка 36532789, заказ 06193): флаг _pending_waybills
снимался СРАЗУ на dequeue, ДО фактической обработки (create_waybill_for_lead
ждёт cdek_number до 60с) — окно в минуту оставалось полностью незащищённым.
office_transfer следом делает ещё несколько PATCH по той же сделке (смена
ответственного, поле «Ответственный МОП», сам переход статуса), каждый рождает
свой lead_change-вебхук с тем же status_id="Сделать накладную" -> повторный
enqueue_waybill проходил и запускал ВТОРОЙ create_waybill_for_lead поверх ещё
не завершённого первого — родился настоящий, принятый СДЭК дубль заказа.

Проверяем: пока первая обработка "waybill" ещё идёт, повторный enqueue_waybill
для того же lead_id НЕ проходит (создатель задачи не вызывается второй раз);
после завершения обработки — проходит снова (флаг освобождён), как и должно
быть для настоящего повторного /retry.
"""

import asyncio

import queue_manager
import waybill_service

_calls: list = []
_release = asyncio.Event()


async def _fake_create_waybill_for_lead(lead_id, *, source="webhook"):
    _calls.append(lead_id)
    # Имитируем «застрявшую в 60-секундном поллинге» обработку: висим, пока
    # тест явно не отпустит — так можно проверить состояние дедупа РОВНО во
    # время обработки, а не только в момент постановки в очередь.
    await _release.wait()
    return {"ok": True}


async def _wait_until(predicate, timeout=2.0, step=0.01):
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        await asyncio.sleep(step)
        waited += step
    return False


async def _main():
    queue_manager._queues.clear()
    queue_manager._pending_waybills.clear()
    queue_manager._queues[queue_manager.LANE_AMO] = asyncio.PriorityQueue()
    queue_manager.is_circuit_open = lambda category: False
    queue_manager.set_breaker_category = lambda category: None
    waybill_service.create_waybill_for_lead = _fake_create_waybill_for_lead

    worker = asyncio.create_task(queue_manager._worker(queue_manager.LANE_AMO))
    try:
        queue_manager.enqueue_waybill(36532789, source="webhook")
        assert "36532789" in queue_manager._pending_waybills, "сразу после enqueue флаг должен стоять"

        assert await _wait_until(lambda: _calls), "воркер должен был начать обработку"
        assert _calls == [36532789]

        # ГЛАВНАЯ ПРОВЕРКА: обработка ещё идёт (застряла в _release.wait()) —
        # флаг дедупа обязан оставаться, иначе повторный вебхук пройдёт.
        assert "36532789" in queue_manager._pending_waybills, (
            "флаг дедупа не должен сниматься на dequeue — обработка ещё не закончена "
            "(это и есть баг 12.08.2026: discard стоял в начале воркера, а не в finally)"
        )
        queue_manager.enqueue_waybill(36532789, source="webhook")
        await asyncio.sleep(0.05)
        assert _calls == [36532789], (
            "повторный вебхук ВО ВРЕМЯ обработки не должен запускать второй "
            "create_waybill_for_lead — именно так родился настоящий дубль заказа "
            "СДЭК 12.08.2026 (сделка 36532789, номера 10306104834/10306103516)"
        )

        # Отпускаем «зависшую» обработку — она завершается, флаг снимается в finally.
        _release.set()
        assert await _wait_until(lambda: "36532789" not in queue_manager._pending_waybills), (
            "после завершения обработки флаг должен освободиться"
        )

        # Новый вебхук по той же сделке (например, настоящий /retry) снова
        # обязан пройти штатно — дедуп не должен залипать навсегда.
        _release.clear()
        queue_manager.enqueue_waybill(36532789, source="webhook")
        assert await _wait_until(lambda: len(_calls) == 2), (
            "после завершения предыдущей обработки новый вебхук обязан пройти"
        )
        _release.set()
        await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


asyncio.run(_main())
print("queue_manager waybill dedup (флаг живёт всю обработку, не только очередь): все тесты прошли")
