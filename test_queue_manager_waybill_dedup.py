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

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде сценарий запускался строкой
`asyncio.run(_main())` на уровне модуля, то есть выполнялся на ИМПОРТЕ: провал читался как
ошибка СБОРА и ронял сбор всего репозитория, а в сводке файл давал ноль тестов.

⚠️ Здесь ОДИН тест, а не пять, и это осознанно. Сценарий последовательный и с состоянием:
поставить в очередь → убедиться, что обработка идёт → проверить дедуп ровно во время
обработки → отпустить → проверить, что дедуп не залип. Разрезать это на отдельные тесты
значит либо поднимать воркер заново на каждый шаг (и потерять проверку «ровно во время
обработки»), либо завести связь между тестами по порядку - то самое, от чего в этом
репозитории уже страдали.
"""

import asyncio

import queue_manager
import waybill_service

LEAD_ID = 36532789


async def _scenario():
    calls: list = []
    release = asyncio.Event()

    async def _fake_create_waybill_for_lead(lead_id, *, source="webhook"):
        calls.append(lead_id)
        # Имитируем «застрявшую в 60-секундном поллинге» обработку: висим, пока
        # тест явно не отпустит — так можно проверить состояние дедупа РОВНО во
        # время обработки, а не только в момент постановки в очередь.
        await release.wait()
        return {"ok": True}

    async def _wait_until(predicate, timeout=2.0, step=0.01):
        waited = 0.0
        while waited < timeout:
            if predicate():
                return True
            await asyncio.sleep(step)
            waited += step
        return False

    # ⚠️ Всё подменённое ниже ВОЗВРАЩАЕТСЯ в finally. Это модули, общие на весь процесс:
    # пока файл был скриптом, оставить за собой фальшивый `create_waybill_for_lead` было
    # безвредно, процесс заканчивался. Под pytest он достался соседям, и
    # `test_waybill_intl_recipient.py` падал четырьмя тестами, потому что звал мою заглушку
    # вместо настоящей функции (поймано 07.10.2026).
    was_circuit = queue_manager.is_circuit_open
    was_breaker = queue_manager.set_breaker_category
    was_create = waybill_service.create_waybill_for_lead

    queue_manager._queues.clear()
    queue_manager._pending_waybills.clear()
    queue_manager._queues[queue_manager.LANE_AMO] = asyncio.PriorityQueue()
    queue_manager.is_circuit_open = lambda category: False
    queue_manager.set_breaker_category = lambda category: None
    waybill_service.create_waybill_for_lead = _fake_create_waybill_for_lead

    worker = asyncio.create_task(queue_manager._worker(queue_manager.LANE_AMO))
    try:
        queue_manager.enqueue_waybill(LEAD_ID, source="webhook")
        assert str(LEAD_ID) in queue_manager._pending_waybills, \
            "сразу после enqueue флаг должен стоять"

        assert await _wait_until(lambda: calls), "воркер должен был начать обработку"
        assert calls == [LEAD_ID]

        # ГЛАВНАЯ ПРОВЕРКА: обработка ещё идёт (застряла в release.wait()) —
        # флаг дедупа обязан оставаться, иначе повторный вебхук пройдёт.
        assert str(LEAD_ID) in queue_manager._pending_waybills, (
            "флаг дедупа не должен сниматься на dequeue — обработка ещё не закончена "
            "(это и есть баг 12.08.2026: discard стоял в начале воркера, а не в finally)"
        )
        queue_manager.enqueue_waybill(LEAD_ID, source="webhook")
        await asyncio.sleep(0.05)
        assert calls == [LEAD_ID], (
            "повторный вебхук ВО ВРЕМЯ обработки не должен запускать второй "
            "create_waybill_for_lead — именно так родился настоящий дубль заказа "
            "СДЭК 12.08.2026 (сделка 36532789, номера 10306104834/10306103516)"
        )

        # Отпускаем «зависшую» обработку — она завершается, флаг снимается в finally.
        release.set()
        assert await _wait_until(lambda: str(LEAD_ID) not in queue_manager._pending_waybills), \
            "после завершения обработки флаг должен освободиться"

        # Новый вебхук по той же сделке (например, настоящий /retry) снова
        # обязан пройти штатно — дедуп не должен залипать навсегда.
        release.clear()
        queue_manager.enqueue_waybill(LEAD_ID, source="webhook")
        assert await _wait_until(lambda: len(calls) == 2), \
            "после завершения предыдущей обработки новый вебхук обязан пройти"
        release.set()
        await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        queue_manager.is_circuit_open = was_circuit
        queue_manager.set_breaker_category = was_breaker
        waybill_service.create_waybill_for_lead = was_create
        queue_manager._queues.clear()
        queue_manager._pending_waybills.clear()


def test_flag_dedupa_zhivet_vsyu_obrabotku_a_ne_tolko_ochered():
    asyncio.run(_scenario())
