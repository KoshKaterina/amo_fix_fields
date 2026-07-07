"""Юнит-тест планировщика unmiss_tag (без сети/прода).

Реконсиляция (сверка тегов) живёт в _apply и требует amo — её проверяем E2E на
тест-контакте. Здесь проверяем только гейт планировщика: задача заводится для
валидного lead_id и НЕ заводится для None. asyncio.create_task подменён заглушкой.
"""

import unmiss_tag

_scheduled: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _apply (в нём сеть)
    _scheduled.append(True)
    return _FakeTask()


unmiss_tag.asyncio.create_task = _fake_create_task


def _fires(lead_id) -> bool:
    _scheduled.clear()
    unmiss_tag.maybe_remove_bg(lead_id)
    return len(_scheduled) == 1


assert _fires(36503585) is True
assert _fires(0) is True          # 0 — валидный id (не None)
assert _fires(None) is False      # нет сделки — не планируем

print("unmiss_tag: тесты планировщика прошли")
