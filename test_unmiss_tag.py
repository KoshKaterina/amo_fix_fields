"""Юнит-тест планировщика unmiss_tag (без сети/прода).

Реконсиляция (сверка тегов) живёт в _apply и требует amo — её проверяем E2E на
тест-контакте. Здесь проверяем только гейт планировщика: задача заводится для
валидного lead_id и НЕ заводится для None. asyncio.create_task подменён заглушкой.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде файл был СКРИПТОМ: проверки стояли голыми
`assert` на уровне модуля, то есть выполнялись на ИМПОРТЕ. Pytest собирает любой `test_*.py`,
а собирая - импортирует, поэтому сценарий отрабатывал на этапе СБОРА: провал выглядел
ошибкой сбора и ронял сбор всего репозитория, а в сводке файл давал ноль тестов.
"""

import pytest
import unmiss_tag

_scheduled: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _apply (в нём сеть)
    _scheduled.append(True)
    return _FakeTask()


@pytest.fixture(autouse=True)
def _no_real_background_tasks():
    """Не заводить настоящие фоновые задачи - и ВЕРНУТЬ create_task на место.

    ⚠️ `unmiss_tag.asyncio` - это НЕ локальная копия, а сам модуль `asyncio`, общий на весь
    процесс. Пока файл был скриптом, подмена на уровне модуля никому не мешала; под pytest
    все файлы живут в одном процессе, и подменённый `create_task` достался бы соседям.
    """
    was = unmiss_tag.asyncio.create_task
    unmiss_tag.asyncio.create_task = _fake_create_task
    try:
        yield
    finally:
        unmiss_tag.asyncio.create_task = was


def _fires(lead_id) -> bool:
    _scheduled.clear()
    unmiss_tag.maybe_remove_bg(lead_id)
    return len(_scheduled) == 1


def test_planiruet_dlya_validnogo_lead_id():
    assert _fires(36503585) is True


def test_nol_eto_validnyy_id_a_ne_pustoe_znachenie():
    """0 — валидный id (не None), поэтому задача заводиться должна."""
    assert _fires(0) is True


def test_bez_sdelki_ne_planiruem():
    assert _fires(None) is False
