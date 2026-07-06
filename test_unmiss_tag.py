"""Юнит-тест гейта unmiss_tag (без сети/прода).

Проверяет разбор тегов из вебхука и условие срабатывания: снимаем только когда
в тегах сделки одновременно «Успешный звонок» И «пропущенный». _apply не вызываем —
подменяем asyncio.create_task на заглушку, чтобы не трогать amo.
"""

import unmiss_tag

_scheduled: list[bool] = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем корутину _apply
    _scheduled.append(True)
    return _FakeTask()


unmiss_tag.asyncio.create_task = _fake_create_task


def _fires(tags_wh, lead_id=100) -> bool:
    _scheduled.clear()
    unmiss_tag.maybe_remove_bg(tags_wh, lead_id)
    return len(_scheduled) == 1


# --- разбор тегов из вебхука ---
assert unmiss_tag._tag_names(
    {"0": {"id": 1, "name": "Успешный звонок"}, "1": {"id": 2, "name": "пропущенный"}}
) == {"успешный звонок", "пропущенный"}
assert unmiss_tag._tag_names([{"name": "пропущенный"}]) == {"пропущенный"}
assert unmiss_tag._tag_names(None) == set()
assert unmiss_tag._tag_names("") == set()
assert unmiss_tag._tag_names({"0": {"id": 5}}) == set()  # тег без имени

# --- гейт: оба тега → срабатывает ---
assert _fires({"0": {"name": "Успешный звонок"}, "1": {"name": "пропущенный"}}) is True
# регистронезависимо
assert _fires({"0": {"name": "успешный ЗВОНОК"}, "1": {"name": "  Пропущенный "}}) is True
# формат list тоже
assert _fires([{"name": "Успешный звонок"}, {"name": "пропущенный"}]) is True

# --- гейт: НЕ срабатывает ---
assert _fires({"0": {"name": "Успешный звонок"}}) is False          # нет «пропущенный»
assert _fires({"0": {"name": "пропущенный"}}) is False              # нет «Успешный звонок»
assert _fires({"0": {"name": "Срочно"}, "1": {"name": "Горячий"}}) is False
assert _fires(None) is False
assert _fires("") is False
assert _fires({"0": {"name": "Успешный звонок"}, "1": {"name": "пропущенный"}}, lead_id=None) is False

print("unmiss_tag: все тесты гейта прошли")
