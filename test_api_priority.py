"""Юнит-тест приоритетов API-пайплайна (`api.py`), без сети.

Закрывает правку 05.10.2026: пайплайн был плоской FIFO, и это обнуляло
приоритизацию уровнем выше — клиентскую задачу снимали с дорожки первой, а её
запрос к amo встаёт в хвост за тысячами фоновых. В инциденте так и вышло:
в пайплайне 4810 запросов, платёжные ссылки ждали больше часа.

Проверяем: вывод приоритета из категории брейкера, порядок выдачи, FIFO внутри
класса, защиту от голодания, контекстный менеджер, срез статистики и то, что
submit_request кладёт запрос в правильную дорожку.

⚠️ Пороги подменяем ПОЛЯМИ модуля, а не переменными окружения — грабля 3 из
knowledge/amo-fix-fields-testy-grabli.md: waybill_config и соседи читают env
один раз на импорте, и setdefault в паре с другим тест-файлом уже не действует.

Запуск: python test_api_priority.py  или  python -m pytest test_api_priority.py -q
"""
import asyncio
import time
from collections import deque

import api


def run(coro):
    return asyncio.run(coro)


def _fresh_lanes():
    """Чистый пайплайн без живого воркера: воркер в тестах не нужен, futures
    разрешаем сами."""
    api._lanes = {
        api.API_PRIORITY_CLIENT: deque(),
        api.API_PRIORITY_NORMAL: deque(),
        api.API_PRIORITY_BACKGROUND: deque(),
    }
    api._served = {}
    api._promoted_by_starvation = 0
    api._wakeup = None
    api.set_api_priority(None)
    api.set_breaker_category("default")


def _put(prio, tag, waited=0.0):
    """Положить запрос-пустышку в дорожку, как будто он ждёт `waited` секунд."""
    req = api.ApiRequest(
        method="GET", url=f"/{tag}", req_headers={}, json_body=None,
        future=None, priority=prio, enqueued_at=time.monotonic() - waited,
    )
    api._lanes[prio].append(req)
    return req


# ── вывод приоритета из категории ───────────────────────────────────────────

def test_priority_derived_from_breaker_category():
    """Второй механизм не заводим: категорию уже ставит воркер дорожки по kind,
    и contextvars наследуются в create_task."""
    _fresh_lanes()
    api.set_breaker_category("invoice")
    assert api.current_api_priority() == api.API_PRIORITY_CLIENT
    api.set_breaker_category("lead_distribution")
    assert api.current_api_priority() == api.API_PRIORITY_CLIENT
    api.set_breaker_category("sync")
    assert api.current_api_priority() == api.API_PRIORITY_BACKGROUND
    api.set_breaker_category("cdek")
    assert api.current_api_priority() == api.API_PRIORITY_NORMAL


def test_unmarked_paths_are_normal_not_background():
    """Непомеченное (старт, админские ручки панели, сторожа из вебхука) должно
    остаться НОРМАЛЬНЫМ. Если бы оно падало в фон, панель стала бы отваливаться
    по таймауту ещё охотнее, чем до правки."""
    _fresh_lanes()
    api.set_breaker_category("default")
    assert api.current_api_priority() == api.API_PRIORITY_NORMAL
    api.set_breaker_category("какая-то-новая-категория")
    assert api.current_api_priority() == api.API_PRIORITY_NORMAL


def test_explicit_priority_wins_over_category():
    _fresh_lanes()
    api.set_breaker_category("sync")          # по категории был бы фон
    api.set_api_priority(api.API_PRIORITY_CLIENT)
    assert api.current_api_priority() == api.API_PRIORITY_CLIENT
    api.set_api_priority(None)
    assert api.current_api_priority() == api.API_PRIORITY_BACKGROUND


def test_context_manager_restores_previous_priority():
    """Сверка счетов работает так: обход фоном, а найденную работу — клиентским.
    Значит менеджер обязан вернуть прежнее значение, иначе остаток обхода
    поедет клиентским приоритетом."""
    _fresh_lanes()
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    with api.api_priority(api.API_PRIORITY_CLIENT):
        assert api.current_api_priority() == api.API_PRIORITY_CLIENT
    assert api.current_api_priority() == api.API_PRIORITY_BACKGROUND


def test_context_manager_restores_even_on_exception():
    _fresh_lanes()
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    try:
        with api.api_priority(api.API_PRIORITY_CLIENT):
            raise RuntimeError("бум")
    except RuntimeError:
        pass
    assert api.current_api_priority() == api.API_PRIORITY_BACKGROUND


# ── порядок выдачи ──────────────────────────────────────────────────────────

def test_client_goes_before_background():
    """Ядро правки: фон, поставленный РАНЬШЕ, не держит клиента."""
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 30
    _put(api.API_PRIORITY_BACKGROUND, "фон-1")
    _put(api.API_PRIORITY_BACKGROUND, "фон-2")
    _put(api.API_PRIORITY_NORMAL, "норма")
    _put(api.API_PRIORITY_CLIENT, "клиент")
    order = []
    while api.api_queue_size():
        order.append(api._take_next().url)
    assert order == ["/клиент", "/норма", "/фон-1", "/фон-2"], order


def test_fifo_inside_one_class():
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 30
    for i in range(4):
        _put(api.API_PRIORITY_CLIENT, f"к{i}")
    order = [api._take_next().url for _ in range(4)]
    assert order == ["/к0", "/к1", "/к2", "/к3"], order


def test_thousands_of_background_do_not_delay_one_client():
    """Сценарий самого инцидента в миниатюре."""
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 30
    for i in range(3000):
        _put(api.API_PRIORITY_BACKGROUND, f"ф{i}")
    _put(api.API_PRIORITY_CLIENT, "ссылка-на-оплату")
    assert api._take_next().url == "/ссылка-на-оплату"


# ── защита от голодания ─────────────────────────────────────────────────────

def test_starved_background_is_promoted():
    """Сверки — наша страховка, насовсем задвинуть их нельзя: переждавший порог
    уходит вперёд даже при живом клиентском потоке."""
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 5
    _put(api.API_PRIORITY_BACKGROUND, "фон-давно-ждёт", waited=10)
    _put(api.API_PRIORITY_CLIENT, "клиент-только-что", waited=0)
    assert api._take_next().url == "/фон-давно-ждёт"
    assert api._promoted_by_starvation == 1
    assert api._take_next().url == "/клиент-только-что"


def test_not_yet_starved_background_waits():
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 30
    _put(api.API_PRIORITY_BACKGROUND, "фон", waited=10)   # ещё не дотерпел
    _put(api.API_PRIORITY_CLIENT, "клиент", waited=0)
    assert api._take_next().url == "/клиент"
    assert api._promoted_by_starvation == 0


def test_oldest_of_several_starved_goes_first():
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 5
    _put(api.API_PRIORITY_NORMAL, "норма-ждёт-20", waited=20)
    _put(api.API_PRIORITY_BACKGROUND, "фон-ждёт-40", waited=40)
    _put(api.API_PRIORITY_CLIENT, "клиент", waited=0)
    assert api._take_next().url == "/фон-ждёт-40"


def test_promotion_not_counted_when_nothing_was_ahead():
    """Переждавший фон при пустых клиентских дорожках — это не обгон, счётчик
    обгонов не должен расти (иначе метрика врёт на холостом ходу)."""
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 5
    _put(api.API_PRIORITY_BACKGROUND, "фон", waited=10)
    assert api._take_next().url == "/фон"
    assert api._promoted_by_starvation == 0


def test_empty_pipeline_returns_none():
    _fresh_lanes()
    assert api._take_next() is None
    assert api.api_queue_size() == 0


# ── статистика ──────────────────────────────────────────────────────────────

def test_stats_show_depth_and_oldest_wait_per_class():
    _fresh_lanes()
    api.API_STARVATION_SECONDS = 30
    _put(api.API_PRIORITY_CLIENT, "к", waited=1)
    _put(api.API_PRIORITY_BACKGROUND, "ф1", waited=12)
    _put(api.API_PRIORITY_BACKGROUND, "ф2", waited=3)
    st = api.api_queue_stats()
    assert st["api_queue"] == 3
    assert st["api_depth"] == {"client": 1, "normal": 0, "background": 2}
    assert st["api_oldest_wait_s"]["background"] >= 11.5   # голова, не хвост
    assert st["api_oldest_wait_s"]["normal"] == 0.0


def test_api_queue_size_still_counts_everything():
    """Ключ api_queue читают монитор и TG-алерты — он обязан остаться суммарным."""
    _fresh_lanes()
    _put(api.API_PRIORITY_CLIENT, "к")
    _put(api.API_PRIORITY_NORMAL, "н")
    _put(api.API_PRIORITY_BACKGROUND, "ф")
    assert api.api_queue_size() == 3


# ── submit_request кладёт в правильную дорожку ──────────────────────────────

def test_submit_request_routes_by_context():
    async def scenario():
        _fresh_lanes()
        api._wakeup = asyncio.Event()
        api.set_breaker_category("invoice")          # клиентская категория
        task = asyncio.create_task(api.submit_request("GET", "/счёт", {}))
        await asyncio.sleep(0)                        # дать задаче поставить запрос
        assert len(api._lanes[api.API_PRIORITY_CLIENT]) == 1
        assert api._lanes[api.API_PRIORITY_CLIENT][0].url == "/счёт"
        assert api._wakeup.is_set(), "воркера надо будить"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    run(scenario())


def test_submit_request_background_context():
    async def scenario():
        _fresh_lanes()
        api._wakeup = asyncio.Event()
        api.set_breaker_category("sync")
        task = asyncio.create_task(api.submit_request("GET", "/метрика", {}))
        await asyncio.sleep(0)
        assert len(api._lanes[api.API_PRIORITY_BACKGROUND]) == 1
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    run(scenario())


def test_submit_request_without_init_raises():
    async def scenario():
        api._lanes = {}
        api._wakeup = None
        try:
            await api.submit_request("GET", "/x", {})
        except RuntimeError as e:
            assert "not initialized" in str(e)
        else:
            raise AssertionError("ждали RuntimeError")
    run(scenario())


def test_unknown_priority_falls_back_to_normal():
    """Если кто-то передал приоритет, которого нет среди дорожек, запрос не
    должен потеряться."""
    async def scenario():
        _fresh_lanes()
        api._wakeup = asyncio.Event()
        task = asyncio.create_task(api.submit_request("GET", "/чужой", {}, priority=777))
        await asyncio.sleep(0)
        assert len(api._lanes[api.API_PRIORITY_NORMAL]) == 1
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    run(scenario())


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
