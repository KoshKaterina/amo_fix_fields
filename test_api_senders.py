"""Параллельные отправители под общим token bucket (api.py), без сети.

⚠️ Эта правка РАЗВОРАЧИВАЕТ решение Кати от 08.07.2026 («параллельная отправка
была хуже»). Лечим названную там же цену последовательной схемы: один подвисший
запрос держал весь контур. Исторический провал почти наверняка был в
нескоординированном превышении темпа, поэтому главное здесь — общий
ограничитель, а не просто несколько отправителей.

Лимитов у amo ДВА (замеры 03–04.08.2026): 7 rps на интеграцию и ~10 rps на
IP-адрес. Второй и есть причина, по которой темп держит ОДИН счётчик на процесс.

Тесты намеренно с запасом по времени: проверяем порядок величин, а не точные
миллисекунды, иначе красное на загруженной машине.

Запуск: python test_api_senders.py  или  python -m pytest test_api_senders.py -q
"""
import asyncio
import time

import api
import pytest


class _Resp:
    status_code = 200


class _FakeClient:
    """Подменяет httpx.AsyncClient: отвечает через `delay`, считает нахлёст."""

    def __init__(self, delay=0.0, delays=None):
        self.delay = delay
        self.delays = dict(delays or {})
        self.sent = []
        self.concurrent = 0
        self.max_concurrent = 0
        self.send_times = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def _do(self, url):
        self.sent.append(url)
        self.send_times.append(time.monotonic())
        self.concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self.concurrent)
        try:
            await asyncio.sleep(self.delays.get(url, self.delay))
            return _Resp()
        finally:
            self.concurrent -= 1

    async def get(self, url, headers=None):
        return await self._do(url)

    async def patch(self, url, headers=None, json=None):
        return await self._do(url)

    async def request(self, method, url, headers=None, json=None):
        return await self._do(url)


def _install(client):
    api.httpx.AsyncClient = lambda **kw: client


_real_async_client = api.httpx.AsyncClient


def _restore():
    api.httpx.AsyncClient = _real_async_client


# ⚠️ ВОЗВРАТ ОБЯЗАТЕЛЕН, И ЭТОГО НЕ ХВАТАЛО. `api.httpx` - это НЕ локальная копия, а сам
# модуль `httpx`, общий на процесс. Подмена `AsyncClient` фейком в тесте, который упал до
# своего `_restore()`, достаётся всему прогону: соседний файл получает фейкового клиента с
# мёртвым циклом событий и ВИСНЕТ. Перебор парами 07.10.2026: этот файл и
# `test_api_priority` вешали `test_autopilot`.
#
# Та же причина у конвейера: тест поднимает его `init_api_pipeline()` и гасит в конце, но
# упавший посередине оставляет поднятым, и следующий зовущий `submit_request` ждёт воркера,
# которого уже нет.
# ⚠️ Имена полей взяты ИЗ `_cfg` ниже, а не по памяти. Первый раз я их угадала
# (`API_RPS` вместо `API_RATE_RPS` и так далее) - несуществующие имена молча пропускались,
# фикстура возвращала почти ничего, и вис остался. Проверка от этого: тест ниже сверяет,
# что каждое имя в модуле действительно есть.
_API_FIELDS = (
    "API_SENDERS",
    "API_RATE_RPS",
    "API_RATE_BURST",
    "API_RATE_PENALTY_S",
    "MIN_REQUEST_INTERVAL_SECONDS",
)
_MISSING = object()


def test_imena_poley_api_sushchestvuyut():
    """Сторож против опечатки в списке выше: нет поля - нет и возврата, молча."""
    missing = [name for name in _API_FIELDS if not hasattr(api, name)]
    assert not missing, f"в модуле api нет полей {missing} - список возврата врёт"


# ⚠️ ПОЧЕМУ СБРОС, А НЕ `shutdown_api_pipeline()`. Каждый тест здесь зовёт `asyncio.run`,
# то есть поднимает СВОЙ цикл событий и закрывает его на выходе. Конвейер при этом остаётся
# в глобалах `api` - вместе с `_wakeup` (Event) и `_rate_lock` (Lock), привязанными к уже
# МЁРТВОМУ циклу. Следующий, кто позовёт `submit_request`, будет ждать их вечно: ровно так
# этот файл вешал `test_autopilot` (перебор по тестам 07.10.2026 - травил КАЖДЫЙ тест файла,
# кроме того, что конвейера не касается).
#
# Гасить конвейер нечем: его цикл закрыт, `await` внутри `shutdown` уже не выполнится.
# Поэтому возвращаем поля к тем значениям, с которыми модуль загружается (строки 321-336
# в api.py) - и следующий `init_api_pipeline()` поднимает всё заново на живом цикле.
_PRISTINE_PIPELINE = {
    "_lanes": dict,
    "_wakeup": lambda: None,
    "_api_worker_tasks": list,
    "_last_sent_at": lambda: 0.0,
    "_served": dict,
    "_promoted_by_starvation": lambda: 0,
    "_congested": lambda: False,
    "_backpressure_skips": dict,
    "_tokens": lambda: 0.0,
    "_bucket_refilled_at": lambda: 0.0,
    "_rate_lock": lambda: None,
    "_rate_penalty_until": lambda: 0.0,
    "_in_flight": lambda: 0,
    "_max_in_flight": lambda: 0,
    "_rate_waits": lambda: 0,
    "_penalty_hits": lambda: 0,
}


def test_imena_poley_konveyera_sushchestvuyut():
    """Сторож против опечатки: нет поля - нет и сброса, молча."""
    missing = [name for name in _PRISTINE_PIPELINE if not hasattr(api, name)]
    assert not missing, f"в модуле api нет полей {missing} - список сброса врёт"


@pytest.fixture(autouse=True)
def _restore_api_state():
    saved = {name: getattr(api, name, _MISSING) for name in _API_FIELDS}
    try:
        yield
    finally:
        _restore()
        for name, value in saved.items():
            if value is not _MISSING:
                setattr(api, name, value)
        for name, make in _PRISTINE_PIPELINE.items():
            setattr(api, name, make())


def run(coro):
    return asyncio.run(coro)


def _cfg(senders=3, rps=6.0, burst=2.0, penalty=1.0):
    api.API_SENDERS = senders
    api.API_RATE_RPS = rps
    api.API_RATE_BURST = burst
    api.API_RATE_PENALTY_S = penalty
    api.MIN_REQUEST_INTERVAL_SECONDS = 0.0


# ── ядро правки: подвисший запрос больше не держит остальные ────────────────

def test_hung_request_does_not_block_the_rest():
    """⚠️ Это и есть смысл всей правки. В заметке цена последовательной схемы
    названа прямо: «если один запрос завис (таймаут до 20 секунд), вся
    остальная работа с amoCRM тоже ждёт своей очереди»."""
    async def scenario():
        _cfg(senders=3, rps=50.0, burst=3.0)
        client = _FakeClient(delay=0.0, delays={"/HUNG": 1.0})
        _install(client)
        try:
            api.init_api_pipeline()
            hung = asyncio.create_task(api.submit_request("GET", "/HUNG", {}))
            await asyncio.sleep(0.02)                      # пусть зависший уйдёт первым
            t0 = time.monotonic()
            await asyncio.gather(*[
                api.submit_request("GET", f"/fast{i}", {}) for i in range(5)
            ])
            fast_done = time.monotonic() - t0
            assert fast_done < 0.6, f"быстрые ждали зависший: {fast_done:.2f}s"
            assert not hung.done(), "зависший должен быть всё ещё в полёте"
            await hung
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_sequential_mode_does_block_as_before():
    """Контроль к тесту выше: с AMO_API_SENDERS=1 поведение прежнее — быстрые
    ждут зависший. Так видно, что тест выше проверяет именно параллельность."""
    async def scenario():
        _cfg(senders=1, rps=50.0, burst=3.0)
        client = _FakeClient(delay=0.0, delays={"/HUNG": 0.5})
        _install(client)
        try:
            api.init_api_pipeline()
            hung = asyncio.create_task(api.submit_request("GET", "/HUNG", {}))
            await asyncio.sleep(0.02)
            t0 = time.monotonic()
            await api.submit_request("GET", "/fast", {})
            waited = time.monotonic() - t0
            assert waited > 0.3, f"в последовательном режиме должен был ждать, а ждал {waited:.2f}s"
            await hung
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


# ── параллельность и откат ──────────────────────────────────────────────────

def test_senders_really_overlap():
    async def scenario():
        _cfg(senders=3, rps=50.0, burst=3.0)
        client = _FakeClient(delay=0.15)
        _install(client)
        try:
            api.init_api_pipeline()
            await asyncio.gather(*[
                api.submit_request("GET", f"/x{i}", {}) for i in range(6)
            ])
            assert client.max_concurrent > 1, "отправка не параллелится"
            assert client.max_concurrent <= 3, client.max_concurrent
            st = api.api_queue_stats()["senders"]
            assert st["count"] == 3
            assert st["max_in_flight"] > 1
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_one_sender_never_overlaps():
    """Откат в одну переменную: AMO_API_SENDERS=1 возвращает строгую
    последовательность."""
    async def scenario():
        _cfg(senders=1, rps=50.0, burst=3.0)
        client = _FakeClient(delay=0.05)
        _install(client)
        try:
            api.init_api_pipeline()
            await asyncio.gather(*[
                api.submit_request("GET", f"/x{i}", {}) for i in range(5)
            ])
            assert client.max_concurrent == 1, client.max_concurrent
            assert api.api_queue_stats()["senders"]["max_in_flight"] == 1
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


# ── общий темп: главное, из-за чего прошлый раз было хуже ───────────────────

def test_shared_bucket_holds_the_rate():
    """Два лимита amo (7 rps на интеграцию и ~10 rps на IP) держит ОДИН
    счётчик на процесс, иначе N отправителей просто превысят темп."""
    async def scenario():
        _cfg(senders=4, rps=10.0, burst=1.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            t0 = time.monotonic()
            n = 11
            await asyncio.gather(*[
                api.submit_request("GET", f"/r{i}", {}) for i in range(n)
            ])
            elapsed = time.monotonic() - t0
            # burst=1 → первый сразу, остальные по 1/10 с
            lower = (n - 1) / 10.0 * 0.7
            assert elapsed >= lower, f"темп не держится: {n} запросов за {elapsed:.2f}s"
            assert len(client.sent) == n
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_burst_lets_first_requests_through_at_once():
    async def scenario():
        _cfg(senders=3, rps=1.0, burst=3.0)   # медленный долив, ведро на 3
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            t0 = time.monotonic()
            await asyncio.gather(*[
                api.submit_request("GET", f"/b{i}", {}) for i in range(3)
            ])
            elapsed = time.monotonic() - t0
            assert elapsed < 0.5, f"ведро не дало всплеск: {elapsed:.2f}s"
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_rate_limiter_is_shared_not_per_sender():
    """Если бы ограничитель был у каждого отправителя свой, четыре отправителя
    выпустили бы вчетверо больше — ровно так ломались прошлый раз."""
    async def scenario():
        _cfg(senders=4, rps=5.0, burst=1.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            t0 = time.monotonic()
            await asyncio.gather(*[
                api.submit_request("GET", f"/s{i}", {}) for i in range(6)
            ])
            elapsed = time.monotonic() - t0
            # честный общий темп: 5 запросов после burst → ~1.0 с
            assert elapsed >= 0.6, f"похоже, ограничитель не общий: {elapsed:.2f}s"
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


# ── реакция на 429 ──────────────────────────────────────────────────────────

def test_429_stalls_all_senders():
    """Брейкер по категориям про «эту категорию больше не трогаем», а 429 от
    сита по IP ни к какой категории не относится — на него нужна короткая
    пауза всего пайплайна."""
    async def scenario():
        _cfg(senders=3, rps=100.0, burst=3.0, penalty=0.4)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            api._record_429("lead")                 # как будто прилетел 429
            assert api.api_queue_stats()["senders"]["penalty_active"] is True
            t0 = time.monotonic()
            await api.submit_request("GET", "/after429", {})
            waited = time.monotonic() - t0
            assert waited >= 0.3, f"штраф не сработал, ждали {waited:.2f}s"
            assert api.api_queue_stats()["senders"]["penalty_hits"] >= 1
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_429_penalty_can_be_disabled():
    async def scenario():
        _cfg(senders=2, rps=100.0, burst=2.0, penalty=0.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            api._record_429("lead")
            assert api.api_queue_stats()["senders"]["penalty_active"] is False
            t0 = time.monotonic()
            await api.submit_request("GET", "/x", {})
            assert time.monotonic() - t0 < 0.3
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


# ── приоритеты не сломались ────────────────────────────────────────────────

def test_priorities_still_hold_with_parallel_senders():
    """Счёт обязан уходить первым и при параллельной отправке."""
    async def scenario():
        _cfg(senders=2, rps=4.0, burst=1.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            api.set_breaker_category("sync")
            bg = [asyncio.create_task(api.submit_request("GET", f"/bg{i}", {}))
                  for i in range(6)]
            await asyncio.sleep(0)
            api.set_breaker_category("invoice")
            inv = asyncio.create_task(api.submit_request("GET", "/SCHET", {}))
            await asyncio.gather(*bg, inv)
            pos = client.sent.index("/SCHET")
            assert pos <= 2, f"счёт ушёл {pos}-м: {client.sent}"
            await api.shutdown_api_pipeline()
        finally:
            _restore()
    run(scenario())


def test_shutdown_cancels_every_sender():
    async def scenario():
        _cfg(senders=4, rps=50.0, burst=4.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            assert len(api._api_worker_tasks) == 4
            await api.shutdown_api_pipeline()
            assert api._api_worker_tasks == []
        finally:
            _restore()
    run(scenario())


def test_stats_expose_sender_block():
    async def scenario():
        _cfg(senders=3, rps=6.0, burst=2.0)
        client = _FakeClient(delay=0.0)
        _install(client)
        try:
            api.init_api_pipeline()
            await api.submit_request("GET", "/x", {})
            st = api.api_queue_stats()["senders"]
            for k in ("count", "rate_rps", "burst", "in_flight", "max_in_flight",
                      "rate_waits", "penalty_hits", "penalty_active"):
                assert k in st, k
            assert st["rate_rps"] == 6.0
            await api.shutdown_api_pipeline()
        finally:
            _restore()
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
