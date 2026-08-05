"""Юнит-тесты team_panel_client (без сети — httpx.AsyncClient подменён фейком).

Запуск: python test_team_panel_client.py
        или python -m pytest test_team_panel_client.py -q
"""
import asyncio

import team_panel_client as tpc
import waybill_config


def run(coro):
    return asyncio.run(coro)


class _FakeResponse:
    def __init__(self, status_code=200, data=None):
        self.status_code = status_code
        self._data = data or {}

    def json(self):
        return self._data


class _FakeAsyncClient:
    """Подменяет httpx.AsyncClient целиком: либо отдаёт заранее заданный
    ответ, либо бросает заранее заданное исключение (сеть легла)."""

    _next_response: _FakeResponse | None = None
    _next_exc: Exception | None = None
    calls: list[dict] = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, params=None, headers=None):
        _FakeAsyncClient.calls.append({"url": url, "params": params, "headers": headers})
        if _FakeAsyncClient._next_exc is not None:
            raise _FakeAsyncClient._next_exc
        return _FakeAsyncClient._next_response


def setup_function(_=None):
    tpc._cache = {}
    tpc._last_fetch_monotonic = None
    tpc.httpx.AsyncClient = _FakeAsyncClient
    _FakeAsyncClient._next_response = _FakeResponse(200, {})
    _FakeAsyncClient._next_exc = None
    _FakeAsyncClient.calls = []
    waybill_config.TEAM_PANEL_BASE_URL = "https://team.example"
    waybill_config.TEAM_PANEL_INGEST_TOKEN = "secret-token"
    waybill_config.TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S = 300
    tpc.TEAM_PANEL_BASE_URL = "https://team.example"
    tpc.TEAM_PANEL_INGEST_TOKEN = "secret-token"
    tpc.TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S = 300


# ════════════════ get_cached: пусто/протухло ════════════════

def test_get_cached_none_when_never_fetched():
    assert tpc.get_cached(111) is None


def test_get_cached_none_when_stale():
    tpc._cache = {111: True}
    # "последний опрос" был давно — намного дольше, чем 2x интервал
    tpc._last_fetch_monotonic = __import__("time").monotonic() - 10_000
    assert tpc.get_cached(111) is None


# ════════════════ fetch_once: успех ════════════════

def test_fetch_once_populates_cache():
    _FakeAsyncClient._next_response = _FakeResponse(200, {"111": True, "222": False})
    ok = run(tpc.fetch_once({111, 222}))
    assert ok is True
    assert tpc.get_cached(111) is True
    assert tpc.get_cached(222) is False
    assert tpc.is_cache_fresh() is True


def test_fetch_once_sends_csv_and_token_header():
    run(tpc.fetch_once({222, 111, 333}))
    call = _FakeAsyncClient.calls[0]
    assert call["params"]["amo_user_ids"] == "111,222,333"  # отсортировано
    assert call["headers"]["X-Ingest-Token"] == "secret-token"
    assert call["url"].endswith("/api/ingest/schedule/on-shift")


def test_fetch_once_empty_ids_is_trivial_success_no_http_call():
    ok = run(tpc.fetch_once(set()))
    assert ok is True
    assert _FakeAsyncClient.calls == []


# ════════════════ fetch_once: сбои — кэш не трогаем ════════════════

def test_fetch_once_bad_status_does_not_touch_cache():
    tpc._cache = {111: True}
    tpc._last_fetch_monotonic = __import__("time").monotonic()
    _FakeAsyncClient._next_response = _FakeResponse(500, {})
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert tpc._cache == {111: True}, "старые данные не должны стираться из-за одного сбоя"


def test_fetch_once_network_exception_does_not_touch_cache():
    tpc._cache = {111: True}
    _FakeAsyncClient._next_exc = ConnectionError("boom")
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert tpc._cache == {111: True}


def test_fetch_once_without_config_fails_fast_no_http_call():
    tpc.TEAM_PANEL_BASE_URL = ""
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert _FakeAsyncClient.calls == []


# ════════════════ интеграция с lead_distribution._is_on_shift ════════════════

def test_lead_distribution_uses_team_panel_cache_when_fresh():
    import lead_distribution as ld
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (0, 0)  # плейсхолдер сказал бы «никто не на месте»
    tpc._cache = {555: True}
    tpc._last_fetch_monotonic = __import__("time").monotonic()
    assert ld._is_on_shift(555) is True, "team-panel должен перебивать плейсхолдер"


def test_lead_distribution_falls_back_to_placeholder_when_stale():
    import lead_distribution as ld
    ld.LEAD_DISTRIBUTION_DEFAULT_WINDOW = (0, 24)  # плейсхолдер: все на месте всегда
    tpc._cache = {}
    tpc._last_fetch_monotonic = None
    assert ld._is_on_shift(555) is True, "нет данных team-panel — работает плейсхолдер"


if __name__ == "__main__":
    import traceback

    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    ok = 0
    for fn in fns:
        setup_function()
        try:
            fn()
            print(f"OK {fn.__name__}")
            ok += 1
        except Exception:
            print(f"FAIL {fn.__name__}")
            traceback.print_exc()
    print(f"\n{ok}/{len(fns)} прошли")
