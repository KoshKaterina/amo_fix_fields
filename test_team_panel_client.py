"""Юнит-тесты team_panel_client (без сети — httpx.AsyncClient подменён фейком).

Запуск: python test_team_panel_client.py
        или python -m pytest test_team_panel_client.py -q
"""
import asyncio
import datetime

import team_panel_client as tpc
import waybill_config


# Настоящий отправитель алертов: тесты его подменяют, setup_function возвращает.
_REAL_ALERT = tpc._alert


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
    ответ, либо бросает заранее заданное исключение (сеть легла).

    `_script` — последовательность ответов/исключений по одному на попытку:
    нужна для проверки повторов (05.10.2026). Пуст — работает прежний режим
    «один и тот же ответ на любой запрос»."""

    _next_response: _FakeResponse | None = None
    _next_exc: Exception | None = None
    _script: list = []
    calls: list[dict] = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, params=None, headers=None):
        _FakeAsyncClient.calls.append({"url": url, "params": params, "headers": headers})
        if _FakeAsyncClient._script:
            item = _FakeAsyncClient._script.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        if _FakeAsyncClient._next_exc is not None:
            raise _FakeAsyncClient._next_exc
        return _FakeAsyncClient._next_response


def setup_function(_=None):
    tpc._cache = {}
    tpc._last_fetch_monotonic = None
    tpc.httpx.AsyncClient = _FakeAsyncClient
    _FakeAsyncClient._next_response = _FakeResponse(200, {})
    _FakeAsyncClient._next_exc = None
    _FakeAsyncClient._script = []
    _FakeAsyncClient.calls = []
    waybill_config.TEAM_PANEL_BASE_URL = "https://team.example"
    waybill_config.TEAM_PANEL_INGEST_TOKEN = "secret-token"
    waybill_config.TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S = 300
    tpc.TEAM_PANEL_BASE_URL = "https://team.example"
    tpc.TEAM_PANEL_INGEST_TOKEN = "secret-token"
    tpc.TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S = 300
    # Повторы и счётчики (05.10.2026): боевые значения по умолчанию.
    tpc.TEAM_PANEL_RETRIES = 2
    tpc.TEAM_PANEL_RETRY_BACKOFF_S = 0.0  # тесты не спят
    tpc.TEAM_PANEL_FAIL_ALERT_AFTER = 2
    tpc._alert_active = False
    tpc._alert = _REAL_ALERT
    for key in tpc._stats:
        tpc._stats[key] = None if key in ("last_status", "last_error") else 0


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


# ════════════════ повторы: ТОЛЬКО быстрые транзиентные сбои ════════════════
#
# Форма повторов задана замером 05.10.2026 (14.7 суток, 4671 запрос): все
# реальные сбои опроса были 34×403 одним инцидентом и 2×499. Поэтому тесты
# проверяют не только «повтор работает», но и что он НЕ случается там, где
# навредил бы.

def test_retry_recovers_after_5xx():
    _FakeAsyncClient._script = [_FakeResponse(500), _FakeResponse(200, {"111": True})]
    ok = run(tpc.fetch_once({111}))
    assert ok is True
    assert len(_FakeAsyncClient.calls) == 2
    assert tpc.get_cached(111) is True
    assert tpc._stats["retry_attempts"] == 1
    assert tpc._stats["retry_recovered"] == 1, "это и есть измеримая польза повтора"
    assert tpc._stats["poll_ok"] == 1


def test_retry_on_connect_error():
    _FakeAsyncClient._script = [tpc.httpx.ConnectError("соединение не поднялось"),
                                _FakeResponse(200, {"111": False})]
    ok = run(tpc.fetch_once({111}))
    assert ok is True
    assert len(_FakeAsyncClient.calls) == 2
    assert tpc._stats["retry_recovered"] == 1


def test_no_retry_on_403_the_25_09_case():
    """25.09.2026: 34 опроса подряд получили 403 за 2.5 часа. Слепой повтор
    сделал бы 102 запроса к сервису, который и так отказывал, и не спас бы ни
    одного: 403 повтором не лечится."""
    _FakeAsyncClient._script = [_FakeResponse(403)]
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert len(_FakeAsyncClient.calls) == 1, "4xx повторять нельзя"
    assert tpc._stats["retry_attempts"] == 0
    assert tpc._stats["no_retry_permanent"] == 1
    assert tpc._stats["poll_fail"] == 1


def test_no_retry_on_timeout():
    """Таймаут = мы уже прождали весь бюджет. На горячем пути повтор удвоил бы
    ожидание лида ради сервиса, который только что молчал."""
    _FakeAsyncClient._script = [tpc.httpx.ReadTimeout("медленно")]
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert len(_FakeAsyncClient.calls) == 1
    assert tpc._stats["no_retry_timeout"] == 1
    assert tpc._stats["retry_attempts"] == 0


def test_retries_are_capped():
    _FakeAsyncClient._script = [_FakeResponse(503) for _ in range(10)]
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert len(_FakeAsyncClient.calls) == tpc.TEAM_PANEL_RETRIES + 1 == 3
    assert tpc._stats["retry_attempts"] == 2


def test_retries_zero_is_exactly_old_behaviour():
    """Откат без деплоя: TEAM_PANEL_RETRIES=0 возвращает прежний один запрос."""
    tpc.TEAM_PANEL_RETRIES = 0
    _FakeAsyncClient._script = [_FakeResponse(503), _FakeResponse(200, {"111": True})]
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert len(_FakeAsyncClient.calls) == 1


def test_all_attempts_failed_does_not_touch_cache():
    tpc._cache = {111: True}
    tpc._last_fetch_monotonic = __import__("time").monotonic()
    _FakeAsyncClient._script = [_FakeResponse(503) for _ in range(5)]
    assert run(tpc.fetch_once({111})) is False
    assert tpc._cache == {111: True}, "старые данные переживают серию сбоев"


def test_broken_json_is_not_retried():
    class _BadJson(_FakeResponse):
        def json(self):
            raise ValueError("не json")

    _FakeAsyncClient._script = [_BadJson(200), _FakeResponse(200, {"111": True})]
    ok = run(tpc.fetch_once({111}))
    assert ok is False
    assert len(_FakeAsyncClient.calls) == 1, "битый ответ повтором не лечится"


# ════════════════ горячий путь: fetch_for_datetime ════════════════

def _at():
    return datetime.datetime(2026, 10, 6, 10, 0)


def test_hot_path_retries_and_returns_data():
    _FakeAsyncClient._script = [_FakeResponse(502), _FakeResponse(200, {"111": True, "222": False})]
    out = run(tpc.fetch_for_datetime({111, 222}, _at()))
    assert out == {111: True, 222: False}
    assert len(_FakeAsyncClient.calls) == 2
    assert tpc._stats["hot_ok"] == 1
    assert tpc._stats["retry_recovered"] == 1


def test_hot_path_403_returns_empty_without_retry():
    _FakeAsyncClient._script = [_FakeResponse(403)]
    out = run(tpc.fetch_for_datetime({111}, _at()))
    assert out == {}
    assert len(_FakeAsyncClient.calls) == 1
    assert tpc._stats["hot_fail"] == 1
    assert tpc._stats["poll_fail"] == 0, "горячий путь не путается со счётчиком опроса"


def test_hot_path_sends_at_parameter():
    run(tpc.fetch_for_datetime({111}, _at()))
    assert _FakeAsyncClient.calls[0]["params"]["at"] == _at().isoformat()


# ════════════════ счётчики опроса ════════════════

def test_consecutive_failures_count_up_and_reset():
    _FakeAsyncClient._script = [_FakeResponse(403), _FakeResponse(403)]
    run(tpc.fetch_once({111}))
    assert tpc._stats["consecutive_poll_failures"] == 1
    run(tpc.fetch_once({111}))
    assert tpc._stats["consecutive_poll_failures"] == 2
    _FakeAsyncClient._script = [_FakeResponse(200, {"111": True})]
    run(tpc.fetch_once({111}))
    assert tpc._stats["consecutive_poll_failures"] == 0
    assert tpc._stats["poll_fail"] == 2 and tpc._stats["poll_ok"] == 1


# ════════════════ алерт: раз на инцидент ════════════════

def _capture_alerts():
    sent = []

    async def fake_alert(text, event=None, values=None):
        sent.append({"text": text, "event": event, "values": values})

    tpc._alert = fake_alert
    return sent


def test_alert_silent_before_threshold():
    sent = _capture_alerts()
    tpc._stats["consecutive_poll_failures"] = 1
    run(tpc._alert_down())
    assert sent == [], "один сбой — кэш ещё жив, график ещё реальный"
    assert tpc._alert_active is False


def test_alert_fires_at_threshold_exactly_once():
    """Порог равен моменту протухания кэша: со второго сбоя распределение
    переходит на плейсхолдер — вот об этом и сообщаем."""
    sent = _capture_alerts()
    tpc._stats["consecutive_poll_failures"] = 2
    tpc._stats["last_status"] = 403
    run(tpc._alert_down())
    assert len(sent) == 1
    assert "403" in sent[0]["text"] and "плейсхолдеру" in sent[0]["text"]
    assert sent[0]["event"] == "team_panel_schedule_down"
    # Инцидент 25.09 длился 34 опроса — но сообщение должно остаться одно
    for n in range(3, 35):
        tpc._stats["consecutive_poll_failures"] = n
        run(tpc._alert_down())
    assert len(sent) == 1, "34 сбоя подряд — одно сообщение, а не 34"


def test_alert_recovery_resets_flag_and_notifies():
    sent = _capture_alerts()
    tpc._stats["consecutive_poll_failures"] = 2
    run(tpc._alert_down())
    run(tpc._alert_up())
    assert tpc._alert_active is False
    assert len(sent) == 2 and sent[1]["event"] == "team_panel_schedule_up"
    # Следующий инцидент должен прозвучать снова
    tpc._stats["consecutive_poll_failures"] = 2
    run(tpc._alert_down())
    assert len(sent) == 3


def test_alert_says_network_reason_when_no_http_status():
    sent = _capture_alerts()
    tpc._stats["consecutive_poll_failures"] = 2
    tpc._stats["last_status"] = None
    tpc._stats["last_error"] = "ConnectError"
    run(tpc._alert_down())
    assert "ConnectError" in sent[0]["text"]


# ════════════════ срез для /health ════════════════

def test_stats_reports_cache_freshness():
    _FakeAsyncClient._script = [_FakeResponse(200, {"111": True})]
    run(tpc.fetch_once({111}))
    st = tpc.stats()
    assert st["cache_fresh"] is True
    assert st["cache_size"] == 1
    assert st["cache_age_s"] is not None and st["cache_age_s"] < 5
    assert st["poll_ok"] == 1 and st["retries"] == 2


def test_stats_cache_age_none_when_never_fetched():
    st = tpc.stats()
    assert st["cache_age_s"] is None
    assert st["cache_fresh"] is False
    assert st["alert_active"] is False


def test_stats_has_every_counter_the_collector_reads():
    """Сборщик метрик на сервере читает эти ключи по именам — молчаливое
    переименование сломало бы замер, а не код."""
    st = tpc.stats()
    for key in ("poll_ok", "poll_fail", "hot_ok", "hot_fail", "retry_attempts",
                "retry_recovered", "no_retry_permanent", "no_retry_timeout",
                "consecutive_poll_failures", "cache_fresh", "cache_age_s", "enabled"):
        assert key in st, key


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
