"""Юнит-тесты lead_distribution_log_client (без сети — httpx.AsyncClient подменён фейком).

Запуск: python test_lead_distribution_log_client.py
        или python -m pytest test_lead_distribution_log_client.py -q
"""
import asyncio

import lead_distribution_log_client as ldlc
import waybill_config


def run(coro):
    return asyncio.run(coro)


class _FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code


class _FakeAsyncClient:
    _next_response: _FakeResponse | None = None
    _next_exc: Exception | None = None
    calls: list[dict] = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, json=None, headers=None):
        _FakeAsyncClient.calls.append({"url": url, "json": json, "headers": headers})
        if _FakeAsyncClient._next_exc is not None:
            raise _FakeAsyncClient._next_exc
        return _FakeAsyncClient._next_response


def setup_function(_=None):
    ldlc.httpx.AsyncClient = _FakeAsyncClient
    _FakeAsyncClient._next_response = _FakeResponse(200)
    _FakeAsyncClient._next_exc = None
    _FakeAsyncClient.calls = []
    waybill_config.TEAM_PANEL_BASE_URL = "https://team.example"
    waybill_config.TEAM_PANEL_INGEST_TOKEN = "secret-token"
    ldlc.TEAM_PANEL_BASE_URL = "https://team.example"
    ldlc.TEAM_PANEL_INGEST_TOKEN = "secret-token"


_PAYLOAD = {"lead_id": 100, "contact_id": 500, "assigned_user_id": 1, "rule": "load"}


def test_send_posts_payload_and_token_header():
    run(ldlc.send(_PAYLOAD))
    call = _FakeAsyncClient.calls[0]
    assert call["url"].endswith("/api/ingest/lead-distribution/log")
    assert call["json"] == _PAYLOAD
    assert call["headers"]["X-Ingest-Token"] == "secret-token"


def test_send_bad_status_does_not_raise():
    _FakeAsyncClient._next_response = _FakeResponse(500)
    run(ldlc.send(_PAYLOAD))  # не должно бросить


def test_send_network_exception_does_not_raise():
    _FakeAsyncClient._next_exc = ConnectionError("boom")
    run(ldlc.send(_PAYLOAD))  # не должно бросить


def test_send_without_config_is_noop_no_http_call():
    ldlc.TEAM_PANEL_BASE_URL = ""
    run(ldlc.send(_PAYLOAD))
    assert _FakeAsyncClient.calls == []


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
