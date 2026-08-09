"""Клиент МойСклад поднимается сам по себе и не зависит от чужих модулей.

Регрессия 05.08.2026: клиент поднимался внутри ms_status_sync.init(), а тот при
MS_STATUS_SYNC_ENABLED=0 выходил раньше строки ms_client.init() — и создание
Ozon-счетов падало («МойСклад не отдал заказ», сделка 36528399). Лечение: init
переехал в lifespan webhooks.py и стал идемпотентным.

Сам ms_status_sync удалён вместе с контуром Фулфилмента в тот же день, поэтому
две прежние проверки про его выключатели тут больше не живут. Осталась суть:
клиент поднимается сам, повторный подъём безопасен, без токена не падает.
"""
import pytest

import ms_client


class FakeAsyncClient:
    def __init__(self, *a, **kw):
        self.closed = False

    async def aclose(self):
        self.closed = True


@pytest.fixture
def mc(monkeypatch):
    """Чистый клиент на каждый тест: сам объект — заглушка, сеть не трогаем."""
    monkeypatch.setattr(ms_client.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(ms_client, "MS_TOKEN", "test-token")
    monkeypatch.setattr(ms_client, "_client", None)
    yield ms_client
    ms_client._client = None


def test_init_idempotent(mc):
    """Повторный init не пересоздаёт клиент — соединения не теряются."""
    mc.init()
    first = mc._client
    assert first is not None
    mc.init()
    assert mc._client is first


def test_init_without_token_does_not_crash(mc, monkeypatch):
    """Нет токена — клиент просто не поднимается, сервис живёт дальше."""
    monkeypatch.setattr(mc, "MS_TOKEN", "")
    mc.init()
    assert mc._client is None


def test_init_is_the_only_entry_point(mc):
    """Клиент поднимается своим init, а не побочным эффектом чужого модуля.

    Держит границу, на которой сломались счета: подъём клиента нельзя прятать
    внутрь модуля, который можно выключить флагом.
    """
    assert mc._client is None
    mc.init()
    assert mc._client is not None
