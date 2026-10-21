"""Маршрут /getcourse/{secret}: выключенный и чужой - 404, JSON и форма принимаются,
200 только после записи в очередь."""

import pytest
from fastapi.testclient import TestClient

import getcourse_service as gs
import getcourse_store as store
import webhooks


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(gs, "GETCOURSE_ENABLED", True)
    monkeypatch.setattr(gs, "GETCOURSE_SECRET", "gcSecret")
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "gc.sqlite3"))
    store.init_db()
    return TestClient(webhooks.app)          # без with: фоновые циклы сервиса не поднимаем


BODY = {"order_id": "910002423", "order_number": "20619", "email": "a@b.ru",
        "page_url": "https://edu.sunscrypt.ru/anketa/strategy"}


def test_disabled_route_is_404(client, monkeypatch):
    monkeypatch.setattr(gs, "GETCOURSE_ENABLED", False)
    assert client.post("/getcourse/gcSecret", json=BODY).status_code == 404


def test_wrong_secret_is_404(client):
    assert client.post("/getcourse/nope", json=BODY).status_code == 404
    assert store.get("910002423:order_created") is None


def test_json_is_queued(client):
    resp = client.post("/getcourse/gcSecret", json=BODY)
    assert resp.status_code == 200 and resp.json() == {"ok": True, "status": "accepted"}
    assert store.get("910002423:order_created")["status"] == "pending"
    assert client.post("/getcourse/gcSecret", json=BODY).json()["status"] == "duplicate"


def test_form_body_is_accepted(client):
    resp = client.post("/getcourse/gcSecret", data={**BODY, "event": "order_paid", "a_level": "x"})
    assert resp.status_code == 200
    assert store.get("910002423:order_paid")["event"] == "order_paid"


def test_bad_bodies(client):
    assert client.post("/getcourse/gcSecret", json={"order_id": "abc"}).status_code == 422
    big = {"order_id": "1", "x": "y" * (gs.MAX_BODY_BYTES + 10)}
    assert client.post("/getcourse/gcSecret", json=big).status_code == 413
