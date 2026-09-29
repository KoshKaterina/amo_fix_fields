"""Offline schema-2 handoff contract for the separate out-of-stock form.

Only a temporary SQLite queue and mocked amo API are used. Network access
is blocked, so this test cannot create a remote MoySklad order.
"""

import asyncio
import json
import socket
import subprocess

import dotenv


def test_unavailable_schema2_handoff_is_exact_and_idempotent(monkeypatch, tmp_path):
    # This test must not import the real API module with credentials or network
    # available. Keep the guard before *all* project imports.
    for name in ("INTEGRATION_ID", "SECRET_KEY", "TOKEN", "SITE_FORM_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: False)

    def no_network(*args, **kwargs):
        raise AssertionError("network and subprocess access are forbidden in this contract test")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket.socket, "connect_ex", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(subprocess, "Popen", no_network)

    import api
    import site_form_service as forms
    import site_form_store as store

    submission_id = "72e8b147-a2e1-4eb8-92c2-f19391370451"
    product = {
        "id": 71234,  # Woo variation ID, not its parent ID.
        "name": "Tangem Ring Sapphire, размер 10",
        "sku": "RING-SAPPHIRE-10",
        "url": "https://test.sunscrypt.ru/product/tangem-ring/?attribute_pa_size=10",
    }
    payload = {
        "schema": 2,
        "form": "test-unavailable",
        "form_type": "unavailable",
        "submission_id": submission_id,
        "client_ip": "203.0.113.17",
        "contact": {"name": "Ирина", "phone": "+79990000000", "telegram": ""},
        "comment": "Сообщите о возможности доставки",
        "context": {
            "title": "Нет в наличии",
            "entry": "product-out-of-stock",
            "page_url": product["url"],
            "page_title": product["name"],
            "referrer": "",
            "utm": {},
            "product": product,
            "service": None,
            "format": None,
        },
    }
    form_map = {
        "test-unavailable": {
            "source": "Форма: нет в наличии",
            "pipeline_id": 8642414,
            "status_id": 70070982,
            "tags": ["тест"],
            "form_type": "unavailable",
        }
    }
    monkeypatch.setattr(forms, "FORM_MAP", form_map)
    monkeypatch.setattr(forms, "_wake", None)
    monkeypatch.setattr(forms, "_client_rate", {})
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "site_form.sqlite3"))
    store.init_db()

    writes = {"incoming": [], "accept": [], "notes": []}

    async def find_contact_id(phone):
        assert phone == "+79990000000"
        return None

    async def create_unsorted_lead_ex(**kwargs):
        writes["incoming"].append(kwargs)
        return {"lead_id": 301, "uid": "U-301"}

    async def accept_unsorted(uid, status_id, user_id=None):
        writes["accept"].append((uid, status_id))
        return 301

    async def set_lead_tags(lead_id, tags):
        assert (lead_id, tags) == (301, ["Форма: нет в наличии", "тест"])
        return True

    async def add_note_to_lead(lead_id, note, *, max_attempts=None):
        assert max_attempts == 1
        writes["notes"].append((lead_id, note))
        return True

    monkeypatch.setattr(api, "find_contact_id", find_contact_id)
    monkeypatch.setattr(api, "create_unsorted_lead_ex", create_unsorted_lead_ex)
    monkeypatch.setattr(api, "accept_unsorted", accept_unsorted)
    monkeypatch.setattr(api, "set_lead_tags", set_lead_tags)
    monkeypatch.setattr(api, "add_note_to_lead", add_note_to_lead)

    # An explicit backend gate is required even when the form map exists.
    monkeypatch.setattr(forms, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    assert asyncio.run(forms.accept_v2(payload)) == (
        422, {"ok": False, "error": "unavailable-disabled"},
    )
    assert store.get(submission_id) is None

    monkeypatch.setattr(forms, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    assert asyncio.run(forms.accept_v2(payload)) == (200, {"ok": True, "status": "accepted"})
    queued = store.get(submission_id)
    assert queued["submission_id"] == submission_id
    assert json.loads(queued["payload"])["context"]["product"] == product
    assert json.loads(queued["payload"])["submission_id"] == submission_id
    assert asyncio.run(forms.accept_v2(payload)) == (200, {"ok": True, "status": "duplicate"})

    # Closing the gate after queueing holds the exact attempt without a CRM call.
    monkeypatch.setattr(forms, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    assert asyncio.run(forms.run_due()) == 1
    assert store.get(submission_id)["status"] == "held"
    assert store.get(submission_id)["attempts"] == 0
    assert writes["incoming"] == []

    monkeypatch.setattr(forms, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    assert asyncio.run(forms.run_due()) == 1
    assert store.get(submission_id)["status"] == "done"
    assert len(writes["incoming"]) == 1
    incoming = writes["incoming"][0]
    assert incoming["pipeline_id"] == 8642414
    assert incoming["lead_name"] == f"Форма: нет в наличии: Ирина [ID заявки: {submission_id}]"
    assert incoming["form_id"] == "test-unavailable"
    assert incoming["source_uid"] == "site_form_test-unavailable"
    assert "custom_fields_values" not in incoming  # No order/preorder enum on the deal.
    assert writes["accept"] == [("U-301", 70070982)]
    assert len(writes["notes"]) == 1
    note = writes["notes"][0][1]
    assert f"Товар: {product['name']}, артикул {product['sku']}" in note
    assert f"Ссылка на товар: {product['url']}" in note
    assert f"Номер заявки: {submission_id[:8]}" in note
    assert f"ID заявки: {submission_id}" in note
    assert "Заказ" not in note and "Предзаказ" not in note
    outbound = json.dumps(writes, ensure_ascii=False)
    assert "Заказ" not in outbound and "Предзаказ" not in outbound

    assert asyncio.run(forms.accept_v2(payload)) == (200, {"ok": True, "status": "duplicate"})
    assert asyncio.run(forms.run_due()) == 0
    assert len(writes["incoming"]) == 1
    assert len(writes["notes"]) == 1
