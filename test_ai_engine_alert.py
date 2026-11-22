"""Дежурные алерты ИИ-сервиса (`POST /internal/ai/v1/alert` с токеном сервиса, 23.11.2026): «нужен руководитель» (`ai_alert_head`)
и «не работает сайт или приложение» (`ai_alert_tech`). Настройки события берутся из панели Team через `alerts.decide`; одинаковый
алерт по сделке за 10 минут не повторяется. Сети нет: подставный мир из test_ai_engine.py.
Запуск: python -m pytest test_ai_engine_alert.py -q
"""
import asyncio
import json

import pytest

import ai_engine as E
import ai_engine_api as API
import ai_engine_config as cfg
import ai_engine_store as store
import alert_settings_client as settings_client
import alerts
from test_ai_engine import CHAN, CHAT_KEY, DEAL, env, run  # noqa: F401  (env - фикстура)

TEXT = "Клиент просит связаться с руководителем по поводу возврата"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch, tmp_path):
    monkeypatch.setattr(E, "_duty_seen", {})                    # память антидубля живёт в модуле: между тестами сбрасываем
    monkeypatch.setattr(alerts, "SHADOW_LOG_PATH", tmp_path / "alert_shadow.jsonl")
    yield
    settings_client.set_settings_for_tests(None)


def panel(monkeypatch, event, **over):
    """Панель в режиме `on` знает событие `event` с настройками `over`."""
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "on")
    ev = {"enabled": True, "channel": "op_notify", "template": "", "recipients_mode": "shift", "recipients": []}
    ev.update(over)
    settings_client.set_settings_for_tests({"events": {event: ev}, "people": []})


def call(body: dict, token="engine-token", contract="2"):
    resp = asyncio.run(API.alert(API.AlertIn(**body), "", token, contract))
    return resp.status_code, json.loads(resp.body)


def test_head_alert_goes_to_sales_notify_with_client_text_and_link(env):
    store.put_session(CHAT_KEY, channel_id=CHAN, chat_id="1920391385", contact_name="Тиана", deal_id=DEAL, state=store.S_AI)
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT}) == (200, {"status": "sent"})
    alert = env.alerts[-1]
    assert alert.startswith("🙋 ИИ-менеджер: нужен руководитель (тест)")
    assert "👤 Тиана" in alert and f"💬 {TEXT}" in alert and f"/leads/detail/{DEAL}" in alert and "@koshkatarina" in alert


def test_tech_alert_without_known_client_has_no_client_line(env):
    assert call({"deal_id": DEAL, "event": "ai_alert_tech", "text": "Не открывается приложение <Tangem>"}) == (200, {"status": "sent"})
    alert = env.alerts[-1]
    assert alert.startswith("🛠 ИИ-менеджер: не работает сайт или приложение")
    assert "👤" not in alert and "&lt;Tangem&gt;" in alert and "<Tangem>" not in alert       # текст сервиса экранирован


def test_panel_template_gets_deal_client_and_text(env, monkeypatch):
    store.put_session(CHAT_KEY, channel_id=CHAN, chat_id="1920391385", contact_name="Тиана", deal_id=DEAL, state=store.S_AI)
    panel(monkeypatch, "ai_alert_head", template="Руководитель нужен\n{{клиент}}\n{{текст}}\n{{сделка}}\n{{теги}}", recipients_mode="nobody")
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT})[1] == {"status": "sent"}
    assert env.alerts[-1] == f"Руководитель нужен\nТиана\n{TEXT}\nhttps://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{DEAL}"


def test_event_disabled_in_panel_sends_nothing(env, monkeypatch):
    panel(monkeypatch, "ai_alert_tech", enabled=False)
    body = {"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}
    assert call(body) == (200, {"status": "disabled"})
    assert call(body) == (200, {"status": "disabled"})                 # выключенное не считается отправленным: дублем не станет
    assert env.alerts == []


def test_same_alert_within_10_minutes_is_a_duplicate_but_other_text_event_or_time_is_not(env):
    body = {"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT}
    assert call(body)[1] == {"status": "sent"}
    assert call(body) == (200, {"status": "duplicate"})
    assert call({**body, "text": "  " + TEXT.replace(" ", "   ") + " "}) == (200, {"status": "duplicate"})   # пробелы не делают текст новым
    assert len(env.alerts) == 1
    assert call({**body, "text": "другое"})[1] == {"status": "sent"}
    assert call({**body, "event": "ai_alert_tech"})[1] == {"status": "sent"}
    assert call({**body, "deal_id": DEAL + 1})[1] == {"status": "sent"}
    assert len(env.alerts) == 4
    env.t += E.DUTY_DEDUPE_S + 1
    assert call(body)[1] == {"status": "sent"}
    assert len(env.alerts) == 5


def test_unknown_event_missing_deal_and_empty_text_are_422(env):
    assert call({"deal_id": DEAL, "event": "ai_alert_boss", "text": TEXT}) == (422, {"detail": "unknown_event"})
    assert call({"deal_id": DEAL, "text": TEXT}) == (422, {"detail": "unknown_event"})
    assert call({"event": "ai_alert_head", "text": TEXT}) == (422, {"detail": "deal_id"})
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": "   "}) == (422, {"detail": "text"})
    assert call({"deal_id": DEAL, "event": "ai_alert_head"}) == (422, {"detail": "text"})
    assert env.alerts == []


def test_long_text_is_cut_to_500(env):
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": "я" * 800})[1] == {"status": "sent"}
    line = next(l for l in env.alerts[-1].split("\n") if l.startswith("💬 "))
    assert len(line[2:]) == E.DUTY_TEXT_MAX and line.endswith("я…")


def test_telegram_failure_is_502_and_a_retry_is_not_a_duplicate(env, monkeypatch):
    state = {"how": "refuse"}
    sent = []

    async def deliver(d, on_sent=None):
        if state["how"] == "refuse":
            return False                                                 # send_alert: бот выключен или Телеграм не принял
        if state["how"] == "boom":
            raise RuntimeError("Телеграм недоступен")
        sent.append(d.text)
        return True

    monkeypatch.setattr(E, "_deliver_alert", deliver)
    body = {"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}
    assert call(body) == (502, {"status": "failed"})
    state["how"] = "boom"
    assert call(body) == (502, {"status": "failed"})
    state["how"] = "ok"
    assert call(body) == (200, {"status": "sent"})                       # неудача не заняла антидубль
    assert call(body) == (200, {"status": "duplicate"})
    assert len(sent) == 1


def test_route_needs_the_service_token_and_contract_version(env):
    body = {"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT}
    assert call(body, token="wrong")[0] == 403
    assert call(body, contract="1")[0] == 426
    assert call(body, token="", contract="")[0] in (403, 426)
    assert call({"deal_id": DEAL, "event": "nope", "text": TEXT}, token="wrong")[0] == 403      # сначала токен, потом тело
    assert call(body, token="admin-token")[0] == 403                                            # админский токен сюда не подходит
    assert env.alerts == []
    assert call(body) == (200, {"status": "sent"})
    import webhooks
    assert "/internal/ai/v1/alert" in {getattr(r, "path", "") for r in webhooks.app.routes}


def test_engine_off_is_503(env, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT})[0] == 503


def test_human_alert_with_admin_token_still_works(env):
    resp = asyncio.run(API.alert(API.AlertIn(key="manual", lines=["Проверка связи"]), "admin-token"))
    assert resp == {"ok": True}
    assert "Проверка связи" in env.alerts[-1]
    bad = asyncio.run(API.alert(API.AlertIn(deal_id=DEAL, event="ai_alert_head", text=TEXT), "admin-token"))
    assert bad.status_code == 422                                        # у человека старое тело: без `lines` нечего слать


def test_over_http_headers_pick_the_owner_of_the_request(env):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(API.router)
    http = TestClient(app)
    url = "/internal/ai/v1/alert"
    svc = {"X-Service-Token": "engine-token", "X-Contract": "2"}
    r = http.post(url, json={"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}, headers=svc)
    assert (r.status_code, r.json()) == (200, {"status": "sent"})
    r = http.post(url, json={"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}, headers=svc)
    assert (r.status_code, r.json()) == (200, {"status": "duplicate"})
    assert http.post(url, json={"deal_id": DEAL, "event": "ai_alert_x", "text": TEXT}, headers=svc).status_code == 422
    assert http.post(url, json={"deal_id": "не число", "event": "ai_alert_tech", "text": TEXT}, headers=svc).status_code == 422
    r = http.post(url, json={"lines": ["Проверка связи"]}, headers={"X-Admin-Token": "admin-token"})
    assert (r.status_code, r.json()) == (200, {"ok": True})
    assert http.post(url, json={"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}).status_code in (403, 426)
    assert len(env.alerts) == 2
