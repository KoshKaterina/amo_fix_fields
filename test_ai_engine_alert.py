"""Дежурные алерты ИИ-сервиса (`POST /internal/ai/v1/team_alert`, 23.11.2026): «нужен руководитель» (`ai_alert_head`)
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
    resp = asyncio.run(API.team_alert(API.TeamAlertIn(**body), token, contract))
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
    assert env.alerts[-1] == f"Руководитель нужен\nТиана\n{TEXT}\nСделка №{DEAL}"         # сделки в amo нет: название по номеру


def test_deal_name_comes_from_amo_and_amo_failure_falls_back_to_number(env, monkeypatch):
    env.lead()["name"] = "Тангем Кошелёк <2 карты>"
    panel(monkeypatch, "ai_alert_head", template="{{сделка}}\n{{текст}}", recipients_mode="nobody")
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT})[1] == {"status": "sent"}
    assert env.alerts[-1] == f"Тангем Кошелёк &lt;2 карты&gt;\n{TEXT}"                    # название экранировано, адреса в нём нет

    async def amo_down(lead_id, with_=()):
        raise RuntimeError("amo недоступна")

    monkeypatch.setattr(E.amo_service, "get_lead_full", amo_down)
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": "другое"})[1] == {"status": "sent"}
    assert env.alerts[-1] == f"Сделка №{DEAL}\nдругое"


# Шаблоны каталога Team (team-panel, ветка feat/ai-duty-alerts, коммит cd0d853, backend/app/alerts/catalog.py) дословно.
TEAM_TEMPLATES = {
    "ai_alert_head": "🆘 ИИ-менеджер: нужен руководитель\n{{текст_события}}\n👤 {{клиент}}\n📝 {{сделка}}\n{{теги}}\n🔗 {{ссылка_на_сделку}}",
    "ai_alert_tech": "🛠 ИИ-менеджер: не работает сайт или приложение\n{{текст_события}}\n👤 {{клиент}}\n📝 {{сделка}}\n{{теги}}\n🔗 {{ссылка_на_сделку}}",
}


@pytest.mark.parametrize("event", sorted(TEAM_TEMPLATES))
def test_team_catalog_template_is_rendered_not_the_code_text(env, monkeypatch, event):
    store.put_session(CHAT_KEY, channel_id=CHAN, chat_id="1920391385", contact_name="Тиана", deal_id=DEAL, state=store.S_AI)
    env.lead()["name"] = "Заказ Tangem"
    panel(monkeypatch, event, template=TEAM_TEMPLATES[event], recipients_mode="listed",
          recipients=[{"name": "Саша", "handle": "@rop_test", "amo_user_id": 1}])
    assert call({"deal_id": DEAL, "event": event, "text": "Клиент: оплата <b>не</b> проходит & всё"})[1] == {"status": "sent"}
    url = f"https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{DEAL}"
    head = TEAM_TEMPLATES[event].split("\n")[0]
    assert env.alerts[-1] == "\n".join([
        head,
        "Клиент: оплата &lt;b&gt;не&lt;/b&gt; проходит &amp; всё",            # текст ИИ экранирован, хоть переменная и «с разметкой»
        "👤 Тиана",
        "📝 Заказ Tangem",                                                    # название сделки; адрес только в ссылке
        "@rop_test",
        f'🔗 <a href="{url}">Открыть сделку</a>',
    ])
    assert "(тест)" not in env.alerts[-1] and "💬" not in env.alerts[-1]        # кодового текста нет


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
    assert call({"deal_id": 0, "event": "ai_alert_head", "text": TEXT}) == (422, {"detail": "deal_id"})
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": "   "}) == (422, {"detail": "text"})
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
    paths = {getattr(r, "path", "") for r in webhooks.app.routes}
    assert "/internal/ai/v1/team_alert" in paths and "/internal/ai/v1/alert" in paths


def test_engine_off_is_503(env, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert call({"deal_id": DEAL, "event": "ai_alert_head", "text": TEXT})[0] == 503


def test_human_alert_route_is_untouched(env):
    resp = asyncio.run(API.alert(API.AlertIn(key="manual", lines=["Проверка связи"]), "admin-token"))
    assert resp == {"ok": True}
    assert "Проверка связи" in env.alerts[-1]
    assert asyncio.run(API.alert(API.AlertIn(lines=["x"]), "engine-token")).status_code == 403     # токен сервиса туда не подходит


def test_over_http(env):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(API.router)
    http = TestClient(app)
    url = "/internal/ai/v1/team_alert"
    svc = {"X-Service-Token": "engine-token", "X-Contract": "2"}
    body = {"deal_id": DEAL, "event": "ai_alert_tech", "text": TEXT}
    r = http.post(url, json=body, headers=svc)
    assert (r.status_code, r.json()) == (200, {"status": "sent"})
    r = http.post(url, json=body, headers=svc)
    assert (r.status_code, r.json()) == (200, {"status": "duplicate"})
    assert http.post(url, json={**body, "event": "ai_alert_x"}, headers=svc).status_code == 422
    assert http.post(url, json={**body, "deal_id": "не число"}, headers=svc).status_code == 422
    assert http.post(url, json={"deal_id": DEAL, "event": "ai_alert_tech"}, headers=svc).status_code == 422     # text обязателен
    assert http.post(url, json={"event": "ai_alert_tech", "text": TEXT}, headers=svc).status_code == 422       # deal_id обязателен
    assert http.post(url, json=body).status_code in (403, 426)
    assert http.post(url, json=body, headers={"X-Admin-Token": "admin-token"}).status_code in (403, 426)
    r = http.post("/internal/ai/v1/alert", json={"lines": ["Проверка связи"]}, headers={"X-Admin-Token": "admin-token"})
    assert (r.status_code, r.json()) == (200, {"ok": True})                                                    # старая ручка человека
    assert http.post("/internal/ai/v1/alert", json=body, headers=svc).status_code in (403, 422)                # туда сервису ходу нет
    assert len(env.alerts) == 2
