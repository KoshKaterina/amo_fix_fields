"""Отказ отправки клиенту (`ai_engine.send_failed`, 13.11.2026): примечание в сделку, алерт в чат ОП с пингами, срочная задача «Связаться с клиентом».
ИИ остаётся владельцем сделки. Один раз на сделку и причину за 15 минут. Сети нет: подставный мир из test_ai_engine.py.
Запуск: python -m pytest test_ai_engine_send_failed.py -q
"""
import asyncio
import json

import ai_engine as E
import ai_engine_api as API
import ai_engine_config as cfg
import ai_engine_store as store
from test_ai_engine import CONTACT, DEAL, env, run  # noqa: F401  (env - фикстура)

REASON = "в Telegram по этому номеру нет аккаунта"


import pytest


@pytest.fixture(autouse=True)
def _fresh_dedupe(monkeypatch):
    monkeypatch.setattr(E, "_send_failed_seen", {})            # память дедупликации живёт в модуле: между тестами сбрасываем


def fail_notes(env):
    return [n for n in env.notes if "Не удалось отправить клиенту сообщение" in n[1]]


def test_failure_writes_note_alert_and_urgent_task_and_keeps_the_owner(env):
    async def go():
        env.lead()
        store.set_owner(DEAL, store.OWNER_AI, reason="t", chat={"chat_key": "k", "chat_id": "1", "channel_id": "c"}, pipeline_id=8642414)
        out = await E.send_failed(DEAL, "CHANNEL_TGAPI_PHONE_NOT_OCCUPIED", REASON)
        assert out == {"ok": True, "deduped": False}
        assert fail_notes(env) == [(DEAL, f"Не удалось отправить клиенту сообщение: {REASON}. Нужно связаться с клиентом вручную.")]
        alert = env.alerts[-1]
        assert "ИИ-менеджер" in alert and "@koshkatarina" in alert and REASON in alert and "Нужно связаться с клиентом вручную" in alert
        assert f"/leads/detail/{DEAL}" in alert
        task = next(t for t in env.tasks_open if t["deal_id"] == DEAL)
        assert task["text"].startswith("[send_failed:CHANNEL_TGAPI_PHONE_NOT_OCCUPIED] Связаться с клиентом, срочно") and REASON in task["text"]
        assert not task["text"].startswith("[handoff:")                       # это не передача: шлюз сервиса ИИ не останавливает
        owner = store.get_owner(DEAL)
        assert owner["owner"] == store.OWNER_AI and owner["epoch"] == 1       # владелец и эпоха прежние
    run(go())


def test_same_failure_within_15_minutes_changes_nothing_but_a_new_cause_or_time_does(env):
    async def go():
        env.lead()
        await E.send_failed(DEAL, "A", REASON)
        again = await E.send_failed(DEAL, "A", REASON)
        assert again == {"ok": True, "deduped": True}
        assert len(fail_notes(env)) == 1 and len(env.alerts) == 1 and len(env.tasks_open) == 1
        await E.send_failed(DEAL, "B", "канал недоступен")                      # другая причина
        assert len(fail_notes(env)) == 2 and len(env.alerts) == 2
        await E.send_failed(DEAL + 1, "A", REASON)                              # другая сделка
        assert len(fail_notes(env)) == 3
        await env.advance(E.SEND_FAILED_DEDUPE_S + 5)
        await E.send_failed(DEAL, "A", REASON)                                  # через 15 минут можно снова
        assert len(fail_notes(env)) == 4 and len(env.alerts) == 4
        assert len([t for t in env.tasks_open if t["text"].startswith("[send_failed:A]") and t["deal_id"] == DEAL]) == 1   # открытая задача не дублируется
    run(go())


def test_alert_goes_out_in_test_mode_with_the_test_header(env):
    async def go():
        env.lead()
        await E.send_failed(DEAL, "A", REASON)
        assert "(тест)" in env.alerts[-1]
    run(go())


def test_a_failed_note_does_not_stop_the_alert_and_the_task(env):
    async def go():
        env.lead()

        async def add_note(lead_id, text):
            raise RuntimeError("amo недоступна")

        env.mp.setattr(E.amo_service, "add_note", add_note)
        await E.send_failed(DEAL, "A", REASON)
        assert len(env.alerts) == 1 and len(env.tasks_open) == 1
    run(go())


def test_reason_is_escaped_in_the_alert(env):
    async def go():
        await E.send_failed(DEAL, "A", "Wazzup отказал: <b>x</b> & ещё")
        assert "<b>x</b>" not in env.alerts[-1] and "&lt;b&gt;x&lt;/b&gt;" in env.alerts[-1]
    run(go())


def test_route_needs_the_service_token_and_contract_version(env, monkeypatch):
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "eng")
    body = API.SendFailedIn(deal_id=DEAL, code="A", reason=REASON)

    def code_of(resp):
        return resp.status_code, json.loads(resp.body)

    assert code_of(asyncio.run(API.send_failed(body, "wrong", "2")))[0] == 403
    assert code_of(asyncio.run(API.send_failed(body, "eng", "1")))[0] == 426
    assert code_of(asyncio.run(API.send_failed(body, "", "")))[0] in (403, 426)
    assert code_of(asyncio.run(API.send_failed(body, "eng", "2"))) == (200, {"ok": True, "deduped": False})
    assert len(fail_notes(env)) == 1
    import webhooks
    assert "/internal/ai/v1/send_failed" in {getattr(r, "path", "") for r in webhooks.app.routes}
