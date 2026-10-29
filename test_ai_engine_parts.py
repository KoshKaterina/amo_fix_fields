"""ИИ-менеджер в движке: хранилище, настройки панели, внутренний API, врезки в соседей.

Здесь то, что не про сценарии хода (они в `test_ai_engine.py`), а про детали, на которых сценарии стоят:

  • эпоха растёт только при смене владельца, номера ходов не начинаются с единицы;
  • настройки панели: сбой не затирает кэш, мусор не принимается, окно отправки считается по МСК;
  • внутренний API закрыт токеном и флагом, ручки человека - отдельным токеном;
  • вебхук Wazzup не ломается от падения ИИ-менеджера, авто-режим и сторож розницы его чаты не трогают;
  • эхо `Admin` закрывает SLA-ожидание (иначе ответ ИИ вызывал бы ложный алерт «клиент без ответа»).

Сети нет. Запуск: python -m pytest test_ai_engine_parts.py -q
"""
import asyncio
import datetime
import inspect
import json
import time

import httpx
import pytest

import ai_engine as E
import ai_engine_api as API
import ai_engine_config as cfg
import ai_engine_settings as settings
import ai_engine_store as store
import ai_engine_dedup as dedup
import amo_service
import autopilot
import retail_lead_guard
import wazzup_sla
from shared.timez import MSK

CHAN = "12d3ccf1-8f15-4144-8b6e-5fffe3a2f647"


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", True)
    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "ai.sqlite3"))
    monkeypatch.setattr(cfg, "SETTINGS_CACHE_PATH", str(tmp_path / "settings.json"))
    store.init()
    return tmp_path


# =============================================================================================
# хранилище
# =============================================================================================

def test_epoch_grows_only_when_owner_changes(db):
    row = store.set_owner(1, "ai", reason="a")
    assert (row["owner"], row["epoch"]) == ("ai", 1)
    assert store.set_owner(1, "ai", reason="again")["epoch"] == 1
    assert store.set_owner(1, "human", reason="b")["epoch"] == 2
    assert store.set_owner(1, "ai", reason="c")["epoch"] == 3
    assert store.set_owner(1, "engine", reason="d", closed=True)["closed"] == 1


def test_set_owner_keeps_untouched_columns(db):
    store.set_owner(1, "ai", reason="a", chat={"chat_id": "c", "chat_type": "t", "channel_id": "ch", "chat_key": "ch:c"},
                    pipeline_id=8642414, contact_ids=[5], mode="test")
    row = store.set_owner(1, "human", reason="b")
    assert row["chat_key"] == "ch:c" and row["pipeline_id"] == 8642414 and json.loads(row["contact_ids"]) == [5]


def test_turn_ids_do_not_restart_from_one(db):
    def build(tid):
        return {"turn_id": tid}
    turn = store.create_turn(deal_id=1, epoch=1, chat_key="k", reason="x", payload_fn=build, message_seqs=[], up_to_seq=0)
    assert turn["turn_id"] > 1_000_000_000          # номера не совпадут с виденными сервисом после потери базы
    assert json.loads(turn["payload"]) == {"turn_id": turn["turn_id"]}
    nxt = store.create_turn(deal_id=1, epoch=1, chat_key="k", reason="x", payload_fn=build, message_seqs=[], up_to_seq=0)
    assert nxt["turn_id"] == turn["turn_id"] + 1


def test_create_turn_marks_messages_in_one_step_and_unconsume_returns_them(db):
    a = store.add_inbox("a", "k", "раз", "text", "", 1.0)
    b = store.add_inbox("b", "k", "два", "text", "", 2.0)
    assert store.add_inbox("a", "k", "раз", "text", "", 1.0) is None            # повтор вебхука
    turn = store.create_turn(deal_id=1, epoch=1, chat_key="k", reason="x", payload_fn=lambda t: {},
                             message_seqs=[a, b], up_to_seq=b)
    assert store.pending_inbox("k") == []
    assert store.unconsume(turn["turn_id"]) == 2
    assert [r["message_id"] for r in store.pending_inbox("k")] == ["a", "b"]
    assert store.has_newer_than("k", a) and not store.has_newer_than("k", b)


def test_claim_event_once_and_release(db):
    assert store.claim_event(1, "payment_received") is True
    assert store.claim_event(1, "payment_received") is False
    store.release_event(1, "payment_received")
    assert store.claim_event(1, "payment_received") is True


def test_unknown_session_field_is_an_error(db):
    with pytest.raises(KeyError):
        store.put_session("k", channel_id="c", chat_id="i", state="collecting", nope=1)


def test_chat_is_ai_owned_and_is_ai_owned(db):
    store.set_owner(7, "ai", reason="a", chat={"chat_key": "ch:c"})
    assert store.is_ai_owned(7) and store.chat_is_ai_owned("ch:c")
    store.set_owner(7, "human", reason="b")
    assert not store.is_ai_owned(7) and not store.chat_is_ai_owned("ch:c")
    assert not store.is_ai_owned(8)


# =============================================================================================
# настройки панели
# =============================================================================================

DOC = {
    "mode": "test", "pipelines": [8642414],
    "contacts": {"scope": "test", "restricted": True, "items": [{"contact_id": 555, "phone_norm": "79260000001", "name": "Т"}]},
    "timers": {"first_reply_wait_s": 61, "debounce_s": 11, "debounce_cap_s": 62, "deal_resolve_timeout_s": 601,
               "ai_down_release_s": 121},
    "post_sale_window_days": 7, "work_hours": [{"start": "09:00", "end": "21:00"}], "updated_at": None,
}


def _panel(monkeypatch, handler):
    real = httpx.AsyncClient
    monkeypatch.setattr(cfg, "PANEL_URL", "https://panel.test")
    monkeypatch.setattr(cfg, "PANEL_TOKEN", "ai-ingest-token")
    monkeypatch.setattr(settings.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))


def test_fetch_once_stores_document_and_cache_and_sends_ai_token(db, monkeypatch):
    seen = {}

    def handler(request):
        seen["token"] = request.headers.get("X-Ingest-Token")
        seen["path"] = request.url.path
        return httpx.Response(200, json=DOC)

    settings.set_document(None)
    _panel(monkeypatch, handler)
    assert asyncio.run(settings.fetch_once()) is True
    assert seen == {"token": "ai-ingest-token", "path": "/api/ingest/ai/settings"}
    assert settings.timers()["first_reply_wait_s"] == 61 and settings.effective_mode() == "test"
    settings.set_document(None)
    settings._load_cache()                                  # после «рестарта» документ приходит из кэша на диске
    assert settings.document()["mode"] == "test" and settings.is_fresh()


@pytest.mark.parametrize("response", [
    httpx.Response(503), httpx.Response(403, json={"detail": "forbidden"}), httpx.Response(200, json=["мусор"]),
    httpx.Response(200, json={"нет": "режима"}),
])
def test_failed_or_garbage_fetch_keeps_previous_document(db, monkeypatch, response):
    settings.set_document(DOC)
    _panel(monkeypatch, lambda request: response)
    assert asyncio.run(settings.fetch_once()) is False
    assert settings.document() == DOC


def test_fetch_without_token_does_nothing(db, monkeypatch):
    monkeypatch.setattr(cfg, "PANEL_TOKEN", "")
    assert asyncio.run(settings.fetch_once()) is False


def test_timers_fall_back_to_defaults_on_garbage(db):
    settings.set_document({**DOC, "timers": {"first_reply_wait_s": "x", "debounce_s": -5}})
    t = settings.timers()
    assert t["first_reply_wait_s"] == 60 and t["debounce_s"] == 10


def test_send_window_msk(db):
    settings.set_document(DOC)
    at = lambda h, m=0: datetime.datetime(2026, 10, 28, h, m, tzinfo=MSK)
    assert settings.send_window(at(10)) == {"open_now": True, "next_open_at": None}
    late = settings.send_window(at(22))
    assert late["open_now"] is False and late["next_open_at"].startswith("2026-10-29T09:00")
    early = settings.send_window(at(7))
    assert early["next_open_at"].startswith("2026-10-28T09:00")
    settings.set_document({**DOC, "work_hours": []})
    assert settings.send_window(at(10)) == {"open_now": False, "next_open_at": None}   # пусто = никогда


def test_phone_matching_helpers(db):
    settings.set_document(DOC)
    assert settings.phone_may_match("+7 (926) 000-00-01")
    assert settings.phone_may_match("89260000001")                      # российская 8 -> 7
    assert not settings.phone_may_match("79990000000", "123")
    assert settings.contact_allowed([555], [])
    assert settings.contact_allowed([1], ["79260000001"])
    assert not settings.contact_allowed([1], ["79990000000"])


# =============================================================================================
# внутренний API
# =============================================================================================

def _code(resp):
    if isinstance(resp, dict):                              # обычный успешный ответ маршрута
        return 200, resp
    return resp.status_code, json.loads(resp.body)


def test_api_is_closed_without_flag_or_token(db, monkeypatch):
    body = API.PermitIn(deal_id=1, epoch=1, turn_id=1)
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "")
    assert _code(asyncio.run(API.permit(body, "x", "2")))[0] == 503
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "eng")
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert _code(asyncio.run(API.permit(body, "eng", "2")))[0] == 503


def test_api_checks_token_and_contract_version(db, monkeypatch):
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "eng")
    body = API.PermitIn(deal_id=1, epoch=1, turn_id=1)
    assert _code(asyncio.run(API.permit(body, "wrong", "2")))[0] == 403
    assert _code(asyncio.run(API.permit(body, "eng", "1")))[0] == 426
    assert _code(asyncio.run(API.permit(body, "", "")))[0] in (403, 426)
    result = API.ResultIn(turn_id=1, deal_id=1, epoch=1, outcome="continue")
    assert _code(asyncio.run(API.result(result, "wrong", "2")))[0] == 403


def test_api_permit_and_result_roundtrip(db, monkeypatch):
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "eng")
    settings.set_document(DOC)
    store.set_owner(5, "ai", reason="t", chat={"chat_key": "k"}, pipeline_id=8642414)
    a = store.add_inbox("m", "k", "привет", "text", "", 1.0)
    turn = store.create_turn(deal_id=5, epoch=1, chat_key="k", reason="first_message", payload_fn=lambda t: {},
                             message_seqs=[a], up_to_seq=a)
    ok = asyncio.run(API.permit(API.PermitIn(deal_id=5, epoch=1, turn_id=turn["turn_id"], up_to_message_id="m"), "eng", "2"))
    assert _code(ok) == (200, {"allowed": True})
    stale = asyncio.run(API.permit(API.PermitIn(deal_id=5, epoch=9, turn_id=turn["turn_id"], up_to_message_id="m"), "eng", "2"))
    assert _code(stale)[0] == 409                           # сервис читает 409 как stale_epoch
    bad = asyncio.run(API.result(API.ResultIn(turn_id=turn["turn_id"], deal_id=5, epoch=1, outcome="сломано"), "eng", "2"))
    assert _code(bad)[0] == 422
    done = asyncio.run(API.result(API.ResultIn(turn_id=turn["turn_id"], deal_id=5, epoch=1, outcome="continue"), "eng", "2"))
    assert _code(done) == (200, {"ok": True})


def test_admin_routes_need_their_own_token(db, monkeypatch):
    monkeypatch.setattr(cfg, "ENGINE_TOKEN", "eng")
    monkeypatch.setattr(cfg, "ADMIN_TOKEN", "adm")
    store.set_owner(5, "human", reason="t", pipeline_id=8642414)
    assert _code(asyncio.run(API.state(5, "eng")))[0] == 403                # токен сервиса сюда не подходит
    assert _code(asyncio.run(API.return_dialog(API.ReturnIn(deal_id=5), "eng")))[0] == 403
    code, body = _code(asyncio.run(API.state(5, "adm")))
    assert code == 200 and body["owner"]["owner"] == "human"
    assert _code(asyncio.run(API.state(404, "adm")))[0] == 404
    monkeypatch.setattr(cfg, "ADMIN_TOKEN", "")
    assert _code(asyncio.run(API.state(5, "")))[0] == 503


def test_routes_are_wired_into_the_app():
    import webhooks
    paths = {getattr(r, "path", "") for r in webhooks.app.routes}
    assert {"/internal/ai/v1/permit", "/internal/ai/v1/result", "/internal/ai/v1/return_dialog"} <= paths


# =============================================================================================
# врезки в соседей
# =============================================================================================

class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


def test_wazzup_webhook_survives_ai_engine_crash_and_calls_it(monkeypatch):
    import webhooks
    monkeypatch.setattr(webhooks, "WAZZUP_WEBHOOK_SECRET", "s3")
    seen = []

    def boom(payload):
        seen.append(payload)
        raise RuntimeError("упало")

    monkeypatch.setattr(webhooks.ai_engine, "on_wazzup", boom)
    payload = {"messages": [], "statuses": []}
    out = asyncio.run(webhooks.wazzup_webhook("s3", _FakeRequest(payload)))
    assert out == {"ok": True} and seen == [payload]


def test_lead_change_hook_is_isolated_in_try_except():
    import webhooks
    src = inspect.getsource(webhooks.lead_change)
    i = src.index("ai_engine.on_lead_change")
    before = src[:i].rsplit("\n", 3)[-3:]
    assert any("try:" in line for line in before)
    assert "except Exception" in src[i:i + 200]


def test_health_endpoint_reports_ai_engine_block(monkeypatch):
    import webhooks
    out = asyncio.run(webhooks.health())
    assert "ai_engine" in out and out["ai_engine"]["enabled"] in (True, False)


def test_autopilot_does_not_touch_deal_owned_by_ai(db, monkeypatch):
    store.set_owner(77, "ai", reason="t")
    assert asyncio.run(autopilot._ai_owns_deal(77)) is True
    assert asyncio.run(autopilot._ai_owns_deal(78)) is False
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert asyncio.run(autopilot._ai_owns_deal(77)) is False       # ИИ выключен: развязка ничего не меняет

    monkeypatch.setattr(cfg, "ENABLED", True)
    monkeypatch.setattr(autopilot, "is_enabled", lambda: True)

    async def lead(lead_id):
        return {"id": lead_id, "pipeline_id": 10593102, "status_id": 83537874}

    def must_not_be_reached():
        raise AssertionError("авто-режим продолжил работу по сделке ИИ")

    monkeypatch.setattr(autopilot, "load_lead", lead)
    monkeypatch.setattr(autopilot.settings_client, "get_pipeline_id", must_not_be_reached)
    asyncio.run(autopilot.handle_lead_change(77))


def test_autopilot_gate_survives_broken_ai_database(db, monkeypatch):
    def boom(_):
        raise RuntimeError("база недоступна")

    monkeypatch.setattr(store, "is_ai_owned", boom)
    assert asyncio.run(autopilot._ai_owns_deal(1)) is False


def test_retail_guard_skips_chats_owned_by_ai(db, monkeypatch):
    monkeypatch.setattr(retail_lead_guard, "RETAIL_GUARD_ENABLED", True)
    monkeypatch.setattr(retail_lead_guard, "RETAIL_GUARD_CHANNELS", {CHAN})
    monkeypatch.setattr(retail_lead_guard, "_checked", {})
    applied = []

    async def apply(st):
        applied.append(st["chat_id"])

    monkeypatch.setattr(retail_lead_guard, "_apply", apply)
    payload = {"messages": [{"channelId": CHAN, "chatId": "100", "isEcho": False, "status": "inbound", "text": "привет"}]}

    async def go():
        retail_lead_guard.on_wazzup(payload)                      # чат ничей: сторож работает как раньше
        await asyncio.sleep(0.05)
        assert applied == ["100"]
        store.set_owner(9, "ai", reason="t", chat={"chat_key": f"{CHAN}:200"})
        retail_lead_guard._checked.clear()
        retail_lead_guard.on_wazzup({"messages": [{**payload["messages"][0], "chatId": "200"}]})
        await asyncio.sleep(0.05)
        assert applied == ["100"]                                  # чат ведёт ИИ: тревога про «нет сделки в рознице» ложная

    asyncio.run(go())


def test_retail_guard_still_works_when_ai_is_disabled(db, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", False)
    assert retail_lead_guard._ai_owns_chat(CHAN, "1") is False


def test_sla_timer_is_closed_by_admin_echo_so_ai_reply_raises_no_false_alert(monkeypatch):
    """Открытый вопрос контракта: закроет ли ответ ИИ (эхо `Admin`) SLA-ожидание «клиент без ответа»."""
    monkeypatch.setattr(wazzup_sla, "_pending", {})
    inbound = {"channelId": CHAN, "chatId": "300", "isEcho": False, "status": "inbound", "text": "сколько стоит?",
               "chatType": "telegram", "contact": {"name": "К"}}
    wazzup_sla.handle_webhook({"messages": [inbound]})
    assert (CHAN, "300") in wazzup_sla._pending
    echo = {"channelId": CHAN, "chatId": "300", "isEcho": True, "status": "sent", "authorName": "Admin",
            "text": "Добрый день!"}
    wazzup_sla.handle_webhook({"messages": [echo]})
    assert (CHAN, "300") not in wazzup_sla._pending


def test_flag_off_means_no_loops_no_db_and_no_hooks(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "ENABLED", False)
    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "never.sqlite3"))
    monkeypatch.setattr(E, "_queue", None)
    asyncio.run(E.init())
    assert not (tmp_path / "never.sqlite3").exists() and E._tasks == []
    E.on_wazzup({"messages": [{"chatId": "1"}]})              # ничего не делает и не падает
    E.on_lead_change(1, None, None, {})
    assert E.status() == {"enabled": False}


# =============================================================================================
# поиск сделок по всем страницам (боевой тест 28.10.2026: у контакта 52 сделки, новая на второй странице)
# =============================================================================================

def _paged_amo(monkeypatch, leads, fail_page=None):
    """Подставной amo: /api/v4/leads отдаёт срез по `page` и `limit`, как настоящий (по возрастанию номера)."""
    calls = []

    async def fake_get(path, params=None):
        q = dict(params or [])
        page, limit = int(q.get("page", 1)), int(q.get("limit", 50))
        calls.append((page, limit, q.get("query")))
        if fail_page == page:
            return None
        chunk = leads[(page - 1) * limit: page * limit]
        return {"_embedded": {"leads": chunk}} if chunk else {}

    monkeypatch.setattr(amo_service, "_do_get", fake_get)
    return calls


def _lead(i, status=142, pipeline=8642414, contact=555):
    return {"id": i, "status_id": status, "pipeline_id": pipeline, "price": 0,
            "_embedded": {"contacts": [{"id": contact, "is_main": True}]}}


def test_find_leads_by_query_all_walks_every_page(monkeypatch):
    old = [_lead(i) for i in range(1, 256)]                   # 255 старых закрытых
    leads = old + [_lead(900, status=1000)]                    # и одна новая открытая на второй странице
    calls = _paged_amo(monkeypatch, leads)
    got = asyncio.run(amo_service.find_leads_by_query_all("1920391385", with_=("contacts",)))
    assert [c[:2] for c in calls] == [(1, 250), (2, 250)]
    assert len(got) == 256 and got[-1]["id"] == 900


def test_find_leads_by_query_all_one_short_page_is_one_request(monkeypatch):
    calls = _paged_amo(monkeypatch, [_lead(1), _lead(2)])
    assert len(asyncio.run(amo_service.find_leads_by_query_all("x"))) == 2 and len(calls) == 1
    calls = _paged_amo(monkeypatch, [])
    assert asyncio.run(amo_service.find_leads_by_query_all("x")) == []     # пустая выдача (204): «не нашлось»


def test_find_leads_by_query_all_missing_page_is_none_not_a_partial_answer(monkeypatch):
    leads = [_lead(i) for i in range(1, 300)]
    _paged_amo(monkeypatch, leads, fail_page=2)
    assert asyncio.run(amo_service.find_leads_by_query_all("x")) is None   # вторая страница не пришла


def test_find_leads_by_query_all_ceiling_is_none(monkeypatch):
    leads = [_lead(i) for i in range(1, 1500)]                 # больше 5 страниц по 250
    calls = _paged_amo(monkeypatch, leads)
    assert asyncio.run(amo_service.find_leads_by_query_all("x")) is None
    assert len(calls) == 5


def test_resolve_finds_new_open_deal_behind_a_long_history(db, monkeypatch):
    """Ровно случай боевого теста: у контакта 52 закрытые и одна новая открытая в «Тест» на второй странице."""
    settings.set_document(DOC)
    leads = [_lead(i) for i in range(1, 53)] + [_lead(36588789, status=1000)]
    leads = leads[:49] + leads[49:]                            # новая последняя по номеру, как в amo
    calls = _paged_amo(monkeypatch, leads)
    res = asyncio.run(dedup.resolve("1920391385", "79260000001"))
    assert res.kind == dedup.K_AI and res.deal["id"] == 36588789
    assert all(limit == 250 for _, limit, _ in calls)


def test_resolve_treats_a_broken_second_page_as_amo_silence(db, monkeypatch):
    settings.set_document(DOC)
    _paged_amo(monkeypatch, [_lead(i) for i in range(1, 300)], fail_page=2)
    assert asyncio.run(dedup.resolve("1920391385", "79260000001")).kind == dedup.K_ERROR


def test_old_single_page_search_is_untouched():
    """Сторож розницы и протез amgroup зовут прежнюю функцию: у неё прежняя сигнатура."""
    import inspect
    assert "limit" in inspect.signature(amo_service.find_leads_by_query).parameters
