"""Стикер Telegram, присланный Wazzup как `document`: путь от вебхука до хода ИИ и панели.

Определение живёт в одном месте, в `webhooks.wazzup_webhook` перед первым потребителем (`wazzup_sticker.normalize_payload`).
Здесь проверяем, что от него выигрывают ВСЕ потребители и ничего не ломается:

  • SLA, контроль доставки, пересылка в панель, авто-режим, сторож розницы и ИИ видят `sticker`, а не `document`;
  • в очереди ИИ (`inbox`) лежит `sticker`, ход получает «[Клиент прислал стикер; содержимого ИИ не видит]» - ровно как
    по настоящему стикеру; настоящий PDF остаётся «файлом»; вложение с подписью не проверяется;
  • зависшее хранилище Wazzup не теряет сообщение и держит вебхук не дольше таймаута.

Сети нет: HTTP хранилища подставной, ссылки выдуманные, остальные потребители вебхука записывают, что увидели.
Запуск: python -m pytest test_ai_engine_sticker.py -q
"""
import asyncio
import inspect
import json
import time
from collections import OrderedDict

import httpx
import pytest

import ai_engine as E
import ai_engine_store as store
import wazzup_forward
import wazzup_sticker as S
from test_ai_engine import CHAT, CHAT_KEY, DEAL, env, run  # noqa: F401  (env - фикстура)
from test_wazzup_sticker import URL, Net

STICKER_LINE = "[Клиент прислал стикер; содержимого ИИ не видит]"
FILE_LINE = "[Клиент прислал файл; содержимого ИИ не видит]"
PDF = {"Content-Type": "application/pdf", "Content-Disposition": 'attachment; filename="invoice.pdf"'}


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


class World:
    """Вебхук как есть, но соседи записывают, какой тип сообщения увидели в момент вызова."""

    def __init__(self, env, mp):
        import webhooks
        self.env, self.mp, self.webhooks = env, mp, webhooks
        self.net = Net()
        self.seen: dict[str, list[str]] = {}
        mp.setattr(S, "_transport", httpx.MockTransport(self.net.handler))
        mp.setattr(S, "_cache", OrderedDict())
        mp.setattr(S, "ENABLED", True)
        mp.setattr(webhooks, "WAZZUP_WEBHOOK_SECRET", "s")
        for module, name in ((webhooks.wazzup_sla, "handle_webhook"), (webhooks.wazzup_delivery, "handle_webhook"),
                             (webhooks.academy_invite_delivery, "record_webhook"), (webhooks.autopilot, "on_wazzup"),
                             (webhooks.retail_lead_guard, "on_wazzup")):
            mp.setattr(module, name, self._recorder(f"{module.__name__}.{name}"))
        mp.setattr(wazzup_forward, "_enabled", True)         # настоящая пересылка: очередь в памяти, воркера нет
        mp.setattr(wazzup_forward, "_queue", asyncio.Queue())
        mp.setattr(E, "_queue", asyncio.Queue())              # настоящий приём ИИ: тело ложится в очередь, цикл гоним руками

    def _recorder(self, name):
        def record(payload):
            self.seen[name] = [str(m.get("type")) for m in payload.get("messages") or []]
        return record

    async def post(self, *messages):
        payload = {"messages": list(messages)}
        out = await self.webhooks.wazzup_webhook("s", _FakeRequest(payload))
        assert out == {"ok": True}
        return payload

    def forwarded(self) -> list[dict]:
        """Что ушло бы в панель: тело из очереди пересылки, прогнанное через JSON, как при POST."""
        out = []
        while not wazzup_forward._queue.empty():
            out.append(json.loads(json.dumps(wazzup_forward._queue.get_nowait())))
        return out

    async def ingest_ai(self):
        """Один оборот приёмного цикла ИИ: всё, что вебхук положил в очередь, разбирается."""
        while not E._queue.empty():
            kind, item = E._queue.get_nowait()
            assert kind == "wazzup"
            await E._handle_payload(item)

    async def turn_texts(self):
        await self.env.advance(1)
        await self.env.advance(120)
        assert len(self.env.turn_requests) == 1, "ход не ушёл сервису"
        return [m["text"] for m in self.env.turn_requests[0]["messages"]]


@pytest.fixture
def world(env, monkeypatch):
    return World(env, monkeypatch)


def sticker_doc(env, mid="doc-1", **over):
    m = env.msg("", typ="document", mid=mid)
    m["contentUri"] = URL
    m.update(over)
    return m


# =============================================================================================
# стикер-«документ»: везде sticker
# =============================================================================================

def test_a_sticker_sent_as_a_document_is_a_sticker_for_every_consumer(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env))
        assert world.net.requests and [r.method for r in world.net.requests] == ["HEAD"]
        assert world.seen == {
            "wazzup_sla.handle_webhook": ["sticker"], "wazzup_delivery.handle_webhook": ["sticker"],
            "academy_invite_delivery.record_webhook": ["sticker"], "autopilot.on_wazzup": ["sticker"],
            "retail_lead_guard.on_wazzup": ["sticker"],
        }
        (to_panel,) = world.forwarded()
        sent = to_panel["messages"][0]
        assert sent["type"] == "sticker" and sent["originalType"] == "document"       # в панели wazzup_message.msg_type = sticker
        assert sent["messageId"] == "doc-1" and sent["chatId"] == CHAT and sent["text"] == ""
        assert sent["contentUri"] == URL                                              # остальное тело как пришло
        await world.ingest_ai()
        rows = store.pending_inbox(CHAT_KEY)
        assert [(r["message_id"], r["msg_type"], r["text"]) for r in rows] == [("doc-1", "sticker", "")]
    run(go())


def test_the_ai_turn_gets_the_sticker_line_not_the_file_line(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env))
        await world.ingest_ai()
        assert await world.turn_texts() == [STICKER_LINE]
        assert store.get_owner(DEAL)["owner"] == "ai" and env.alerts == []             # как по любому вложению: сделку ведёт ИИ
    run(go())


def test_a_real_sticker_gives_the_same_line_and_the_same_inbox_row_without_the_network(world, env):
    """Контрольный образец: настоящий `sticker` от Wazzup. Строка в ход и строка в inbox те же, что у определённого по
    заголовкам (см. тест выше: те же `STICKER_LINE` и `sticker`), а сеть не нужна."""
    async def go():
        env.lead()
        await world.post(env.msg("", typ="sticker", mid="real-1"))
        await world.ingest_ai()
        assert [(r["msg_type"], r["text"]) for r in store.pending_inbox(CHAT_KEY)] == [("sticker", "")]
        assert await world.turn_texts() == [STICKER_LINE] and world.net.requests == []
    run(go())


def test_a_sticker_with_the_document_wording_is_gone_from_the_turn(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env))
        await world.ingest_ai()
        assert FILE_LINE not in await world.turn_texts()
    run(go())


def test_a_video_sticker_by_file_name_works_the_same(world, env):
    async def go():
        env.lead()
        world.net.answer(200, **{"Content-Type": "video/webm", "Content-Disposition": "attachment; filename=VideoSticker.webm"})
        await world.post(sticker_doc(env))
        await world.ingest_ai()
        assert await world.turn_texts() == [STICKER_LINE]
        assert world.forwarded()[0]["messages"][0]["type"] == "sticker"
    run(go())


def test_the_same_sticker_link_costs_one_head_for_all_messages(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env, mid="doc-1"))
        await world.post(sticker_doc(env, mid="doc-2"))
        assert len(world.net.requests) == 1
        assert [m["messages"][0]["type"] for m in world.forwarded()] == ["sticker", "sticker"]
    run(go())


# =============================================================================================
# настоящие файлы и подписи остаются как были
# =============================================================================================

def test_a_real_pdf_stays_a_file_everywhere(world, env):
    async def go():
        env.lead()
        world.net.answer(200, **PDF)
        await world.post(sticker_doc(env))
        assert set(sum(world.seen.values(), [])) == {"document"}
        sent = world.forwarded()[0]["messages"][0]
        assert sent["type"] == "document" and "originalType" not in sent
        await world.ingest_ai()
        assert [r["msg_type"] for r in store.pending_inbox(CHAT_KEY)] == ["document"]
        assert await world.turn_texts() == [FILE_LINE]
    run(go())


def test_a_document_with_a_caption_is_not_checked_and_keeps_its_caption(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env, text="вот чек за заказ"))
        assert world.net.requests == []
        assert world.forwarded()[0]["messages"][0]["type"] == "document"
        await world.ingest_ai()
        assert await world.turn_texts() == [f"{FILE_LINE} Подпись клиента: вот чек за заказ"]
    run(go())


def test_an_outgoing_document_is_not_checked(world, env):
    async def go():
        env.lead()
        await world.post(sticker_doc(env, isEcho=True, status="sent", authorName="Менеджер"))
        assert world.net.requests == []
        assert world.forwarded()[0]["messages"][0]["type"] == "document"
        assert world.seen["wazzup_delivery.handle_webhook"] == ["document"]
    run(go())


def test_a_plain_text_webhook_is_untouched_and_needs_no_network(world, env):
    async def go():
        env.lead()
        await world.post(env.msg("здравствуйте", mid="t-1"))
        assert world.net.requests == []
        assert world.forwarded()[0]["messages"][0]["type"] == "text"
        await world.ingest_ai()
        assert await world.turn_texts() == ["здравствуйте"]
    run(go())


# =============================================================================================
# сбой хранилища: сообщение цело, вебхук не стоит дольше таймаута
# =============================================================================================

@pytest.mark.parametrize("reply", [
    lambda r: httpx.ConnectError("нет связи", request=r),
    lambda r: httpx.Response(405),
    lambda r: httpx.Response(503),
], ids=["network", "405", "503"])
def test_a_store_failure_keeps_the_message_as_a_file(world, env, reply):
    async def go():
        env.lead()
        world.net.reply = lambda request: reply(request)
        await world.post(sticker_doc(env))
        assert world.forwarded()[0]["messages"][0]["type"] == "document"
        await world.ingest_ai()
        assert await world.turn_texts() == [FILE_LINE]                # ровно как было до определения
    run(go())


def test_a_hanging_store_delays_the_webhook_by_the_timeout_at_most_and_loses_nothing(world, env, monkeypatch):
    async def go():
        env.lead()
        monkeypatch.setattr(S, "HEAD_TIMEOUT_S", 0.3)

        async def hang(request):
            await asyncio.sleep(60)
        world.net.reply = hang
        t0 = time.monotonic()
        await world.post(sticker_doc(env))
        assert time.monotonic() - t0 < 3.0
        assert world.forwarded()[0]["messages"][0]["type"] == "document"
        assert set(sum(world.seen.values(), [])) == {"document"}      # все пятеро получили сообщение
        await world.ingest_ai()
        assert await world.turn_texts() == [FILE_LINE]
    run(go())


def test_the_kill_switch_gives_the_old_behaviour(world, env, monkeypatch):
    async def go():
        env.lead()
        monkeypatch.setattr(S, "ENABLED", False)
        await world.post(sticker_doc(env))
        assert world.net.requests == []
        await world.ingest_ai()
        assert await world.turn_texts() == [FILE_LINE]
    run(go())


# =============================================================================================
# место врезки
# =============================================================================================

def test_the_detection_stands_in_the_webhook_before_every_consumer_and_is_isolated():
    import webhooks
    src = inspect.getsource(webhooks.wazzup_webhook)
    i = src.index("wazzup_sticker.normalize_payload")
    for consumer in ("wazzup_sla.handle_webhook", "wazzup_delivery.handle_webhook", "academy_invite_delivery.record_webhook",
                     "wazzup_forward.enqueue", "autopilot.on_wazzup", "retail_lead_guard.on_wazzup", "ai_engine.on_wazzup"):
        assert i < src.index(consumer), f"определение стикера должно стоять раньше {consumer}"
    before = src[:i].rsplit("\n", 3)[-3:]
    assert any("try:" in line for line in before)
    assert "except Exception" in src[i:i + 200]
    assert src.count("wazzup_sticker.normalize_payload") == 1            # одно место, не по одному на потребителя


def test_nobody_else_calls_the_detection():
    """Единое место: ни приём ИИ, ни пересылка, ни SLA не определяют тип сами (иначе у соседей расходились бы ответы)."""
    for module in ("ai_engine", "wazzup_forward", "wazzup_sla", "wazzup_delivery", "autopilot", "retail_lead_guard", "sla_filter"):
        src = inspect.getsource(__import__(module))
        assert "wazzup_sticker" not in src, module
