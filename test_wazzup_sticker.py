"""Определение стикеров Telegram среди вложений `document` (wazzup_sticker.py).

Wazzup присылает анимированный стикер как `type: "document"` без подписи и без имени файла в ссылке; HEAD по ссылке
называет вещь заголовками. Проверяем правила решения Кати 23.11.2026:

  • стикер: `Content-Type: application/x-tgsticker` ИЛИ имя файла в `Content-Disposition` содержит `sticker`
    (`AnimatedSticker.tgs`, `VideoSticker.webm`, без учёта регистра); всё остальное остаётся `document`;
  • любой сбой (таймаут, сеть, 403/405/404, 5xx, нет заголовков, чужой хост) = `document`, исключение наружу не выходит;
  • только HEAD (тело не скачивается), один запрос на ссылку (кэш, ограниченный), сбой не запоминается;
  • проверяется только входящее `document` без подписи; с подписью и исходящие не трогаем;
  • ссылка и полный идентификатор сообщения в лог не попадают.

Сети нет: HTTP подставной (`httpx.MockTransport` через `wazzup_sticker._transport`), ссылки выдуманные.
Запуск: python -m pytest test_wazzup_sticker.py -q
"""
import asyncio
import logging
import time
from collections import OrderedDict

import httpx
import pytest

import sla_filter
import wazzup_sticker as S

URL = "https://store.wazzup24.com/00000000-0000-4000-8000-0000000000a1"
URL2 = "https://store.wazzup24.com/00000000-0000-4000-8000-0000000000a2"
MID = "9f3c1b7e-0000-4000-8000-00000000beef"
TGS = {"Content-Type": "application/x-tgsticker", "Content-Length": "21878",
       "Content-Disposition": "attachment; filename*=UTF-8''AnimatedSticker.tgs"}


class Net:
    """Подставной Wazzup store: пишет запросы, ответ задаёт `reply` (функция запроса, может быть async)."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.reply = lambda request: httpx.Response(200, headers=TGS)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        out = self.reply(request)
        if asyncio.iscoroutine(out):
            out = await out
        if isinstance(out, Exception):
            raise out
        return out

    def answer(self, status=200, **headers):
        self.reply = lambda request: httpx.Response(status, headers=headers)


@pytest.fixture
def net(monkeypatch):
    n = Net()
    monkeypatch.setattr(S, "_transport", httpx.MockTransport(n.handler))
    monkeypatch.setattr(S, "_cache", OrderedDict())
    monkeypatch.setattr(S, "ENABLED", True)
    return n


def run(coro):
    return asyncio.run(coro)


def doc(text="", uri=URL, mid=MID, **over):
    m = {"messageId": mid, "channelId": "ch", "chatId": "1920391385", "chatType": "telegram", "type": "document",
         "text": text, "contentUri": uri, "isEcho": False, "status": "inbound"}
    m.update(over)
    return m


def normalize(*messages):
    payload = {"messages": list(messages)}
    return run(S.normalize_payload(payload)), payload


# =============================================================================================
# что считается стикером
# =============================================================================================

def test_content_type_of_tgsticker_makes_the_document_a_sticker(net):
    net.answer(200, **{"Content-Type": "application/x-tgsticker"})
    changed, payload = normalize(doc())
    m = payload["messages"][0]
    assert changed == 1 and m["type"] == "sticker" and m["originalType"] == "document"
    assert len(net.requests) == 1


def test_the_real_prod_answer_is_a_sticker(net):
    changed, payload = normalize(doc())                          # заголовки со слов проверки на проде 23.11.2026
    assert changed == 1 and payload["messages"][0]["type"] == "sticker"


@pytest.mark.parametrize("disposition", [
    "attachment; filename*=UTF-8''AnimatedSticker.tgs",
    "attachment; filename*=UTF-8''VideoSticker.webm",
    'attachment; filename="VideoSticker.webm"',
    "attachment; filename=animatedsticker.TGS",
    "attachment; filename*=UTF-8''My%20STICKER.webp",
    'inline; filename="x.bin"; filename*=UTF-8\'\'Sticker.tgs',
])
def test_file_name_with_sticker_in_content_disposition_makes_a_sticker(net, disposition):
    net.answer(200, **{"Content-Type": "application/octet-stream", "Content-Disposition": disposition})
    changed, payload = normalize(doc())
    assert changed == 1 and payload["messages"][0]["type"] == "sticker"


@pytest.mark.parametrize("headers", [
    {"Content-Type": "application/pdf", "Content-Disposition": 'attachment; filename="invoice.pdf"'},
    {"Content-Type": "application/pdf"},
    {"Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
     "Content-Disposition": "attachment; filename*=UTF-8''%D0%A1%D1%87%D1%91%D1%82.docx"},
    {"Content-Type": "application/zip", "Content-Disposition": "attachment; filename=archive.zip"},
    {"Content-Type": "image/png"},
    {"Content-Type": "application/x-tgsticker-not"},
    {"Content-Disposition": "attachment"},                           # слово не в имени файла
    {"Content-Disposition": "attachment; sticker=1"},
    {},
])
def test_everything_else_stays_a_document(net, headers):
    net.answer(200, **headers)
    changed, payload = normalize(doc())
    m = payload["messages"][0]
    assert changed == 0 and m["type"] == "document" and "originalType" not in m


def test_content_type_parameters_and_case_do_not_matter(net):
    net.answer(200, **{"Content-Type": "Application/X-TgSticker; charset=binary"})
    changed, payload = normalize(doc())
    assert changed == 1


def test_header_rules_as_a_pure_function():
    assert S.is_sticker_headers("application/x-tgsticker", None) == (True, "content-type")
    assert S.is_sticker_headers(None, "attachment; filename=VideoSticker.webm") == (True, "имя файла")
    assert S.is_sticker_headers("application/pdf", 'attachment; filename="a.pdf"')[0] is False
    assert S.is_sticker_headers(None, None)[0] is False


# =============================================================================================
# сбой всегда значит «остаётся document»
# =============================================================================================

@pytest.mark.parametrize("reply", [
    lambda r: httpx.ReadTimeout("таймаут", request=r),
    lambda r: httpx.ConnectError("нет связи", request=r),
    lambda r: RuntimeError("что угодно"),
    lambda r: httpx.Response(405, headers=TGS),
    lambda r: httpx.Response(403, headers=TGS),
    lambda r: httpx.Response(404),
    lambda r: httpx.Response(500, headers=TGS),
    lambda r: httpx.Response(503),
    lambda r: httpx.Response(301, headers={"Location": "https://other.example/x", **TGS}),
    lambda r: httpx.Response(204, headers=TGS),
], ids=["timeout", "connect", "other", "405", "403", "404", "500", "503", "redirect", "204"])
def test_any_failure_leaves_the_message_a_document(net, reply):
    net.reply = lambda request: reply(request)
    changed, payload = normalize(doc())
    m = payload["messages"][0]
    assert changed == 0 and m["type"] == "document"
    assert m["text"] == "" and m["contentUri"] == URL and m["messageId"] == MID        # сообщение цело


def test_a_hanging_store_is_cut_by_the_timeout(net, monkeypatch):
    monkeypatch.setattr(S, "HEAD_TIMEOUT_S", 0.2)

    async def hang(request):
        await asyncio.sleep(30)
    net.reply = hang
    t0 = time.monotonic()
    changed, payload = normalize(doc(), doc(uri=URL2, mid="2-" + MID))
    assert time.monotonic() - t0 < 2.0                              # два зависших ответа ждём вместе, а не по очереди
    assert changed == 0 and [m["type"] for m in payload["messages"]] == ["document", "document"]


def test_the_timeout_is_three_seconds_and_the_cache_is_five_hundred():
    assert S.HEAD_TIMEOUT_S == 3.0 and S.CACHE_MAX == 500


# =============================================================================================
# только HEAD, только нужные сообщения
# =============================================================================================

def test_only_head_is_sent_and_the_body_is_never_asked_for(net):
    normalize(doc())
    assert [r.method for r in net.requests] == ["HEAD"]
    assert str(net.requests[0].url) == URL
    assert "range" not in net.requests[0].headers and net.requests[0].content == b""


def test_a_caption_means_no_detection_at_all(net):
    changed, payload = normalize(doc(text="вот счёт на оплату"))
    assert changed == 0 and net.requests == [] and payload["messages"][0]["type"] == "document"


@pytest.mark.parametrize("text", [None, "", "   ", "\n"])
def test_an_empty_caption_is_still_a_candidate(net, text):
    changed, payload = normalize(doc(text=text))
    assert changed == 1 and len(net.requests) == 1


def test_outgoing_messages_are_left_alone(net):
    echo = doc(isEcho=True, status="sent")
    status_only = doc(isEcho=None, status="delivered")
    changed, payload = normalize(echo, status_only)
    assert changed == 0 and net.requests == []
    assert [m["type"] for m in payload["messages"]] == ["document", "document"]


@pytest.mark.parametrize("typ", ["image", "video", "audio", "text", "sticker", "unsupported", "vcard", "", None])
def test_other_types_are_not_checked(net, typ):
    changed, payload = normalize(doc(type=typ))
    assert changed == 0 and net.requests == []


def test_the_type_is_matched_without_case_and_spaces(net):
    changed, payload = normalize(doc(type=" Document "))
    assert changed == 1 and payload["messages"][0]["type"] == "sticker"


@pytest.mark.parametrize("uri", [None, "", "   ", 0, ["https://store.wazzup24.com/x"], {"u": 1}])
def test_a_message_without_a_usable_link_is_not_checked(net, uri):
    m = doc()
    m["contentUri"] = uri
    changed, payload = normalize(m)
    assert changed == 0 and net.requests == []


@pytest.mark.parametrize("uri", [
    "https://evil.example/stickers/1",
    "http://127.0.0.1:8000/x",
    "http://169.254.169.254/latest/meta-data",
    "https://wazzup24.com.evil.example/x",
    "https://evilwazzup24.com/x",
    "file:///etc/passwd",
    "ftp://store.wazzup24.com/x",
    "https:///nohost",
    "not a url",
])
def test_only_the_wazzup_storage_is_asked(net, uri):
    changed, payload = normalize(doc(uri=uri))
    assert changed == 0 and net.requests == [] and payload["messages"][0]["type"] == "document"


def test_the_apex_domain_and_subdomains_of_the_storage_are_allowed(net):
    for uri in ("https://wazzup24.com/x", "https://store.wazzup24.com/x", "https://a.b.wazzup24.com/x"):
        S._cache.clear()
        changed, _ = normalize(doc(uri=uri))
        assert changed == 1


def test_the_kill_switch_turns_the_whole_thing_off(net, monkeypatch):
    monkeypatch.setattr(S, "ENABLED", False)
    changed, payload = normalize(doc())
    assert changed == 0 and net.requests == [] and payload["messages"][0]["type"] == "document"


@pytest.mark.parametrize("payload", [None, [], "x", 5, {}, {"messages": None}, {"messages": "x"}, {"messages": [None, 5, "x", {}, []]},
                                     {"statuses": [{"messageId": "a", "status": "read"}]}])
def test_junk_bodies_do_not_raise(net, payload):
    assert run(S.normalize_payload(payload)) == 0
    assert net.requests == []


def test_a_body_without_candidates_does_not_wait_at_all(net, monkeypatch):
    async def never(uri):
        raise AssertionError("сеть не нужна")
    monkeypatch.setattr(S, "_head", never)
    changed, _ = normalize({"messageId": "t", "type": "text", "text": "привет", "isEcho": False},
                           doc(text="с подписью"), doc(isEcho=True))
    assert changed == 0


def test_a_batch_changes_only_the_stickers(net):
    def reply(request):
        if str(request.url) == URL2:
            return httpx.Response(200, headers={"Content-Type": "application/pdf"})
        return httpx.Response(200, headers=TGS)
    net.reply = reply
    text = {"messageId": "t-1", "type": "text", "text": "привет", "isEcho": False}
    changed, payload = normalize(doc(), doc(uri=URL2, mid="b-" + MID), text, doc(text="чек"))
    assert changed == 1
    assert [m["type"] for m in payload["messages"]] == ["sticker", "document", "text", "document"]


# =============================================================================================
# кэш: один запрос на ссылку, ограниченный, сбой не запоминается
# =============================================================================================

def test_the_same_link_is_asked_once(net):
    changed, payload = normalize(doc(mid="m-1"))
    changed2, payload2 = normalize(doc(mid="m-2"))
    assert changed == 1 and changed2 == 1 and len(net.requests) == 1
    assert payload2["messages"][0]["type"] == "sticker"


def test_the_same_link_in_the_cache_also_remembers_that_it_is_not_a_sticker(net):
    net.answer(200, **{"Content-Type": "application/pdf"})
    normalize(doc())
    changed, _ = normalize(doc(mid="m-2"))
    assert changed == 0 and len(net.requests) == 1


@pytest.mark.parametrize("status", [403, 404, 405])
def test_a_definite_refusal_is_remembered_too(net, status):
    net.answer(status)
    normalize(doc())
    normalize(doc(mid="m-2"))
    assert len(net.requests) == 1


@pytest.mark.parametrize("reply", [
    lambda r: httpx.ReadTimeout("таймаут", request=r),
    lambda r: httpx.ConnectError("нет связи", request=r),
    lambda r: httpx.Response(502),
], ids=["timeout", "connect", "502"])
def test_a_failure_is_not_remembered_and_the_next_try_can_succeed(net, reply):
    net.reply = lambda request: reply(request)
    changed, _ = normalize(doc())
    assert changed == 0 and len(net.requests) == 1
    net.reply = lambda request: httpx.Response(200, headers=TGS)               # хранилище ожило, Wazzup повторил вебхук
    changed, payload = normalize(doc())
    assert changed == 1 and len(net.requests) == 2 and payload["messages"][0]["type"] == "sticker"


def test_the_cache_is_bounded_and_drops_the_oldest(net, monkeypatch):
    monkeypatch.setattr(S, "CACHE_MAX", 3)
    links = [f"https://store.wazzup24.com/{i}" for i in range(5)]
    for link in links:
        normalize(doc(uri=link))
    assert len(net.requests) == 5 and len(S._cache) == 3
    assert list(S._cache) == links[2:]
    normalize(doc(uri=links[4]))                                  # свежая ссылка ещё в кэше
    assert len(net.requests) == 5
    normalize(doc(uri=links[0]))                                  # самая старая вытеснена: запрос ещё раз
    assert len(net.requests) == 6


def test_a_cache_hit_refreshes_the_entry(net, monkeypatch):
    monkeypatch.setattr(S, "CACHE_MAX", 2)
    a, b, c = (f"https://store.wazzup24.com/{x}" for x in "abc")
    normalize(doc(uri=a))
    normalize(doc(uri=b))
    normalize(doc(uri=a))                                         # a снова свежая
    normalize(doc(uri=c))                                         # вытесняется b, а не a
    assert set(S._cache) == {a, c}


# =============================================================================================
# лог и соседи
# =============================================================================================

def test_the_log_has_the_result_and_a_short_message_id_but_no_link(net, caplog):
    with caplog.at_level(logging.INFO, logger="uvicorn"):
        normalize(doc())
        net.answer(200, **{"Content-Type": "application/pdf"})
        normalize(doc(uri=URL2, mid="77777777-1111-4000-8000-000000000001"))
    text = caplog.text
    assert "document -> sticker" in text and "остался document" in text
    assert MID[:8] in text and "77777777" in text
    assert MID not in text and "0000000000a1" not in text and "0000000000a2" not in text and "wazzup24" not in text


def test_a_failure_is_logged_without_the_link(net, caplog):
    net.reply = lambda request: httpx.ConnectError("нет связи с store.wazzup24.com", request=request)
    with caplog.at_level(logging.INFO, logger="uvicorn"):
        normalize(doc())
    assert "сбой ConnectError" in caplog.text and "wazzup24" not in caplog.text and "0000000000a1" not in caplog.text


def test_sla_filter_treats_a_sticker_exactly_like_the_document_it_was(net):
    """Тип `sticker` не в списке молчаливых типов SLA: тихо подмена на ожидание клиента не влияет."""
    before = sla_filter.is_closing_message({"type": "document", "text": ""})
    after = sla_filter.is_closing_message({"type": "sticker", "text": ""})
    assert before == after == (False, "")
