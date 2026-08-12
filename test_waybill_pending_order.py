"""Юнит-тест _resolve_pending_order / _commit_success в waybill_service (без сети).

Разбор 12.08.2026 (сделка 36532789, заказ 06193): 60-секундный поллинг
cdek_number считал молчание СДЭК отказом и приглашал создать ВТОРОЙ заказ
(человека через /retry или эхо-вебхук через enqueue_waybill) — оба
«протаймаутивших» заказа в реальности оказались валидными, СДЭК просто ответил
дольше 60с. Проверяем: номер пришёл позже -> коммитим как обычный успех, тег
ошибки НЕ ставим; СДЭК явно отклонил позже -> ТЕПЕРЬ ставим тег с настоящей
причиной; молчание до конца окна -> сдаёмся и алертим, но БЕЗ фразы «удалите
дубль» (дубль не создавался — раньше именно эта фраза и провоцировала его).

Окно ожидания сжато до долей секунды (реальный asyncio.sleep, без подмены) —
WAYBILL_BACKGROUND_POLL_SECONDS/_INTERVAL_S переставлены на модуле перед
запуском.
"""

import asyncio

import cdek_client
import waybill_service

waybill_service.WAYBILL_BACKGROUND_POLL_SECONDS = 0.05
waybill_service.WAYBILL_BACKGROUND_POLL_INTERVAL_S = 0.01

_patched: list = []
_notes: list = []
_alerted: list = []
_commits: list = []


def _tags():
    return [{"id": 1, "name": "Горячий"}]


def _stub(get_order_responses, commit_ok=True, patch_ok=True):
    _patched.clear()
    _notes.clear()
    _alerted.clear()
    _commits.clear()
    responses = list(get_order_responses)

    async def _fake_get_order(uuid):
        if not responses:
            return responses_default()
        return responses.pop(0)

    def responses_default():
        # Пул исчерпан раньше, чем истекло окно, — считаем, что СДЭК
        # по-прежнему молчит (тот же ответ, что и «ещё не решено»).
        return _still_pending()

    async def _fake_commit_waybill(lead_id, cdek_value, current_tags, **kw):
        _commits.append((lead_id, cdek_value))
        return {"ok": commit_ok}

    async def _fake_add_note(lead_id, text):
        _notes.append(text)
        return {"ok": True}

    async def _fake_patch_lead(lead_id, **kw):
        _patched.append((lead_id, kw))
        return {"ok": patch_ok}

    async def _fake_alert(text):
        _alerted.append(text)

    waybill_service.cdek_client.get_order = _fake_get_order
    waybill_service.amo_service.commit_waybill = _fake_commit_waybill
    waybill_service.amo_service.add_note = _fake_add_note
    waybill_service.amo_service.patch_lead = _fake_patch_lead
    waybill_service._alert = _fake_alert


def _still_pending():
    return {"entity": {}, "requests": [{"type": "CREATE", "state": "ACCEPTED"}]}


def _with_number(number):
    return {"entity": {"cdek_number": number}, "requests": [{"type": "CREATE", "state": "SUCCESSFUL"}]}


def _rejected(reason):
    return {
        "entity": {},
        "requests": [{"type": "CREATE", "state": "INVALID", "errors": [{"message": reason}]}],
    }


# --- сценарий А: номер пришёл на втором опросе -> обычный успешный коммит ---
_stub([_still_pending(), _with_number("10306104834")])
asyncio.run(waybill_service._resolve_pending_order(
    36532789, "f109cdab-uuid", "webhook", _tags(), False,
))
assert _commits == [(36532789, "10306104834")], f"должен закоммитить пришедший номер: {_commits!r}"
assert _patched == [], "успех НЕ должен ставить тег ошибки — дубля не было, тегать нечего"
assert not any("дубл" in a.lower() for a in _alerted), "успех не должен упоминать дубли"

# --- сценарий Б: СДЭК отклонил уже в фоне -> ТЕПЕРЬ ставим тег+причину ---
_stub([_rejected("Некорректный телефон получателя")])
asyncio.run(waybill_service._resolve_pending_order(
    36532789, "0e230765-uuid", "webhook", _tags(), False,
))
assert _commits == [], "явный отказ — коммитить нечего"
assert len(_patched) == 1, "явный отказ должен пометить сделку тегом ошибки"
assert any("Некорректный телефон" in n for n in _notes), f"причина отказа должна попасть в примечание: {_notes!r}"
assert not any("удалите дубль" in n.lower() for n in _notes), (
    "INVALID — реального отправления нет, удалять нечего (кейс 29-30.07, сделка 36524113)"
)

# --- сценарий В: СДЭК молчит до конца окна -> сдаёмся, но без «удалите дубль» ---
_stub([_still_pending()] * 3)
asyncio.run(waybill_service._resolve_pending_order(
    36532789, "some-uuid", "webhook", _tags(), False,
))
assert _commits == [], "молчание — коммитить нечего"
assert len(_patched) == 1, "по истечении окна тег ошибки нужен — человеку пора смотреть руками"
assert len(_alerted) == 1
assert "второй заказ" in _alerted[0].lower() or "дубль" in _alerted[0].lower(), (
    f"алерт должен явно сказать, что второй заказ НЕ создавался: {_alerted[0]!r}"
)
assert "удалите дубль" not in _alerted[0].lower(), "нечего удалять — второй заказ автоматически не создавался"

# --- сценарий Г: retry-источник не должен шуметь в TG (тот же контракт, что у _fail) ---
_stub([_rejected("Некорректный телефон получателя")])
asyncio.run(waybill_service._resolve_pending_order(
    36532789, "retry-uuid", "retry", _tags(), False,
))
assert len(_patched) == 1, "тег ошибки ставится независимо от source"
assert _alerted == [], "source=retry не должен слать алерт в TG (агрегатор /retry сам сведёт сводку)"

print("waybill_service _resolve_pending_order (не дождался ответа СДЭК -> не дублируем): все тесты прошли")
