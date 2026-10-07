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

⚠️ ЭТОТ ФАЙЛ БЫЛ ВЫВЕДЕН ИЗ СБОРА, и вот почему. Он был скриптом: сценарии выполнялись на
ИМПОРТЕ, а ветка отказа зовёт `_fresh_tags`, который дочитывает теги сделки из amoCRM
(`amo_service.get_lead_full`). Этого вызова в заглушках не было - значит сбор тестов уходил
в СЕТЬ и вставал. `pytest --collect-only` по репозиторию не укладывался в 600 секунд, и
07.10.2026 файл пришлось записать в `collect_ignore` в conftest.py.

Теперь он честный pytest-модуль, а `get_lead_full` подменён заглушкой - сети нет ни на
импорте, ни в тестах. Строка в `collect_ignore` снята.
"""

import asyncio

import pytest
import waybill_service

LEAD_ID = 36532789


@pytest.fixture(autouse=True)
def _short_poll_window():
    """Сжать окно ожидания до долей секунды - и ВЕРНУТЬ прежние значения.

    ⚠️ Возврат обязателен. Это константы модуля `waybill_service`, общего на весь процесс.
    Пока файл был скриптом, подмена на уровне модуля никому не мешала. Под pytest все файлы
    живут в одном процессе, а импортируются ВСЕ до начала прогона - то есть окно 0,05 с
    действовало бы и у соседей. `test_waybill_intl_recipient.py` из-за этого падал четырьмя
    тестами: его опрос номера СДЭК сдавался раньше, чем приходил ответ (поймано 07.10.2026).
    """
    was_seconds = waybill_service.WAYBILL_BACKGROUND_POLL_SECONDS
    was_interval = waybill_service.WAYBILL_BACKGROUND_POLL_INTERVAL_S
    waybill_service.WAYBILL_BACKGROUND_POLL_SECONDS = 0.05
    waybill_service.WAYBILL_BACKGROUND_POLL_INTERVAL_S = 0.01
    try:
        yield
    finally:
        waybill_service.WAYBILL_BACKGROUND_POLL_SECONDS = was_seconds
        waybill_service.WAYBILL_BACKGROUND_POLL_INTERVAL_S = was_interval


_patched: list = []
_notes: list = []
_alerted: list = []
_commits: list = []


def _tags():
    return [{"id": 1, "name": "Горячий"}]


def _still_pending():
    return {"entity": {}, "requests": [{"type": "CREATE", "state": "ACCEPTED"}]}


def _with_number(number):
    return {
        "entity": {"cdek_number": number},
        "requests": [{"type": "CREATE", "state": "SUCCESSFUL"}],
    }


def _rejected(reason):
    return {
        "entity": {},
        "requests": [{"type": "CREATE", "state": "INVALID", "errors": [{"message": reason}]}],
    }


def _stub(get_order_responses, commit_ok=True, patch_ok=True):
    _patched.clear()
    _notes.clear()
    _alerted.clear()
    _commits.clear()
    responses = list(get_order_responses)

    async def _fake_get_order(uuid):
        if not responses:
            # Пул исчерпан раньше, чем истекло окно, — считаем, что СДЭК
            # по-прежнему молчит (тот же ответ, что и «ещё не решено»).
            return _still_pending()
        return responses.pop(0)

    async def _fake_commit_waybill(lead_id, cdek_value, current_tags, **kw):
        _commits.append((lead_id, cdek_value))
        return {"ok": commit_ok}

    async def _fake_add_note(lead_id, text):
        _notes.append(text)
        return {"ok": True}

    async def _fake_patch_lead(lead_id, **kw):
        _patched.append((lead_id, kw))
        return {"ok": patch_ok}

    async def _fake_alert(text, *args, **kwargs):  # ключ события и значения панели - мимо
        _alerted.append(text)

    async def _fake_get_lead_full(lead_id, with_=()):
        # ⚠️ Именно этой заглушки тут не хватало, и из-за неё файл ходил в СЕТЬ.
        # Ветка отказа зовёт `_fresh_tags`, а тот дочитывает теги сделки из amoCRM.
        return {"id": lead_id, "_embedded": {"tags": _tags()}}

    waybill_service.cdek_client.get_order = _fake_get_order
    waybill_service.amo_service.commit_waybill = _fake_commit_waybill
    waybill_service.amo_service.add_note = _fake_add_note
    waybill_service.amo_service.patch_lead = _fake_patch_lead
    waybill_service.amo_service.get_lead_full = _fake_get_lead_full
    waybill_service._alert = _fake_alert


def test_nomer_prishel_na_vtorom_oprose_obychnyy_uspeshnyy_kommit():
    _stub([_still_pending(), _with_number("10306104834")])
    asyncio.run(waybill_service._resolve_pending_order(
        LEAD_ID, "f109cdab-uuid", "webhook", _tags(), False,
    ))

    assert _commits == [(LEAD_ID, "10306104834")], \
        f"должен закоммитить пришедший номер: {_commits!r}"
    assert _patched == [], "успех НЕ должен ставить тег ошибки — дубля не было, тегать нечего"
    assert not any("дубл" in a.lower() for a in _alerted), "успех не должен упоминать дубли"


def test_sdek_otklonil_v_fone_stavim_teg_i_prichinu():
    _stub([_rejected("Некорректный телефон получателя")])
    asyncio.run(waybill_service._resolve_pending_order(
        LEAD_ID, "0e230765-uuid", "webhook", _tags(), False,
    ))

    assert _commits == [], "явный отказ — коммитить нечего"
    assert len(_patched) == 1, "явный отказ должен пометить сделку тегом ошибки"
    assert any("Некорректный телефон" in n for n in _notes), \
        f"причина отказа должна попасть в примечание: {_notes!r}"
    assert not any("удалите дубль" in n.lower() for n in _notes), (
        "INVALID — реального отправления нет, удалять нечего (кейс 29-30.07, сделка 36524113)"
    )


def test_sdek_molchit_do_konca_okna_sdaemsya_bez_udalite_dubl():
    _stub([_still_pending()] * 3)
    asyncio.run(waybill_service._resolve_pending_order(
        LEAD_ID, "some-uuid", "webhook", _tags(), False,
    ))

    assert _commits == [], "молчание — коммитить нечего"
    assert len(_patched) == 1, \
        "по истечении окна тег ошибки нужен — человеку пора смотреть руками"
    assert len(_alerted) == 1
    assert "второй заказ" in _alerted[0].lower() or "дубль" in _alerted[0].lower(), \
        f"алерт должен явно сказать, что второй заказ НЕ создавался: {_alerted[0]!r}"
    assert "удалите дубль" not in _alerted[0].lower(), \
        "нечего удалять — второй заказ автоматически не создавался"


def test_retry_istochnik_ne_shumit_v_tg():
    """Тот же контракт, что у `_fail`: агрегатор /retry сам сведёт сводку."""
    _stub([_rejected("Некорректный телефон получателя")])
    asyncio.run(waybill_service._resolve_pending_order(
        LEAD_ID, "retry-uuid", "retry", _tags(), False,
    ))

    assert len(_patched) == 1, "тег ошибки ставится независимо от source"
    assert _alerted == [], "source=retry не должен слать алерт в TG"
