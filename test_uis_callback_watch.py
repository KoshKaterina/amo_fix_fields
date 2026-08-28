"""Счётчик перезвонов: час прошёл, клиенту так и не перезвонили (выбор Кати 28.08.2026).

Второй контур по пропущенным звонкам. В чат ОП уходит СОБЫТИЕ («перезвоните»), сюда -
ПРОВАЛ («не перезвонили»), и адресат другой: группа руководства.

Защищаем обещания, на которых держится доверие к этому чату:
  • перезвонили - руководство молчит, даже если тег снял не менеджер, а автоматика;
  • amo не ответил - НЕ считаем это провалом и пробуем ещё раз;
  • без сделки не сторожим совсем: проверить перезвон не по чему, гадать нельзя;
  • ночью счётчик стоит - звонок в 19:50 не будит руководителя в 20:50;
  • в тексте имя человека, а не @ник и не ID.

Запуск: python3 -m pytest test_uis_callback_watch.py -q
"""

import asyncio
import datetime
import sys
import types


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)
_stub("telegram_bot", send_alert=None)

import uis_missed_call as U  # noqa: E402
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME  # noqa: E402

ROP_CHAT = -5358037627
EGOR = 13929334
_ORIG_IN_WINDOW = U._in_window
# amo_service тут НАСТОЯЩИЙ (импортируется без сети), подменяем только функции и
# возвращаем их обратно. Заглушка целым модулем ломала бы сборку соседних тестов:
# test_office_transfer и другие работают с живым модулем.
_ORIG_GET_LEAD = U.amo_service.get_lead_full
_ORIG_HAS_TAG = U.amo_service.has_tag


def setup_function(_=None):
    U._callback_pending.clear()
    U.ROP_CHAT_ID = ROP_CHAT
    U._in_window = lambda now=None: True
    U.amo_service.get_lead_full = _ORIG_GET_LEAD
    U.amo_service.has_tag = _ORIG_HAS_TAG


def _catch_sends():
    sent = []

    async def fake_send(text, **kw):
        sent.append((text, kw.get("chat_id")))
        return True

    U.telegram_bot.send_alert = fake_send
    return sent


def _lead_with(tags):
    """Сделка с набором тегов + честная реализация has_tag поверх неё."""
    lead = {"id": 36500001, "_tags": set(tags)}

    async def get_lead_full(lead_id, with_=()):
        return lead

    U.amo_service.get_lead_full = get_lead_full
    U.amo_service.has_tag = lambda entity, name: name in (entity or {}).get("_tags", set())
    return lead


def _waiting(minutes_ago=61, lead_id=36500001, responsible=EGOR):
    U._watch_callback(lead_id, "+79990000000", "Иван Петров", responsible)
    st = U._callback_pending.get(lead_id)
    if st is not None:
        st["since"] -= minutes_ago * 60
    return st


# ─────────────────────────── кого вообще сторожим ───────────────────────────


def test_no_lead_no_watch():
    """Пропущенный без сделки не сторожим: перезвон проверять не по чему, а слать
    руководству догадку хуже, чем промолчать."""
    U._watch_callback(None, "+79990000000", "Иван", EGOR)
    assert U._callback_pending == {}


def test_second_missed_call_does_not_reset_the_clock():
    """Клиент перезвонил сам и снова не дозвонился - счётчик идёт от ПЕРВОГО пропущенного,
    иначе настойчивый клиент бесконечно отодвигал бы эскалацию."""
    st = _waiting(minutes_ago=40)
    first = st["since"]
    U._watch_callback(36500001, "+79990000000", "Иван Петров", EGOR)
    assert U._callback_pending[36500001]["since"] == first


def test_watch_off_without_leadership_chat():
    U.ROP_CHAT_ID = None
    U._watch_callback(36500001, "+79990000000", "Иван", EGOR)
    assert U._callback_pending == {}


# ─────────────────────────── перезвонили или нет ───────────────────────────


def test_silence_for_an_hour_reaches_leadership():
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    assert len(sent) == 1
    text, chat = sent[0]
    assert chat == ROP_CHAT, "эскалация идёт в чат руководства, а не в чат ОП"
    assert "не перезвонили" in text
    assert U._callback_pending == {}, "отработанное ожидание снимается"


def test_successful_call_tag_cancels_escalation():
    """UIS сам вешает «Успешный звонок» на дозвон. Появился - руководство молчит."""
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME})
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    assert sent == []
    assert U._callback_pending == {}


def test_removed_missed_tag_cancels_escalation():
    """Тег «пропущенный» снимает unmiss_tag по факту дозвона. Его отсутствие - тоже
    признак перезвона, и второй признак нужен: UIS ставит «Успешный звонок» не всегда."""
    sent = _catch_sends()
    _lead_with(set())
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    assert sent == []


def test_amo_silence_is_not_a_verdict():
    """Не прочитали сделку - это не «не перезвонили». Ожидание остаётся, попробуем позже."""
    sent = _catch_sends()

    async def dead(lead_id, with_=()):
        return None

    U.amo_service.get_lead_full = dead
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    assert sent == []
    assert 36500001 in U._callback_pending, "ожидание нельзя терять из-за молчания amo"


def test_amo_exception_is_not_a_verdict():
    sent = _catch_sends()

    async def boom(lead_id, with_=()):
        raise RuntimeError("amo down")

    U.amo_service.get_lead_full = boom
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    assert sent == []
    assert 36500001 in U._callback_pending


# ─────────────────────────── время ───────────────────────────


def test_not_yet_an_hour_is_silent():
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(45)
    asyncio.run(U._sweep_callbacks())
    assert sent == []
    assert 36500001 in U._callback_pending


def test_outside_window_the_clock_stands_still():
    """Звонок в 19:50 не повод будить руководителя в 20:50. Ожидание доживёт до утра."""
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(61)
    U._in_window = lambda now=None: False
    asyncio.run(U._sweep_callbacks())
    assert sent == []
    assert 36500001 in U._callback_pending


def test_window_bounds():
    U._in_window = _ORIG_IN_WINDOW  # тут проверяем настоящее окно, а не заглушку
    at = lambda h: datetime.datetime(2026, 8, 28, h, 0, tzinfo=U._MSK)  # noqa: E731
    assert U._in_window(at(10)) is True
    assert U._in_window(at(19)) is True
    assert U._in_window(at(9)) is False
    assert U._in_window(at(20)) is False


def test_stale_waiting_is_dropped():
    """Сутки - потолок: позавчерашний непрозвон это работа с базой, а не срочное."""
    _lead_with({TAG_MISSED_NAME})
    _waiting(61)
    U._callback_pending[36500001]["since"] -= U._TTL_SECONDS
    asyncio.run(U._sweep_callbacks())
    assert U._callback_pending == {}


# ─────────────────────────── как это читает человек ───────────────────────────


def test_text_names_the_manager_and_hides_ids():
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(61)
    asyncio.run(U._sweep_callbacks())
    text = sent[0][0]
    assert "Егор Константинов" in text
    assert "@" not in text, "в чате руководства теги бесполезны"
    assert "·" not in text, "точка посередине запрещена (правило Кати 26.08.2026)"
    assert "Иван Петров" in text


def test_unknown_manager_is_named_in_words():
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(61, responsible=777777)
    asyncio.run(U._sweep_callbacks())
    text = sent[0][0]
    assert "менеджер не определён" in text
    assert "777777" not in text


def test_hours_are_readable_after_ninety_minutes():
    sent = _catch_sends()
    _lead_with({TAG_MISSED_NAME})
    _waiting(180)
    asyncio.run(U._sweep_callbacks())
    assert "3 часа" in sent[0][0]


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    ok = 0
    for fn in fns:
        setup_function()
        try:
            fn()
            print(f"✅ {fn.__name__}")
            ok += 1
        except Exception:
            print(f"❌ {fn.__name__}")
            traceback.print_exc()
    print(f"\n{ok}/{len(fns)} прошли")
