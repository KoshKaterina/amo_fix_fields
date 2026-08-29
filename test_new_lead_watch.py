"""Новый лид не взяли в работу: счётчик рабочего времени на входе воронки.

Третий триггер группы «ОП срочные уведомления» (выбор Кати 28.08.2026). Держим то, на
чём он стоит:

  • часы РАБОЧИЕ - лид, упавший ночью, к утру не «просрочен на десять часов»;
  • отсчёт не раньше 12:00 (ТЗ точек контроля: до полудня разгребают ночную пачку);
  • ушёл с входа - счётчик снят, даже если это случилось за минуту до порога;
  • сделка перечитывается из amo перед отправкой: вебхук о смене этапа мог не дойти;
  • amo молчит - это не приговор «не взяли».

Запуск: python3 -m pytest test_new_lead_watch.py -q
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

import new_lead_watch as N  # noqa: E402
from waybill_config import (  # noqa: E402
    PIPELINE_CLEVER_MAIN,
    STATUS_CLEVER_IN_PROGRESS,
    STATUS_NEW_LEAD,
    STATUS_NEW_LEAD_BUFFERS,
)

ROP_CHAT = -5358037627
EGOR = 13929334
LEAD = 36500777

# amo_service тут НАСТОЯЩИЙ (он импортируется без сети), подменяем только функции и
# возвращаем их обратно. Заглушка целым модулем ломала бы сборку соседних тестов:
# test_office_transfer и другие работают с живым модулем.
_ORIG_GET_LEAD = N.amo_service.get_lead_full


def setup_function(_=None):
    N._pending.clear()
    N.ROP_CHAT_ID = ROP_CHAT
    N.amo_service.get_lead_full = _ORIG_GET_LEAD


def _at(day: int, hour: int, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 8, day, hour, minute, tzinfo=N._MSK)


def _catch_sends():
    sent = []

    async def fake_send(text, **kw):
        sent.append((text, kw.get("chat_id")))
        return True

    N.telegram_bot.send_alert = fake_send
    return sent


def _lead_at_status(status_id, *, responsible=EGOR, name="Заявка с сайта"):
    async def get_lead_full(lead_id, with_=()):
        return {"id": lead_id, "status_id": status_id,
                "responsible_user_id": responsible, "name": name}

    N.amo_service.get_lead_full = get_lead_full


def _waiting_since(moment):
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    N._pending[LEAD]["since"] = moment


# ─────────────────────────── рабочее время ───────────────────────────


def test_night_does_not_count():
    """Лид упал в 23:40, сейчас 10:00 - рабочего времени ноль, а не десять часов."""
    assert N.worktime_minutes(_at(27, 23, 40), _at(28, 10, 0)) == 0


def test_morning_before_noon_does_not_count():
    """До 12:00 менеджеры разгребают ночную пачку - это время в счёт не идёт."""
    assert N.worktime_minutes(_at(28, 10, 0), _at(28, 11, 59)) == 0


def test_plain_working_hours():
    assert N.worktime_minutes(_at(28, 12, 0), _at(28, 14, 0)) == 120


def test_evening_is_cut_at_window_end():
    assert N.worktime_minutes(_at(28, 18, 0), _at(28, 23, 0)) == 60


def test_hours_add_up_across_days():
    """Лид с вечера четверга до полудня пятницы: час вечером плюс ноль утром."""
    assert N.worktime_minutes(_at(27, 18, 0), _at(28, 12, 0)) == 60


# ─────────────────────────── кого сторожим ───────────────────────────


def test_entry_stage_starts_the_clock():
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    assert LEAD in N._pending


def test_buffer_stages_count_as_entry():
    """Заявки падают не только в хаб «Новый лид», но и в четыре буферных этапа."""
    for i, status in enumerate(STATUS_NEW_LEAD_BUFFERS):
        N.note_lead(LEAD + i, PIPELINE_CLEVER_MAIN, status)
    assert len(N._pending) == len(STATUS_NEW_LEAD_BUFFERS)


def test_taking_the_lead_removes_the_clock():
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_IN_PROGRESS)
    assert N._pending == {}


def test_other_pipelines_are_not_watched():
    """ОПТ и сервисные воронки живут по своим правилам - тут только розница."""
    N.note_lead(LEAD, 10131762, STATUS_NEW_LEAD)
    assert N._pending == {}


def test_repeated_webhook_does_not_reset_the_clock():
    """amo шлёт вебхук на каждое изменение сделки. Правка поля не должна обнулять счётчик."""
    _waiting_since(_at(28, 12, 0))
    first = N._pending[LEAD]["since"]
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    assert N._pending[LEAD]["since"] == first


def test_watch_off_without_leadership_chat():
    N.ROP_CHAT_ID = None
    N.note_lead(LEAD, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    assert N._pending == {}


def test_garbage_ids_do_not_crash():
    N.note_lead("не число", PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    N.note_lead(LEAD, None, None)
    assert N._pending == {}


# ─────────────────────────── эскалация ───────────────────────────


def test_two_working_hours_reach_leadership():
    sent = _catch_sends()
    _lead_at_status(STATUS_NEW_LEAD)
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))  # вчера, рабочего времени с запасом
    asyncio.run(N._sweep())
    assert len(sent) == 1
    text, chat = sent[0]
    assert chat == ROP_CHAT
    assert "не взяли в работу" in text
    assert N._pending == {}


def test_lead_already_taken_is_not_reported():
    """Вебхук о смене этапа мог не дойти. Перед отправкой сделка перечитывается, и если
    её уже взяли - руководство молчит."""
    sent = _catch_sends()
    _lead_at_status(STATUS_CLEVER_IN_PROGRESS)
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent == []
    assert N._pending == {}


def test_amo_silence_is_not_a_verdict():
    sent = _catch_sends()

    async def dead(lead_id, with_=()):
        return None

    N.amo_service.get_lead_full = dead
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent == []
    assert LEAD in N._pending


def test_fresh_lead_is_silent():
    sent = _catch_sends()
    _lead_at_status(STATUS_NEW_LEAD)
    _waiting_since(N._now_msk())
    asyncio.run(N._sweep())
    assert sent == []
    assert LEAD in N._pending


def test_stale_waiting_is_dropped():
    _lead_at_status(STATUS_NEW_LEAD)
    _waiting_since(N._now_msk() - datetime.timedelta(hours=N._TTL_HOURS + 1))
    asyncio.run(N._sweep())
    assert N._pending == {}


# ─────────────────────────── как это читает человек ───────────────────────────


def test_message_names_the_manager_and_hides_ids():
    sent = _catch_sends()
    _lead_at_status(STATUS_NEW_LEAD)
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    text = sent[0][0]
    assert "Егор Константинов" in text
    assert "@" not in text
    assert "·" not in text
    assert "Заявка с сайта" in text


def test_unknown_manager_is_named_in_words():
    sent = _catch_sends()
    _lead_at_status(STATUS_NEW_LEAD, responsible=555555)
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert "менеджер не определён" in sent[0][0]
    assert "555555" not in sent[0][0]


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
