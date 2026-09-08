"""Авто-режим: правила, на которых он стоит.

Проверяем ровно то, ошибка в чём означает сообщение живому человеку или сделку, уехавшую
не туда:

  • гейт от повторного вебхука - вторая заявка на ту же пару «сделка и этап» отбивается;
  • две отметки запуска - запись «попытка была, подтверждения нет» находится и НЕ
    перезапускается;
  • «не дошло» - это когда отбили ВСЕ каналы, а не первый: бот перебирает каналы, и жалоба
    первого при доставленном втором уже однажды увела нас в ложный вывод (08.09.2026);
  • у Telegram статуса «доставлено» не бывает вовсе, там успех это «отправлено»;
  • сравнение ответа съедает «ё» - ловушка «Да, все верно» стоила живого кейса 23.08.2026;
  • условия считаются слева направо, как обещано экраном, а не «сначала все И»;
  • пустые часы работы значат «никогда», а не «круглосуточно»;
  • спящая сделка просыпается в начало ближайшего промежутка, а не «завтра».

Запуск без сети и без базы:  python -m pytest test_autopilot.py -q
"""

import asyncio
import datetime
import os
import sys
import tempfile
import types

import pytest


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)

_SENT: list[dict] = []


async def _fake_send(text, parse_mode=None, chat_id=None, message_thread_id=None):
    _SENT.append({"text": text, "chat_id": chat_id})
    return True


_stub("telegram_bot", send_alert=_fake_send)

# База движка - во временный файл: тест не должен трогать боевое состояние в /app/var.
os.environ["AUTOPILOT_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "autopilot_test.sqlite3")

import autopilot as A  # noqa: E402
import autopilot_settings_client as SC  # noqa: E402
import autopilot_store as S  # noqa: E402

_MSK = datetime.timezone(datetime.timedelta(hours=3))


def _settings(**over):
    base = {
        "settings": {"mode": "test", "work_hours": [{"start": "10:00", "end": "19:00"}]},
        "pipeline_id": 8642414,
        "route": [],
        "test_contact_ids": [48594653],
    }
    base.update(over)
    SC._cache = base
    return base


@pytest.fixture(autouse=True)
def _clean():
    _SENT.clear()
    A.reset_hour_bucket()
    _settings()
    yield


# ── хранилище: гейт от повторного вебхука ───────────────────────────────────────

def test_claim_takes_pair_once():
    """Главный гейт. `/lead_change` приходит на любое изменение сделки, и без этого робот
    перезапускал бы бота на каждый чих."""
    S.init()
    assert S.claim(101, 555, 900) is True
    assert S.claim(101, 555, 900) is False
    # Другой этап той же сделки - это другая работа, её брать можно.
    assert S.claim(101, 556, 900) is True


def test_unfinished_launch_is_found_and_not_restarted():
    S.init()
    S.claim(202, 555, 900)
    S.mark_launch_attempted(202, 555, 7131)
    rows = S.list_unfinished_launches()
    assert [r["lead_id"] for r in rows] == [202]

    S.mark_launch_ok(202, 555, chat_id="79001234567")
    assert S.list_unfinished_launches() == []


def test_finished_launch_moves_to_delivery_phase():
    S.init()
    S.claim(203, 555, 900)
    S.mark_launch_attempted(203, 555, 7131)
    S.mark_launch_ok(203, 555, chat_id="79007654321")
    row = S.get(203, 555)
    assert row["phase"] == S.PHASE_DELIVERY
    assert row["chat_id"] == "79007654321"
    assert [r["lead_id"] for r in S.find_by_chat("79007654321")] == [203]


def test_update_rejects_unknown_field():
    """Опечатка в имени колонки иначе тихо ничего не сделает, а искать её будут в логике."""
    S.init()
    S.claim(204, 555, 900)
    with pytest.raises(ValueError):
        S.update(204, 555, phaze="delivery")


def test_drop_lead_removes_all_its_stages():
    S.init()
    S.claim(205, 555, 900)
    S.claim(205, 556, 900)
    assert S.drop_lead(205) == 2


# ── доставка ────────────────────────────────────────────────────────────────────

WAIT = 15 * 60  # окно ожидания доставки, секунд


def test_one_channel_refused_is_not_a_failure_yet():
    """Главное. Бот перебирает каналы: телеграм отбил - ватсап ещё может доставить. Пока
    окно не вышло, отказ ОДНОГО канала это «ждём», а не «не дошло». Ровно на этом мы
    ошиблись 08.09.2026, прочитав жалобу первого канала как приговор."""
    statuses = [{"status": "error", "chatType": "telegram"}]
    assert A.delivery_verdict(statuses, waited_s=60, wait_limit_s=WAIT) == A.VERDICT_WAIT


def test_success_on_any_channel_wins_over_refusal():
    statuses = [
        {"status": "error", "chatType": "telegram"},
        {"status": "delivered", "chatType": "whatsapp"},
    ]
    assert A.delivery_verdict(statuses, waited_s=1, wait_limit_s=WAIT) == A.VERDICT_OK
    assert A.delivery_ok(statuses) is True


def test_failure_only_after_the_window_closed():
    statuses = [
        {"status": "error", "chatType": "telegram"},
        {"status": "error", "chatType": "whatsapp"},
    ]
    assert A.delivery_verdict(statuses, waited_s=WAIT - 1, wait_limit_s=WAIT) == A.VERDICT_WAIT
    assert A.delivery_verdict(statuses, waited_s=WAIT, wait_limit_s=WAIT) == A.VERDICT_FAILED


def test_no_statuses_at_all_is_a_separate_outcome():
    """Ни одного статуса за всё окно - это не «не дошло», а «похоже, не отправлялось»:
    в карточке нет ни телефона, ни юзернейма, и канал до неё не работает вовсе. Человеку об
    этом надо сказать другими словами."""
    assert A.delivery_verdict([], waited_s=1, wait_limit_s=WAIT) == A.VERDICT_WAIT
    assert A.delivery_verdict([], waited_s=WAIT, wait_limit_s=WAIT) == A.VERDICT_SILENT


def test_telegram_sent_counts_as_delivered():
    """У Telegram статуса «доставлено» не бывает вовсе - ждать его значит ждать вечно."""
    statuses = [{"status": "sent", "chatType": "telegram"}]
    assert A.delivery_verdict(statuses, waited_s=1, wait_limit_s=WAIT) == A.VERDICT_OK


def test_whatsapp_sent_is_not_delivery_yet():
    """А у WhatsApp `sent` - это ещё не доставка: там `delivered` приходит, и его ждём."""
    statuses = [{"status": "sent", "chatType": "whatsapp"}]
    assert A.delivery_verdict(statuses, waited_s=1, wait_limit_s=WAIT) == A.VERDICT_WAIT


def test_delivery_note_explains_telegram_separately():
    """Иначе «отправлено» у телеграма читается как слабее «доставлено» у ватсапа, и человек
    идёт искать поломку там, где её нет."""
    note = A.delivery_note([
        {"status": "sent", "chatType": "telegram"},
        {"status": "delivered", "chatType": "whatsapp"},
    ])
    assert "телеграм не присылает" in note
    assert "WhatsApp: доставлено" in note



# ── ответ клиента ───────────────────────────────────────────────────────────────

def test_normalize_answer_eats_yo_case_and_spaces():
    assert A.normalize_answer("  Да, всё верно ") == "да, все верно"
    assert A.normalize_answer("Да, все верно") == A.normalize_answer("Да, всё верно")


def test_listed_mode_advances_only_on_listed_answers():
    bot = {"stop_mode": "listed", "stop_answers_norm": ["да, все верно"]}
    assert A.answer_decision(bot, "Да, всё верно") == "advance"
    assert A.answer_decision(bot, "а можно завтра?") == "stop"


def test_except_mode_stops_only_on_listed_answers():
    bot = {"stop_mode": "except", "stop_answers_norm": ["нет, нужно исправить"]}
    assert A.answer_decision(bot, "Нет, нужно исправить") == "stop"
    assert A.answer_decision(bot, "всё ок") == "advance"


def test_any_and_never_modes():
    assert A.answer_decision({"stop_mode": "any"}, "что угодно") == "stop"
    assert A.answer_decision({"stop_mode": "never"}, "что угодно") == "advance"


# ── условия ─────────────────────────────────────────────────────────────────────

_LEAD = {
    "id": 1,
    "responsible_user_id": 9291546,
    "source_id": 23478413,
    "custom_fields_values": [
        {"field_id": 577373, "values": [{"value": "При получении"}]},
    ],
    "_embedded": {"tags": [{"name": "Горячий"}]},
}


def test_empty_conditions_always_match():
    assert A.conditions_match(_LEAD, []) is True


def test_condition_on_custom_field():
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "cf:577373", "op": "eq", "value": "при получении"},
    ]) is True
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "cf:577373", "op": "eq", "value": "онлайн"},
    ]) is False


def test_condition_on_tag_looks_into_list():
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "tag", "op": "eq", "value": "Горячий"},
    ]) is True


def test_filled_and_not_filled():
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "cf:577373", "op": "filled", "value": None},
    ]) is True
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "cf:999999", "op": "not_filled", "value": None},
    ]) is True


def test_conditions_are_evaluated_left_to_right():
    """Экран обещает человеку «считается слева направо, скобок нет». Считать «сначала все И»
    значит соврать экрану: ложное И в начале не должно спасаться истинным ИЛИ в конце по
    правилам приоритета, которых мы не показывали."""
    conditions = [
        {"join": "and", "field": "cf:577373", "op": "eq", "value": "онлайн"},   # ложь
        {"join": "or", "field": "tag", "op": "eq", "value": "Горячий"},          # истина
        {"join": "and", "field": "cf:577373", "op": "eq", "value": "онлайн"},   # ложь
    ]
    # (ложь ИЛИ истина) И ложь = ложь
    assert A.conditions_match(_LEAD, conditions) is False


def test_unknown_field_does_not_crash_the_whole_check():
    assert A.conditions_match(_LEAD, [
        {"join": "and", "field": "cf:не_число", "op": "eq", "value": "что-то"},
    ]) is False


# ── рабочие часы ────────────────────────────────────────────────────────────────

def _at(hh, mm=0):
    return datetime.datetime(2026, 9, 8, hh, mm, tzinfo=_MSK)


def test_in_work_hours():
    assert A.in_work_hours(_at(11)) is True
    assert A.in_work_hours(_at(9, 59)) is False
    assert A.in_work_hours(_at(19)) is False  # конец промежутка не включаем


def test_empty_hours_mean_never_not_always():
    """Пустые часы - способ приостановить робота, не теряя настроек. Прочитать их как
    «круглосуточно» значит включить его ровно тогда, когда человек выключал."""
    _settings(settings={"mode": "test", "work_hours": []})
    assert A.in_work_hours(_at(11)) is False
    assert A.next_work_moment(_at(11)) is None


def test_next_work_moment_today_when_break_is_ahead():
    """Два промежутка с перерывом: в перерыве просыпаемся сегодня же, а не завтра утром."""
    _settings(settings={
        "mode": "test",
        "work_hours": [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "19:00"}],
    })
    assert A.next_work_moment(_at(13, 30)) == _at(14)


def test_next_work_moment_tomorrow_when_day_is_over():
    got = A.next_work_moment(_at(22))
    assert got.date() == datetime.date(2026, 9, 9)
    assert got.strftime("%H:%M") == "10:00"


# ── рубильники и потолок ────────────────────────────────────────────────────────

def test_mode_off_disables_engine_even_if_flag_is_on(monkeypatch):
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    _settings(settings={"mode": "off", "work_hours": []})
    assert A.is_enabled() is False


def test_server_flag_disables_engine_even_if_panel_says_live(monkeypatch):
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", False)
    _settings(settings={"mode": "live", "work_hours": []})
    assert A.is_enabled() is False


def test_hourly_cap_stops_and_warns_once(monkeypatch):
    monkeypatch.setattr(A, "AUTOPILOT_HOURLY_CAP", 2)
    A.reset_hour_bucket()

    async def run():
        assert A.allow_action() is True
        assert A.allow_action() is True
        assert A.allow_action() is False
        assert A.allow_action() is False
        await asyncio.sleep(0)  # даём фоновой отправке алерта дойти до стаба

    asyncio.run(run())
    assert len([m for m in _SENT if "потолок" in m["text"]]) == 1


# ── белый список тест-режима ────────────────────────────────────────────────────

def test_test_whitelist_is_empty_means_nobody():
    """Пустой список - «никому», а не «всем». Иначе слово «тест» защищено только
    дисциплиной: в воронке «Тест» ничто не мешает завести сделку с реальным человеком."""
    _settings(test_contact_ids=[])
    assert SC.get_test_contact_ids() == set()
    _settings()
    assert SC.get_test_contact_ids() == {48594653}
