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
        "payment_status_id": 70070986,
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


def test_any_and_never_both_go_forward():
    """Заголовок на экране - «Когда идём дальше», и оба режима именно ведут дальше: `any` -
    получив любой ответ, `never` - не дожидаясь ответа вовсе. Останавливаться умеют `listed`
    и `except`, там для этого есть список ответов. Правка Кати 09.09.2026: движок обязан
    соответствовать обещанию экрана, а не читать его наоборот."""
    assert A.answer_decision({"stop_mode": "any"}, "что угодно") == "advance"
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


# ── маршрут: выбор бота и порядок этапов ────────────────────────────────────────

def _bot(bot_id, **over):
    bot = {
        "bot_id": bot_id, "bot_name": f"бот {bot_id}", "launched_by": "engine",
        "stop_mode": "any", "stop_answers": [], "stop_answers_norm": [],
        "conditions": [], "enabled": True,
    }
    bot.update(over)
    return bot


def _cf(field_id, value):
    return {"field_id": field_id, "values": [{"value": value}]}


def _lead(**over):
    lead = {
        "id": 111, "name": "Заказ 42", "pipeline_id": 8642414, "status_id": 70070982,
        "responsible_user_id": 0, "custom_fields_values": [], "_embedded": {"contacts": []},
    }
    lead.update(over)
    return lead


def _route(*stages):
    _settings(route=list(stages))
    return list(stages)


def _stage(status_id, name, bots, **over):
    stage = {"status_id": status_id, "status_name": name, "position": 0,
             "is_final": False, "bots": bots}
    stage.update(over)
    return stage


def _capture(monkeypatch) -> list[dict]:
    """Журнал в список вместо панели: строки журнала - показания робота о себе, и проверять
    надо именно их, а не то, что мы думаем про его поведение."""
    rows: list[dict] = []
    monkeypatch.setattr(A, "journal_bg", rows.append)
    return rows


def test_first_matching_bot_wins_and_disabled_is_skipped():
    """Ботов на этапе может быть сколько угодно, но запускаем ОДНОГО.

    Запусти мы всех подошедших, клиент получил бы два сообщения подряд - худшее, что робот
    умеет делать. Старшинство задаёт порядок на экране, его и слушаемся.
    """
    lead = _lead(custom_fields_values=[_cf(577373, "Счет")])
    stage = _stage(70070982, "Первичный контакт", [
        _bot(1, enabled=False),
        _bot(2, conditions=[{"join": "and", "field": "cf:577373", "op": "eq", "value": "нал"}]),
        _bot(3),
        _bot(4),
    ])
    assert A.pick_bot(lead, stage)["bot_id"] == 3


def test_route_order_is_the_order_on_the_screen():
    _route(
        _stage(1, "Первый", [_bot(10)]),
        _stage(2, "Второй", [_bot(11)]),
        _stage(3, "Третий", [_bot(12)]),
    )
    assert A.stage_position(2) == 1
    assert A.next_stage(1)["status_id"] == 2
    assert A.next_stage(3) is None
    assert A.is_last_stage({"status_id": 3, "is_final": False}) is True


def test_success_is_entered_only_through_the_payment_fork(monkeypatch):
    """В успешную реализацию маршрут не «переходит» по порядку карточек: туда пускает только
    развилка оплаты. Иначе решение о деньгах зависело бы от того, как человек перетащил
    карточки на экране."""
    _route(
        _stage(1, "Условия согласованы", [_bot(10)]),
        _stage(A.STATUS_SUCCESS, "Успешно реализовано", [_bot(11)]),
    )
    called = []
    monkeypatch.setattr(A, "payment_fork", lambda *a, **k: _noop(called.append("fork")))
    monkeypatch.setattr(A, "move_to", lambda *a, **k: _noop(called.append("move")))
    asyncio.run(A.advance(_lead(status_id=1), _stage(1, "Условия согласованы", []), "тест"))
    assert called == ["fork"]


async def _noop(_value=None):
    return None


# ── развилка оплаты ─────────────────────────────────────────────────────────────

def test_cod_is_narrow_showroom_is_not_cod():
    """Наложка - строго «При получении». Соседнее определение в `waybill_config` шире, в нём
    есть «Эвотор» и «наличные», а это шоурум: там наложки нет, деньги берут на месте. Возьми
    мы широкое определение, шоурумные заказы уехали бы в успех мимо оплаты."""
    assert A.is_cod_strict("При получении") is True
    assert A.is_cod_strict("при получении, курьеру") is True
    assert A.is_cod_strict("Эвотор") is False
    assert A.is_cod_strict("наличные Менеджер") is False
    assert A.is_cod_strict("") is False


def _fork_env(monkeypatch, *, paid, method, mode="live"):
    _settings(settings={"mode": mode, "work_hours": [{"start": "00:00", "end": "23:59"}]},
              payment_status_id=87280230 if mode == "live" else 70070986)
    rows = _capture(monkeypatch)
    moves: list[tuple] = []

    async def fake_ms_get(path, params=None, retries=3):
        return None if paid is None else {"payedSum": 100 if paid else 0}

    async def fake_move(lead, stage, status_id, status_name, reason):
        moves.append((status_id, status_name, reason))

    monkeypatch.setattr(A.ms_client, "get", fake_ms_get)
    monkeypatch.setattr(A, "move_to", fake_move)
    fields = [_cf(576689, "uuid-1")]
    if method:
        fields.append(_cf(577373, method))
    return rows, moves, _lead(custom_fields_values=fields)


def test_paid_order_goes_to_success(monkeypatch):
    rows, moves, lead = _fork_env(monkeypatch, paid=True, method="Счет")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves and moves[0][0] == A.STATUS_SUCCESS


def test_cash_on_delivery_goes_to_success_without_invoice(monkeypatch):
    rows, moves, lead = _fork_env(monkeypatch, paid=False, method="При получении")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves and moves[0][0] == A.STATUS_SUCCESS


def test_unpaid_online_stays_put_with_red_alert(monkeypatch):
    """Правка Кати 12.09.2026: счёт больше не наш ход. Неоплаченный онлайн-заказ робот
    никуда не ведёт - сделка стоит где стояла, человек получает красный алерт."""
    rows, moves, lead = _fork_env(monkeypatch, paid=False, method="Счет")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_unpaid"


def test_silent_warehouse_stops_instead_of_guessing(monkeypatch):
    """МойСклад не ответил - это НЕ «не оплачен»: у неизвестности своя причина остановки,
    и человек в алерте видит «не смог узнать», а не ложное «заказ не оплачен»."""
    rows, moves, lead = _fork_env(monkeypatch, paid=None, method="Счет")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "failed"


def test_empty_payment_method_stops(monkeypatch):
    rows, moves, lead = _fork_env(monkeypatch, paid=False, method="")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_no_payment_method"


def test_test_mode_forks_the_same_way_as_live(monkeypatch):
    """Развилка одна на оба режима (правка Кати 12.09.2026): неоплаченный онлайн и в
    «Тесте» стоит на месте. Прежний форс-перевод в этап «Оплата» отменён вместе с самой
    идеей вести сделку на этап запроса оплаты."""
    rows, moves, lead = _fork_env(monkeypatch, paid=False, method="Счет", mode="test")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_unpaid"


def test_force_ur_never_closes_an_unpaid_online_order(monkeypatch):
    """Тумблер «вести в успех, если шаблоны не ушли» снимает стоп только там, где деньги уже
    не под вопросом. Неоплаченный онлайн-заказ так не проводим никогда."""
    rows, moves, lead = _fork_env(monkeypatch, paid=False, method="Счет")
    asyncio.run(A.force_ur(lead, None, "все каналы отбили"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_not_delivered"


def test_force_ur_closes_a_paid_order_and_says_so(monkeypatch):
    rows, moves, lead = _fork_env(monkeypatch, paid=True, method="Счет")
    _SENT.clear()

    async def run():
        await A.force_ur(lead, None, "все каналы отбили")
        await asyncio.sleep(0)

    asyncio.run(run())
    assert moves and moves[0][0] == A.STATUS_SUCCESS
    assert any("БЕЗ подтверждения" in m["text"] for m in _SENT)


# ── ожидание доставки и ответа ──────────────────────────────────────────────────

def test_waiting_counts_from_launch_not_from_creation():
    """Сделка могла проспать ночь. Считай мы ожидание от создания строки, утром робот
    объявил бы шаблон недоставленным, ещё не отправив его."""
    now = datetime.datetime(2026, 9, 9, 12, 0, tzinfo=datetime.timezone.utc)
    row = {
        "created_at": "2026-09-08T22:00:00+00:00",
        "launch_ok_at": "2026-09-09T11:55:00+00:00",
    }
    assert A.waited_s(row, now) == 300


def test_client_reply_is_proof_of_delivery(monkeypatch):
    """Человек не отвечает на сообщение, которого не видел. Ответ клиента закрывает ожидание
    доставки сам, не дожидаясь отдельного статуса от Wazzup."""
    _route(_stage(70070982, "Первичный контакт", [_bot(7131, stop_mode="never")]))
    S.claim(222, 70070982, 8642414)
    S.update(222, 70070982, bot_id=7131, chat_id="79099371845", phase=S.PHASE_DELIVERY)
    seen: list[str] = []

    async def fake_answer(row, text, chat_type=""):
        seen.append(text)

    monkeypatch.setattr(A, "on_client_answer", fake_answer)
    asyncio.run(A.handle_wazzup({"messages": [{
        "chatId": "79099371845", "chatType": "whatsapp", "isEcho": False,
        "text": "Да, всё верно", "messageId": "m-1",
    }]}))
    assert seen == ["Да, всё верно"]


def test_moved_away_while_waiting_is_not_an_alert(monkeypatch):
    """Менеджер увёл сделку с этапа, пока робот ждал. Это его право, а не поломка: пишем
    строку в журнал и молчим в чате."""
    _route(_stage(70070982, "Первичный контакт", [_bot(7131)]))
    rows = _capture(monkeypatch)
    _SENT.clear()
    S.claim(333, 70070982, 8642414)

    async def fake_load(lead_id):
        return _lead(id=333, status_id=99999)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.on_delivered({"lead_id": 333, "status_id": 70070982, "bot_id": 7131}, [])
        await asyncio.sleep(0)

    asyncio.run(run())
    assert rows[-1]["outcome"] == "stop_left_stage"
    assert _SENT == []


# ── цепочка ботов на этапе ──────────────────────────────────────────────────────

def test_bots_of_one_stage_run_in_turn_not_at_once():
    """Ботов на этапе бывает несколько, и они идут ОЧЕРЕДЬЮ: первый спросил, клиент ответил -
    слово берёт второй. Запусти движок всех разом, клиент получил бы несколько сообщений
    подряд; поэтому одновременно бот всегда один, а `pick_bot` умеет «следующий после этого».
    """
    lead = _lead()
    stage = _stage(70070982, "Первичный контакт", [_bot(1), _bot(2), _bot(3)])
    assert A.pick_bot(lead, stage)["bot_id"] == 1
    assert A.pick_bot(lead, stage, after_bot_id=1)["bot_id"] == 2
    assert A.pick_bot(lead, stage, after_bot_id=2)["bot_id"] == 3
    assert A.pick_bot(lead, stage, after_bot_id=3) is None


def test_chain_skips_the_bot_whose_conditions_did_not_match():
    """Условия отбирают участников очереди: несошедшийся бот не запускается, ход переходит
    дальше по списку, а не обрывается."""
    lead = _lead(custom_fields_values=[_cf(577373, "Счет")])
    stage = _stage(70070982, "Первичный контакт", [
        _bot(1),
        _bot(2, conditions=[{"join": "and", "field": "cf:577373", "op": "eq", "value": "нал"}]),
        _bot(3, enabled=False),
        _bot(4),
    ])
    assert A.pick_bot(lead, stage, after_bot_id=1)["bot_id"] == 4


def test_answer_passes_the_turn_to_the_next_bot_not_to_the_next_stage(monkeypatch):
    """Пока цепочка не кончилась, сделка с места не двигается."""
    stage = _stage(70070982, "Первичный контакт", [_bot(1), _bot(2)])
    _route(stage, _stage(72186654, "В работе", [_bot(9)]))
    rows = _capture(monkeypatch)
    started: list[int] = []
    moves: list[int] = []

    async def fake_run_bot(lead, stage_, bot):
        started.append(int(bot["bot_id"]))

    async def fake_move(lead, stage_, status_id, status_name, reason):
        moves.append(status_id)

    monkeypatch.setattr(A, "run_bot", fake_run_bot)
    monkeypatch.setattr(A, "move_to", fake_move)
    S.claim(444, 70070982, 8642414)

    asyncio.run(A.advance(_lead(id=444), stage, "клиент ответил", from_bot=_bot(1)))
    assert started == [2] and moves == []

    # А когда боты кончились - едем на следующий этап.
    asyncio.run(A.advance(_lead(id=444), stage, "клиент ответил", from_bot=_bot(2)))
    assert moves == [72186654]


def test_next_bot_starts_with_a_clean_delivery_slate():
    """Строка состояния одна на пару «сделка и этап». Не обнули мы её под нового бота, он
    унаследовал бы чужие отметки запуска и чужую копилку статусов - и окно ожидания истекло
    бы у него ещё до отправки."""
    S.claim(555, 70070982, 8642414)
    S.mark_launch_ok(555, 70070982, "79099371845")
    S.add_delivery_status(555, 70070982, {"status": "error", "chatType": "telegram"})

    S.start_next_bot(555, 70070982, 7137)
    row = S.get(555, 70070982)
    assert row["bot_id"] == 7137
    assert row["delivery"] == []
    assert row["launch_ok_at"] is None
    assert row["phase"] == S.PHASE_LAUNCHING


# ── оплата: в успех только через развилку (плюс вход «Оплата получена») ────────

def test_paid_order_still_walks_the_route(monkeypatch):
    """Правка Кати 12.09.2026, отменяет правку 09.09: оплаченность на входе не проверяем.
    Независимо от статуса оплаты сделка идёт по ВСЕМ шагам маршрута, и в успех её пускает
    только развилка после них: оплаченный заказ, телепортом уезжавший в УР без единого
    слова клиенту («Заказ №19005»), человека только путал."""
    _settings(settings={"mode": "test", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "stock_check_enabled": False},
              route=[_stage(70070982, "Первичный контакт", [_bot(7131)])])
    rows = _capture(monkeypatch)
    moves: list[int] = []
    launched: list[int] = []

    async def fake_ms_get(path, params=None, retries=3):
        return {"payedSum": 12000}

    async def fake_move(lead, stage_, status_id, status_name, reason):
        moves.append(status_id)

    async def fake_run_bot(lead, stage_, bot):
        launched.append(int(bot["bot_id"]))

    monkeypatch.setattr(A.ms_client, "get", fake_ms_get)
    monkeypatch.setattr(A, "move_to", fake_move)
    monkeypatch.setattr(A, "run_bot", fake_run_bot)

    lead = _lead(custom_fields_values=[_cf(576689, "uuid-1")])
    asyncio.run(A.run_stage(lead, _stage(70070982, "Первичный контакт", [_bot(7131)])))
    assert moves == []
    assert launched == [7131], "оплаченный заказ идёт по маршруту, как все"


def test_entry_never_asks_the_warehouse_about_payment(monkeypatch):
    """Вход маршрута в МойСклад за оплатой не ходит вовсе - вопрос оплаты живёт в одной
    точке, развилке в конце. Меньше точек решения - меньше расхождений."""
    _settings(settings={"mode": "test", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "stock_check_enabled": False},
              route=[_stage(70070982, "Первичный контакт", [_bot(7131)])])
    _capture(monkeypatch)
    ms_calls: list[str] = []
    launched: list[int] = []

    async def fake_ms_get(path, params=None, retries=3):
        ms_calls.append(str(path))
        return {"payedSum": 12000}

    async def fake_run_bot(lead, stage_, bot):
        launched.append(int(bot["bot_id"]))

    monkeypatch.setattr(A.ms_client, "get", fake_ms_get)
    monkeypatch.setattr(A, "run_bot", fake_run_bot)

    lead = _lead(custom_fields_values=[_cf(576689, "uuid-1")])
    asyncio.run(A.run_stage(lead, _stage(70070982, "Первичный контакт", [_bot(7131)])))
    assert launched == [7131]
    assert ms_calls == []


def test_payment_received_is_checked_against_the_warehouse(monkeypatch):
    """На «Оплата получена» сделку переводит скрипт по вебхуку платёжной системы. Источник
    хороший, но одинокий: склад видит те же деньги с другой стороны."""
    _settings()
    rows = _capture(monkeypatch)
    S.claim(666, 70070982, 8642414)
    moves: list[int] = []

    async def fake_move(lead, stage_, status_id, status_name, reason):
        moves.append(status_id)

    monkeypatch.setattr(A, "move_to", fake_move)

    async def paid_no(path, params=None, retries=3):
        return {"payedSum": 0}

    monkeypatch.setattr(A.ms_client, "get", paid_no)
    lead = _lead(id=666, custom_fields_values=[_cf(576689, "uuid-1")])

    async def run():
        await A.on_payment_received(lead)
        await asyncio.sleep(0)

    asyncio.run(run())
    assert moves == [], "источники разошлись - решать человеку"
    assert rows[-1]["outcome"] == "failed"


def test_payment_received_still_goes_through_when_warehouse_is_silent(monkeypatch):
    """А молчание склада успех не блокирует: событие об оплате уже пришло, держать сделку
    из-за неотвечающего отчёта значит наказывать клиента за наш склад."""
    _settings()
    rows = _capture(monkeypatch)
    S.claim(777, 70070982, 8642414)
    moves: list[int] = []

    async def fake_move(lead, stage_, status_id, status_name, reason):
        moves.append(status_id)

    async def silent(path, params=None, retries=3):
        return None

    monkeypatch.setattr(A, "move_to", fake_move)
    monkeypatch.setattr(A.ms_client, "get", silent)

    async def run():
        await A.on_payment_received(_lead(id=777, custom_fields_values=[_cf(576689, "u")]))
        await asyncio.sleep(0)

    asyncio.run(run())
    assert moves == [A.STATUS_SUCCESS]
    assert "промолчал" in rows[-1]["reason"]


# ── ручные тест-сделки: без заказа МойСклада ────────────────────────────────────

def _no_order_env(monkeypatch, *, method, mode):
    _settings(settings={"mode": mode, "work_hours": [{"start": "00:00", "end": "23:59"}]},
              payment_status_id=87280230 if mode == "live" else 70070986)
    rows = _capture(monkeypatch)
    moves: list[int] = []

    async def fake_move(lead, stage_, status_id, status_name, reason):
        moves.append(status_id)

    monkeypatch.setattr(A, "move_to", fake_move)
    fields = [_cf(577373, method)] if method else []
    return rows, moves, _lead(custom_fields_values=fields)


def test_manual_test_lead_without_ms_order_stops_with_alert(monkeypatch):
    """Ручная тест-сделка без заказа МойСклада: онлайн-оплату подтвердить нечем, исход
    тот же, что в бою, - стоим и зовём человека. Кейс «онлайн оплачен» в тесте требует
    привязанного заказа МойСклада, как в бою."""
    rows, moves, lead = _no_order_env(monkeypatch, method="Счет", mode="test")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_unpaid"


def test_route_never_walks_into_payment_stage_by_card_order(monkeypatch):
    """На этап оплаты, как и в успех, по порядку карточек не переходим - только через
    развилку: в бою вход туда выставляет клиенту счёт, и решение о деньгах не должно
    зависеть от расстановки карточек."""
    _route(
        _stage(70070982, "Первичный контакт", [_bot(1)]),
        _stage(70070986, "Оплата", []),
        _stage(A.STATUS_SUCCESS, "Успешно реализовано", []),
    )
    called = []
    monkeypatch.setattr(A, "payment_fork", lambda *a, **k: _noop(called.append("fork")))
    monkeypatch.setattr(A, "move_to", lambda *a, **k: _noop(called.append("move")))
    asyncio.run(A.advance(_lead(status_id=70070982), _stage(70070982, "Первичный контакт", []),
                          "тест"))
    assert called == ["fork"]


def test_manual_test_lead_with_cod_still_reaches_success(monkeypatch):
    """Наложке факт оплаты не нужен вовсе - тестовая сделка с «При получении» доезжает
    до успешной реализации даже без заказа МойСклада."""
    rows, moves, lead = _no_order_env(monkeypatch, method="При получении", mode="test")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == [A.STATUS_SUCCESS]


def test_live_lead_without_ms_order_stops(monkeypatch):
    """А в бою сделка без заказа МойСклада - аномалия: робот заведён под заказы, и
    «спрашивать не о чем» здесь повод позвать человека, а не ехать дальше."""
    rows, moves, lead = _no_order_env(monkeypatch, method="Счет", mode="live")
    asyncio.run(A.payment_fork(lead, None, "конец маршрута"))
    assert moves == []
    assert rows[-1]["outcome"] == "stop_unpaid"
    assert "нет заказа" in rows[-1]["reason"]


def test_test_mode_alerts_go_to_tech_chat_not_managers(monkeypatch):
    """Алерты тестового прогона не дёргают менеджеров: иначе в рабочий топик УВЕДОМЛЕНИЯ
    полетело бы «клиент ответил...» по сделке, которой не существует."""
    _settings(settings={"mode": "test", "work_hours": []})
    _SENT.clear()

    async def run():
        A.alert_op("тестовое событие", responsible_id=123)
        await asyncio.sleep(0)

    asyncio.run(run())
    assert len(_SENT) == 1
    assert _SENT[0]["chat_id"] is None, "в тесте алерт идёт в технический чат по умолчанию"
    assert "ТЕСТОВЫЙ прогон" in _SENT[0]["text"]

    # Пилот боя (тумблер включён - а он включён по умолчанию) - тоже технический чат.
    _settings(settings={"mode": "live", "work_hours": []})
    _SENT.clear()

    async def run_pilot():
        A.alert_op("событие пилота")
        await asyncio.sleep(0)

    asyncio.run(run_pilot())
    assert _SENT[0]["chat_id"] is None
    assert "ПИЛОТ" in _SENT[0]["text"]

    # И только ПОЛНЫЙ бой - с выключенным тумблером - дёргает менеджеров.
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False})
    _SENT.clear()

    async def run2():
        A.alert_op("боевое событие")
        await asyncio.sleep(0)

    asyncio.run(run2())
    assert _SENT[0]["chat_id"] is not None, "в полном бою алерт идёт в чат отдела продаж"


# ── телеграм: чат живёт не под телефоном ────────────────────────────────────────

def test_telegram_reply_is_matched_by_contact_phone(monkeypatch):
    """Первый живой прогон 09.09.2026: бот написал в Telegram, человек ответил - робот не
    увидел ответа. Телеграмный `chatId` с телефоном не совпадает никогда; склейка идёт по
    `contact.phone` из того же вебхука."""
    _route(_stage(70070982, "Первичный контакт", [_bot(7169)]))
    S.claim(888, 70070982, 8642414)
    S.update(888, 70070982, bot_id=7169, chat_id="79956109902", phase=S.PHASE_REPLY)
    seen: list[str] = []

    async def fake_answer(row, text, chat_type=""):
        seen.append((row["lead_id"], text))

    monkeypatch.setattr(A, "on_client_answer", fake_answer)
    asyncio.run(A.handle_wazzup({"messages": [{
        "chatId": "1920391385", "chatType": "telegram", "isEcho": False,
        "text": "Всё верно", "messageId": "tg-1",
        "contact": {"name": "Тиана Василькова", "phone": "79956109902",
                    "username": "teanochk"},
    }]}))
    assert seen == [(888, "Всё верно")]


def test_telegram_outbound_status_lands_in_the_right_piggy_bank(monkeypatch):
    """Исходящее ботом в Telegram тоже матчится по телефону - иначе статус «прочитано» падал
    бы мимо копилки, и окно ожидания честно истекало бы у доставленного сообщения."""
    _route(_stage(70070982, "Первичный контакт", [_bot(7169)]))
    S.claim(999, 70070982, 8642414)
    S.update(999, 70070982, bot_id=7169, chat_id="79956109902", phase=S.PHASE_DELIVERY)
    S.mark_launch_ok(999, 70070982, "79956109902")
    delivered: list[int] = []

    async def fake_on_delivered(row, items):
        delivered.append(row["lead_id"])

    monkeypatch.setattr(A, "on_delivered", fake_on_delivered)
    asyncio.run(A.handle_wazzup({"messages": [{
        "chatId": "1920391385", "chatType": "telegram", "isEcho": True,
        "text": "Здравствуйте!", "messageId": "tg-2", "status": "sent",
        "contact": {"name": "Тиана Василькова", "phone": "79956109902"},
    }]}))
    assert delivered == [999], "sent у телеграма - доставка, и она должна найтись"
    row = S.get(999, 70070982)
    assert row["delivery"] and row["delivery"][0]["chatType"] == "telegram"


def test_contact_without_phone_stays_invisible_and_that_is_the_limit():
    """Контакт без телефона в карточке Wazzup не сматчится - предел способа, зафиксирован."""
    assert A._chat_candidates({
        "chatId": "864542860", "chatType": "telegram",
        "contact": {"name": "Без телефона", "username": "nickname"},
    }) == ["864542860"]


# ── пилот боевого режима: белый список в бою ────────────────────────────────────

def _lead_with_contact(contact_id: int) -> dict:
    return _lead(_embedded={"contacts": [{"id": contact_id}]})


def test_live_pilot_touches_only_whitelisted_contacts():
    """Правка Кати 12.09.2026: бой обкатывается на живой воронке, но робот трогает только
    сделки белого списка. Тумблер включён по умолчанию - первый запуск боя начинается
    пилотом, а полный запуск это осознанное выключение, а не случайное умолчание."""
    _settings(settings={"mode": "live", "work_hours": []})
    assert A.limited_mode() == "пилот"
    assert A.whitelist_ok(_lead_with_contact(48594653)) is True
    assert A.whitelist_ok(_lead_with_contact(11111111)) is False,         "реальный клиент в пилоте невидим для робота"


def test_full_live_mode_has_no_whitelist():
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False})
    assert A.limited_mode() is None
    assert A.whitelist_ok(_lead_with_contact(11111111)) is True


# ── инбокс без менеджера: уведомления в ленту панели ────────────────────────────

def test_every_new_lead_wakes_the_panel_inbox_in_live(monkeypatch):
    """Правка Кати 12.09.2026: на ПОЛНОМ проде КАЖДАЯ заявка на входе воронки поднимает
    уведомление в ленте панели - любого типа, до белого списка и условий ботов: без
    менеджера инбокс проверяет панель."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              test_contact_ids=[], route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    sent: list[dict] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    A._lead_notified.clear()

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714,
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(424242))
    assert len(sent) == 1
    assert sent[0]["kind"] == "autopilot_lead"
    assert "424242" in sent[0]["url"]
    # Контакт НЕ из белого списка: робот сделку не повёл, а уведомление всё равно ушло.

    # Повторный вебхук той же сделки панель больше не дёргает.
    asyncio.run(A.handle_lead_change(424242))
    assert len(sent) == 1


def test_test_mode_does_not_touch_the_panel_inbox(monkeypatch):
    """В «Тесте» менеджеры работают как обычно - тестовый шум в ленте приучил бы людей
    её игнорировать."""
    _settings(settings={"mode": "test", "work_hours": []},
              entry_status_id=70070982,
              route=[_stage(70070982, "Первичный контакт", [_bot(7131)])])
    sent: list[dict] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    A._lead_notified.clear()

    async def fake_load(lead_id):
        return _lead(id=lead_id, status_id=70070982,
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(424243))
    assert sent == []


def test_op_alert_is_mirrored_to_the_panel_feed_in_live(monkeypatch):
    """Алерт робота дублируется в ленту панели: лента - основной канал, Телеграм уже
    глушился на сутки одним сетевым сбоем. Html-ссылка алерта в ленту едет словами,
    адрес - отдельным полем."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False},
              payment_status_id=87280230)
    sent: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    _SENT.clear()

    async def run():
        A.alert_op('<a href="https://amo/lead/1">сделка</a>: клиент ответил не по кнопке')
        await asyncio.sleep(0)

    asyncio.run(run())
    assert len(sent) == 1
    assert sent[0]["level"] == "critical"
    assert sent[0]["body"].startswith("сделка: клиент ответил")
    assert sent[0]["url"] == "https://amo/lead/1"
    assert "<a" not in sent[0]["body"]


# ── только заказы ───────────────────────────────────────────────────────────────

def test_preorder_and_reserve_are_not_orders():
    """Робот ведёт только тип «Заказ». Сравнение строгим равенством: «Предзаказ» содержит
    слово «заказ», и поиск подстроки брал бы его в работу."""
    _settings(settings={"mode": "live", "work_hours": []})
    assert A.is_order(_lead(custom_fields_values=[_cf(577671, "Заказ")])) is True
    assert A.is_order(_lead(custom_fields_values=[_cf(577671, "Предзаказ")])) is False
    assert A.is_order(_lead(custom_fields_values=[_cf(577671, "Резерв")])) is False


def test_empty_application_type_is_forgiven_only_in_test():
    """Тестовые сделки заводятся руками и тип у них пуст - «Тест» это прощает. В бою тип
    заполняет интеграция, и пустое поле означает НЕ заказ."""
    _settings(settings={"mode": "test", "work_hours": []})
    assert A.is_order(_lead()) is True
    _settings(settings={"mode": "live", "work_hours": []})
    assert A.is_order(_lead()) is False


def test_non_order_lead_is_left_alone_but_still_notifies(monkeypatch):
    """Не-заказ робот не трогает СОВСЕМ - ни ботов, ни переводов. А уведомление о заявке
    в ленту идёт, и тип в нём назван: инбокс без менеджера работает для любого типа."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              test_contact_ids=[],
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    sent: list[dict] = []
    ran: list[int] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop(ran.append(1)))
    A._lead_notified.clear()

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714,
                     custom_fields_values=[_cf(577671, "Резерв")],
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(424244))
    assert len(sent) == 1
    assert "Резерв" in sent[0]["body"]
    assert ran == []


# ── пилот: лента живёт только тест-контактами ───────────────────────────────────

def test_pilot_notifies_only_whitelisted_leads(monkeypatch):
    """Правка Кати 12.09.2026 (вечер): пока включён тумблер «На проде вести только
    тестовые контакты», заявки НЕ из белого списка ленту не будят - живой поток не должен
    шуметь, пока робот обкатывается. Заявка тест-контакта будит как раньше."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": True},
              pipeline_id=10593102, entry_status_id=83537714,
              test_contact_ids=[48595431],
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    sent: list[dict] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    A._lead_notified.clear()

    async def fake_load_alien(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714,
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load_alien)
    asyncio.run(A.handle_lead_change(424245))
    assert sent == []

    async def fake_load_ours(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714,
                     _embedded={"contacts": [{"id": 48595431}]})

    monkeypatch.setattr(A, "load_lead", fake_load_ours)
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop())
    asyncio.run(A.handle_lead_change(424246))
    assert len(sent) == 1
    assert sent[0]["kind"] == "autopilot_lead"


def test_pilot_engine_forwards_inbound_of_led_chat_to_the_feed(monkeypatch):
    """В пилоте панель молчит про входящие (см. панельный inbox) - сообщение ведомого
    чата доносит движок: чат нашёлся в его состоянии, значит контакт из белого списка.
    Чужой чат в состоянии не находится - и в ленту не попадает."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": True},
              test_contact_ids=[48595431])
    sent: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    monkeypatch.setattr(A.store, "find_by_chat",
                        lambda chat: [{"lead_id": 1, "status_id": 2, "phase": "done"}]
                        if chat == "79956109902" else [])

    async def run(chat_id):
        await A._handle_message({
            "messageId": "m-77", "chatId": chat_id, "chatType": "whatsapp",
            "isEcho": False, "text": "Да, всё верно",
            "contact": {"name": "Тиана", "phone": chat_id},
        })

    asyncio.run(run("79000000000"))
    assert sent == []
    asyncio.run(run("79956109902"))
    assert len(sent) == 1
    assert sent[0]["kind"] == "autopilot_inbox"
    assert sent[0]["dedupe_key"] == "ap-inbox-m-77"
    assert sent[0]["body"].startswith("Да")
