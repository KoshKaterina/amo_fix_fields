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
    A.reset_op_burst()
    _settings()
    # Отметки «об этой заявке уже сказали» живут на диске и переживают рестарт - значит
    # переживут и соседний тест. Чистим, иначе второй тест про заявку молчит «по дедупу».
    S.init()
    with S._connect() as conn:
        conn.execute("DELETE FROM autopilot_notified")
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
        # Свежая по умолчанию: уведомление о заявке живёт за гейтом возраста, и сделка без
        # `created_at` для него - «неизвестно когда создана», то есть молчим.
        "created_at": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
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
    """Правка Кати 12.09.2026: на ПОЛНОМ проде заявка на входе воронки поднимает уведомление
    в ленте панели. Заказ, который робот ведёт сам, - только лента: в чат идёт то, что
    требует человека, а исправная работа робота этого не требует."""
    _settings(settings={"mode": "live", "work_hours": [],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              test_contact_ids=[], route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    sent: list[dict] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop())
    A._lead_notified.clear()

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714,
                     custom_fields_values=[_cf(577671, "Заказ")],
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(424242))
    assert len(sent) == 1
    assert sent[0]["kind"] == "autopilot_lead"
    assert "424242" in sent[0]["url"]
    assert _SENT == []  # в чат про исправный заказ не пишем

    # Повторный вебхук той же сделки панель больше не дёргает.
    asyncio.run(A.handle_lead_change(424242))
    assert len(sent) == 1


def test_stale_lead_on_entry_stage_does_not_look_new(monkeypatch):
    """⚠️ Гейт «этого захода» (Катя 27.09.2026). `/lead_change` приходит на ЛЮБОЕ изменение, и
    сделка, неделю стоящая на входном этапе, от правки поля выглядела «новой заявкой». Ровно это
    и случилось 27.09: уведомление о вчерашней заявке пришло утром, когда её тронул синк."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              test_contact_ids=[], route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    sent: list[dict] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: sent.append(kw))
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop())
    A._lead_notified.clear()
    old = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 3 * 24 * 3600

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714, created_at=old,
                     custom_fields_values=[_cf(577671, "Заказ")],
                     _embedded={"contacts": [{"id": 11111111}]})

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(424247))
    assert sent == []
    assert _SENT == []


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
    """Не-заказ робот не трогает СОВСЕМ - ни ботов, ни переводов. Зато теперь о такой
    заявке говорят и в рабочий чат с тегом (Катя 27.09.2026): «уведы в чат тг при новой
    сделке, если бот её не обрабатывает». Иначе предзаказ ложится в воронку молча."""
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

    async def run():
        await A.handle_lead_change(424244)
        await asyncio.sleep(0)  # даём фоновой отправке дойти до стаба Телеграма

    asyncio.run(run())
    assert len(sent) == 1
    assert sent[0]["kind"] == "autopilot_lead_unhandled"
    assert "Резерв" in sent[0]["body"]
    assert ran == []
    # ... и то же самое в чат: тег смены, суть и ссылка на сделку.
    assert len(_SENT) == 1
    assert "Резерв" in _SENT[0]["text"]
    assert "424244" in _SENT[0]["text"]
    assert _SENT[0]["chat_id"] == A.NOTIFY_CHAT_ID

    # Второй вебхук по той же сделке в чат уже не пишет - дедуп на диске.
    asyncio.run(A.handle_lead_change(424244))
    assert len(_SENT) == 1


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
    # Тип заявки у ручной сделки пуст, значит в бою это НЕ заказ: уведомление про заявку,
    # которую робот не ведёт.
    assert sent[0]["kind"] == "autopilot_lead_unhandled"


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


# ── алерты 27.09.2026: недоставка, непонятная оплата, любой сбой ────────────────

def test_not_delivered_calls_the_manager_with_its_own_event(monkeypatch):
    """Требование Кати 27.09.2026: «алерт в рабочий чат, если сообщение не доставлено».

    Событие отдельное (`autopilot_not_delivered`), а не общий «нужен человек»: выключить
    шум по одному поводу должно быть можно, не заглушив робота целиком.
    """
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False, "delivery_wait_minutes": 0},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131, bot_name="Бот подтверждения")])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 555, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_DELIVERY,
        "launch_ok_at": "2020-01-01T00:00:00+00:00", "delivery": [
            {"status": "error", "chatType": "whatsapp"},
        ],
    }])
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19294", pipeline_id=10593102,
                     status_id=83537714, responsible_user_id=13929334)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.check_delivery_windows()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert rows[-1]["outcome"] == "stop_not_delivered"
    assert feed and feed[-1]["kind"] == "autopilot_not_delivered"
    assert feed[-1]["title"] == "Авто-режим: сообщение не дошло"
    text = _SENT[-1]["text"]
    assert "не дошло" in text and "555" in text  # суть и ссылка на сделку
    assert "@" in text                            # тег ответственного
    assert _SENT[-1]["chat_id"] == A.NOTIFY_CHAT_ID


def test_other_payment_method_stops_and_calls_a_human(monkeypatch):
    """«Другой способ» (Катя 27.09.2026): робот такую сделку в успех НЕ ведёт и говорит об
    этом. До правки она считалась онлайном и при оплате в МойСкладе уезжала в УР - то есть
    решение о деньгах принималось по способу, которого робот не понимает."""
    rows, moves, lead = _fork_env(monkeypatch, paid=True, method="Другой способ")
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))

    async def run():
        await A.payment_fork(lead, None, "конец маршрута")
        await asyncio.sleep(0)

    asyncio.run(run())
    assert moves == []
    assert rows[-1]["outcome"] == "stop_payment_unclear"
    assert feed and feed[-1]["kind"] == "autopilot_payment_other"
    assert "Другой способ" in _SENT[-1]["text"]


def test_cash_and_evotor_are_not_touched_by_the_other_method_rule():
    """Список «непонятных» способов узкий намеренно: «Наличными» и «Эвотор» - это шоурум,
    там деньги берут на месте и заказ в МойСкладе помечен оплаченным. Расширять список -
    решением Кати, а не догадкой кода."""
    assert A.is_ambiguous_payment("Другой способ") is True
    assert A.is_ambiguous_payment("другой способ оплаты") is True
    assert A.is_ambiguous_payment("Наличными") is False
    assert A.is_ambiguous_payment("Эвотор") is False
    assert A.is_ambiguous_payment("Онлайн-оплата") is False
    assert A.is_ambiguous_payment("") is False


def test_any_crash_calls_a_human_with_link_and_tag(monkeypatch):
    """«Любой сбой = алерт в чат» (Катя 27.09.2026). До правки необработанное исключение в
    разборе вебхука уходило в `logger.exception` и всё: сделка стояла недоведённой, а знал
    об этом только тот, кто открыл логи контейнера."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False})
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))

    async def boom():
        raise RuntimeError("amo вернул 500")

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19294", responsible_user_id=13929334)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.guarded(boom(), what="разбор изменения сделки", lead_id=777)
        await asyncio.sleep(0)

    asyncio.run(run())
    assert feed and feed[-1]["kind"] == "autopilot_error"
    text = _SENT[-1]["text"]
    assert "amo вернул 500" in text and "777" in text and "@" in text


def test_the_same_crash_is_not_repeated_in_the_chat(monkeypatch):
    """Тик идёт раз в минуту: без дедупа одна незалеченная ошибка дала бы шестьдесят
    одинаковых сообщений в час."""
    _settings(settings={"mode": "live", "work_hours": [], "live_whitelist_enabled": False})
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)

    async def run():
        A.alert_error("ошибка фонового цикла: RuntimeError: раз", dedupe="tick")
        A.alert_error("ошибка фонового цикла: RuntimeError: раз", dedupe="tick")
        await asyncio.sleep(0)

    asyncio.run(run())
    assert len(_SENT) == 1


def test_op_burst_mutes_the_chat_but_never_the_panel_feed(monkeypatch):
    """Антиспам рабочего чата. Лента панели им НЕ режется: она основной канал, и полная
    картина должна быть там даже тогда, когда чат замолчал."""
    _settings(settings={"mode": "live", "work_hours": [], "live_whitelist_enabled": False})
    monkeypatch.setattr(A, "AUTOPILOT_OP_BURST_MAX", 2)
    A.reset_op_burst()
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))

    async def run():
        for i in range(5):
            A.dispatch_op(A.EVENT_EVENT, f"сделка: событие {i}",
                          values={"текст_события": f"событие {i}"},
                          panel_title="Авто-режим: нужен человек")
        await asyncio.sleep(0)

    asyncio.run(run())
    assert len(feed) == 5                       # в ленте все пять
    said = [m for m in _SENT if "событие" in m["text"]]
    assert len(said) == 2                       # в чат ушли два
    assert any("молчу" in m["text"] for m in _SENT)   # и одно объявление технарям


def test_stale_lead_on_entry_stage_is_not_taken_into_work(monkeypatch):
    """⚠️ Поймано боем 27.09.2026. В 11:13 робот забрал девять заказов, простоявших на «Новом
    лиде» с вечера (вебхук приходит на любое изменение), а в 11:28 выдал по ним девять алертов
    «сообщение до клиента не дошло»."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    ran: list[int] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop(ran.append(1)))
    old = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 3 * 24 * 3600

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714, created_at=old,
                     custom_fields_values=[_cf(577671, "Заказ")])

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(36564965))
    assert ran == []
    assert rows[-1]["outcome"] == "skipped_stale_entry"


def test_entry_window_covers_the_night_before_the_shift():
    """Слово Кати 27.09.2026: «если бота включили сегодня в 10, то он будет работать со всем, что
    появилось сегодня плюс сделки с 19 вчерашней даты до 10 сегодняшней».

    Порогом в часах это не выражается - считаем по рабочим окнам. Проверяем на фиксированном
    времени, иначе тест начал бы зависеть от часа своего запуска."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]})
    now = datetime.datetime(2026, 9, 27, 11, 13, tzinfo=_MSK)
    assert A.entry_window_start(now) == datetime.datetime(2026, 9, 26, 19, 0, tzinfo=_MSK)

    def lead_at(y, m, d, hh, mm):
        ts = int(datetime.datetime(y, m, d, hh, mm, tzinfo=_MSK).timestamp())
        return _lead(created_at=ts)

    # ночной заказ 21:12 - наш: смены не было, его никто не видел
    assert A.lead_is_fresh(lead_at(2026, 9, 26, 21, 12), now=now) is True
    # заказ этого утра - наш
    assert A.lead_is_fresh(lead_at(2026, 9, 27, 10, 40), now=now) is True
    # заказ, пролежавший вчерашний рабочий день, - не наш, его видели люди
    assert A.lead_is_fresh(lead_at(2026, 9, 26, 15, 55), now=now) is False
    # до начала работы: правило то же, ночь остаётся нашей
    early = datetime.datetime(2026, 9, 27, 8, 0, tzinfo=_MSK)
    assert A.lead_is_fresh(lead_at(2026, 9, 27, 1, 38), now=early) is True


def test_grid_bot_without_statuses_keeps_listening_for_the_answer(monkeypatch):
    """⚠️ Правка 27.09.2026, вечер. «Ни одного статуса» по боту ГРИДА - не «не доставлено» и не
    повод бросать сделку: по заказу 19288 бот грида отправил шаблон вечером, статусы прошли до
    включения робота, а клиент ответил «Да, всё верно» в 14:00 - и ответ ушёл в пустоту, потому
    что сделку сняли с ведения. Теперь переходим к ожиданию ОТВЕТА и слушаем чат дальше."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False, "delivery_wait_minutes": 0},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид",
                            [_bot(7131, launched_by="amo_grid", bot_name="Бот грида")])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    updates: list[dict] = []
    finished: list[tuple] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 36564989, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_DELIVERY,
        "launch_ok_at": "2020-01-01T00:00:00+00:00", "delivery": [],
    }] if phase == S.PHASE_DELIVERY else [])
    monkeypatch.setattr(A.store, "update", lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: finished.append(a))

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19288", pipeline_id=10593102,
                     status_id=83537714, responsible_user_id=13929334)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.check_delivery_windows()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert rows[-1]["outcome"] == "waiting_reply"
    assert updates and updates[-1]["phase"] == S.PHASE_REPLY   # слушаем дальше
    assert finished == []                                      # с ведения НЕ снимаем
    assert feed == []                                          # ленту не будим
    assert _SENT and _SENT[-1]["chat_id"] is None              # технический чат


def test_engine_bot_without_statuses_still_alerts(monkeypatch):
    """А вот бота, которого запускал САМ робот, молчание Wazzup изобличает: мы точно
    отправляли, значит «ни одного статуса» - это повод звать человека."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False, "delivery_wait_minutes": 0},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид",
                            [_bot(7131, launched_by="engine", bot_name="Бот робота")])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 36564966, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_DELIVERY,
        "launch_ok_at": "2020-01-01T00:00:00+00:00", "delivery": [],
    }])
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19295", pipeline_id=10593102,
                     status_id=83537714, responsible_user_id=13929334)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.check_delivery_windows()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert rows[-1]["outcome"] == "stop_not_delivered"
    assert feed and feed[-1]["kind"] == "autopilot_not_delivered"
    assert _SENT[-1]["chat_id"] == A.NOTIFY_CHAT_ID


def test_other_payment_method_is_named_at_the_entry_not_at_the_end(monkeypatch):
    """Катя 27.09.2026: по заказу 19296 алерт «Другой способ» не пришёл вовсе - робот честно
    ждал ответа клиента на бота, а развилка оплаты стоит в КОНЦЕ маршрута. Менеджеру надо знать
    в момент заказа: такую оплату роботу не понять, сколько шагов он ни пройди."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    launched: list[int] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A, "run_bot", lambda *a, **k: _noop(launched.append(1)))
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    monkeypatch.setattr(A, "stock_gate", lambda lead: _stock_ok())

    lead = _lead(id=36565195, name="Заказ №19296", pipeline_id=10593102, status_id=83537714,
                 custom_fields_values=[_cf(577671, "Заказ"), _cf(577373, "Другой способ")])

    async def run():
        await A.run_stage(lead, _stage(83537714, "Новый лид", [_bot(7131)]))
        await asyncio.sleep(0)

    asyncio.run(run())
    assert launched == []                       # бота не запускаем, маршрут не начинаем
    assert rows[-1]["outcome"] == "stop_payment_unclear"
    assert feed and feed[-1]["kind"] == "autopilot_payment_other"
    assert "Другой способ" in _SENT[-1]["text"]


async def _stock_ok():
    return True, "остаток есть"


def test_contact_without_phone_calls_a_human_right_away(monkeypatch):
    """Кейс Кати 27.09.2026: «у контакта нет телефона в карточке Wazzup - это кейс для алерта».

    Склейка с Wazzup идёт по телефону контакта. Нет телефона - робот не увидит ни статусов
    доставки, ни ответа: с виду ведёт сделку, а на деле слеп. Раньше такая сделка просто
    провисала до конца окна ожидания."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    started: list[int] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    monkeypatch.setattr(A, "launch_bot", lambda *a, **k: _noop(started.append(1)))
    monkeypatch.setattr(A, "AUTOPILOT_CONTACT_RETRY_S", 0)

    async def no_contact(lead):
        return None

    monkeypatch.setattr(A, "main_contact", no_contact)
    lead = _lead(id=36565196, name="Заказ №19297", pipeline_id=10593102, status_id=83537714,
                 custom_fields_values=[_cf(577671, "Заказ")])

    async def run():
        await A.run_bot(lead, _stage(83537714, "Новый лид", [_bot(7131)]), _bot(7131))
        await asyncio.sleep(0)

    asyncio.run(run())
    assert started == []                        # до отправки дело не дошло
    assert rows[-1]["outcome"] == "stop_no_chat"
    assert feed and feed[-1]["kind"] == "autopilot_no_chat"
    assert "нет телефона" in _SENT[-1]["text"]


def test_informational_bot_does_not_need_a_chat(monkeypatch):
    """Бот с режимом «ответ не нужен» ничего не ждёт, поэтому отсутствие телефона ему не
    мешает - алерта тут быть не должно, иначе робот начнёт шуметь на информационных шагах."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714)
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    monkeypatch.setattr(A, "advance", lambda *a, **k: _noop())
    monkeypatch.setattr(A, "AUTOPILOT_CONTACT_RETRY_S", 0)

    async def no_contact(lead):
        return None

    monkeypatch.setattr(A, "main_contact", no_contact)
    lead = _lead(id=36565197, pipeline_id=10593102, status_id=83537714)

    asyncio.run(A.run_bot(lead, _stage(83537714, "Новый лид", []),
                          _bot(7131, launched_by="amo_grid", stop_mode="never")))
    assert all(r["outcome"] != "stop_no_chat" for r in rows)


def test_success_stage_of_a_lead_we_never_led_is_left_alone(monkeypatch):
    """⚠️ Поймано наблюдением 27.09.2026. В маршруте есть карточка «Успешно реализовано», и по
    ней робот брал в ведение ЛЮБУЮ успешную сделку компании: августовский «Заказ №17860» попал
    в УР и получил две строки журнала. Записи и блокировки на сделки, которых робот не касался,
    не нужны никому."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)]),
                     _stage(A.STATUS_SUCCESS, "Успешно реализовано", [], is_final=True)])
    rows = _capture(monkeypatch)
    claims: list[tuple] = []
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "claim", lambda *a: claims.append(a) or True)
    monkeypatch.setattr(A.store, "list_for_lead", lambda lead_id: [])

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №17860", pipeline_id=10593102,
                     status_id=A.STATUS_SUCCESS,
                     custom_fields_values=[_cf(577671, "Заказ")])

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(36508695))
    assert claims == []                 # ведение не заводим
    assert rows == []                   # и журнал не пачкаем


def test_success_stage_of_our_own_lead_still_finishes_the_route(monkeypatch):
    """А свою сделку робот в финале дочитывает: иначе успешный прогон остался бы без записи
    «маршрут пройден», и следующая сессия не поняла бы, чем он кончился."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)]),
                     _stage(A.STATUS_SUCCESS, "Успешно реализовано", [], is_final=True)])
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "claim", lambda *a: True)
    monkeypatch.setattr(A.store, "list_for_lead",
                        lambda lead_id: [{"lead_id": lead_id, "status_id": 83537714}])
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19294", pipeline_id=10593102,
                     status_id=A.STATUS_SUCCESS,
                     custom_fields_values=[_cf(577671, "Заказ")])

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.handle_lead_change(36565053))
    assert rows and rows[-1]["outcome"] == "done"


def test_stale_entry_is_journalled_once_not_on_every_webhook(monkeypatch):
    """⚠️ Поймано в бою 27.09.2026, через час после выкатки гейта: по одному залежавшемуся
    заказу натекло 28 строк журнала за семь минут. Гейт стоит ДО `claim`, значит отбивает
    каждый вебхук, а их по стоящей сделке десятки."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A, "AUTOPILOT_ENABLED", True)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A, "run_stage", lambda *a, **k: _noop())
    old = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 3 * 24 * 3600

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537714, created_at=old,
                     custom_fields_values=[_cf(577671, "Заказ")])

    monkeypatch.setattr(A, "load_lead", fake_load)
    for _ in range(5):
        asyncio.run(A.handle_lead_change(36564965))
    stale = [r for r in rows if r["outcome"] == "skipped_stale_entry"]
    assert len(stale) == 1


def test_payment_mismatch_alert_does_not_repeat(monkeypatch):
    """⚠️ Поймано в бою 27.09.2026: «платёжная система говорит оплачено, а в МойСкладе оплаты
    нет» ушло дважды за полторы минуты по одному заказу. Ветка «Оплата получена» живёт ДО
    `store.claim`, поэтому гейт от повторного вебхука её не защищает, а вебхуков по сделке,
    стоящей на этапе, приходят десятки."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "00:00", "end": "23:59"}],
                        "live_whitelist_enabled": False}, pipeline_id=10593102)
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "list_for_lead",
                        lambda lead_id: [{"lead_id": lead_id, "status_id": 83537714}])
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)

    async def unpaid(path, params=None, retries=3):
        return {"payedSum": 0}

    monkeypatch.setattr(A.ms_client, "get", unpaid)
    lead = _lead(id=36564965, name="Заказ №19286", pipeline_id=10593102,
                 status_id=A.STATUS_PAYMENT_RECEIVED,
                 custom_fields_values=[_cf(576689, "uuid-1"), _cf(577373, "Онлайн-оплата")])

    async def run():
        await A.on_payment_received(lead)
        await A.on_payment_received(lead)
        await asyncio.sleep(0)

    asyncio.run(run())
    said = [m for m in _SENT if "МойСкладе оплаты нет" in m["text"]]
    assert len(said) == 1
    assert len([r for r in rows if r["outcome"] == "failed"]) == 1


# ── подбор пропущенного из переписки панели (27.09.2026) ────────────────────────

def test_catchup_picks_up_the_answer_a_webhook_lost(monkeypatch):
    """⚠️ Кейс заказа 19288. Бот грида отправил шаблон вечером, клиент ответил «Да, всё верно» в
    14:00 - робот ответа не увидел, потому что вебхука не было (или сделка уже не слушалась), и
    заказ, уже ОПЛАЧЕННЫЙ, остался стоять на «Новом лиде».

    Просьба Кати: «отслеживаемые сделки надо проверять хотя бы каждые 5 мин». Подбор спрашивает
    панель - у неё вся переписка - и ведёт себя точно так же, как по вебхуку."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131, launched_by="amo_grid")])])
    A._catchup_at = 0.0
    answers: list[tuple] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "list_by_phase",
                        lambda phase: [{
                            "lead_id": 36564989, "status_id": 83537714, "bot_id": 7131,
                            "phase": phase, "chat_id": "79998993959",
                            "launch_ok_at": "2026-09-27T08:13:00+00:00", "delivery": [],
                        }] if phase == S.PHASE_REPLY else [])

    async def fake_activity(chat_id, since):
        assert chat_id == "79998993959"
        return {"inbound": [{"text": "Да, всё верно", "chat_type": "whatsapp",
                             "at": "2026-09-27T11:00:00+00:00"}], "echo": []}

    async def fake_answer(row, text, chat_type=""):
        answers.append((row["lead_id"], text, chat_type))

    monkeypatch.setattr(A, "fetch_chat_activity", fake_activity)
    monkeypatch.setattr(A, "on_client_answer", fake_answer)
    asyncio.run(A.catch_up_on_chats())
    assert answers == [(36564989, "Да, всё верно", "whatsapp")]


def test_catchup_runs_not_more_often_than_its_interval(monkeypatch):
    """Подбор ходит в панель по КАЖДОЙ ведомой сделке, поэтому частить ему нельзя: тик робота
    раз в минуту, а спрашиваем раз в пять."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]})
    A._catchup_at = 0.0
    calls: list[str] = []
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [])
    monkeypatch.setattr(A, "fetch_chat_activity",
                        lambda chat_id, since: calls.append(chat_id))

    async def run():
        await A.catch_up_on_chats()
        await A.catch_up_on_chats()

    asyncio.run(run())
    assert calls == []          # сделок нет - и ходить незачем
    assert A._catchup_at > 0    # но отметка времени поставлена, значит второй проход отложен


def test_catchup_records_delivery_when_only_echo_is_there(monkeypatch):
    """Ответа нет, но эхо с подтверждением доставки есть - дозаписываем статус: вебхук статуса
    мог не дойти так же, как вебхук сообщения."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]})
    A._catchup_at = 0.0
    recorded: list[tuple] = []
    monkeypatch.setattr(A.store, "list_by_phase",
                        lambda phase: [{
                            "lead_id": 36565195, "status_id": 83537714, "bot_id": 7131,
                            "phase": phase, "chat_id": "79935370419",
                            "launch_ok_at": "2026-09-27T09:29:17+00:00", "delivery": [],
                        }] if phase == S.PHASE_DELIVERY else [])

    async def fake_activity(chat_id, since):
        return {"inbound": [], "echo": [{"status": "delivered", "chat_type": "whatsapp"}]}

    async def fake_record(row, status, chat_type):
        recorded.append((row["lead_id"], status, chat_type))

    monkeypatch.setattr(A, "fetch_chat_activity", fake_activity)
    monkeypatch.setattr(A, "record_delivery", fake_record)
    asyncio.run(A.catch_up_on_chats())
    assert recorded == [(36565195, "delivered", "whatsapp")]


def test_journal_says_who_moved_the_lead_into_success(monkeypatch):
    """Замечание Кати 27.09.2026 по сделке 36564965: «двинули вперёд не мы, но в лог ушло, будто
    это бот двинул». Теперь причина в журнале называет автора перевода."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]},
              route=[_stage(A.STATUS_SUCCESS, "Успешно реализовано", [], is_final=True)])
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    lead = _lead(id=36564965, status_id=A.STATUS_SUCCESS)
    stage = _stage(A.STATUS_SUCCESS, "Успешно реализовано", [], is_final=True)

    asyncio.run(A.advance(lead, stage, "конец", moved_by_us=False))
    assert "перевёл не робот" in rows[-1]["reason"]

    asyncio.run(A.advance(lead, stage, "конец", moved_by_us=True))
    assert "уводит перевод в офис" in rows[-1]["reason"]


# ── клиент молчит сутки (Катя 27.09.2026) ───────────────────────────────────────

def test_silent_client_calls_a_human_after_the_wait(monkeypatch):
    """«Он должен ждать ответа день, потом слать алерт». До этой правки у фазы ожидания срока не
    было вовсе: сделка висела, пока её молча не уберёт уборка по давности, и о неподтверждённом
    заказе не узнавал никто."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}],
                        "live_whitelist_enabled": False},
              pipeline_id=10593102, entry_status_id=83537714,
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    feed: list[dict] = []
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: feed.append(kw))
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 36565195, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_REPLY,
        "chat_id": "79935370419", "launch_ok_at": "2026-09-26T09:00:00+00:00",
        "updated_at": "2026-09-26T09:05:00+00:00", "delivery": [],
    }] if phase == S.PHASE_REPLY else [])

    async def fake_load(lead_id):
        return _lead(id=lead_id, name="Заказ №19296", pipeline_id=10593102,
                     status_id=83537714, responsible_user_id=13929334)

    monkeypatch.setattr(A, "load_lead", fake_load)

    async def run():
        await A.check_reply_windows()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert rows[-1]["outcome"] == "stop_no_reply"
    assert feed and feed[-1]["kind"] == "autopilot_no_reply"
    assert "не подтвердил заказ" in _SENT[-1]["text"]
    assert _SENT[-1]["chat_id"] == A.NOTIFY_CHAT_ID


def test_client_still_within_the_wait_is_left_alone(monkeypatch):
    """Сутки не вышли - молчим: клиент имеет право подумать, а лишний алерт учит игнорировать чат."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]},
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    recent = (datetime.datetime.now(datetime.timezone.utc)
              - datetime.timedelta(hours=3)).isoformat()
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 36565195, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_REPLY,
        "chat_id": "79935370419", "launch_ok_at": recent, "updated_at": recent, "delivery": [],
    }] if phase == S.PHASE_REPLY else [])
    asyncio.run(A.check_reply_windows())
    assert rows == []


def test_lead_moved_away_while_waiting_does_not_alert(monkeypatch):
    """Сделку увели с этапа, пока ждали ответа, - это право человека, шуметь не о чем."""
    _settings(settings={"mode": "live", "work_hours": [{"start": "10:00", "end": "19:00"}]},
              route=[_stage(83537714, "Новый лид", [_bot(7131)])])
    rows = _capture(monkeypatch)
    monkeypatch.setattr(A, "panel_notify_bg", lambda **kw: None)
    monkeypatch.setattr(A.store, "finish", lambda *a, **k: None)
    monkeypatch.setattr(A.store, "list_by_phase", lambda phase: [{
        "lead_id": 36565195, "status_id": 83537714, "bot_id": 7131, "phase": S.PHASE_REPLY,
        "chat_id": "79935370419", "launch_ok_at": "2026-09-26T09:00:00+00:00",
        "updated_at": "2026-09-26T09:05:00+00:00", "delivery": [],
    }] if phase == S.PHASE_REPLY else [])

    async def fake_load(lead_id):
        return _lead(id=lead_id, pipeline_id=10593102, status_id=83537718)

    monkeypatch.setattr(A, "load_lead", fake_load)
    asyncio.run(A.check_reply_windows())
    assert rows == []
    assert _SENT == []
