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
import json
import os
import sys
import tempfile
import types


# Что лежало в sys.modules до наших заглушек - чтобы вернуть это после импортов.
_SAVED_SYS_MODULES: dict = {}


def _restore_sys_modules() -> None:
    """Вернуть `sys.modules` как было. Зовётся после импортов кода под тестом.

    ⚠️ Зачем. Заглушка обязана стоять ДО импорта модуля под тестом, иначе он возьмёт
    настоящую зависимость. Но оставленная в `sys.modules` навсегда, она достаётся всем
    файлам, импортированным позже: их подмены ложатся на НАШУ заглушку, боевой код зовёт
    настоящую отправку, запрос уходит в сеть, и прогон висит. Перебор парами 07.10.2026
    показал ровно это - прогон одним процессом не доходил до конца.

    Модуль под тестом уже держит свои ссылки на заглушки, возврат ему не мешает.
    """
    for name, original in _SAVED_SYS_MODULES.items():
        if original is not None:
            sys.modules[name] = original
        else:
            sys.modules.pop(name, None)


def _stub(name, **attrs):
    _SAVED_SYS_MODULES.setdefault(name, sys.modules.get(name))
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)
_stub("telegram_bot", send_alert=None)

# Счётчики лежат на диске - в тесте во временном файле, а не в боевом /app/var.
os.environ["NEW_LEAD_WATCH_PATH"] = os.path.join(tempfile.mkdtemp(), "new_lead_watch.json")

import new_lead_watch as N  # noqa: E402
import tg_recipients  # noqa: E402
from waybill_config import (  # noqa: E402
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_TANGEMSHOP,
    STATUS_ACADEMY_INBOUND_LEAD,
    STATUS_CLEVER_IN_PROGRESS,
    STATUS_NEW_LEAD,
    STATUS_NEW_LEAD_BUFFERS,
    STATUS_TANGEM_IN_PROGRESS,
    STATUS_TANGEM_NEW_ORDER,
)

# ⚠️ Код под тестом импортирован и держит заглушки - возвращаем sys.modules, чтобы
# соседние файлы получили НАСТОЯЩИЕ модули. Разбор - в шапке _restore_sys_modules.
_restore_sys_modules()


ROP_CHAT = -5358037627
OP_CHAT = -1003680811996
EGOR = 13929334
LEAD = 36500777
# Менеджер Академии: в amo он «Менеджер1», в Телеграме @CrPetr. Номер здесь выдуманный -
# на 29.09.2026 боевой ещё не снят, а тесту важен сам факт попадания в список.
ACADEMY_MOP = 99000001
ACADEMY_TEAM_CHAT = -4777000111

# amo_service тут НАСТОЯЩИЙ (он импортируется без сети), подменяем только функции и
# возвращаем их обратно. Заглушка целым модулем ломала бы сборку соседних тестов:
# test_office_transfer и другие работают с живым модулем.
_ORIG_GET_LEAD = N.amo_service.get_lead_full


def setup_function(_=None):
    N._pending.clear()
    N.ROP_CHAT_ID = ROP_CHAT
    N.NOTIFY_CHAT_ID = OP_CHAT
    N.NOTIFY_THREAD_ID = 10479
    N.ACADEMY_LEAD_UNTAKEN_ENABLED = True
    # Маршрут по ответственному выключен по умолчанию, как на свежевыкаченном проде:
    # список пуст - адреса не меняются. Включают его отдельные тесты.
    tg_recipients.ACADEMY_TEAM_AMO_IDS = frozenset()
    tg_recipients.ACADEMY_TEAM_CHAT = None
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


# ── счётчики переживают рестарт (27.09.2026) ────────────────────────────────────

def test_clock_survives_a_restart():
    """⚠️ Поймано наблюдением 27.09.2026. Состояние жило в памяти процесса, и пересборка
    контейнера тихо съедала эскалации: пять заказов, простоявших на входе с вечера, не дали
    алерта руководству вообще - ожидания потерялись первой же пересборкой.

    Отметка времени поднимается КАК БЫЛА: лид, пролежавший три рабочих часа до рестарта,
    обязан остаться просроченным и после него.
    """
    N._pending.clear()
    was = datetime.datetime(2026, 9, 26, 20, 19, tzinfo=N._MSK)
    N._pending[36564965] = {"since": was}
    N._save()

    N._pending.clear()          # как будто контейнер пересобрали
    N._load()
    assert list(N._pending) == [36564965]
    assert N._pending[36564965]["since"] == was


def test_broken_state_file_does_not_break_the_watch():
    """Битый файл - начинаем с чистого листа и работаем как раньше, а не падаем на старте."""
    N.STATE_PATH.write_text("{не json", encoding="utf-8")
    N._pending.clear()
    N._load()
    assert N._pending == {}


def test_state_file_keeps_only_pending_leads():
    """Ушёл с входа - из файла исчез: иначе после рестарта сторож сторожил бы взятые лиды."""
    N._pending.clear()
    N.note_lead(36565217, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    saved = json.loads(N.STATE_PATH.read_text(encoding="utf-8"))
    assert list(saved["pending"]) == ["36565217"]

    N.note_lead(36565217, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_IN_PROGRESS)
    saved = json.loads(N.STATE_PATH.read_text(encoding="utf-8"))
    assert saved["pending"] == {}


# ── Академия: вторая воронка того же сторожа (29.09.2026) ────────────────────────
#
# Держим то, чем Академия ОТЛИЧАЕТСЯ от розницы: входной этап один, адресат - чат
# менеджеров (значит нужен @тег), адрес уточняется по ответственному. Порог и окно
# общие и проверены тестами выше - здесь их не дублируем.


def _academy_lead(status_id=STATUS_ACADEMY_INBOUND_LEAD, *, responsible=ACADEMY_MOP,
                  name="Заявка из бота"):
    async def get_lead_full(lead_id, with_=()):
        return {"id": lead_id, "status_id": status_id, "pipeline_id": PIPELINE_ACADEMY,
                "responsible_user_id": responsible, "name": name}

    N.amo_service.get_lead_full = get_lead_full


def _academy_waiting_since(moment):
    N.note_lead(LEAD, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    N._pending[LEAD]["since"] = moment


def _enable_route():
    tg_recipients.ACADEMY_TEAM_AMO_IDS = frozenset({ACADEMY_MOP})
    tg_recipients.ACADEMY_TEAM_CHAT = ACADEMY_TEAM_CHAT


def test_academy_inbound_lead_starts_the_clock():
    N.note_lead(LEAD, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert N._pending[LEAD]["pipeline"] == PIPELINE_ACADEMY


def test_academy_other_stages_are_not_watched():
    """⚠️ Живой повод: 29.09.2026 в Академию заливали 200 сделок базы «Не купили DEFI-3»
    (этап 88943002). Входной этап у Академии ОДИН - «Входящий лид», и массовый прогон по
    другим этапам сторожа будить не должен, иначе чат зальёт двумя сотнями сообщений."""
    N.note_lead(LEAD, PIPELINE_ACADEMY, 88943002)
    N.note_lead(LEAD + 1, PIPELINE_ACADEMY, 70070966)      # Лист ожидания
    N.note_lead(LEAD + 2, PIPELINE_ACADEMY, 88485802)      # Не трогать этих клиентов
    assert N._pending == {}


def test_academy_retail_entry_stage_is_not_academy_entry():
    """Розничный «Новый лид» в воронке Академии входным этапом не считается: номера
    этапов у воронок разные, и спутать наборы легко."""
    N.note_lead(LEAD, PIPELINE_ACADEMY, STATUS_NEW_LEAD)
    assert N._pending == {}


def test_academy_taking_the_lead_removes_the_clock():
    N.note_lead(LEAD, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    N.note_lead(LEAD, PIPELINE_ACADEMY, 88464034)          # Взят в работу
    assert N._pending == {}


def test_academy_goes_to_department_chat_with_a_tag():
    """Адресат - чат менеджеров, значит в тексте @тег и призыв, а не имя словами:
    в чате руководства звать некого, а здесь читать сообщение должен человек."""
    sent = _catch_sends()
    _academy_lead()
    _academy_waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert len(sent) == 1
    text, chat = sent[0]
    assert chat == OP_CHAT
    assert "@" in text
    assert "возьмите" in text
    assert "·" not in text
    assert "Заявка из бота" in text


def test_academy_alert_routes_to_team_chat():
    """Сделку ведёт менеджер Академии - уведомление уходит в группу его команды, а не в
    топик розницы. Это и есть маршрут по ответственному."""
    _enable_route()
    sent = _catch_sends()
    _academy_lead(responsible=ACADEMY_MOP)
    _academy_waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent[0][1] == ACADEMY_TEAM_CHAT


def test_academy_lead_of_an_old_manager_stays_in_the_department_chat():
    """Сделку Академии ведёт кто-то из старых менеджеров - сообщение остаётся в
    Store [Отдел продаж] (прямое указание Кати 29.09.2026)."""
    _enable_route()
    sent = _catch_sends()
    _academy_lead(responsible=EGOR)
    _academy_waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent[0][1] == OP_CHAT


def test_route_without_team_chat_keeps_the_old_address():
    """Список менеджеров заполнили, а chat_id группы ещё нет - уведомление идёт по
    старому адресу, а не пропадает. Это штатное состояние свежей выкатки."""
    tg_recipients.ACADEMY_TEAM_AMO_IDS = frozenset({ACADEMY_MOP})
    tg_recipients.ACADEMY_TEAM_CHAT = None
    sent = _catch_sends()
    _academy_lead()
    _academy_waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent[0][1] == OP_CHAT


def test_retail_is_never_routed_to_the_academy_chat():
    """Маршрут просят только воронки, у которых он включён в таблице. Розничная эскалация
    руководству по ответственному не переезжает никогда."""
    _enable_route()
    sent = _catch_sends()
    _lead_at_status(STATUS_NEW_LEAD, responsible=ACADEMY_MOP)
    _waiting_since(N._now_msk() - datetime.timedelta(days=1))
    asyncio.run(N._sweep())
    assert sent[0][1] == ROP_CHAT


def test_academy_flag_does_not_silence_retail():
    """Выключатели раздельные: погасили сторож Академии - розница работает как раньше."""
    N.ACADEMY_LEAD_UNTAKEN_ENABLED = False
    N.note_lead(LEAD, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    assert N._pending == {}
    N.note_lead(LEAD + 1, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    assert list(N._pending) == [LEAD + 1]


def test_lead_moved_to_another_pipeline_is_not_reported():
    """Сделку увели из Академии - этот сторож своё отработал и молчит, даже если этап в
    новой воронке случайно совпал по номеру со входным."""
    sent = _catch_sends()

    async def moved(lead_id, with_=()):
        return {"id": lead_id, "status_id": STATUS_ACADEMY_INBOUND_LEAD,
                "pipeline_id": PIPELINE_CLEVER_MAIN, "responsible_user_id": EGOR, "name": "x"}

    _academy_waiting_since(N._now_msk() - datetime.timedelta(days=1))
    N.amo_service.get_lead_full = moved
    asyncio.run(N._sweep())
    assert sent == []
    assert N._pending == {}


def test_old_state_file_without_pipeline_is_read_as_retail():
    """Файл, записанный до 29.09.2026, воронки не знает. Такие счётчики поднимаем как
    розничные, а не выбрасываем: выброс - это те же потерянные эскалации, из-за которых
    файл на диске и появился."""
    N._pending.clear()
    was = datetime.datetime(2026, 9, 26, 20, 19, tzinfo=N._MSK)
    N.STATE_PATH.write_text(
        json.dumps({"pending": {"36564965": {"since": was.isoformat()}}}), encoding="utf-8")
    N._load()
    assert N._pending[36564965]["pipeline"] == PIPELINE_CLEVER_MAIN
    assert N._pending[36564965]["since"] == was


def test_state_file_remembers_the_pipeline():
    N._pending.clear()
    N.note_lead(36565218, PIPELINE_ACADEMY, STATUS_ACADEMY_INBOUND_LEAD)
    saved = json.loads(N.STATE_PATH.read_text(encoding="utf-8"))
    assert saved["pending"]["36565218"]["pipeline"] == PIPELINE_ACADEMY


# ───────────────────────────── TangemShop (29.09.2026) ─────────────────────────────


def test_tangemshop_is_off_by_default():
    """Выключатель свой, и по умолчанию он опущен: воронку включают отдельным шагом."""
    assert N.NEW_LEAD_WATCH_TANGEMSHOP is False
    N.note_lead(LEAD, PIPELINE_TANGEMSHOP, STATUS_TANGEM_NEW_ORDER)
    assert N._pending == {}


def test_tangemshop_counts_like_retail_when_enabled():
    """Включили - «Новый заказ» заводит счётчик, уход с него снимает. Как у розницы."""
    N.NEW_LEAD_WATCH_TANGEMSHOP = True
    try:
        N.note_lead(LEAD, PIPELINE_TANGEMSHOP, STATUS_TANGEM_NEW_ORDER)
        assert N._pending[LEAD]["pipeline"] == PIPELINE_TANGEMSHOP
        N.note_lead(LEAD, PIPELINE_TANGEMSHOP, STATUS_TANGEM_IN_PROGRESS)
        assert N._pending == {}
    finally:
        N.NEW_LEAD_WATCH_TANGEMSHOP = False


def test_tangemshop_shares_the_retail_event_key():
    """Событие каталога одно на два магазина - осознанно: воронку ведут те же люди по
    тем же правилам, и двум «новым лидам» в панели разойтись нечем. Различаются только
    подписью в тексте, чтобы руководитель видел, чей это заказ."""
    tg, retail = N._WATCHED[PIPELINE_TANGEMSHOP], N._WATCHED[PIPELINE_CLEVER_MAIN]
    assert tg.event == retail.event
    assert tg.chat == retail.chat
    assert tg.subject != retail.subject


def test_tangemshop_does_not_wake_the_loop_when_off():
    """Выключенная воронка не должна сама по себе поднимать цикл опроса."""
    was_rop = N.ROP_CHAT_ID
    N.ROP_CHAT_ID = None
    N.ACADEMY_LEAD_UNTAKEN_ENABLED = False
    try:
        assert [w for pid, w in N._WATCHED.items() if N._is_on(w, pid)] == []
    finally:
        N.ROP_CHAT_ID = was_rop
        N.ACADEMY_LEAD_UNTAKEN_ENABLED = True
