"""Сторож просроченной записи в офис: на чём стоит правильность.

Держим то, что легко сломать правкой «на глазок»:

  • судим по полю «Дата окончания записи» (578065), а не по «Дата начала записи» (578063);
  • порог ровно «конец записи плюс запас», ни минутой раньше;
  • запись старше потолка давности задачи не получает - иначе первый проход после
    выкатки поставил бы 13 задач по архиву;
  • ключ дедупа меняется при ПЕРЕЗАПИСИ и не меняется сам по себе со временем;
  • увели с этапа, перезаписали или amo молчит - ключ НЕ сожжён, второй шанс остался;
  • режим отчёта ключи не жжёт, иначе фича уехала бы в бой навсегда молчащей;
  • срок задачи никогда не в прошлом и не в три ночи;
  • в тексте задачи нет ID, точек посередине и собачек.

Запуск: python3 -m pytest test_office_record_watch.py -q
"""

import asyncio
import datetime
import os
import sys
import tempfile
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

# ⚠ФАЙЛОВАЯ ЛОВУШКА. `autopilot_store` берёт путь к базе НА ИМПОРТЕ
# (`DB_PATH = os.getenv(...)` на уровне модуля), а наш модуль его импортирует ради дедупа.
# Кто импортировал первым - тот и выбрал путь на всю сессию pytest. Без этих двух строк
# мы уводим `test_autopilot` на боевой путь `/app/var/autopilot_state.sqlite3` и ломаем ему
# два теста (поймано 27.09.2026 прогоном двух файлов вместе). Ставим СВОЮ временную
# базу ДО импорта - так же, как это делает test_autopilot.py.
os.environ.setdefault(
    "AUTOPILOT_DB_PATH", os.path.join(tempfile.mkdtemp(), "office_record_test.sqlite3")
)

import office_record_watch as O  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_OFFICE_RECORD_END,
    FIELD_OFFICE_RECORD_START,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_TANGEMSHOP,
    STATUS_CLEVER_OFFICE_RECORD,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
    STATUS_TANGEM_OFFICE_RECORD,
)

LEAD = 36555973
CONTACT = 48599231
OTHER_LEAD_SAME_CLIENT = 36500001
OTHER_CLIENT_LEAD = 36500002
ZUBALIY = 13963494
POLESSKIY = 13946318

# Живой слепок: запись 15.09.2026 с 14:00 до 14:15 МСК (сделка 36555973).
END_TS = 1789467300
START_TS = 1789466400

# amo_service и api тут НАСТОЯЩИЕ (импортируются без сети), подменяем только функции и
# возвращаем их обратно - заглушка целым модулем ломала бы сборку соседних тестов.
_ORIG_GET_LEAD = O.amo_service.get_lead_full
_ORIG_BY_STATUS = O.amo_service.get_leads_by_status
_ORIG_ADD_NOTE = O.amo_service.add_note
_ORIG_CREATE_TASK = O.api.create_task
_ORIG_OPEN_TASKS = O.api.get_open_tasks_by_responsible
_ORIG_CONTACT = O.amo_service.get_contact_by_id
_ORIG_CLAIM = O.notices.claim_notice
_ORIG_PURGE = O.notices.purge_notices_older_than


def setup_function(_=None):
    O.amo_service.get_lead_full = _ORIG_GET_LEAD
    O.amo_service.get_leads_by_status = _ORIG_BY_STATUS
    O.amo_service.add_note = _ORIG_ADD_NOTE
    O.api.create_task = _ORIG_CREATE_TASK
    O.api.get_open_tasks_by_responsible = _ORIG_OPEN_TASKS
    O.amo_service.get_contact_by_id = _ORIG_CONTACT
    O.notices.claim_notice = _ORIG_CLAIM
    O.notices.purge_notices_older_than = _ORIG_PURGE
    O._last_run.clear()
    # Уборку отметок в тестах не гоняем: она полезла бы в боевую sqlite.
    O._last_purge_ts = 10**12
    O.OFFICE_RECORD_GRACE_MIN = 10
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3
    O.OFFICE_RECORD_MAX_PER_PASS = 5
    O.OFFICE_RECORD_WATCH_ENABLED = True
    O.OFFICE_RECORD_WATCH_CREATE_ENABLED = True
    O.OFFICE_RECORD_NOTE_ENABLED = False
    O.OFFICE_RECORD_SKIP_IF_CLIENT_BUSY = True
    O.OFFICE_RECORD_ALERT_ENABLED = False
    O.OFFICE_RECORD_TASK_RESPONSIBLE_USER_ID = 0
    O.OFFICE_RECORD_TASK_TYPE_ID = 1
    O.OFFICE_RECORD_TASK_DEADLINE_H = 4
    O.OFFICE_RECORD_WINDOW_START_H = 10
    O.OFFICE_RECORD_WINDOW_END_H = 19


def teardown_function(_=None):
    """⚠️ Убираем подмены ЗА СОБОЙ, а не только перед собой.

    `O.notices` это живой `autopilot_store`, общий на всю сессию pytest. Пока уборки не
    было, последний мой тест оставлял в нём фальшивый `claim_notice`, и соседний
    `test_autopilot` падал - но ТОЛЬКО когда мой файл шёл первым. Порядок файлов в
    команде не должен менять результат (поймано 29.09.2026).
    """
    setup_function()


def _lead(end_ts=END_TS, *, lead_id=LEAD, status=STATUS_CLEVER_OFFICE_RECORD,
          pipeline=PIPELINE_CLEVER_MAIN, responsible=ZUBALIY, with_start=True,
          contact_id=CONTACT):
    cfs = []
    if with_start:
        cfs.append({"field_id": FIELD_OFFICE_RECORD_START, "field_type": "date_time",
                    "values": [{"value": START_TS}]})
    if end_ts is not None:
        cfs.append({"field_id": FIELD_OFFICE_RECORD_END, "field_type": "date_time",
                    "values": [{"value": end_ts}]})
    lead = {"id": lead_id, "status_id": status, "pipeline_id": pipeline,
            "responsible_user_id": responsible, "custom_fields_values": cfs}
    if contact_id:
        lead["_embedded"] = {"contacts": [{"id": contact_id}]}
    return lead


class _FakeClaims:
    """Ведёт себя как настоящий первичный ключ (lead_id, kind): второй INSERT - False."""

    def __init__(self):
        self.taken = set()

    def __call__(self, lead_id, kind):
        key = (int(lead_id), kind)
        if key in self.taken:
            return False
        self.taken.add(key)
        return True


def _wire(leads, *, fresh=None, task_ok=True, open_tasks=(), client_leads=(),
          contact_silent=False):
    """Подменяет amo и дедуп. Возвращает (claims, созданные задачи).

    open_tasks: что отдаёт api.get_open_tasks_by_responsible. () - нет, None - молчит.
    client_leads: сделки клиента помимо самой сторожимой.
    contact_silent: контакт не читается (amo молчит).
    """
    claims = _FakeClaims()
    tasks = []

    async def fake_by_status(status_id, with_=("contacts",), page_limit=50):
        return list(leads)

    async def fake_full(lead_id, with_=()):
        if fresh is None:
            return _lead()
        return fresh(int(lead_id)) if callable(fresh) else fresh

    async def fake_create_task(entity_id, text, responsible_user_id, complete_till,
                               task_type_id=None, entity_type="leads"):
        tasks.append({"lead_id": entity_id, "text": text, "responsible": responsible_user_id,
                      "complete_till": complete_till, "type": task_type_id})
        return task_ok

    async def fake_open_tasks(responsible_user_id):
        return None if open_tasks is None else list(open_tasks)

    async def fake_contact(contact_id, with_=()):
        if contact_silent:
            return None
        return {"id": int(contact_id),
                "_embedded": {"leads": [{"id": i} for i in client_leads]}}

    O.amo_service.get_leads_by_status = fake_by_status
    O.amo_service.get_lead_full = fake_full
    O.api.create_task = fake_create_task
    O.api.get_open_tasks_by_responsible = fake_open_tasks
    O.amo_service.get_contact_by_id = fake_contact
    O.notices.claim_notice = claims
    return claims, tasks


# ─────────────────────────── чистые функции ───────────────────────────


def test_end_ts_читается_из_поля_окончания_а_не_начала():
    assert O.end_ts_of(_lead()) == END_TS
    # Осталось только «Дата начала записи» - судить не по чему.
    assert O.end_ts_of(_lead(end_ts=None)) is None


def test_end_ts_мусор_и_пустота():
    assert O.end_ts_of({"custom_fields_values": []}) is None
    assert O.end_ts_of({}) is None
    assert O.end_ts_of(_lead(end_ts="")) is None
    assert O.end_ts_of(_lead(end_ts=0)) is None
    # amo иногда отдаёт unix строкой - это валидное значение.
    assert O.end_ts_of(_lead(end_ts=str(END_TS))) == END_TS


def test_порог_ровно_конец_записи_плюс_запас():
    assert O.decide(_lead(), END_TS + 10 * 60 - 1)[0] == "not-due"
    assert O.decide(_lead(), END_TS + 10 * 60)[0] == "fire"


def test_запись_старше_потолка_давности_не_трогаем():
    # Это тот самый предохранитель, который спасает 13 просроченных сделок этапа.
    assert O.decide(_lead(), END_TS + 3 * 86400 + 1)[0] == "too-old"
    assert O.decide(_lead(), END_TS + 3 * 86400 - 1)[0] == "fire"


def test_без_даты_записи_молчим():
    assert O.decide(_lead(end_ts=None), END_TS + 86400) == ("no-date", None)


def test_закрытая_сделка_и_чужая_воронка():
    now = END_TS + 3600
    assert O.decide(_lead(status=STATUS_SUCCESS), now)[0] == "other-stage"
    assert O.decide(_lead(status=STATUS_CLOSED_LOST), now)[0] == "other-stage"
    assert O.decide(_lead(pipeline=9421022), now)[0] == "other-pipeline"


def test_ключ_дедупа_взводится_только_перезаписью():
    # Время идёт - ключ тот же: одна задача на одну запись (решение Кати 27.09.2026).
    assert O.notice_kind(END_TS) == O.notice_kind(END_TS)
    # Перезаписали клиента - ключ другой, сторож взводится заново.
    assert O.notice_kind(END_TS) != O.notice_kind(END_TS + 86400)


# ── TangemShop (29.09.2026): свой этап «Запись в офис», свой выключатель ─────


def test_tangemshop_при_выключенном_флаге_не_наша_воронка():
    assert O.OFFICE_RECORD_WATCH_TANGEMSHOP is False
    assert O.watched_stages() == {PIPELINE_CLEVER_MAIN: STATUS_CLEVER_OFFICE_RECORD}
    lead = _lead(pipeline=PIPELINE_TANGEMSHOP, status=STATUS_TANGEM_OFFICE_RECORD)
    assert O.decide(lead, END_TS + 3600)[0] == "other-pipeline"


def test_tangemshop_с_флагом_судится_по_своему_этапу():
    O.OFFICE_RECORD_WATCH_TANGEMSHOP = True
    try:
        assert O.watched_stages() == {
            PIPELINE_CLEVER_MAIN: STATUS_CLEVER_OFFICE_RECORD,
            PIPELINE_TANGEMSHOP: STATUS_TANGEM_OFFICE_RECORD,
        }
        lead = _lead(pipeline=PIPELINE_TANGEMSHOP, status=STATUS_TANGEM_OFFICE_RECORD)
        assert O.decide(lead, END_TS + 10 * 60)[0] == "fire"
        # чужой этап внутри своей воронки - не наш случай
        assert O.decide(
            _lead(pipeline=PIPELINE_TANGEMSHOP, status=STATUS_CLEVER_OFFICE_RECORD),
            END_TS + 3600)[0] == "other-stage"
        # и наоборот: этап Tangemshop внутри розницы тоже мимо
        assert O.decide(
            _lead(pipeline=PIPELINE_CLEVER_MAIN, status=STATUS_TANGEM_OFFICE_RECORD),
            END_TS + 3600)[0] == "other-stage"
        # без полей виджета NOVA задач не появится - оговорка плана, проверенная кодом
        assert O.decide(
            _lead(end_ts=None, pipeline=PIPELINE_TANGEMSHOP,
                  status=STATUS_TANGEM_OFFICE_RECORD), END_TS + 86400) == ("no-date", None)
    finally:
        O.OFFICE_RECORD_WATCH_TANGEMSHOP = False


def test_выключенная_воронка_не_стоит_лишнего_запроса():
    """Проход по этапам: с опущенным флагом запрос ровно один, розничный."""
    asked: list = []

    async def fake_by_status(status_id, with_=()):
        asked.append(status_id)
        return []

    saved = O.amo_service.get_leads_by_status
    O.amo_service.get_leads_by_status = fake_by_status
    try:
        asyncio.run(O._stage_leads())
        assert asked == [STATUS_CLEVER_OFFICE_RECORD]
        asked.clear()
        O.OFFICE_RECORD_WATCH_TANGEMSHOP = True
        asyncio.run(O._stage_leads())
        assert asked == [STATUS_CLEVER_OFFICE_RECORD, STATUS_TANGEM_OFFICE_RECORD]
    finally:
        O.OFFICE_RECORD_WATCH_TANGEMSHOP = False
        O.amo_service.get_leads_by_status = saved
    assert str(END_TS) in O.notice_kind(END_TS)


def test_срок_задачи_всегда_в_будущем_и_в_рабочем_окне():
    msk = O._MSK
    for hour in range(0, 24):
        now = int(datetime.datetime(2026, 9, 28, hour, 30, tzinfo=msk).timestamp())
        deadline = O.task_deadline(now)
        assert deadline > now, hour
        got = datetime.datetime.fromtimestamp(deadline, msk)
        assert 10 <= got.hour < 19, (hour, got)


def test_срок_из_ночи_переносится_на_утро():
    msk = O._MSK
    now = int(datetime.datetime(2026, 9, 28, 23, 0, tzinfo=msk).timestamp())
    got = datetime.datetime.fromtimestamp(O.task_deadline(now), msk)
    assert (got.day, got.hour) == (29, 10)


def test_текст_задачи_человеческий():
    text = O.task_text(END_TS)
    assert O.fmt_when(END_TS) in text
    assert "15.09" in text
    for forbidden in (str(LEAD), str(FIELD_OFFICE_RECORD_END), "·", "@", "{"):
        assert forbidden not in text, forbidden


# ─────────────────────────── проход ───────────────────────────


def test_счастливый_путь_одна_задача_на_свежего_ответственного():
    now = END_TS + 3600
    # В списке ответственный один, в свежей сделке уже другой: на входе в этап работает
    # change_responsible, и брать надо свежего.
    claims, tasks = _wire([_lead()], fresh=_lead(responsible=POLESSKIY))
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    decisions = asyncio.run(O.sweep_once())
    assert decisions == {"created": 1}
    assert len(tasks) == 1
    assert tasks[0]["lead_id"] == LEAD
    assert tasks[0]["responsible"] == POLESSKIY
    assert tasks[0]["type"] == 1
    assert tasks[0]["complete_till"] > now


def test_увели_с_этапа_между_чтениями_ключ_не_сожжён():
    claims, tasks = _wire([_lead()], fresh=_lead(status=STATUS_SUCCESS))
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    assert asyncio.run(O.sweep_once()) == {"moved": 1}
    assert tasks == []
    assert claims.taken == set()


def test_перезаписали_пока_шёл_проход_и_новая_дата_ловится():
    new_end = END_TS + 7 * 86400
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(end_ts=new_end))
    assert asyncio.run(O.sweep_once()) == {"rescheduled": 1}
    assert tasks == []
    assert claims.taken == set()

    # Следующий проход видит уже новую дату - и по ней задачу ставит.
    O.amo_service.get_leads_by_status = _mk_by_status([_lead(end_ts=new_end)])
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1
    assert O.notice_kind(new_end) in {k for _, k in claims.taken}


def test_amo_молчит_на_перечитывании_ключ_не_сожжён():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=None)

    async def silent(lead_id, with_=()):
        return None

    O.amo_service.get_lead_full = silent
    assert asyncio.run(O.sweep_once()) == {"silent": 1}
    assert claims.taken == set()


def test_пустой_этап_ничего_не_делает():
    claims, tasks = _wire([])
    assert asyncio.run(O.sweep_once()) == {}
    assert tasks == []


def test_второй_проход_по_той_же_записи_задачу_не_дублирует():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead())
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert asyncio.run(O.sweep_once()) == {"already": 1}
    assert len(tasks) == 1


def test_режим_отчёта_ключи_не_жжёт():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    O.OFFICE_RECORD_WATCH_CREATE_ENABLED = False
    claims, tasks = _wire([_lead()], fresh=_lead())
    assert asyncio.run(O.sweep_once()) == {"would-fire": 1}
    assert tasks == []
    assert claims.taken == set()

    # Включили запись - задача ставится, ключ не был выжжен обкаткой.
    O.OFFICE_RECORD_WATCH_CREATE_ENABLED = True
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_report_once_не_создаёт_даже_при_включённом_флаге():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead())
    assert asyncio.run(O.report_once()) == {"would-fire": 1}
    assert tasks == []
    assert claims.taken == set()
    # Флаг вернулся на место.
    assert O.OFFICE_RECORD_WATCH_CREATE_ENABLED is True


def test_предохранитель_на_число_задач_за_проход():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    O.OFFICE_RECORD_MAX_PER_PASS = 1
    leads = [_lead(lead_id=LEAD + i) for i in range(3)]
    claims, tasks = _wire(leads, fresh=lambda lead_id: _lead(lead_id=lead_id))
    decisions = asyncio.run(O.sweep_once())
    assert decisions == {"created": 1, "capped": 2}
    assert len(tasks) == 1


def test_упавший_post_ключ_не_освобождает():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), task_ok=False)
    assert asyncio.run(O.sweep_once()) == {"failed": 1}
    assert len(tasks) == 1  # попытка была
    # Второй проход не долбит amo повторно: лучше не поставить, чем поставить дважды.
    assert asyncio.run(O.sweep_once()) == {"already": 1}
    assert len(tasks) == 1


def test_примечание_ставится_только_по_флагу():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    O.OFFICE_RECORD_NOTE_ENABLED = True
    notes = []

    async def fake_note(lead_id, text):
        notes.append((lead_id, text))
        return {}

    claims, tasks = _wire([_lead()], fresh=_lead())
    O.amo_service.add_note = fake_note
    asyncio.run(O.sweep_once())
    assert len(notes) == 1
    assert "15.09" in notes[0][1]


def test_status_молчит_пока_флаг_выключен():
    O.OFFICE_RECORD_WATCH_ENABLED = False
    try:
        assert O.status() == {"enabled": False}
    finally:
        O.OFFICE_RECORD_WATCH_ENABLED = True
    assert O.status()["enabled"] is True


def _task(entity_type, entity_id, text="Перезвонить"):
    return {"id": 1, "entity_type": entity_type, "entity_id": entity_id, "text": text}


def test_задача_менеджера_по_этому_же_клиенту_глушит():
    """Формулировка Кати 29.09.2026: задачи ЭТОГО менеджера по ЭТОМУ клиенту."""
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[_task("leads", LEAD)])
    assert asyncio.run(O.sweep_once()) == {"client-busy": 1}
    assert tasks == []
    # Ключ НЕ сожжён: закроет задачу и не двинет сделку - напомним.
    assert claims.taken == set()


def test_задача_на_соседней_сделке_того_же_клиента_тоже_глушит():
    """Клиент - это не одна сделка. Для менеджера это одна работа."""
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(),
                          client_leads=[OTHER_LEAD_SAME_CLIENT],
                          open_tasks=[_task("leads", OTHER_LEAD_SAME_CLIENT)])
    assert asyncio.run(O.sweep_once()) == {"client-busy": 1}
    assert tasks == []


def test_задача_на_самом_контакте_тоже_глушит():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[_task("contacts", CONTACT)])
    assert asyncio.run(O.sweep_once()) == {"client-busy": 1}
    assert tasks == []


def test_чужая_задача_не_глушит():
    """Ошибка первой версии 29.09: чужая задача нашего ответственного не касается.

    Спрашиваем задачи ИМЕННО нашего менеджера, поэтому задача другого человека
    в его список не попадает."""
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[])
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_задача_менеджера_по_другому_клиенту_не_глушит():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(),
                          open_tasks=[_task("leads", OTHER_CLIENT_LEAD)])
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_сделка_без_контакта_гейт_не_применяем():
    """Клиента не знаем - молчать не за что, задача нужна тем более."""
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead(contact_id=None)], fresh=_lead(contact_id=None),
                          open_tasks=[_task("leads", LEAD)])
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_amo_молчит_про_задачи_ключ_не_сожжён():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=None)
    assert asyncio.run(O.sweep_once()) == {"tasks-silent": 1}
    assert tasks == []
    assert claims.taken == set()


def test_amo_молчит_про_контакт_ключ_не_сожжён():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), contact_silent=True)
    assert asyncio.run(O.sweep_once()) == {"tasks-silent": 1}
    assert tasks == []
    assert claims.taken == set()


def test_после_закрытия_задачи_сторож_срабатывает():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[_task("leads", LEAD)])
    assert asyncio.run(O.sweep_once()) == {"client-busy": 1}

    async def no_tasks(responsible_user_id):
        return []

    O.api.get_open_tasks_by_responsible = no_tasks
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_гейт_выключен_флагом():
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    O.OFFICE_RECORD_SKIP_IF_CLIENT_BUSY = False
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[_task("leads", LEAD)])
    assert asyncio.run(O.sweep_once()) == {"created": 1}
    assert len(tasks) == 1


def test_гейт_работает_и_в_режиме_отчёта():
    """Отчёт должен предсказывать бой, а не врать в оптимистичную сторону."""
    O.OFFICE_RECORD_MAX_AGE_DAYS = 3650
    O.OFFICE_RECORD_WATCH_CREATE_ENABLED = False
    claims, tasks = _wire([_lead()], fresh=_lead(), open_tasks=[_task("leads", LEAD)])
    assert asyncio.run(O.sweep_once()) == {"client-busy": 1}
    assert tasks == []


def _mk_by_status(leads):
    async def fake(status_id, with_=("contacts",), page_limit=50):
        return list(leads)

    return fake


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            setup_function()
            try:
                fn()
                print(f"ok   {name}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"FAIL {name}: {exc!r}")
    print("провалов:", failed)
    sys.exit(1 if failed else 0)
