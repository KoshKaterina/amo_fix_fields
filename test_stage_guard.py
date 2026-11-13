"""Сторож перемещений: на чём стоит правильность.

  • робот и исключённые (Гладков = токен интеграции, Перфилов) не судятся никогда - иначе
    сторож откатывал бы сам себя;
  • G5: из розницы/TangemShop/ОПТ в чужую воронку - нарушение, в том числе для офиса;
    «Лист ожидания → розница» законен;
  • G6: онлайн-МОП в Офисе - только «Предзаказ оплачен» → 3 этапа;
  • G2: УР Офиса - только офис и Артём;
  • G7: возврат из Офиса в ОП офисом - уведомление без отката; офис внутри Офиса свободен;
  • откат: только если сделка ещё на том этапе; не чаще раза в 30 минут; dry ничего не
    двигает и не шлёт; одно событие судится один раз даже при перекрытии окон.

Запуск: python3 -m pytest test_stage_guard.py -q
"""
import asyncio
import os
import sys
import tempfile
import types

_SAVED = {}


def _stub(name: str, **attrs):
    _SAVED.setdefault(name, sys.modules.get(name))
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


_tmp = tempfile.mkdtemp()
os.environ["STAGE_GUARD_DB"] = os.path.join(_tmp, "sg.sqlite")
os.environ["STAGE_GUARD_JOURNAL"] = os.path.join(_tmp, "sg.jsonl")

sent: list[dict] = []
patched: list[tuple] = []
lead_state: dict[int, tuple[int, int]] = {}
events_feed: list[dict] = []


class _D:
    def __init__(self, text, chat_id, thread_id, parse_mode):
        self.text, self.chat_id, self.thread_id, self.parse_mode = text, chat_id, thread_id, parse_mode

    def send_kwargs(self):
        return {"chat_id": self.chat_id, "message_thread_id": self.thread_id, "parse_mode": self.parse_mode}


async def _do_get(path, params):
    if path == "/api/v4/events":
        page = int(dict(params)["page"])
        return {"_embedded": {"events": events_feed if page == 1 else []}}
    return None


async def _pipes():
    return [
        {"id": 10593102, "name": "ОП розница", "_embedded": {"statuses": [
            {"id": 83537710, "name": "Неразобранное", "type": 1},
            {"id": 83537718, "name": "Взят в работу", "type": 0},
            {"id": 142, "name": "Успешно реализовано", "type": 0}]}},
        {"id": 9421022, "name": "Офис", "_embedded": {"statuses": [
            {"id": 83953914, "name": "Предзаказ оплачен", "type": 0},
            {"id": 75426822, "name": "Сделать накладную", "type": 0},
            {"id": 75428410, "name": "Первичка", "type": 0},
            {"id": 142, "name": "Успешно реализовано", "type": 0}]}},
        {"id": 10131762, "name": "ОПТ", "_embedded": {"statuses": [
            {"id": 80276162, "name": "Первичный контакт", "type": 0}]}},
    ]


async def _get_lead_full(lead_id, with_=()):
    st = lead_state.get(lead_id)
    return None if st is None else {"id": lead_id, "pipeline_id": st[0], "status_id": st[1]}


async def _patch_lead(lead_id, status_id=None, pipeline_id=None, **kw):
    patched.append((lead_id, pipeline_id, status_id))
    lead_state[lead_id] = (pipeline_id, status_id)
    return {"ok": True}


async def _user_name(uid):
    return "Сотрудник"


async def _send_alert(text, **kw):
    sent.append({"text": text, **kw})
    return True


_stub("amo_service", _do_get=_do_get, get_pipelines_with_statuses=_pipes,
      get_lead_full=_get_lead_full, patch_lead=_patch_lead, get_user_name=_user_name)
_stub("telegram_bot", send_alert=_send_alert)
_stub("alerts", decide=lambda key, legacy_text, values, chat_id=None, thread_id=None, parse_mode=None:
      _D(legacy_text, chat_id, thread_id, parse_mode),
      lead_link=lambda lid: f"<a>{lid}</a>")
_stub("tg_recipients", NOTIFY_CHAT_ID=-100, NOTIFY_THREAD_ID=10479)
_stub("waybill_config", MANAGER_NAMES={9291546: "Игорь Оанча", 13963494: "Екатерина Зубалий"},
      WAZZUP_TG_HANDLES={9291546: "@thebarsa1"})

import stage_guard  # noqa: E402
import stage_guard_rules as R  # noqa: E402

for _n, _orig in _SAVED.items():
    if _orig is not None:
        sys.modules[_n] = _orig
    else:
        sys.modules.pop(_n, None)

CFG = R.Config(exempt=frozenset({11513202, 6653350}),
               online_mops=frozenset({9291546, 13929334, 14310230}),
               office_won_allowed=frozenset({13946318, 13963494, 13822630}))

IGOR, ZUBALIY, KIRILL, ARTEM, GLADKOV, SERGEY = 9291546, 13963494, 13946318, 13822630, 11513202, 13794378
RET, OFF, OPT, TANG, WAIT = 10593102, 9421022, 10131762, 9822330, 10611934


def mv(user, fp, fs, tp, ts, eid="1", lead=1, at=1000):
    return R.Move(eid, lead, user, at, fp, fs, tp, ts)


# ─────────────────────────────── правила ───────────────────────────────

def test_robot_and_exempt_never_judged():
    assert R.judge(mv(0, RET, 83537718, OFF, 75426822), CFG) is None
    assert R.judge(mv(GLADKOV, RET, 83537718, OFF, 75426822), CFG) is None
    assert R.judge(mv(6653350, RET, 83537718, OPT, 80276162), CFG) is None


def test_g5_cross_pipeline_from_locked_sources():
    assert R.judge(mv(KIRILL, RET, 83537718, OPT, 80276162), CFG).rule == "G5"
    assert R.judge(mv(ARTEM, RET, 83537718, OPT, 80276162), CFG).rule == "G5"
    assert R.judge(mv(ZUBALIY, OPT, 80276162, RET, 83537718), CFG).rule == "G5"
    assert R.judge(mv(ZUBALIY, TANG, 78157070, OFF, 75426822), CFG).rule == "G5"


def test_g5_free_inside_pipeline_and_from_waitlist():
    assert R.judge(mv(IGOR, RET, 83537718, RET, 142), CFG) is None
    assert R.judge(mv(IGOR, WAIT, 83669950, RET, 83537718), CFG) is None


def test_g6_online_mop_in_office():
    assert R.judge(mv(IGOR, OFF, 83953914, OFF, 75426822), CFG) is None
    assert R.judge(mv(IGOR, OFF, 83953914, OFF, 143), CFG) is None
    assert R.judge(mv(IGOR, OFF, 75426874, OFF, 75426822), CFG).rule == "G6"   # перезаказ накладной
    assert R.judge(mv(IGOR, OFF, 83953914, OFF, 142), CFG).rule == "G6"
    assert R.judge(mv(IGOR, OFF, 83953914, RET, 83537718), CFG).rule == "G6"
    assert R.judge(mv(14310230, RET, 83537718, OFF, 75426822), CFG).rule == "G6"


def test_g2_office_won_only_office_and_artem():
    assert R.judge(mv(SERGEY, OFF, 75426822, OFF, 142), CFG).rule == "G2"
    assert R.judge(mv(KIRILL, OFF, 75426822, OFF, 142), CFG) is None
    assert R.judge(mv(ARTEM, OFF, 75426822, OFF, 142), CFG) is None


def test_office_free_inside_office_and_g7_notify():
    assert R.judge(mv(ZUBALIY, OFF, 75426874, OFF, 75426822), CFG) is None
    v = R.judge(mv(ZUBALIY, OFF, 83953914, RET, 86706902), CFG)
    assert v.rule == "G7" and v.action == "notify"


def test_parse_event():
    ev = {"id": "abc", "type": "lead_status_changed", "entity_id": 5, "created_by": 9,
          "created_at": 100,
          "value_before": [{"lead_status": {"id": 1, "pipeline_id": 2}}],
          "value_after": [{"lead_status": {"id": 3, "pipeline_id": 4}}]}
    m = R.parse_event(ev)
    assert (m.lead_id, m.user_id, m.from_pipeline, m.to_status) == (5, 9, 2, 3)
    assert R.parse_event({"id": "x"}) is None


# ─────────────────────────────── проход ───────────────────────────────

def _ev(eid, lead, user, fp, fs, tp, ts, at):
    return {"id": eid, "type": "lead_status_changed", "entity_id": lead, "created_by": user,
            "created_at": at, "value_before": [{"lead_status": {"id": fs, "pipeline_id": fp}}],
            "value_after": [{"lead_status": {"id": ts, "pipeline_id": tp}}]}


def _reset(mode):
    stage_guard.STAGE_GUARD_MODE = mode
    stage_guard._cfg = CFG
    sent.clear(); patched.clear(); lead_state.clear(); events_feed.clear()
    stage_guard._violations_ts.clear()
    stage_guard._last_f5_alarm = 0
    for f in (os.environ["STAGE_GUARD_DB"],):
        if os.path.exists(f):
            os.remove(f)


def test_rollback_and_notify(monkeypatch=None):
    _reset("rollback")
    import time
    now = int(time.time())
    events_feed.append(_ev("e1", 11, IGOR, OFF, 75426874, OFF, 75426822, now - 10))
    lead_state[11] = (OFF, 75426822)
    asyncio.run(stage_guard.sweep_once())
    assert patched == [(11, OFF, 75426874)]
    assert len(sent) == 1 and "@thebarsa1" in sent[0]["text"] and sent[0]["message_thread_id"] == 10479
    # Повтор того же события при перекрытии окна - не судится второй раз.
    asyncio.run(stage_guard.sweep_once())
    assert len(patched) == 1 and len(sent) == 1


def test_cooldown_second_violation_only_notifies():
    _reset("rollback")
    import time
    now = int(time.time())
    lead_state[12] = (OFF, 75426822)
    events_feed.append(_ev("a", 12, IGOR, OFF, 75426874, OFF, 75426822, now - 20))
    asyncio.run(stage_guard.sweep_once())
    lead_state[12] = (OFF, 75426822)
    events_feed.append(_ev("b", 12, IGOR, OFF, 75426874, OFF, 75426822, now - 5))
    asyncio.run(stage_guard.sweep_once())
    assert len(patched) == 1
    assert "полчаса" in sent[-1]["text"]


def test_moved_on_not_rolled_back():
    _reset("rollback")
    import time
    now = int(time.time())
    lead_state[13] = (OFF, 75426874)   # робот уже увёл дальше
    events_feed.append(_ev("c", 13, IGOR, OFF, 83953914, OFF, 142, now - 5))
    asyncio.run(stage_guard.sweep_once())
    assert patched == [] and "ушла с этого этапа" in sent[0]["text"]


def test_dry_does_nothing():
    _reset("dry")
    import time
    now = int(time.time())
    lead_state[14] = (OFF, 75426822)
    events_feed.append(_ev("d", 14, IGOR, OFF, 75426874, OFF, 75426822, now - 5))
    r = asyncio.run(stage_guard.sweep_once())
    assert patched == [] and sent == [] and r["violations"] == 1


def test_g7_notify_without_rollback_and_f5_alarm():
    _reset("rollback")
    import time
    now = int(time.time())
    events_feed.append(_ev("g7", 20, ZUBALIY, OFF, 83953914, RET, 83537718, now - 30))
    for i in range(3):
        lead_state[30 + i] = (OPT, 80276162)
        events_feed.append(_ev(f"v{i}", 30 + i, KIRILL, RET, 83537718, OPT, 80276162, now - 20 + i))
    asyncio.run(stage_guard.sweep_once())
    assert (20, RET, 83537718) not in patched
    assert len(patched) == 3
    assert any("вернул(а) сделку из Офиса" in s["text"] for s in sent)
    assert any("виджет F5" in s["text"] and s["chat_id"] is None for s in sent)
