"""Защита самовывоза (пункт 4): на чём стоит правильность.

  • онлайн-МОП перевёл сделку с самовывозом в УР → перенос остановлен, сделка вернулась
    на тот этап, откуда её перевели, в чат ОП ушёл тег менеджера;
  • офис (Кирилл, Зубалий) или робот перевели → переносим как раньше;
  • не самовывоз (СДЭК), не та воронка, флаг выключен → защита не вмешивается;
  • журнал молчит или события ещё нет → переносим как раньше (сбой журнала не стопорит офис);
  • событие появилось со второй попытки → ловим;
  • вернуть не вышло → перенос всё равно остановлен, текст просит вернуть руками.

Запуск: python3 -m pytest test_showroom_ur_guard.py -q
"""
import asyncio
import sys
import types

_SAVED = {}


def _stub(name, **attrs):
    _SAVED.setdefault(name, sys.modules.get(name))
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


RET, OFF, TANG, OPT = 10593102, 9421022, 9822330, 10131762
FIELD_DELIVERY = 577315
IGOR, KIRILL = 9291546, 13946318

state = {"events": [], "calls": 0, "fail_reads": 0, "patch_ok": True}
patched, sent = [], []


async def _do_get(path, params):
    state["calls"] += 1
    if state["fail_reads"]:
        state["fail_reads"] -= 1
        return None
    return {"_embedded": {"events": list(state["events"])}}


async def _patch_lead(lead_id, pipeline_id=None, status_id=None, **kw):
    patched.append((lead_id, pipeline_id, status_id))
    return {"ok": state["patch_ok"]}


def _cfv(lead, fid):
    for f in lead.get("custom_fields_values") or []:
        if f["field_id"] == fid:
            return f["values"][0]["value"]
    return None


async def _send_alert(text, **kw):
    sent.append({"text": text, **kw})


class _D:
    def __init__(self, t, c, th, p):
        self.text, self.c, self.th, self.p = t, c, th, p

    def send_kwargs(self):
        return {"chat_id": self.c, "message_thread_id": self.th, "parse_mode": self.p}


_stub("amo_service", _do_get=_do_get, patch_lead=_patch_lead, get_custom_field_value=_cfv)
_stub("telegram_bot", send_alert=_send_alert)
_stub("alerts", lead_link=lambda i: f"<a>{i}</a>",
      decide=lambda key, legacy_text, values, chat_id=None, thread_id=None, parse_mode=None:
      _D(legacy_text, chat_id, thread_id, parse_mode))
_stub("tg_recipients", NOTIFY_CHAT_ID=-100, NOTIFY_THREAD_ID=10479)
_stub("waybill_config", DELIVERY_PICKUP_MARKERS=("самовывоз из офиса", "самовывоз из шоурума"),
      FIELD_DELIVERY_TYPE=FIELD_DELIVERY, MANAGER_NAMES={IGOR: "Игорь Оанча"},
      PIPELINE_CLEVER_MAIN=RET, PIPELINE_TANGEMSHOP=TANG, STATUS_SUCCESS=142,
      WAZZUP_TG_HANDLES={IGOR: "@thebarsa1"})

# Модуль под тестом мог быть уже загружен другим файлом (его импортирует webhooks.py) -
# тогда он держит настоящие зависимости. Грузим свежую копию поверх заглушек.
_SAVED.setdefault("showroom_ur_guard", sys.modules.get("showroom_ur_guard"))
sys.modules.pop("showroom_ur_guard", None)
import showroom_ur_guard as g  # noqa: E402

for _n, _o in _SAVED.items():
    if _o is not None:
        sys.modules[_n] = _o
    else:
        sys.modules.pop(_n, None)

g.LOOKUP_PAUSE_S = 0


def _lead(delivery="Самовывоз из шоурума", pipeline=RET, status=142):
    return {"id": 77, "pipeline_id": pipeline, "status_id": status,
            "custom_fields_values": [{"field_id": FIELD_DELIVERY, "values": [{"value": delivery}]}]}


def _ev(user, before_p=RET, before_s=83537718, at=100, after_p=RET):
    return {"id": f"e{at}", "created_by": user, "created_at": at,
            "value_before": [{"lead_status": {"id": before_s, "pipeline_id": before_p}}],
            "value_after": [{"lead_status": {"id": 142, "pipeline_id": after_p}}]}


def _reset(events=(), enabled=True, fail_reads=0, patch_ok=True):
    g.SHOWROOM_GUARD_ENABLED = enabled
    state.update(events=list(events), calls=0, fail_reads=fail_reads, patch_ok=patch_ok)
    patched.clear(); sent.clear()


def run(c):
    return asyncio.run(c)


def test_online_mop_blocked_and_returned():
    _reset([_ev(KIRILL, at=50), _ev(IGOR, before_s=86706902, at=100)])
    assert run(g.check(_lead())) == "blocked-showroom-online-mop"
    assert patched == [(77, RET, 86706902)]          # куда перевёл последний раз
    assert "@thebarsa1" in sent[0]["text"] and sent[0]["message_thread_id"] == 10479


def test_office_and_robot_pass():
    _reset([_ev(KIRILL)])
    assert run(g.check(_lead("Самовывоз из офиса"))) is None
    _reset([_ev(0)])
    assert run(g.check(_lead())) is None
    assert patched == [] and sent == []


def test_not_applicable():
    _reset([_ev(IGOR)])
    assert run(g.check(_lead("CDEK: Самовывоз"))) is None
    assert run(g.check(_lead(pipeline=OPT))) is None
    assert run(g.check(_lead(status=83537718))) is None
    _reset([_ev(IGOR)], enabled=False)
    assert run(g.check(_lead())) is None
    assert state["calls"] == 0 and patched == []


def test_tangemshop_guarded():
    _reset([_ev(IGOR, before_p=TANG, before_s=78157070, after_p=TANG)])
    assert run(g.check(_lead(pipeline=TANG))) == "blocked-showroom-online-mop"
    assert patched == [(77, TANG, 78157070)]


def test_journal_silent_falls_back_to_transfer():
    _reset([_ev(IGOR)], fail_reads=5)
    assert run(g.check(_lead())) is None
    assert patched == []


def test_event_appears_on_retry():
    _reset([_ev(IGOR)], fail_reads=1)
    assert run(g.check(_lead())) == "blocked-showroom-online-mop"


def test_rollback_failed_still_blocks():
    _reset([_ev(IGOR)], patch_ok=False)
    assert run(g.check(_lead())) == "blocked-showroom-rollback-failed"
    assert "верните руками" in sent[0]["text"]
