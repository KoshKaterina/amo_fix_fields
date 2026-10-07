"""Юнит-тесты сторожа розничных лидов (без сети, amo и телеграма).

Запуск: python3 -m pytest test_retail_lead_guard.py -q
"""
import asyncio
import datetime
import os
import sys
import tempfile
import types

# --- окружение ДО импорта: waybill_config читает env на импорте ------------------
os.environ.setdefault("RETAIL_GUARD_DELAY_S", "0")
os.environ.setdefault("RETAIL_GUARD_WINDOW_START_H", "0")
os.environ.setdefault("RETAIL_GUARD_WINDOW_END_H", "24")
os.environ.setdefault(
    "RETAIL_GUARD_SEEN_PATH",
    os.path.join(tempfile.mkdtemp(), "retail_guard_seen.json"),
)


# --- стабы тяжёлых зависимостей -------------------------------------------------
# Что лежало в sys.modules до наших заглушек - чтобы вернуть это после импорта.
_SAVED_SYS_MODULES: dict = {}


def _restore_sys_modules() -> None:
    """Вернуть `sys.modules` как было. Звать СРАЗУ после импорта кода под тестом.

    ⚠️ Зачем. Заглушка обязана стоять ДО импорта модуля под тестом, иначе он возьмёт
    настоящие зависимости. Но оставленная в `sys.modules` навсегда, она достаётся всем
    файлам, импортированным позже: их подмены ложатся на заглушку, боевой код зовёт
    настоящую отправку, запрос уходит в сеть и прогон висит. Перебор парами 07.10.2026:
    этот файл вешал `test_lead_distribution` намертво.

    Модуль под тестом уже держит свои ссылки на заглушки - возврат ему не мешает.
    """
    for name, original in _SAVED_SYS_MODULES.items():
        if original is not None:
            sys.modules[name] = original
        else:
            sys.modules.pop(name, None)


def _stub(name, *, base_on_real=False, **attrs):
    _SAVED_SYS_MODULES.setdefault(name, sys.modules.get(name))
    """Положить в `sys.modules` заглушку модуля.

    ⚠️ `base_on_real=True` - для модулей, которые читают СОСЕДНИЕ тестовые файлы. Заглушка
    тогда начинается с КОПИИ настоящего модуля, и подмены ложатся поверх: ничего не
    исчезает. Без этого сосед, импортированный позже, обращается к отсутствующему имени и
    падает на ИМПОРТЕ - то есть ошибкой СБОРА, которая роняет сбор всего репозитория. Так
    ломались `test_uis_callback_watch.py` (ему нужен `amo_service.get_lead_full`) и
    `test_wazzup_sla.py` (`alerts.panel_notify_bg`). Правило записано в шапке первого из
    них: «заглушка целым модулем ломала бы сборку соседних тестов» (07.10.2026).

    ⚠️ Копируем, а НЕ правим настоящий модуль: иначе подмены вроде `find_leads_by_query=None`
    достались бы всем, кто зовёт эту функцию по-настоящему.
    """
    m = None
    if base_on_real:
        try:
            import importlib

            real = importlib.import_module(name)
            m = types.ModuleType(name)
            m.__dict__.update(real.__dict__)
        except Exception:                      # noqa: BLE001 - нет модуля, обойдёмся пустым
            m = None
    if m is None:
        m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)

_sent: list[dict] = []


async def _send_alert(text, **kwargs):
    _sent.append({"text": text, **kwargs})
    return True


_stub("telegram_bot", send_alert=_send_alert, record_sent=lambda **k: None)


def _cf_value(entity, field_id):
    for f in (entity or {}).get("custom_fields_values") or []:
        if f.get("field_id") == field_id:
            values = f.get("values") or []
            if values:
                return values[0].get("value")
    return None


_leads_answer: list | None = []


async def _find_leads_by_query(query, **kwargs):
    return _leads_answer


_stub("amo_service", base_on_real=True, find_leads_by_query=_find_leads_by_query,
      find_contacts_by_query=None, get_talks_by_contact=None,
      get_custom_field_value=_cf_value)
_stub("api", BASE_URL="https://amo.example")
_stub("httpx", AsyncClient=object)


class _Decision:
    def __init__(self, text, chat_id, thread_id):
        self.text = text
        self.chat_id = chat_id
        self.thread_id = thread_id

    def send_kwargs(self):
        return {"chat_id": self.chat_id, "message_thread_id": self.thread_id}


_panel_off = False


def _decide(event_key, *, legacy_text, values=None, chat_id=None, thread_id=None, **kwargs):
    if _panel_off:
        return None
    return _Decision(legacy_text, chat_id, thread_id)


_stub("alerts", base_on_real=True, decide=_decide,
      lead_link=lambda lead_id: f'<a href="https://amo.example/leads/detail/{lead_id}">Открыть сделку</a>')

import retail_lead_guard as G  # noqa: E402

# ⚠️ Код под тестом импортирован и уже держит заглушки - возвращаем sys.modules,
# чтобы соседние файлы получили НАСТОЯЩИЕ модули. Разбор - в шапке _restore_sys_modules.
_restore_sys_modules()
from waybill_config import (  # noqa: E402
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OFFICE,
    PIPELINE_WAITLIST,
)

RETAIL_CHANNEL = "12d3ccf1-8f15-4144-8b6e-5fffe3a2f647"   # телеграм ОП
OTHER_CHANNEL = "33be01a6-7d00-4fae-b797-93fd66e9f0f4"    # партнёрский телеграм Саши


def _msg(chat_id="79990000000", is_echo=False, channel=RETAIL_CHANNEL,
         chat_type="whatsapp", text="здравствуйте, хочу кошелёк", name="Иван",
         username=""):
    contact = {"name": name}
    if username:
        contact["username"] = username
    return {"channelId": channel, "chatId": chat_id, "chatType": chat_type,
            "text": text, "isEcho": is_echo, "contact": contact}


def _lead(lead_id, pipeline_id, status_id=83537714, updated_at=100):
    return {"id": lead_id, "pipeline_id": pipeline_id, "status_id": status_id,
            "updated_at": updated_at}


def _reset(leads=None, panel_off=False, dry_run=False):
    """Пороги задаём ПОЛЯМИ модуля, а не переменными окружения. waybill_config читает
    env один раз на импорте, и если соседний тест-файл затянул его раньше (в паре с
    test_wazzup_sla так и происходит), наши env уже ни на что не влияют - пауза
    осталась боевой, и тест висит пять минут. Грабли записаны в заметке папки
    knowledge/amo-fix-fields-testy-grabli.md."""
    global _leads_answer, _panel_off
    _leads_answer = leads
    _panel_off = panel_off
    G.RETAIL_GUARD_DELAY_S = 0
    G.RETAIL_GUARD_CHECK_EVERY_MIN = 15
    G.RETAIL_GUARD_ALERT_DEDUP_H = 24
    G.RETAIL_GUARD_WINDOW_START_H = 0
    G.RETAIL_GUARD_WINDOW_END_H = 24
    _sent.clear()
    G._checked.clear()
    G._alerted.clear()
    G._alerted_loaded = True          # файл дедупа не читаем: состояние задаём здесь
    G._sent_times.clear()
    G._burst_notified = False
    G.RETAIL_GUARD_DRY_RUN = dry_run


def _apply(chat_id="79990000000", chat_type="whatsapp", username="", name="Иван"):
    st = {"chat_id": chat_id, "channel_id": RETAIL_CHANNEL, "chat_type": chat_type,
          "contact_name": name, "username": username, "text": "хочу кошелёк"}
    asyncio.run(G._apply(st))


# --- приём вебхука --------------------------------------------------------------

def test_ishodyashchee_ignoriruem():
    _reset(leads=[])
    G.on_wazzup({"messages": [_msg(is_echo=True)]})
    assert G._checked == {}


def test_chuzhoy_kanal_ignoriruem():
    _reset(leads=[])
    G.on_wazzup({"messages": [_msg(channel=OTHER_CHANNEL)]})
    assert G._checked == {}


def test_statusy_bez_messages_ne_padayut():
    _reset(leads=[])
    G.on_wazzup({"statuses": [{"messageId": "x", "status": "delivered"}]})
    assert G._checked == {}


def test_seriya_soobshcheniy_odna_proverka():
    """Человек пишет три сообщения подряд - в amo идём один раз.

    Гоняем внутри event loop: on_wazzup вешает фоновую задачу, а в бою он всегда
    зовётся из обработчика FastAPI, где loop есть."""
    _reset(leads=[])

    async def run():
        G.on_wazzup({"messages": [_msg(), _msg(), _msg()]})
        await asyncio.sleep(0)
        for task in list(G._bg_tasks):
            await task

    asyncio.run(run())
    assert list(G._checked) == ["79990000000"]
    assert _sent == []                 # открытых сделок нет - повода нет


# --- решение: слать или нет -----------------------------------------------------

def test_otkrytaya_roznica_molchim():
    _reset(leads=[_lead(1, PIPELINE_CLEVER_MAIN)])
    _apply()
    assert _sent == []


def test_otkrytyy_ofis_molchim():
    """Решение Кати 30.09.2026: Офис - своя воронка, человек пишет по своему заказу."""
    _reset(leads=[_lead(2, PIPELINE_OFFICE)])
    _apply()
    assert _sent == []


def test_otkrytyh_sdelok_net_molchim():
    """Все сделки закрыты - amo создаст новую сам, повода нет."""
    _reset(leads=[_lead(3, PIPELINE_CLEVER_MAIN, status_id=143),
                  _lead(4, PIPELINE_ACADEMY, status_id=142)])
    _apply()
    assert _sent == []


def test_amo_molchit_trevogu_ne_podnimaem():
    """None от amo - это сбой, а не «сделок нет»."""
    _reset(leads=None)
    _apply()
    assert _sent == []


def test_akademiya_daet_alert():
    _reset(leads=[_lead(36563901, PIPELINE_ACADEMY, status_id=88943006)])
    _apply()
    assert len(_sent) == 1
    text = _sent[0]["text"]
    assert "сделки в рознице нет" in text
    assert "Академия" in text
    assert "36563901" in text          # только внутри ссылки
    assert "Открыть сделку" in text


def test_list_ozhidaniya_storozhim():
    """Решение Кати 30.09.2026: Лист ожидания - чужая воронка, сторожим."""
    _reset(leads=[_lead(5, PIPELINE_WAITLIST)])
    _apply()
    assert len(_sent) == 1
    assert "Лист ожидания" in _sent[0]["text"]


def test_ssylka_na_samuyu_svezhuyu_chuzhuyu():
    _reset(leads=[_lead(10, PIPELINE_ACADEMY, updated_at=100),
                  _lead(11, PIPELINE_WAITLIST, updated_at=200)])
    _apply()
    assert "Лист ожидания" in _sent[0]["text"]
    assert "/leads/detail/11" in _sent[0]["text"]


# --- дедуп, окно, сухой прогон --------------------------------------------------

def test_dedup_odin_alert_na_chat():
    _reset(leads=[_lead(6, PIPELINE_ACADEMY)])
    _apply()
    _apply()
    assert len(_sent) == 1


def test_vne_okna_molchim():
    _reset(leads=[_lead(7, PIPELINE_ACADEMY)])
    real = G._now_msk
    G._now_msk = lambda: datetime.datetime(2026, 9, 30, 3, 0, tzinfo=G._MSK)
    G.RETAIL_GUARD_WINDOW_START_H, G.RETAIL_GUARD_WINDOW_END_H = 9, 21
    try:
        _apply()
    finally:
        G._now_msk = real
        G.RETAIL_GUARD_WINDOW_START_H, G.RETAIL_GUARD_WINDOW_END_H = 0, 24
    assert _sent == []
    assert G._alerted == {}            # отметку не тратим: днём случай должен дойти


def test_suhoy_progon_ne_shlet_i_ne_tratit_otmetku():
    _reset(leads=[_lead(8, PIPELINE_ACADEMY)], dry_run=True)
    try:
        _apply()
    finally:
        G.RETAIL_GUARD_DRY_RUN = False
    assert _sent == []
    assert G._alerted == {}


def test_vyklyucheno_v_paneli_ne_shlem():
    _reset(leads=[_lead(9, PIPELINE_ACADEMY)], panel_off=True)
    _apply()
    assert _sent == []
    assert G._alerted == {}


def test_chasovoy_limit_gasit_potok():
    _reset(leads=[_lead(12, PIPELINE_ACADEMY)])
    G._sent_times.extend([__import__("time").time()] * G.RETAIL_GUARD_HOUR_LIMIT)
    _apply()
    assert len(_sent) == 1
    assert "приглушены" in _sent[0]["text"]


def test_suhoy_progon_pishet_kazhduyu_proverku():
    """Иначе по журналу не отличить «молчит, потому что порядок» от «не работает»."""
    _reset(leads=[_lead(17, PIPELINE_CLEVER_MAIN)], dry_run=True)
    lines = []
    real = G.logger.info
    G.logger.info = lambda msg, *a: lines.append(msg % a if a else msg)
    try:
        _apply()
    finally:
        G.logger.info = real
        G.RETAIL_GUARD_DRY_RUN = False
    assert any("своя открытая сделка есть" in l for l in lines)


def test_boevoy_rezhim_zhurnal_ne_zasoryaet():
    _reset(leads=[_lead(18, PIPELINE_CLEVER_MAIN)])
    lines = []
    real = G.logger.info
    G.logger.info = lambda msg, *a: lines.append(msg % a if a else msg)
    try:
        _apply()
    finally:
        G.logger.info = real
    assert not any("сухой прогон" in l for l in lines)


# --- текст ----------------------------------------------------------------------

def test_telegram_pokazyvaet_nik_a_ne_nomer_chata():
    """chatId телеграма - анонимный номер, человеку его показывать нельзя."""
    _reset(leads=[_lead(13, PIPELINE_ACADEMY)])
    _apply(chat_id="462778787", chat_type="telegram", username="pavelox", name="Павел")
    text = _sent[0]["text"]
    assert "@pavelox" in text
    assert "462778787" not in text


def test_whatsapp_pokazyvaet_telefon():
    _reset(leads=[_lead(14, PIPELINE_ACADEMY)])
    _apply(chat_id="79092547774", chat_type="whatsapp", name="Павел")
    assert "79092547774" in _sent[0]["text"]


def test_kanal_nazvan_slovami():
    _reset(leads=[_lead(15, PIPELINE_ACADEMY)])
    _apply()
    assert "Телеграм 7 926 082-36-03" in _sent[0]["text"]


def test_tochki_poseredine_v_tekste_net():
    """Правило Кати 26.08.2026."""
    _reset(leads=[_lead(16, PIPELINE_ACADEMY)])
    _apply()
    assert "·" not in _sent[0]["text"]


if __name__ == "__main__":
    import traceback

    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok  ", name)
            except Exception:
                fails += 1
                print("FAIL", name)
                traceback.print_exc()
    print("failed:", fails)
