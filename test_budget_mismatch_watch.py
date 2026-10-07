"""Сторож бюджета: на чём стоит правильность.

Держим то, что легко сломать правкой «на глазок»:

  • «Итого» читается из состава заказа при любых пробелах-разделителях, включая
    неразрывные - именно их ставит мост;
  • нет поля «Состав заказа» - молчим: сделка не из заказа, сверять не с чем;
  • отстойник: сделку, изменённую только что, НЕ судим - там идёт гонка двух писателей,
    и любое значение бюджета законно;
  • порог отсекает ручные округления менеджеров (13 500 против 13 483) и не отсекает
    настоящие случаи (недосчитанная позиция, нулевой бюджет);
  • ключ дедупа меняется при СМЕНЕ ПАРЫ чисел и не меняется сам по себе со временем;
  • сошлось, пока шёл проход, или amo молчит - ключ НЕ сожжён, второй шанс остался;
  • режим отчёта ключи не жжёт, иначе фича уехала бы в бой навсегда молчащей;
  • в тексте тревоги нет ID и точек посередине.

Запуск: python3 -m pytest test_budget_mismatch_watch.py -q
"""

import asyncio
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

# ⚠ФАЙЛОВАЯ ЛОВУШКА. `autopilot_store` берёт путь к базе НА ИМПОРТЕ, а наш модуль его
# импортирует ради дедупа. Кто импортировал первым - тот и выбрал путь на всю сессию
# pytest, и `test_autopilot` уедет на боевой путь. Ставим свою временную базу ДО импорта
# (разбор - knowledge/amo-fix-fields-testy-grabli.md).
os.environ.setdefault(
    "AUTOPILOT_DB_PATH", os.path.join(tempfile.mkdtemp(), "budget_watch_test.sqlite3")
)

import budget_mismatch_watch as B  # noqa: E402
from waybill_config import FIELD_ORDER_TOTAL, PIPELINE_CLEVER_MAIN  # noqa: E402

# ⚠️ Код под тестом импортирован и держит заглушки - возвращаем sys.modules, чтобы
# соседние файлы получили НАСТОЯЩИЕ модули. Разбор - в шапке _restore_sys_modules.
_restore_sys_modules()


NOW = 1790500000


def lead(total_text=None, price=0, updated_at=NOW - 3600, lead_id=1, name="Заказ №19298"):
    d = {"id": lead_id, "name": name, "price": price, "updated_at": updated_at,
         "pipeline_id": PIPELINE_CLEVER_MAIN, "custom_fields_values": []}
    if total_text is not None:
        d["custom_fields_values"] = [
            {"field_id": FIELD_ORDER_TOTAL, "values": [{"value": total_text}]}
        ]
    return d


ORDER = (
    "Заказ № 07973 от 27.09.2026:\n"
    "1. Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля\n"
    "2. Tangem 2.0 WHITE (3 Карты), 1 шт, 6 990.00 рублей\n"
    "3. Keystone Tablet, 3 шт, 13 473.00 рубля\n"
    "4. СДЭК: Доставка в постамат, (1-2 дней), 1 , 446.00 рублей\n"
    "НДС: 0.00 рублей\n"
    "Итого: 25 802.00 рубля"
)


# ─────────────────────────────── чтение «Итого» ───────────────────────────────


def test_total_from_real_order():
    assert B.order_total(lead(ORDER)) == 25802.0


def test_total_survives_nbsp_and_narrow_space():
    """Мост ставит разные пробелы в разделителе тысяч - все три должны читаться."""
    for space in (" ", " ", " "):
        text = f"Итого: 25{space}802.00 рубля"
        assert B.order_total(lead(text)) == 25802.0, space


def test_total_accepts_comma_decimal():
    assert B.order_total(lead("Итого: 12 248,00 рубля")) == 12248.0


def test_total_ignores_vat_line():
    """Рядом с «Итого» живёт строка НДС - берём именно итог, а не первое число."""
    assert B.order_total(lead("НДС: 0.00 рублей\nИтого: 12 248.00 рубля")) == 12248.0


def test_no_field_means_none():
    assert B.order_total(lead(None)) is None


def test_garbage_total_means_none():
    assert B.order_total(lead("Итого: по договорённости")) is None


# ─────────────────────────────── решение по сделке ───────────────────────────────


def test_lead_without_order_is_skipped():
    """Звонок, чат, ручная сделка - сверять не с чем, и это не повод для тревоги."""
    assert B.decide(lead(None, price=5000), NOW)[0] == "no-order"


def test_fresh_lead_is_not_judged():
    """Сделку, изменённую минуту назад, не судим: там гонка моста и пересчёта."""
    fresh = lead(ORDER, price=20909, updated_at=NOW - 60)
    assert B.decide(fresh, NOW)[0] == "fresh"


def test_settled_lead_with_gap_fires():
    """Та самая сделка 27.09: пересчёт по товарам недосчитал строку за 4 893."""
    old = lead(ORDER, price=20909, updated_at=NOW - 3600)
    decision, price, total = B.decide(old, NOW)
    assert decision == "fire"
    assert (price, total) == (20909.0, 25802.0)


def test_exact_match_is_ok():
    assert B.decide(lead(ORDER, price=25802, updated_at=NOW - 3600), NOW)[0] == "ok"


def test_manual_rounding_is_ok():
    """13 500 вместо 13 483 - менеджер округлил руками, это не поломка."""
    l = lead("Итого: 13 483.00 рубля", price=13500, updated_at=NOW - 3600)
    assert B.decide(l, NOW)[0] == "ok"


def test_zero_budget_fires():
    """Бюджет 0 при живом заказе - единственное настоящее расхождение из 1250 сделок."""
    l = lead("Итого: 5 490.00 рублей", price=0, updated_at=NOW - 3600)
    assert B.decide(l, NOW)[0] == "fire"


def test_threshold_is_strict_below_and_inclusive_above():
    """Граница порога проверяется явно: 99 рублей молчат, 100 зовут."""
    base = "Итого: 10 000.00 рублей"
    assert B.decide(lead(base, price=9901, updated_at=NOW - 3600), NOW)[0] == "ok"
    assert B.decide(lead(base, price=9900, updated_at=NOW - 3600), NOW)[0] == "fire"


def test_budget_above_order_also_fires():
    """Расхождение вверх тоже поломка: бюджет больше заказа на цену позиции."""
    l = lead("Итого: 10 000.00 рублей", price=14893, updated_at=NOW - 3600)
    assert B.decide(l, NOW)[0] == "fire"


# ─────────────────────────────── ключ дедупа ───────────────────────────────


def test_notice_key_depends_on_pair():
    assert B.notice_kind(20909, 25802) != B.notice_kind(21000, 25802)
    assert B.notice_kind(20909, 25802) != B.notice_kind(20909, 25000)


def test_notice_key_is_stable_over_time_and_kopecks():
    """Ключ не должен меняться сам по себе - иначе сторож зовёт по кругу."""
    assert B.notice_kind(20909.0, 25802.0) == B.notice_kind(20909.4, 25801.6)


# ─────────────────────── перечитывание перед тревогой ───────────────────────


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def _with_lead(monkey_lead):
    async def fake_get_lead_full(lead_id, with_=()):
        return monkey_lead
    B.amo_service.get_lead_full = fake_get_lead_full


def test_recheck_says_settled_when_fixed_meanwhile():
    """Пока шёл проход, мост дописал верную сумму - молчим и ключ не жжём."""
    _with_lead(lead(ORDER, price=25802))
    state, _ = _run(B._still_mismatched(1, 20909, 25802))
    assert state == "settled"


def test_recheck_says_changed_when_numbers_moved():
    """Числа стали другими - судить будем следующим проходом по свежей паре."""
    _with_lead(lead(ORDER, price=19000))
    state, _ = _run(B._still_mismatched(1, 20909, 25802))
    assert state == "changed"


def test_recheck_says_silent_when_amo_quiet():
    """amo не ответил - это не «всё хорошо», ключ не жжём."""
    _with_lead(None)
    state, _ = _run(B._still_mismatched(1, 20909, 25802))
    assert state == "silent"


def test_recheck_confirms_live_mismatch():
    _with_lead(lead(ORDER, price=20909))
    state, _ = _run(B._still_mismatched(1, 20909, 25802))
    assert state == "ok"


# ─────────────────────────────── проход целиком ───────────────────────────────


def _with_window(leads):
    async def fake_recent(now_ts):
        return leads
    B._recent_leads = fake_recent
    # По умолчанию считаем, что человек бюджет не трогал: иначе каждый тест прохода
    # полез бы в живой amo за авторами правок.
    _with_human(False)


def test_report_mode_does_not_burn_keys_and_does_not_write():
    """Сутки обкатки не должны выжечь ключи - иначе бой выйдет молчащим."""
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=777)
    _with_window([bad])
    _with_lead(bad)
    sent = []
    B.telegram_bot.send_alert = lambda *a, **k: sent.append(a)
    claimed = []
    # ⚠️ Подмену дедупа возвращаем на место в finally: без этого следующий тест получит
    # вечное «ключ свободен» и перестанет проверять повтор (поймано прогоном 27.09).
    real_claim = B.notices.claim_notice
    B.notices.claim_notice = lambda lead_id, kind: claimed.append(kind) or True
    try:
        decisions = _run(B.report_once())
    finally:
        B.notices.claim_notice = real_claim

    assert decisions.get("fire") == 1
    assert decisions.get("would-fire") == 1
    assert claimed == [], "в режиме отчёта ключи дедупа не жжём"
    assert sent == [], "в режиме отчёта в чат не пишем"


def test_second_pass_is_silent_on_same_pair():
    """Одно и то же расхождение зовёт один раз, а не каждые 15 минут."""
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=778)
    _with_window([bad])
    _with_lead(bad)
    B.telegram_bot.send_alert = _noop_alert
    # Дедуп тут НАСТОЯЩИЙ, на временной базе - иначе тест ничего не проверяет.
    B.notices.init()
    B.BUDGET_WATCH_ALERT_ENABLED = True
    try:
        first = _run(B.sweep_once())
        second = _run(B.sweep_once())
    finally:
        B.BUDGET_WATCH_ALERT_ENABLED = False
    assert first.get("fire") == 1
    assert second.get("already") == 1 and "fire" not in second


async def _noop_alert(*a, **k):
    return None


# ─────────────────────────────── текст человеку ───────────────────────────────


# ─────────────────────────────── починка с прививкой ───────────────────────────────


def _with_human(answer):
    async def fake(lead_id):
        return answer
    B._human_touched = fake


def _capture_patch():
    calls = []

    async def fake_patch(path, body):
        calls.append((path, body))
        return {"ok": True, "status_code": 200}

    B.amo_service._do_patch = fake_patch
    return calls


def test_fix_sends_updated_by():
    """⚠️ Сердце фичи: без updated_by пересчёт перебьёт нашу правку обратно."""
    calls = _capture_patch()
    B.amo_service.add_note = _noop_alert
    out = _run(B._fix(lead(ORDER, price=20909, lead_id=55), 20909, 25802))
    assert out == 'fixed'
    path, body = calls[0]
    assert path.endswith('/leads/55')
    assert body['price'] == 25802
    assert body['updated_by'] == B.BUDGET_WATCH_FIX_AS_USER_ID
    assert B.BUDGET_WATCH_FIX_AS_USER_ID, 'без пользователя прививка не ставится'


def test_fix_refuses_insane_total():
    """Мусор в составе заказа не должен стать боевым бюджетом и выключить пересчёт."""
    calls = _capture_patch()
    assert _run(B._fix(lead(ORDER, lead_id=56), 100, 99_000_000)) == 'unsafe'
    assert _run(B._fix(lead(ORDER, lead_id=56), 100, 0)) == 'unsafe'
    assert calls == [], 'в amo ничего не ушло'


def test_fix_refuses_without_user():
    """Пустой BUDGET_WATCH_FIX_AS_USER_ID — правка была бы бесполезной, а сделка испорчена."""
    calls = _capture_patch()
    saved = B.BUDGET_WATCH_FIX_AS_USER_ID
    B.BUDGET_WATCH_FIX_AS_USER_ID = 0
    try:
        assert _run(B._fix(lead(ORDER, lead_id=57), 100, 25802)) == 'unsafe'
    finally:
        B.BUDGET_WATCH_FIX_AS_USER_ID = saved
    assert calls == []


def test_sweep_fixes_and_leaves_note():
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=800)
    _with_window([bad])
    _with_lead(bad)
    _with_human(False)
    calls = _capture_patch()
    notes = []

    async def fake_note(lead_id, text):
        notes.append((lead_id, text))

    B.amo_service.add_note = fake_note
    B.telegram_bot.send_alert = _noop_alert
    B.notices.claim_notice = lambda lead_id, kind: True
    B.BUDGET_WATCH_FIX_ENABLED = True
    try:
        decisions = _run(B.sweep_once())
    finally:
        B.BUDGET_WATCH_FIX_ENABLED = False

    assert decisions.get('fixed') == 1
    assert calls[0][1]['price'] == 25802
    assert notes and notes[0][0] == 800
    assert 'ID' not in notes[0][1] and '·' not in notes[0][1]


def test_sweep_never_touches_human_edit():
    """Менеджер поправил бюджет сам — это решение, а не поломка. Не трогаем и не зовём."""
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=801)
    _with_window([bad])
    _with_lead(bad)
    _with_human(True)
    calls = _capture_patch()
    sent = []
    B.telegram_bot.send_alert = lambda *a, **k: sent.append(a)
    B.BUDGET_WATCH_FIX_ENABLED = True
    try:
        decisions = _run(B.sweep_once())
    finally:
        B.BUDGET_WATCH_FIX_ENABLED = False

    assert decisions.get('human-edited') == 1
    assert 'fire' not in decisions
    assert calls == [] and sent == []


def test_sweep_is_silent_when_amo_does_not_answer_about_author():
    """amo не ответил, кто правил — это не «правил робот». Молчим и ключ не жжём."""
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=802)
    _with_window([bad])
    _with_lead(bad)
    _with_human(None)
    calls = _capture_patch()
    claimed = []
    B.notices.claim_notice = lambda lead_id, kind: claimed.append(kind) or True
    B.BUDGET_WATCH_FIX_ENABLED = True
    try:
        decisions = _run(B.sweep_once())
    finally:
        B.BUDGET_WATCH_FIX_ENABLED = False

    assert decisions.get('silent') == 1
    assert calls == [] and claimed == []


def test_report_mode_does_not_fix():
    """Сутки обкатки не должны ничего править в бою."""
    bad = lead(ORDER, price=20909, updated_at=NOW - 3600, lead_id=803)
    _with_window([bad])
    _with_lead(bad)
    _with_human(False)
    calls = _capture_patch()
    B.BUDGET_WATCH_FIX_ENABLED = True
    try:
        decisions = _run(B.report_once())
        assert B.BUDGET_WATCH_FIX_ENABLED is True, 'флаг возвращён на место после прохода'
    finally:
        B.BUDGET_WATCH_FIX_ENABLED = False

    assert decisions.get('would-fire') == 1
    assert calls == [], 'в режиме отчёта бюджет не правим'


def test_money_is_readable():
    assert B.fmt_money(25802) == "25 802 ₽"
    assert B.fmt_money(446.0) == "446 ₽"


def test_alert_text_has_no_ids_and_no_middle_dots():
    """Правила Кати: в том, что читает человек, нет голых ID и нет точки посередине."""
    captured = {}

    async def fake_send(text, **kwargs):
        captured["text"] = text

    B.telegram_bot.send_alert = fake_send
    B.notices.claim_notice = lambda lead_id, kind: True
    _run(B._notify([{"lead_id": 36565229, "name": "Заказ №19298",
                     "price": 20909.0, "total": 25802.0}]))

    text = captured["text"]
    assert "·" not in text
    assert "36565229" not in text.replace("leads/detail/36565229", "")
    assert "25 802 ₽" in text and "20 909 ₽" in text
    assert "меньше заказа на 4 893 ₽" in text
