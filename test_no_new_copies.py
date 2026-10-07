"""Сторож: новых копий общего кода не появляется. Старые доживают свой век спокойно.

ЗАЧЕМ. Общие мелочи собраны в `shared/` (московское время, атомарная запись json, примитивы
над телефоном). Но старые копии НЕ вычищены разом - так решено 07.10.2026, потому что
массовый перенос файлов оставил бы на проде сирот, а выкатку целым каталогом разрешать нельзя
при 37 открытых ветвях. Значит правило такое: **старое живёт, новое берёт из `shared/`.**

Правило без сторожа живёт до первой спешки. Этот сторож и есть ограничитель: он не требует
чистить старое, он требует не плодить новое.

КАК УСТРОЕНО. Ниже - список файлов, в которых копия УЖЕ была на 07.10.2026 (собран кодом, не
руками). Сторож проверяет два условия:

1. **В новых файлах копий нет.** Появилась копия в файле вне списка - тест падает и называет
   файл. Лечение: взять функцию из `shared/`.
2. **Список не врёт: он только сокращается.** Если в файле из списка копии больше нет (её
   убрали, потому что файл и так трогали) - тест падает и просит убрать строку из списка.
   Так число копий остаётся честным и видно, что долг уменьшается, а не стоит.

Второе условие - не придирка. Список, который никто не сокращает, через полгода перестаёт
соответствовать коду, и тогда сторож начинает пропускать настоящие новые копии.

⚠️ Тестовые файлы (`test_*.py`) не проверяются: в них `timedelta(hours=3)` и подобное бывает
нужно как ожидаемое значение, а не как копия логики.
"""
from __future__ import annotations

import io
import pathlib
import re
import tokenize

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent


def _without_comments(source: str) -> str:
    """Исходник, в котором комментарии ЗАТЁРТЫ пробелами (позиции символов сохранены).

    ⚠️ Зачем вообще. Без этого сторож срабатывает на ПРОЗУ. Поймано сразу же, 07.10.2026:
    я перевела два файла на `shared.timez`, а в комментарии написала «было
    `ZoneInfo(...)`» - и сторож решил, что копия на месте. То есть он показал бы «долг не
    уменьшился» ровно там, где он уменьшился, а значит и настоящую копию однажды пропустил бы.

    ⚠️ Почему именно ЗАТИРАЕМ, а не выбрасываем. Первая попытка собирала строку из токенов
    через перевод строки - и `os.replace(` распалось на `os`, `.`, `replace`, `(` по разным
    строкам, после чего образец перестал находиться вообще. Затирание на месте сохраняет
    соседство символов, то есть код остаётся кодом.

    ⚠️ Строковые литералы НЕ трогаем намеренно: часть образцов сама состоит из строки
    (`re.sub(r"\\D", ...)`). Плата за это - docstring, дословно цитирующий образец, будет
    посчитан копией. Лечение простое: в документации не приводить образец дословно.
    """
    lines = source.splitlines(keepends=True)
    try:
        comments = [
            t for t in tokenize.generate_tokens(io.StringIO(source).readline)
            if t.type == tokenize.COMMENT
        ]
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return source                       # не разобрался - лучше перебдеть, чем пропустить
    for token in comments:
        row = token.start[0] - 1
        if row >= len(lines):
            continue
        line = lines[row]
        start, end = token.start[1], token.end[1]
        lines[row] = line[:start] + " " * (end - start) + line[end:]
    return "".join(lines)

# Что считаем копией: образец и человеческое объяснение, чем его заменить.
PATTERNS = {
    "MSK_OFFSET": (
        r"timedelta\(hours=3\)",
        "московский сдвиг вшит по месту — возьмите `from shared.timez import MSK`",
    ),
    "ZONEINFO_MOSCOW": (
        r"ZoneInfo\(\s*['\"]Europe/Moscow['\"]\s*\)",
        "московская зона через ZoneInfo — возьмите `from shared.timez import MSK` "
        "(для дат после 26.10.2014 результат тот же, см. шапку shared/timez.py)",
    ),
    "ATOMIC_WRITE": (
        r"os\.replace\(",
        "своя атомарная запись — возьмите `from shared.files import write_json_atomic`",
    ),
    "PHONE_DIGITS": (
        r"re\.sub\(\s*r?['\"]\\D",
        "своя чистка номера до цифр — возьмите `from shared.phones import digits_only`",
    ),
}

# Файлы, где копия УЖЕ была на 07.10.2026. Список только сокращается.
BASELINE = {
    "MSK_OFFSET": {
        "academy_consent_stamp.py",
        "amgroup_duplicate_watch.py",
        "amgroup_fallback.py",
        "autopilot.py",
        "lead_distribution.py",
        "metrika_sync.py",
        "new_lead_watch.py",
        "office_record_watch.py",
        "order_watchdog.py",
        "ozon_invoice.py",
        "retail_lead_guard.py",
        "sales_sheet_feed.py",
        "showroom_store.py",
        "uis_missed_call.py",
        "waybill_config.py",
        "wazzup_delivery.py",
        "wazzup_sla.py",
        "webhooks.py",
    },
    # ✅ Закрыто полностью 07.10.2026. Были `picking_pdf.py` и `telegram_bot.py`; оба
    # переведены на `shared.timez.now_msk`. Повод был не только в дублировании: `ZoneInfo`
    # требует базу часовых поясов, а её нет ни в `requirements.txt`, ни на машинах с Windows -
    # вызовы падали `ZoneInfoNotFoundError`, то есть лист сборки там не собирался вообще.
    # Пустое множество значит «теперь так нельзя нигде», и это правильное правило.
    "ZONEINFO_MOSCOW": set(),
    "ATOMIC_WRITE": {
        "academy_chat_join.py",
        "academy_intent_alert.py",
        "academy_lead_alert.py",
        "alert_settings_client.py",
        "alerts.py",
        "amgroup_duplicate_watch.py",
        "amgroup_fallback.py",
        "amgroup_shipment.py",
        "autopilot_settings_client.py",
        "lead_distribution.py",
        "lead_distribution_profiles_client.py",
        "new_lead_watch.py",
        "order_watchdog.py",
        "retail_lead_guard.py",
        "showroom_alert.py",
    },
    "PHONE_DIGITS": {
        "academy_bothelp_upsert.py",
        "academy_invite_delivery.py",
        "amgroup_lead_builder.py",
        "site_form_service.py",
        "jivo_service.py",
    },
}


def _scan(pattern: str) -> set[str]:
    """Имена файлов корня (кроме тестов), где образец встречается В КОДЕ, а не в тексте."""
    rx = re.compile(pattern)
    found = set()
    for path in sorted(_ROOT.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        code = _without_comments(path.read_text(encoding="utf-8", errors="replace"))
        if rx.search(code):
            found.add(path.name)
    return found


@pytest.mark.parametrize("key", sorted(PATTERNS))
def test_novyh_kopiy_ne_poyavilos(key):
    pattern, how_to_fix = PATTERNS[key]
    new = _scan(pattern) - BASELINE[key]
    assert not new, (
        f"новая копия общего кода в {sorted(new)}: {how_to_fix}.\n"
        f"Если копия там нужна осознанно - допишите файл в BASELINE «{key}» "
        f"и объясните в коммите, почему общая функция не подошла."
    )


@pytest.mark.parametrize("key", sorted(PATTERNS))
def test_spisok_tolko_sokrashchaetsya(key):
    gone = BASELINE[key] - _scan(pattern=PATTERNS[key][0])
    assert not gone, (
        f"в файлах {sorted(gone)} копии «{key}» больше нет - уберите их из BASELINE.\n"
        f"Это хорошая новость: долг уменьшился, пусть список это показывает. "
        f"Список, который не сокращают, со временем начинает пропускать настоящие новые копии."
    )


def test_shared_sam_ne_popal_pod_storozha():
    """`shared/` - это и есть законное место образцов, корень сторож смотрит отдельно."""
    assert (_ROOT / "shared" / "timez.py").exists()
    assert "shared" not in {p.name for p in _ROOT.glob("*.py")}
