"""Рендер текста уведомления по шаблону из панели.

Шаблон - обычный текст с переменными `{{имя}}` (ключи по-русски, как на экране панели:
`{{клиент}}`, `{{ссылка_на_сделку}}`). Значения подставляет сендер: он один знает, что
такое «клиент» в его событии. Поля сделки amoCRM - `{{amo.<номер поля>}}` - берутся из
самой сделки, если сендер её передал; не передал - поле считается пустым.

Правила, которые тут несущие:

- Шаблон не экранируется никогда, значения - всегда, кроме тех, что сами несут разметку
  (ссылка на сделку, готовый текст робота). Иначе либо умрёт форматирование, либо имя
  клиента с угловой скобкой уронит сообщение целиком.
- Строка, все переменные которой оказались пустыми, выбрасывается. Это замена условных
  `if phone: lines.append(...)` из старых сборщиков: «📞 {{телефон}}» без телефона не
  превращается в голое «📞».
- Неизвестная переменная - отказ (исключение), не пустота. Сендер такого значения не даёт,
  значит панель и код разошлись; честнее отправить старый текст, чем текст с дырой.
  На стороне панели такое не сохраняется, но кэш мог пережить смену кода.
"""
from __future__ import annotations

import html
import re
from typing import Any

# Переменные, чьё значение приходит уже с разметкой Телеграма и НЕ экранируется.
HTML_VARIABLES = frozenset({"ссылка_на_сделку", "текст_события", "текст_поломки"})

_VAR_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_AMO_RE = re.compile(r"^amo\.(\d+)$")


class UnknownVariable(KeyError):
    """В шаблоне переменная, которой сендер не даёт."""


def variables_in(template: str) -> list[str]:
    """Ключи переменных в порядке появления, без повторов."""
    seen: list[str] = []
    for m in _VAR_RE.finditer(template or ""):
        key = m.group(1)
        if key not in seen:
            seen.append(key)
    return seen


def _amo_value(lead: dict | None, field_id: int) -> str:
    """Значение поля сделки: у мультиполей - все значения через запятую."""
    if not lead:
        return ""
    for f in lead.get("custom_fields_values") or []:
        try:
            if int(f.get("field_id")) != field_id:
                continue
        except (TypeError, ValueError):
            continue
        vals = []
        for v in f.get("values") or []:
            raw = v.get("value")
            if raw in (None, ""):
                continue
            vals.append(str(raw).strip())
        return ", ".join(v for v in vals if v)
    return ""


def render(template: str, values: dict[str, Any], *, lead: dict | None = None) -> str:
    """Собрать текст. Бросает UnknownVariable, если шаблон просит то, чего у сендера нет."""
    out_lines: list[str] = []
    for line in (template or "").split("\n"):
        keys = variables_in(line)
        if not keys:
            out_lines.append(line)
            continue
        non_empty = 0

        def _sub(m: re.Match) -> str:
            nonlocal non_empty
            key = m.group(1)
            amo = _AMO_RE.match(key)
            if amo:
                val = html.escape(_amo_value(lead, int(amo.group(1))), quote=False)
            elif key in values:
                raw = values[key]
                val = "" if raw is None else str(raw).strip()
                if key not in HTML_VARIABLES:
                    val = html.escape(val, quote=False)
            else:
                raise UnknownVariable(key)
            if val:
                non_empty += 1
            return val

        rendered = _VAR_RE.sub(_sub, line)
        if non_empty == 0:
            continue  # все переменные строки пустые - строку выбрасываем
        # «💬 Telegram, » без имени клиента - хвостовую запятую подчищаем. Только запятую:
        # точка с запятой - хвост html-сущности вроде &gt;, её трогать нельзя.
        out_lines.append(re.sub(r"[\s,]+$", "", rendered))
    text = "\n".join(out_lines)
    # Выброшенные строки могли оставить тройной перевод строки - сжимаем до пустой строки.
    return re.sub(r"\n{3,}", "\n\n", text).strip("\n")
