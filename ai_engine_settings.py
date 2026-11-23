"""Настройки ИИ-менеджера из панели: режим, воронки, списки контактов, таймеры, часы работы.

Устройство то же, что у `autopilot_settings_client`: фоновая задача раз в `AI_SETTINGS_POLL_INTERVAL_S`
спрашивает `GET /api/ingest/ai/settings`, успешный ответ ложится в файл-кэш, при недоступности панели
берётся последняя копия. Разница одна, и она намеренная: у авто-режима «панель молчит» значит «работаем по
последним правилам» без срока, а у ИИ срок есть (`AI_SETTINGS_STALE_S`). ИИ пишет живым людям, и вчерашний
`prod`, которого никто уже не может выключить, не должен жить вечно.

⚠️ Четыре случая, в которых `effective_mode()` возвращает `off`, хотя в панели написано иное:
  • флаг `AI_ENGINE_ENABLED` на сервере не стоит;
  • настроек нет вовсе (первый запуск без панели) или они старше `AI_SETTINGS_STALE_S`;
  • воронки в документе не совпали с зашитыми в движке для этого режима. Панель и движок держат одни и те
    же числа в двух местах, и расхождение означает опечатку в одном из них: лучше выключиться, чем увести
    ИИ в чужую воронку;
  • (версия 2) `mode_id` противоречит `mode`: например `mode: prod` и `mode_id: test`.

⚠️ Пустой список контактов при `restricted` означает «никого», а не «всех».

⚠️ Документ бывает версий 1 и 2 (`settings_version`). Версия 2 только дополняет первую (`mode_id`, `review`, справочный `deals`),
старые ключи значат то же, поэтому движок можно выкатывать в любом порядке с панелью. Нет `settings_version` или он 1: читаем как раньше.

⚠️ Этап 2 (план «режимы ИИ-менеджера»): версия 2 несёт ещё `functions` (лестница тишины, напоминание, окна УР, срок касания), `call`, `eta`,
`pipelines_any` и `initiative_pipelines`. Функции со временем берут значения и включение отсюда (запас: прежние переменные окружения), а
воронку сделки решают ТОЛЬКО `pipeline_allowed` (можно ли вести) и `initiative_allowed` (можно ли инициативные события); голый
`allowed_pipelines()` для этого не используется.

⚠️ Нет промпта: не пишем (решение Кати 23.11.2026). К воронке режима (`MODE_PIPELINES` или `pipelines_any`) `pipeline_allowed` добавляет второе
условие: для воронки есть промпт в ИИ-сервисе (`ai_engine_prompts`, словарь из `GET /v1/funnels`). Режим говорит, где ИИ можно работать, словарь
говорит, где ему есть чем. Даже `pipelines_any: true` воронку без промпта не открывает; `initiative_allowed` проходит через тот же шлюз.
"""
import asyncio
import datetime
import logging
import re
import time
from typing import Any

import httpx

import ai_engine_config as cfg
import ai_engine_prompts as prompts
from shared.files import read_json, write_json_atomic
from shared.phones import digits_only, ru_8_to_7
from shared.timez import MSK

logger = logging.getLogger("uvicorn")

DEFAULT_TIMERS = {
    "first_reply_wait_s": 60,
    "debounce_s": 10,
    "debounce_cap_s": 60,
    "deal_resolve_timeout_s": 600,
    "ai_down_release_s": 120,
}

_doc: dict[str, Any] | None = None      # последний принятый документ
_fetched_at: float | None = None        # когда его получили (стенные часы; переживает рестарт через кэш)
_poll_task: asyncio.Task | None = None
_mismatch_logged = ""


def _now() -> float:
    return time.time()


# ---------------------------------------------------------------------------------------------
# документ
# ---------------------------------------------------------------------------------------------

def set_document(doc: dict[str, Any] | None, fetched_at: float | None = None) -> None:
    """Подменить документ. Нужно опросу после успешного ответа и тестам."""
    global _doc, _fetched_at
    _doc = doc
    _fetched_at = fetched_at if fetched_at is not None else (_now() if doc is not None else None)


def _load_cache() -> None:
    global _doc, _fetched_at
    data = read_json(cfg.SETTINGS_CACHE_PATH, None)
    if isinstance(data, dict) and isinstance(data.get("doc"), dict):
        _doc = data["doc"]
        _fetched_at = float(data.get("fetched_at") or 0) or None


def document() -> dict[str, Any] | None:
    return _doc


def age_s() -> float | None:
    return None if _fetched_at is None else max(0.0, _now() - _fetched_at)


def is_fresh() -> bool:
    age = age_s()
    return age is not None and age <= cfg.SETTINGS_STALE_S


# ---------------------------------------------------------------------------------------------
# режим
# ---------------------------------------------------------------------------------------------

def effective_mode() -> str:
    """`off` | `test` | `prod` - то, чему движок верит ПРЯМО СЕЙЧАС."""
    global _mismatch_logged
    if not cfg.ENABLED or _doc is None or not is_fresh():
        return "off"
    mode = str(_doc.get("mode") or "off")
    if mode not in cfg.MODE_PIPELINES:
        return "off"
    raw_id = _doc.get("mode_id") if is_v2() else None
    if raw_id == "off" or (raw_id in cfg.MODE_IDS and cfg.MODE_ID_TO_LEGACY[raw_id] != mode):
        # Панель дала два ответа на вопрос «какой режим» (скажем, `mode: prod` и `mode_id: test`): верить ни одному нельзя.
        sig = f"{mode}/{raw_id}"
        if _mismatch_logged != sig:
            _mismatch_logged = sig
            logger.error("ai_engine: mode=%s не согласуется с mode_id=%s - считаю off", mode, raw_id)
        return "off"
    try:
        got = {int(p) for p in (_doc.get("pipelines") or [])}
    except (TypeError, ValueError):
        got = set()
    if not got or not got <= cfg.MODE_PIPELINES[mode]:
        sig = f"{mode}:{sorted(got)}"
        if _mismatch_logged != sig:
            _mismatch_logged = sig
            logger.error(
                "ai_engine: воронки панели %s не согласуются с режимом %s (ожидалось не больше %s) - считаю off",
                sorted(got), mode, sorted(cfg.MODE_PIPELINES[mode]),
            )
        return "off"
    return mode


def allowed_pipelines() -> frozenset[int]:
    """Воронки режима из зашитого в движке списка. Для решения «можно ли вести сделку этой воронки» НЕ годится: при `pipelines_any` ИИ ведёт
    сделку любой воронки, и это решает `pipeline_allowed`. Здесь остаётся справочный набор (срез состояния, тесты)."""
    mode = effective_mode()
    return cfg.MODE_PIPELINES.get(mode, frozenset())


def pipelines_any_allowed() -> bool:
    """Разрешил ли оператор «любую воронку» на стороне движка: `AI_PIPELINES_ANY_ALLOWED=1` в .env (`ai_engine_config`). Без этого `pipelines_any` ложь."""
    return bool(cfg.PIPELINES_ANY_ALLOWED)


def pipelines_any() -> bool:
    """`pipelines_any` документа версии 2: ИИ может вести сделку ЛЮБОЙ воронки (взятие по сообщению клиента). Только настоящее `true`; нет ключа,
    мусор или документ версии 1: нет, то есть поведение этапа 1 (воронки режима). Пока движок не разрешил это переменной `AI_PIPELINES_ANY_ALLOWED=1`,
    всегда нет, что бы ни прислала панель (все точки решения читают именно эту функцию). Режим `off` проверяет `pipeline_allowed`."""
    return pipelines_any_allowed() and is_v2() and (_doc or {}).get("pipelines_any") is True


def pipeline_allowed(pipeline_id: Any) -> bool:
    """Можно ли ИИ вести сделку этой воронки: разрешено РЕЖИМОМ и для воронки есть ПРОМПТ в ИИ-сервисе (решение Кати 23.11.2026, нет промпта: не пишем).
    Режим `off` и ошибки чтения (`effective_mode`): нет. Разрешено режимом: `pipelines_any` - любая воронка (известный номер), иначе воронки режима
    (`cfg.MODE_PIPELINES`), как до этапа 2. Есть промпт: `ai_engine_prompts.has_prompt` (словарь сервиса, последний удачный ответ, при его отсутствии
    две воронки розницы; «Тест» только в режиме `test`). Единственное место этого решения: следить, чтобы не появилось второе."""
    mode = effective_mode()
    if mode == "off":
        return False
    pid = _pos_int(pipeline_id)
    if pid is None:
        return False
    if not (True if pipelines_any() else pid in cfg.MODE_PIPELINES.get(mode, frozenset())):
        return False
    return prompts.has_prompt(pid, mode)


def _initiative_list() -> frozenset[int] | None:
    """`initiative_pipelines` документа версии 2 как множество. Нет ключа, не список или в непустом списке ни одного номера: None (панель
    ничего не сказала, действует умолчание). Пустой список это ответ «нигде». Мусорные элементы списка пропускаются."""
    raw = (_doc or {}).get("initiative_pipelines") if is_v2() else None
    if not isinstance(raw, list):
        return None
    ids = {pid for pid in (_pos_int(x) for x in raw) if pid is not None}
    return frozenset(ids) if ids or not raw else None


def initiative_pipelines() -> frozenset[int]:
    """Воронки, где разрешены инициативные события: список панели, иначе две розничные воронки."""
    listed = _initiative_list()
    return cfg.INITIATIVE_PIPELINES_DEFAULT if listed is None else listed


def initiative_allowed(pipeline_id: Any) -> bool:
    """Можно ли запускать ИНИЦИАТИВНЫЕ события по сделке этой воронки: лестница тишины, окна УР, отложенные касания, напоминание, и ход
    получает `initiative: true`. Воронка вне `initiative_pipelines` значит «только отвечаем». Условия: воронку вообще можно вести
    (`pipeline_allowed`) и она в списке панели; нет списка: воронки самого режима (до этапа 2 так и было: везде, где ИИ ведёт сделки; для
    «Теста» это «Тест») либо две розничные воронки. Режим `off` и ошибки чтения: нигде."""
    if not pipeline_allowed(pipeline_id):
        return False
    pid = _pos_int(pipeline_id)
    listed = _initiative_list()
    if listed is not None:
        return pid in listed
    return pid in cfg.MODE_PIPELINES.get(effective_mode(), frozenset()) or pid in cfg.INITIATIVE_PIPELINES_DEFAULT


# ---------------------------------------------------------------------------------------------
# документ версии 2: активный режим, пилот, справочник сделок (этап 1 плана «режимы ИИ-менеджера»)
# ---------------------------------------------------------------------------------------------
# Версия 2 только ДОПОЛНЯЕТ документ: все прежние ключи (`mode`, `pipelines`, `contacts`, `timers`, ...) значат то же самое, и решение
# «разрешён ли контакт» по-прежнему принимается по `contacts`. Документ без `settings_version` (или с 1) читается ровно как раньше.

def settings_version() -> int:
    """Версия документа панели. Нет ключа или мусор: 1, то есть «читай как раньше»."""
    raw = (_doc or {}).get("settings_version")
    return raw if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 1 else 1


def is_v2() -> bool:
    """Версии выше второй читаются как вторая: схема только дополняется, известные блоки не меняют смысл."""
    return settings_version() >= 2


def active_mode_id() -> str:
    """`off` | `test` | `full` | `help` | `intercept`. Нет `mode_id` (документ версии 1 или версия 2 без ключа) или в нём мусор:
    выводится из старого `mode` (`test` -> `test`, `prod` -> `full`). Несогласованные `mode` и `mode_id` отсеял `effective_mode`."""
    mode = effective_mode()
    if mode == "off":
        return "off"
    if is_v2():
        raw = (_doc or {}).get("mode_id")
        if isinstance(raw, str) and raw in cfg.MODE_IDS:
            return raw
    return cfg.LEGACY_TO_MODE_ID[mode]


def review_enabled() -> bool:
    """Включена ли проверка человеком (`review.enabled`). Нет блока, мусор или документ версии 1: включена, потому что ошибка в эту
    сторону лишь замедляет ответ, а в другую отправляет клиенту текст без проверки."""
    block = (_doc or {}).get("review") if is_v2() else None
    if isinstance(block, dict) and isinstance(block.get("enabled"), bool):
        return block["enabled"]
    return True


def deal_items() -> list[dict[str, int]]:
    """Справочный список разрешённых сделок (`deals.items`). Для решений НЕ используется: контакт разрешён или нет, движок смотрит по
    `contacts`. Нужен срезу состояния и проверкам; мусорные записи пропускаются, нет блока: пусто."""
    block = (_doc or {}).get("deals") if is_v2() else None
    raw = block.get("items") if isinstance(block, dict) else None
    out: list[dict[str, int]] = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict):
            continue
        deal, contact = _pos_int(item.get("deal_id")), _pos_int(item.get("contact_id"))
        if deal is not None:
            out.append({"deal_id": deal, "contact_id": contact or 0})
    return out


def _pos_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        number = value
    elif isinstance(value, str) and value.strip().isdigit():
        number = int(value)
    else:
        return None
    return number if number > 0 else None


def mode_settings(pipeline_id: Any = None, *, initiative: bool | None = None) -> dict[str, Any] | None:
    """Блок `mode_settings` хода (`POST /v1/turn`, необязательное поле). Форма и имена договорены с ИИ-сервисом, не менять:

        {"mode_id": ..., "review": {"enabled": ...}, "initiative": <bool>,
         "functions": {"silence": {...}, "reminder": {"enabled", "after_h"}, "ur": {...}, "touch_max_days": N},      # только если есть в документе
         "call": {"route_to_mop": ...}, "eta": {"source": ..., "person": {"id", "name"}}}                           # только если есть в документе

    `initiative` кладётся всегда при версии 2: можно ли по сделке этой воронки (`pipeline_id`) инициативные действия; нет воронки: нет.
    Вызывающий, у которого есть особые правила (перехваченная сделка), решает сам и передаёт `initiative`. Блоки `functions`, `call`, `eta`
    собираются из эффективных значений (мусор и пропуски заменены умолчаниями), в Перехвате из функций остаётся лестница тишины. Нет
    настроек версии 2 или режим выключен: None, и поле в ход не кладётся."""
    if not is_v2():
        return None
    mode_id = active_mode_id()
    if mode_id == "off":
        return None
    out: dict[str, Any] = {
        "mode_id": mode_id, "review": {"enabled": review_enabled()},
        "initiative": bool(initiative) if initiative is not None else initiative_allowed(pipeline_id),
    }
    if _block("functions") is not None:
        out["functions"] = functions_wire()
    if _block("call") is not None:
        out["call"] = call_config()
    if _block("eta") is not None:
        out["eta"] = eta_config()
    return out


# ---------------------------------------------------------------------------------------------
# функции со временем, звонок, «когда прибудет товар» (этап 2 плана «режимы ИИ-менеджера», схема 3.4 и 3.5)
# ---------------------------------------------------------------------------------------------
# Блоки документа версии 2: `functions {silence, reminder, ur, touch_max_days}`, `call`, `eta`. Читаются КАЖДЫЙ РАЗ из последнего документа
# панели, поэтому правка в панели действует через один опрос (30 с) без перезапуска. Нет блока, нет ключа или мусор (не тот тип, вне
# границ): ЗАПАСНОЕ значение, оно равно сегодняшнему поведению; для лестницы тишины и окон УР запасное значение это прежние переменные
# окружения `AI_SILENCE_ALT_S`, `AI_SILENCE_ALERT_S`, `AI_UR_WAVE_1`, `AI_UR_WAVE_2`, `AI_UR_LATE_FROM` (`cfg`). Документ версии 1 блоков не читает.
# Времена суток московские, `ЧЧ:ММ`.

DEFAULT_ETA_PERSON = {"id": 13963494, "name": "Екатерина Зубалий"}
ETA_SOURCES = ("stock_date", "call_person")
DEFAULT_REMINDER_AFTER_H = 3
DEFAULT_TOUCH_MAX_DAYS = 7
TOUCH_SLACK_S = 3600                         # запас на расхождение часов: касание «через N суток» может прийти чуть позже
UR_DEFAULT_TIMES = {"wave1": "17:50", "wave2": "18:45", "late_from": "17:00"}


def _block(name: str) -> dict[str, Any] | None:
    """Блок документа версии 2 словарём. Версия 1, нет ключа или не словарь: None."""
    raw = (_doc or {}).get(name) if is_v2() else None
    return raw if isinstance(raw, dict) else None


def _sub(block: dict[str, Any] | None, name: str) -> dict[str, Any]:
    raw = (block or {}).get(name)
    return raw if isinstance(raw, dict) else {}


def _bool_or(value: Any, default: bool) -> bool:
    return value if isinstance(value, bool) else default


def _int_in(value: Any, low: int, high: int) -> int | None:
    """Целое в границах; булево, дробное, строка и выход за границы: None (мусор)."""
    return value if isinstance(value, int) and not isinstance(value, bool) and low <= value <= high else None


def _clock(value: Any) -> str | None:
    """Время суток `ЧЧ:ММ` в нормальном виде (`7:05` -> `07:05`); не время: None. Нормальный вид сравнивается как строка."""
    if not isinstance(value, str):
        return None
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", value)
    if m is None or int(m.group(1)) > 23 or int(m.group(2)) > 59:
        return None
    return f"{int(m.group(1)):02d}:{m.group(2)}"


def silence_config() -> dict[str, Any]:
    """`functions.silence`: лестница тишины. `enabled` (да), `alt_after_s` (через сколько писать в другом канале, 5-1440 мин в панели),
    `alert_after_s` (ещё через сколько алерт менеджерам), `alert_on_failed_send` (алерт сразу, если отправка не вышла, да). Запасные
    значения секунд: `AI_SILENCE_ALT_S`, `AI_SILENCE_ALERT_S` (по 3600)."""
    block = _sub(_block("functions"), "silence")
    alt, alert = _int_in(block.get("alt_after_min"), 5, 1440), _int_in(block.get("alert_after_min"), 5, 1440)
    return {
        "enabled": _bool_or(block.get("enabled"), True),
        "alt_after_s": alt * 60 if alt is not None else cfg.SILENCE_ALT_S,
        "alert_after_s": alert * 60 if alert is not None else cfg.SILENCE_ALERT_S,
        "alert_on_failed_send": _bool_or(block.get("alert_on_failed_send"), True),
    }


def reminder_config() -> dict[str, Any]:
    """`functions.reminder`: напоминание «жду подтверждения заказа». Живёт в промпте ИИ-сервиса, движок только передаёт значение в ход.
    В Перехвате напоминания нет."""
    block = _sub(_block("functions"), "reminder")
    after = _int_in(block.get("after_h"), 1, 72)
    return {
        "enabled": _bool_or(block.get("enabled"), True) and active_mode_id() != "intercept",
        "after_h": after if after is not None else DEFAULT_REMINDER_AFTER_H,
    }


def ur_config() -> dict[str, Any]:
    """`functions.ur`: окна перевода в УР. `enabled`; `wave1`, `wave2`, `late_from` (`ЧЧ:ММ`, `wave2` позже `wave1`, `late_from` не позже
    `wave1`). Три времени связаны, поэтому берутся все вместе: пропущено, мусор или связь нарушена, значит все три запасные
    (`AI_UR_WAVE_1`, `AI_UR_WAVE_2`, `AI_UR_LATE_FROM`, при мусоре в них 17:50, 18:45, 17:00). Запасное включение: пусто в `AI_UR_WAVE_1`
    значит окна выключены, как было. В Перехвате окон УР нет."""
    block = _sub(_block("functions"), "ur")
    env = {
        "wave1": _clock(cfg.UR_WAVE_1) or UR_DEFAULT_TIMES["wave1"],
        "wave2": _clock(cfg.UR_WAVE_2) or UR_DEFAULT_TIMES["wave2"],
        "late_from": _clock(cfg.UR_LATE_FROM) or UR_DEFAULT_TIMES["late_from"],
    }
    mine = {key: _clock(block.get(key)) for key in env}
    ok = all(mine.values()) and mine["wave2"] > mine["wave1"] and mine["late_from"] <= mine["wave1"]
    return {
        "enabled": _bool_or(block.get("enabled"), bool(cfg.UR_WAVE_1)) and active_mode_id() != "intercept",
        **(mine if ok else env),
    }


def touch_max_days() -> int:
    """`functions.touch_max_days`: самый дальний срок отложенного касания, 1-30 суток (7)."""
    value = _int_in((_block("functions") or {}).get("touch_max_days"), 1, 30)          # ключ лежит прямо в `functions`, не в подблоке
    return value if value is not None else DEFAULT_TOUCH_MAX_DAYS


def touch_max_ahead_s() -> int:
    """Дальше скольких секунд от сейчас касание не регистрируется: `touch_max_days` суток и час запаса."""
    return touch_max_days() * 86400 + TOUCH_SLACK_S


def touches_available() -> bool:
    """Есть ли отложенные касания в этом режиме. В Перехвате нет: ИИ там только отвечает на неотвеченное."""
    return active_mode_id() != "intercept"


def functions_wire() -> dict[str, Any]:
    """Блок `functions` для `mode_settings`: эффективные значения (минуты и часы, как в панели). В Перехвате только лестница тишины."""
    sil = silence_config()
    out: dict[str, Any] = {"silence": {
        "enabled": sil["enabled"], "alt_after_min": max(1, round(sil["alt_after_s"] / 60)),
        "alert_after_min": max(1, round(sil["alert_after_s"] / 60)), "alert_on_failed_send": sil["alert_on_failed_send"],
    }}
    if touches_available():
        rem, ur = reminder_config(), ur_config()
        out["reminder"] = {"enabled": rem["enabled"], "after_h": rem["after_h"]}
        out["ur"] = {key: ur[key] for key in ("enabled", "wave1", "wave2", "late_from")}
        out["touch_max_days"] = touch_max_days()
    return out


def call_config() -> dict[str, Any]:
    """`call.route_to_mop`: выводить клиента на звонок с МОПом (шаблон А) или предлагать время звонка (шаблон Б). Умолчание: да."""
    return {"route_to_mop": _bool_or((_block("call") or {}).get("route_to_mop"), True)}


def eta_config() -> dict[str, Any]:
    """`eta`: откуда брать «когда прибудет товар»: `stock_date` (дата приёмки из МойСклад) или `call_person` (звать сотрудника `person`).
    Умолчание: `stock_date`, Екатерина Зубалий 13963494. Человек с мусорным `id` или пустым именем заменяется умолчанием целиком."""
    block = _block("eta") or {}
    source = block.get("source") if block.get("source") in ETA_SOURCES else "stock_date"
    person = block.get("person") if isinstance(block.get("person"), dict) else {}
    pid, name = _pos_int(person.get("id")), person.get("name")
    ok = pid is not None and isinstance(name, str) and bool(name.strip())
    return {"source": source, "person": {"id": pid, "name": name.strip()} if ok else dict(DEFAULT_ETA_PERSON)}


def timers() -> dict[str, int]:
    raw = (_doc or {}).get("timers")
    raw = raw if isinstance(raw, dict) else {}
    out = dict(DEFAULT_TIMERS)
    for key in out:
        try:
            value = int(raw.get(key, out[key]))
        except (TypeError, ValueError):
            continue
        if value >= 0:
            out[key] = value
    return out


def intercept_enabled() -> bool:
    """Тумблер «Перехватывать сделки» из панели (временная функция, `ai_engine_intercept`). Нет ключа: выключено."""
    block = (_doc or {}).get("intercept")
    return isinstance(block, dict) and block.get("enabled") is True


INTERCEPT_DEFAULT_MANAGERS = [{"id": 9291546, "name": "Оанча Игорь"}]          # Игорь Оанча: прежнее поведение, если панель ключей не прислала


def intercept_config() -> dict[str, Any]:
    """Настройки перехвата из панели с безопасными значениями по умолчанию (прежнее поведение): менеджеры, «Офис» (выкл), режим `take`,
    минуты молчания (1-240), сделки со звонками (не трогаем). Мусор в значении заменяется умолчанием, а не ломает перехват."""
    block = (_doc or {}).get("intercept")
    block = block if isinstance(block, dict) else {}
    managers: list[dict[str, Any]] = []
    raw = block.get("managers")
    for m in raw if isinstance(raw, list) else []:
        try:
            uid, name = int(m["id"]), str(m["name"]).strip()
        except (KeyError, TypeError, ValueError):
            continue
        if uid > 0 and name:
            managers.append({"id": uid, "name": name})
    if not isinstance(raw, list):
        managers = [dict(m) for m in INTERCEPT_DEFAULT_MANAGERS]
    wait = block.get("wait_min", 15)
    wait = wait if isinstance(wait, int) and not isinstance(wait, bool) and 1 <= wait <= 240 else 15
    return {
        "enabled": block.get("enabled") is True,
        "managers": managers,
        "office": block.get("office") is True,
        "mode": "reply" if block.get("mode") == "reply" else "take",
        "wait_min": wait,
        "with_calls": block.get("with_calls") is True,
    }


def post_sale_window_days() -> int:
    try:
        return int((_doc or {}).get("post_sale_window_days", 7))
    except (TypeError, ValueError):
        return 7


# ---------------------------------------------------------------------------------------------
# контакты
# ---------------------------------------------------------------------------------------------

def contacts_restricted() -> bool:
    """Ограничен ли режим списком. Нет документа - ограничен (пустой список = никого)."""
    contacts = (_doc or {}).get("contacts")
    if not isinstance(contacts, dict):
        return True
    return bool(contacts.get("restricted", True))


def contact_items() -> list[dict[str, Any]]:
    contacts = (_doc or {}).get("contacts")
    if not isinstance(contacts, dict):
        return []
    items = contacts.get("items")
    return [i for i in (items if isinstance(items, list) else []) if isinstance(i, dict)]


def phone_key(raw: Any) -> str:
    """Телефон для сравнения: цифры, российская 8 -> 7. Короче десяти цифр - ключа нет."""
    digits = ru_8_to_7(digits_only(str(raw or "")))
    return digits if len(digits) >= 10 else ""


def _same_phone(a: str, b: str) -> bool:
    return bool(a) and bool(b) and a[-10:] == b[-10:]


def phone_may_match(*phones: Any) -> bool:
    """Дешёвая проверка ДО похода в amo: может ли чат принадлежать списку.

    Вебхук Wazzup приходит на каждое сообщение аккаунта, а ходить в amo на каждый чужой чат
    нельзя. Если у всех записей списка есть телефон и ни один не совпал с телефонами чата - чат
    точно не наш. Если у какой-то записи телефона нет, проверить без amo нельзя - пропускаем дальше.
    """
    if not contacts_restricted():
        return True
    items = contact_items()
    keys = [phone_key(p) for p in phones]
    keys = [k for k in keys if k]
    unknown = False
    for item in items:
        pk = phone_key(item.get("phone_norm"))
        if not pk:
            unknown = True
            continue
        if any(_same_phone(pk, k) for k in keys):
            return True
    return unknown


def contact_allowed(contact_ids: list[int], phones: list[Any]) -> bool:
    """Решение по контакту сделки: он в списке (по id в amo или по телефону) либо список не нужен."""
    if not contacts_restricted():
        return True
    ids = {int(c) for c in contact_ids}
    keys = [k for k in (phone_key(p) for p in phones) if k]
    for item in contact_items():
        try:
            if int(item.get("contact_id") or 0) in ids:
                return True
        except (TypeError, ValueError):
            pass
        pk = phone_key(item.get("phone_norm"))
        if pk and any(_same_phone(pk, k) for k in keys):
            return True
    return False


# ---------------------------------------------------------------------------------------------
# рабочие часы
# ---------------------------------------------------------------------------------------------

def _hhmm(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"\d{1,2}:\d{2}", value) is not None


def work_hours() -> list[dict[str, str]]:
    """Интервалы окна отправки. Не список, интервал не словарь или время не вида `ЧЧ:ММ`: такой интервал пропускается, а пустой
    результат значит «не отправляет никогда» (см. `send_window`), то есть мусор закрывает окно, а не роняет проход."""
    raw = (_doc or {}).get("work_hours")
    return [h for h in (raw if isinstance(raw, list) else []) if isinstance(h, dict) and _hhmm(h.get("start")) and _hhmm(h.get("end"))]


def send_window(now: datetime.datetime | None = None) -> dict[str, Any]:
    """Окно отправки на сейчас по часам работы (МСК). Пустые часы = никогда, а не круглосуточно:
    так читает их и авто-режим, пустые часы способ приостановить отправки, не теряя настроек."""
    hours = work_hours()
    moment = (now or datetime.datetime.now(MSK)).astimezone(MSK)
    if not hours:
        return {"open_now": False, "next_open_at": None}
    hhmm = moment.strftime("%H:%M")
    if any(h["start"] <= hhmm < h["end"] for h in hours):
        return {"open_now": True, "next_open_at": None}
    starts = []
    for h in hours:
        hh, mm = (int(x) for x in str(h["start"]).split(":"))
        starts.append(datetime.datetime.combine(moment.date(), datetime.time(hh, mm), tzinfo=MSK))
    upcoming = [t for t in starts if t > moment]
    if upcoming:
        nxt = min(upcoming)
    else:
        hh, mm = (int(x) for x in min(h["start"] for h in hours).split(":"))
        nxt = datetime.datetime.combine(
            moment.date() + datetime.timedelta(days=1), datetime.time(hh, mm), tzinfo=MSK,
        )
    return {"open_now": False, "next_open_at": nxt.isoformat()}


# ---------------------------------------------------------------------------------------------
# опрос панели
# ---------------------------------------------------------------------------------------------

async def fetch_once() -> bool:
    """Один опрос. True - успех. Сбой кэш НЕ трогает (но срок годности продолжает идти)."""
    if not cfg.PANEL_URL or not cfg.PANEL_TOKEN:
        return False
    url = f"{cfg.PANEL_URL}/api/ingest/ai/settings"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"X-Ingest-Token": cfg.PANEL_TOKEN})
        if resp.status_code != 200:
            logger.warning("ai_engine_settings: HTTP %s на /ai/settings", resp.status_code)
            return False
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ai_engine_settings: сбой запроса настроек")
        return False
    if not isinstance(data, dict) or "mode" not in data:
        logger.warning("ai_engine_settings: ответ не похож на настройки ИИ, кэш не трогаю")
        return False
    set_document(data)
    try:
        write_json_atomic(cfg.SETTINGS_CACHE_PATH, {"fetched_at": _fetched_at, "doc": data}, ensure_ascii=False)
    except OSError:
        logger.exception("ai_engine_settings: не записался кэш %s", cfg.SETTINGS_CACHE_PATH)
    return True


async def _poll_loop() -> None:
    while True:
        try:
            if not await fetch_once():
                logger.warning("ai_engine_settings: опрос не удался, работаем на кэше (возраст %s с)", age_s())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine_settings: ошибка цикла опроса")
        await asyncio.sleep(cfg.SETTINGS_POLL_INTERVAL_S)


def start() -> None:
    global _poll_task
    if not cfg.ENABLED:
        return
    _load_cache()
    if not cfg.PANEL_URL or not cfg.PANEL_TOKEN:
        logger.error("ai_engine_settings: нет AI_INGEST_TOKEN/адреса панели - режим останется off")
        return
    _poll_task = asyncio.create_task(_poll_loop())


async def stop() -> None:
    global _poll_task
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
