"""Взятие сделки ИИ: фильтры, бюджет, товары по артикулам, «существующие сделки» (этап 2 плана «режимы ИИ-менеджера», 20.11.2026, часть 1).

Схема настроек: разделы 3.3, 3.3а, 7. Эффективный документ панели версии 2 получает блоки `filters`, `budget`, `existing`, `target_skus`,
`ai_user`. Всё здесь читается ТОЛЬКО при документе версии 2 и только для режимов, где ключ существует (фильтры и бюджет: `full`, `help`,
`intercept`; `existing`: только `full`). Нет блока: ограничения нет, поведение прежнее. Повреждённая подгруппа (не тот тип, незнакомая
политика) не действует, причина идёт в лог один раз; остальные подгруппы работают.

Когда проверяется. ОДИН раз, когда ИИ берёт сделку (первое сообщение клиента в разрешённой воронке: `_apply_resolution`, `owner is None`; перехват:
`ai_engine_intercept._skip_reason`). Уже взятую сделку изменение полей не снимает. Не применяется: ручное «Дать сделку ИИ» (`give_deal`: решение
человека), возврат диалога (`return_dialog`: сделка уже была у ИИ), правило C (сделку уже ведёт ИИ).

Две стадии, чтобы не звать людей зря и не держать общий замок:
  1. `cheap_refusal(lead)`: чистая проверка по полям самой сделки (существующая сделка при `leave`, бюджет, тип заявки, способ оплаты, статус
     оплаты, тип доставки). Идёт внутри `dedup.resolve` по каждой подходящей сделке: сделка, которую ИИ всё равно не возьмёт, не считается
     подходящей, и правила A и D между такими сделками людей не зовут.
  2. `decide_take(lead)` (async, вне общего замка): то же плюс товары (МойСклад, кэш, таймаут), «менеджер активен» и смена ответственного
     при `redistribute`. Любой сбой МойСклад значит «нераспознанная позиция», то есть безопасный отказ.

Сопоставление значений amo с кодами (сравнение без регистра, «ё» как «е», дефис как пробел, лишние пробелы не важны; пусто это `unset`,
незнакомое это `other`). Коды те же, что в панели: `CODES` это золотой список для контрактного теста в обоих репозиториях.
Правила сверены 23.11.2026 по 2266 свежим сделкам amo (воронки «ОП розница», TangemShop, «Офис», ОПТ), фикстуры в `test_ai_engine_filters.py`.

«Существующие сделки» (только `full`): существующая это сделка, созданная до `existing.since`. `leave`: ИИ берёт только новые сделки, сделки
разрешённых контактов (явное разрешение сильнее переключателя) и отданные вручную. `redistribute`: берёт и существующие открытые сделки, подходящие
под фильтры и бюджет, и назначает им ответственным пользователя ИИ (`ai_user`): запись `responsible_user_id` в amo, примечание «Ответственный
изменён с ... на ... (ИИ, режим Полный)», строка в таблице `reassign_log`; прежнему ответственному пинг не уходит. Условия:
  • пилот (`review.enabled`) включён: смена ответственного без подтверждения человека недопустима, а у движка механизма «действие ждёт проверки»
    нет (проверка живёт в ИИ-сервисе и касается ответов клиенту), поэтому такую сделку ИИ НЕ берёт (безопасный отказ);
  • запись в боевую amo стоит за флагом `AI_REASSIGN_ENABLED=1` (по умолчанию выключен): без него `redistribute` существующих сделок не берёт;
  • менеджер писал клиенту менее `existing.manager_quiet_min` минут назад: ждём (`retry_at`), 0 значит не проверять;
  • смена не удалась: сделку не берём, причина в логе.

Охрана инициативы. По взятой существующей сделке лестница тишины, напоминание, окна УР и касания включаются только после первого сообщения
ИИ клиенту (иначе окно УР в 17:50 пройдёт по всем старым сделкам разом): `initiative_allowed_for_deal(owner_row)`. Первым сообщением считается
первое разрешение на отправку (`/permit`, `mark_first_ai_message`). Контракт для тех, кто запускает инициативные функции: перед запуском спросить
`initiative_allowed_for_deal(owner)` и при `False` не запускать (и не помечать запуск выполненным: после первого сообщения функция должна
сработать как обычно).

Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`.
"""
import asyncio
import datetime
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import ai_engine_dedup as dedup
import ai_engine_give as give
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
import ms_client
from help_function import DELIVERY_PREFIXES

logger = logging.getLogger("uvicorn")

# --- поля amo -------------------------------------------------------------------------------
FIELD_REQUEST_TYPE = 577671      # «Тип заявки» (список)
FIELD_PAYMENT_METHOD = 577373    # «Способ оплаты» (текст)
FIELD_PAYMENT_STATUS = 576669    # «Статус оплаты» (список)
FIELD_DELIVERY = 577315          # «Тип доставки» (текст)
FIELD_CART = 577313              # «Корзина» (текст, только товары)
FIELD_COMPOSITION = 576703       # «Состав заказа» (текст: заказ, товары, доставка, итог)
FIELD_MS_ORDER = 576689          # «ID Заказа» (UUID заказа покупателя в МойСклад)

# --- коды фильтров (золотой список, один в панели и в движке) ------------------------------
CODES: dict[str, tuple[str, ...]] = {
    "request_type": ("order", "preorder", "reserve", "other", "unset"),
    "payment_method": ("online", "on_receipt", "cod", "cash", "crypto", "invoice", "other_method", "other", "unset"),
    "payment_status": ("paid", "unpaid", "partial", "other", "unset"),
    "delivery_type": ("cdek", "showroom", "office", "courier_msk", "other", "unset"),
}
GROUP_LABELS = {"request_type": "тип заявки", "payment_method": "способ оплаты", "payment_status": "статус оплаты", "delivery_type": "тип доставки"}
FILTER_MODES = frozenset({"full", "help", "intercept"})      # в Тесте фильтров и бюджета нет
EXISTING_MODES = frozenset({"full"})
POLICIES = ("leave", "redistribute")

DEFAULT_AI_USER = {"id": 11513202, "name": "Гладков Александер"}
DEFAULT_QUIET_MIN = 15

REASSIGN_ENABLED = os.getenv("AI_REASSIGN_ENABLED", "").strip() == "1"     # запись ответственного в боевую amo: по умолчанию выключена
REASSIGN_TIMEOUT_S = 15
MS_CALL_TIMEOUT_S = 6                  # один запрос к МойСклад (без повторов)
PRODUCTS_DEADLINE_S = 10               # вся проверка товаров одной сделки
PRODUCTS_MAX_LOOKUPS = 6               # названий из корзины, которые ищем в каталоге за одну сделку; остальные «нераспознаны»
ORDER_TTL_S, ORDER_FAIL_TTL_S = 300, 30
NAME_TTL_S, NAME_FAIL_TTL_S = 600, 120
MANAGER_OUT_KEEP_S = 3 * 86400
GUARD_KEEP_S = 30 * 86400

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _now() -> float:
    return time.time()


def _mono() -> float:
    return time.monotonic()


# =============================================================================================
# хранилище: когда писал менеджер, охрана инициативы, журнал смены ответственного, служебное
# =============================================================================================

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS filters_meta (key TEXT PRIMARY KEY, value REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS manager_out (chat_key TEXT PRIMARY KEY, at REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS existing_guard (deal_id INTEGER PRIMARY KEY, taken_at REAL NOT NULL, first_msg_at REAL);"
    "CREATE TABLE IF NOT EXISTS reassign_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, "
    "from_id INTEGER, to_id INTEGER NOT NULL, ok INTEGER NOT NULL, note TEXT NOT NULL DEFAULT '')"
)


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def note_manager_out(chat_key: str, now: float | None = None) -> None:
    """Человек (не автоматика и не ИИ) написал клиенту в чат: нужно «менеджер активен» при `redistribute`."""
    if not chat_key:
        return
    at = float(now if now is not None else _now())
    with _Db() as conn:
        conn.execute("INSERT INTO manager_out (chat_key, at) VALUES (?,?) ON CONFLICT(chat_key) DO UPDATE SET at=MAX(at, excluded.at)", (chat_key, at))
        conn.execute("DELETE FROM manager_out WHERE at<?", (at - MANAGER_OUT_KEEP_S,))


def manager_last_out(chat_key: str) -> float | None:
    if not chat_key:
        return None
    with _Db() as conn:
        row = conn.execute("SELECT at FROM manager_out WHERE chat_key=?", (chat_key,)).fetchone()
    return float(row["at"]) if row else None


def _first_seen() -> float:
    """Запасная граница «существующих», когда панель не прислала `existing.since`: момент первой проверки (хранится в базе, переживает рестарт)."""
    now = _now()
    with _Db() as conn:
        conn.execute("INSERT OR IGNORE INTO filters_meta (key, value) VALUES ('existing_first_seen', ?)", (now,))
        return float(conn.execute("SELECT value FROM filters_meta WHERE key='existing_first_seen'").fetchone()["value"])


def guard_on_take(deal_id: int, now: float | None = None) -> None:
    """Существующая сделка взята: инициативные функции по ней ждут первого сообщения ИИ клиенту."""
    at = float(now if now is not None else _now())
    with _Db() as conn:
        conn.execute("INSERT OR REPLACE INTO existing_guard (deal_id, taken_at, first_msg_at) VALUES (?,?,NULL)", (int(deal_id), at))
        conn.execute("DELETE FROM existing_guard WHERE first_msg_at IS NOT NULL AND first_msg_at<?", (at - GUARD_KEEP_S,))


def mark_first_ai_message(deal_id: int, now: float | None = None) -> None:
    """ИИ получил разрешение написать клиенту (`/permit`): охрана по сделке снимается. Нет охраны: ничего не делает. Не бросает."""
    try:
        with _Db() as conn:
            conn.execute("UPDATE existing_guard SET first_msg_at=? WHERE deal_id=? AND first_msg_at IS NULL",
                         (float(now if now is not None else _now()), int(deal_id)))
    except Exception:  # noqa: BLE001 - снятие охраны не должно ломать разрешение на отправку
        logger.exception("ai_engine_filters: не снял охрану инициативы по сделке %s", deal_id)


def guard_row(deal_id: int) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM existing_guard WHERE deal_id=?", (int(deal_id),)).fetchone()
    return dict(row) if row else None


def initiative_allowed_for_deal(deal_row: dict[str, Any] | None, guard: dict[str, Any] | None = None) -> bool:
    """Можно ли запускать по сделке инициативные функции (лестница тишины, напоминание, окна УР, касания). `deal_row` это строка `deal_owner`
    (нужен `deal_id`). Нет охраны (сделка не из «существующих», либо настройки без блока `existing`) или ИИ уже писал клиенту: можно. Охрана
    есть, а первого сообщения ИИ ещё не было: нельзя. `guard` для проверок без базы (строка `existing_guard` или None). Не бросает: на сбое базы
    отвечает «можно» (прежнее поведение)."""
    try:
        deal_id = int((deal_row or {}).get("deal_id"))
    except (TypeError, ValueError):
        return True
    try:
        row = guard if guard is not None else guard_row(deal_id)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_filters: не прочитал охрану инициативы по сделке %s, считаю, что можно", deal_id)
        return True
    return row is None or row.get("first_msg_at") is not None


def log_reassign(deal_id: int, from_id: int | None, to_id: int, ok: bool, note: str = "", now: float | None = None) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO reassign_log (deal_id, at, from_id, to_id, ok, note) VALUES (?,?,?,?,?,?)",
                     (int(deal_id), float(now if now is not None else _now()), from_id, int(to_id), 1 if ok else 0, str(note)[:300]))


def reassign_journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние смены ответственного, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM reassign_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


# =============================================================================================
# настройки: безопасные чтения блоков документа версии 2
# =============================================================================================

_warned: set[str] = set()


def _warn(where: str, why: str) -> None:
    """Повреждённая подгруппа: в лог один раз на (подгруппа, причина), подгруппа не действует."""
    key = f"{where}:{why}"
    if key not in _warned:
        if len(_warned) > 200:
            _warned.clear()
        _warned.add(key)
        logger.error("ai_engine_filters: подгруппа %s в настройках повреждена (%s), не действует", where, why)


def _block(name: str) -> Any:
    """Блок эффективного документа. Нет документа версии 2 или нет ключа: None."""
    if not settings.is_v2():
        return None
    return (settings.document() or {}).get(name)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def filter_groups() -> dict[str, frozenset[str]]:
    """Действующие группы фильтров: имя -> отмеченные коды. Группа без отмеченных кодов не действует; незнакомые коды отбрасываются."""
    block = _block("filters")
    if block is None:
        return {}
    if not isinstance(block, dict):
        _warn("filters", "не словарь")
        return {}
    out: dict[str, frozenset[str]] = {}
    for name, known in CODES.items():
        raw = block.get(name)
        if raw is None:
            continue
        if not isinstance(raw, list):
            _warn(f"filters.{name}", "не список")
            continue
        codes = frozenset(c for c in raw if isinstance(c, str) and c in known)
        if any(not (isinstance(c, str) and c in known) for c in raw):
            _warn(f"filters.{name}", "незнакомые коды отброшены")
        if codes:
            out[name] = codes
    return out


def _norm_sku(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def products_config() -> dict[str, Any] | None:
    """Фильтр по товарам: None, если не действует (нет блока, выключен, повреждён). `target_skus` читается только при включённом фильтре."""
    block = _block("filters")
    prod = block.get("products") if isinstance(block, dict) else None
    if prod is None:
        return None
    if not isinstance(prod, dict):
        _warn("filters.products", "не словарь")
        return None
    if prod.get("enabled") is not True:
        return None
    raw = (settings.document() or {}).get("target_skus")
    if not isinstance(raw, list):
        _warn("target_skus", "нет списка артикулов при включённом фильтре товаров")
        return None
    skus = set()
    for item in raw:
        article = item.get("article") if isinstance(item, dict) else item
        if isinstance(article, str) and _norm_sku(article):
            skus.add(_norm_sku(article))
        else:
            _warn("target_skus", "запись не строка и не {article}")
    allow_empty = prod.get("allow_empty") if isinstance(prod.get("allow_empty"), bool) else True
    allow_unknown = prod.get("allow_unknown") if isinstance(prod.get("allow_unknown"), bool) else False
    return {"allow_empty": allow_empty, "allow_unknown": allow_unknown, "skus": frozenset(skus)}


def budget_limit() -> int | None:
    """`budget.skip_from_rub`: целое от 1 (сделка с бюджетом от этой суммы включительно не берётся). Пусто, 0, нет блока, мусор: без лимита."""
    block = _block("budget")
    if block is None:
        return None
    if not isinstance(block, dict):
        _warn("budget", "не словарь")
        return None
    raw = block.get("skip_from_rub")
    if isinstance(raw, bool):
        _warn("budget.skip_from_rub", "bool вместо числа")
        return None
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    try:
        value = int(float(raw.strip().replace(",", ".") if isinstance(raw, str) else raw))
    except (TypeError, ValueError, OverflowError):
        value = None
    if value is None or value < 0:
        _warn("budget.skip_from_rub", "не число или отрицательное")
        return None
    return value or None


def _parse_ts(value: Any) -> float | None:
    """Метка времени панели: unix-секунды (число или строка из цифр) либо ISO 8601 (без пояса значит UTC). Пусто или мусор: None."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    text = str(value).strip()
    if text.replace(".", "", 1).isdigit():
        return float(text) or None
    try:
        moment = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        _warn("existing.since", "не метка времени")
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.timestamp()


def existing_config() -> dict[str, Any] | None:
    """`existing`: `{policy, since (unix | None), manager_quiet_min}`. Нет блока или повреждена политика: None (ограничения нет, как сегодня)."""
    block = _block("existing")
    if block is None:
        return None
    if not isinstance(block, dict):
        _warn("existing", "не словарь")
        return None
    policy = block.get("policy")
    if policy not in POLICIES:
        _warn("existing.policy", "не leave и не redistribute")
        return None
    quiet = block.get("manager_quiet_min", DEFAULT_QUIET_MIN)
    if not _is_int(quiet) or not 0 <= quiet <= 240:
        _warn("existing.manager_quiet_min", "вне 0-240")
        quiet = DEFAULT_QUIET_MIN
    return {"policy": policy, "since": _parse_ts(block.get("since")), "manager_quiet_min": quiet}


def ai_user() -> dict[str, Any] | None:
    """Пользователь ИИ (`ai_user {id, name}`). Нет блока: Гладков Александер, как по умолчанию в панели. Повреждён: None."""
    block = _block("ai_user")
    if block is None:
        return dict(DEFAULT_AI_USER)
    if not isinstance(block, dict) or not _is_int(block.get("id")) or block["id"] <= 0:
        _warn("ai_user", "нет положительного id")
        return None
    name = str(block.get("name") or "").strip() or ("ИИ-менеджер" if block["id"] != DEFAULT_AI_USER["id"] else DEFAULT_AI_USER["name"])
    return {"id": int(block["id"]), "name": name}


def is_configured(mode_id: str) -> bool:
    """Есть ли что проверять при взятии сделки в этом режиме. Нет: `dedup.resolve` и `_apply_resolution` работают ровно как раньше."""
    if mode_id in FILTER_MODES and (filter_groups() or products_config() is not None or budget_limit() is not None):
        return True
    return mode_id in EXISTING_MODES and existing_config() is not None


# =============================================================================================
# сопоставление значений amo с кодами
# =============================================================================================

def norm(value: Any) -> str:
    """Для сравнения: без регистра, «ё» как «е», дефисы и тире как пробел, лишние пробелы убраны."""
    text = str(value if value is not None else "").casefold().replace("ё", "е")
    text = re.sub(r"[\-‐-―]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _field(lead: dict, field_id: int) -> str:
    value = amo_service.get_custom_field_value(lead, field_id)
    return "" if value is None else str(value)


_REQUEST_TYPES = {"заказ": "order", "предзаказ": "preorder", "резерв": "reserve"}
_PAYMENT_STATUSES = {"оплачен": "paid", "не оплачен": "unpaid", "частично оплачен": "partial"}
# Способ оплаты (текст). «receipt» это «При получении», он становится `cod` или `on_receipt` по доставке.
_PAYMENT_METHODS = {
    "онлайн оплата": "online", "оплата онлайн": "online", "онлайн по ссылке": "online", "сбп": "online",
    "при получении": "receipt", "оплата при получении": "receipt",
    "наличные менеджер": "cash", "эвотор": "cash", "наличные": "cash",
    "крипта": "crypto",
    "расчетный счет": "invoice", "счет юр лицо": "invoice", "счет": "invoice", "банковский перевод": "invoice",
    "другой способ": "other_method",
}
_DELIVERY_COURIER_HEADS = frozenset({"курьерская доставка", "доставка курьером по москве", "доставка курьером", "доставка"})


def request_type_code(lead: dict) -> str:
    raw = norm(_field(lead, FIELD_REQUEST_TYPE))
    return "unset" if not raw else _REQUEST_TYPES.get(raw, "other")


def payment_status_code(lead: dict) -> str:
    raw = norm(_field(lead, FIELD_PAYMENT_STATUS))
    return "unset" if not raw else _PAYMENT_STATUSES.get(raw, "other")


def _is_cdek(text: str) -> bool:
    return "cdek" in text or "сдэк" in text


def delivery_type_code(lead: dict) -> str:
    """СДЭК (CDEK/СДЭК в тексте: пункт, постамат, курьер, склад-дверь) | шоурум | офис | курьер по Москве | other | unset."""
    raw = norm(_field(lead, FIELD_DELIVERY))
    if not raw:
        return "unset"
    if _is_cdek(raw):
        return "cdek"
    if "шоурум" in raw:
        return "showroom"
    if "самовывоз" in raw and "офис" in raw:
        return "office"
    if raw.split(",")[0].strip() in _DELIVERY_COURIER_HEADS:
        return "courier_msk"
    return "other"


def payment_method_code(lead: dict) -> str:
    """Способ оплаты. «При получении» и «Оплата при получении»: при доставке СДЭК это `cod` (наложка), при любой другой, включая пустую, `on_receipt`;
    запасной признак `cod`: в строке доставки слова про наложенный платёж. Составное значение («А + Б») берётся, только если обе части дают один код."""
    raw = norm(_field(lead, FIELD_PAYMENT_METHOD))
    if not raw:
        return "unset"
    kinds = {_PAYMENT_METHODS.get(part.strip()) for part in raw.split("+")}
    if len(kinds) != 1 or None in kinds:
        return "other"
    kind = kinds.pop()
    if kind != "receipt":
        return kind
    delivery = norm(_field(lead, FIELD_DELIVERY))
    return "cod" if _is_cdek(delivery) or "наложенн" in delivery else "on_receipt"


_CODE_GETTERS = {
    "request_type": request_type_code, "payment_method": payment_method_code,
    "payment_status": payment_status_code, "delivery_type": delivery_type_code,
}


# =============================================================================================
# проверки по полям сделки
# =============================================================================================

def lead_is_existing(lead: dict, boundary: float) -> bool:
    """Сделка создана до границы. Возраст неизвестен: считается существующей (осторожно)."""
    try:
        return float(lead.get("created_at")) < boundary
    except (TypeError, ValueError):
        return True


def _boundary(cfg_existing: dict[str, Any]) -> float:
    return cfg_existing["since"] if cfg_existing["since"] is not None else _first_seen()


def existing_for_take(lead: dict, mode_id: str) -> bool:
    """Существует ли эта сделка в смысле `existing` (для охраны инициативы). Нет блока, не Полный режим: нет."""
    if mode_id not in EXISTING_MODES:
        return False
    ex = existing_config()
    return ex is not None and lead_is_existing(lead, _boundary(ex))


def cheap_refusal(lead: dict, *, mode_id: str, explicit: bool = False, given: bool = False, intercept: bool = False) -> str | None:
    """Причина, по которой ИИ не берёт сделку, по её собственным полям; None: можно. `explicit`: контакт разрешён списком режима (явное
    разрешение сильнее переключателя «не трогать существующие»), `given`: сделку отдали вручную, `intercept`: взятие перехватом (`existing` его не касается).
    Фильтры, бюджет и товары к разрешённым контактам применяются: освобождает от них только ручная передача (`give_deal`, сюда не заходит)."""
    if mode_id in EXISTING_MODES and not intercept and not given and not explicit:
        ex = existing_config()
        if ex is not None and ex["policy"] == "leave" and lead_is_existing(lead, _boundary(ex)):
            return "существующая сделка, политика «не трогать существующие»"
    if mode_id not in FILTER_MODES:
        return None
    limit = budget_limit()
    if limit is not None:
        try:
            price = float(lead.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price >= limit:
            return f"бюджет сделки не меньше лимита {limit} руб."
    for name, codes in filter_groups().items():
        code = _CODE_GETTERS[name](lead)
        if code not in codes:
            return f"фильтр «{GROUP_LABELS[name]}»: значение {code} не отмечено"
    return None


def make_eligibility(mode_id: str, phones: list[Any]):
    """Проверка для `dedup.resolve(eligible=...)`: None, если проверять нечего. Сделки, у которых в базе движка уже есть владелец, не проверяются
    (решение по ним принято раньше). Сбой проверки это отказ."""
    if not is_configured(mode_id):
        return None

    def check(lead: dict) -> str | None:
        try:
            if store.get_owner(int(lead["id"])) is not None:
                return None
            explicit = settings.contacts_restricted() and settings.contact_allowed(dedup.contact_ids_of(lead), phones)
            return cheap_refusal(lead, mode_id=mode_id, explicit=explicit)
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_filters: сбой проверки сделки %s, сделку не берём", lead.get("id"))
            return "сбой проверки фильтров"
    return check


# =============================================================================================
# товары сделки: артикулы по заказу МойСклад, по названию из каталога, нераспознанные
# =============================================================================================

_POSITION_RE = re.compile(
    r"(?P<name>[^\n]+?),\s*(?P<qty>\d+(?:[.,]\d+)?)\s*(?:шт\.?|ед\.?)?\s*,\s*(?P<price>\d[\d\s ]*(?:[.,]\d+)?)\s*(?:руб\w*|₽|р\.)",
    re.IGNORECASE)
_NON_GOODS_PREFIXES = tuple(p.casefold() for p in DELIVERY_PREFIXES) + ("скидка", "промокод", "купон", "наценка", "итого", "ндс")


def parse_cart_items(text: Any) -> list[str]:
    """Названия товаров из текста «Корзина» или «Состав заказа». Позиции вида «Название, 1 шт, 5 990.00 рублей» идут склеенными пробелом либо по
    строкам с нумерацией «1. ...»; строки доставки, наценок, скидок, промокодов и итогов отбрасываются (список префиксов общий с `parse_the_cart_field`)."""
    names: list[str] = []
    for line in str(text or "").splitlines():
        line = re.sub(r"^\s*\d+[.)]\s+", "", line)
        for m in _POSITION_RE.finditer(line):
            name = m.group("name").strip(" ;,")
            if name and not name.casefold().startswith(_NON_GOODS_PREFIXES):
                names.append(name)
    return names


@dataclass
class Item:
    title: str
    article: str | None      # None: нераспознан


_cache: dict[Any, tuple[float, Any]] = {}
_inflight: dict[Any, asyncio.Future] = {}


async def _ms_get(path: str, params: dict | None = None) -> dict | None:
    """Один запрос к МойСклад с таймаутом и без повторов. Любой сбой: None."""
    try:
        return await asyncio.wait_for(ms_client.get(path, params, retries=1), timeout=MS_CALL_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.warning("ai_engine_filters: запрос к МойСклад не вышел (%s)", "/".join(path.split("/")[:2]))
        return None


async def _cached(key: Any, factory, *, ok_ttl: float, fail_ttl: float) -> Any:
    """Результат запроса с кэшем на минуты. Ждём не дольше `MS_CALL_TIMEOUT_S + 1`: при таймауте отвечаем None (безопасный отказ), а сам запрос
    дописывает кэш в фоне, и следующая проверка той же сделки идёт быстро."""
    hit = _cache.get(key)
    if hit is not None and hit[0] > _mono():
        return hit[1]

    async def fill():
        try:
            value = await factory()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_filters: сбой запроса товаров")
            value = None
        if len(_cache) > 2000:
            _cache.clear()
        _cache[key] = (_mono() + (ok_ttl if value is not None else fail_ttl), value)
        if _inflight.get(key) is task:
            _inflight.pop(key, None)
        return value

    task = _inflight.get(key)
    if task is None or task.done():
        task = asyncio.ensure_future(fill())
        _inflight[key] = task
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=MS_CALL_TIMEOUT_S + 1)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 (в том числе TimeoutError)
        return None


async def _order_goods(uuid: str) -> list[dict[str, Any]] | None:
    """Товарные позиции заказа покупателя МойСклад: `[{name, article}]`. Услуги (доставка) отброшены. Сбой, обрезанный список: None."""
    async def load():
        data = await _ms_get(f"entity/customerorder/{uuid}/positions", {"expand": "assortment", "limit": 100})
        rows = data.get("rows") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return None
        size = ((data.get("meta") or {}).get("size")) if isinstance(data, dict) else None
        if isinstance(size, int) and size > len(rows):
            return None
        goods = []
        for row in rows:
            assortment = row.get("assortment") if isinstance(row, dict) else None
            assortment = assortment if isinstance(assortment, dict) else {}
            if (assortment.get("meta") or {}).get("type") == "service":
                continue
            article = str(assortment.get("article") or "").strip()
            goods.append({"name": str(assortment.get("name") or ""), "article": article or None})
        return goods

    return await _cached(("order", uuid), load, ok_ttl=ORDER_TTL_S, fail_ttl=ORDER_FAIL_TTL_S)


async def _catalog_article(name: str) -> str | None:
    """Артикул товара каталога по точному названию (без регистра). Не нашли, несколько разных артикулов, сбой: None."""
    wanted = norm(name)

    async def load():
        if ";" in name:
            return ""                                       # `;` разделяет условия фильтра МойСклад
        for path in ("entity/assortment", "entity/product"):
            data = await _ms_get(path, {"filter": f"name={name}", "limit": 10})
            rows = data.get("rows") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                continue                                    # запрос не удался или фильтр не принят: пробуем следующий каталог
            found = {
                str(r.get("article") or "").strip() for r in rows
                if isinstance(r, dict) and norm(r.get("name")) == wanted and (r.get("meta") or {}).get("type") != "service"
            } - {""}
            return next(iter(found)) if len(found) == 1 else ""
        return None

    value = await _cached(("name", wanted), load, ok_ttl=NAME_TTL_S, fail_ttl=NAME_FAIL_TTL_S)
    return value or None


async def deal_items(lead: dict) -> list[Item]:
    """Позиции сделки с артикулами. (1) Есть «ID Заказа» (UUID): позиции заказа МойСклад, так точно. Заказ не прочитался: одна нераспознанная
    позиция. (2) Заказа нет: названия из «Корзина» (запасное «Состав заказа») ищутся в каталоге МойСклад. (3) Не нашлось: артикул None.
    Пустой список значит «товаров нет»."""
    uuid = _field(lead, FIELD_MS_ORDER).strip().lower()
    if _UUID_RE.match(uuid):
        goods = await _order_goods(uuid)
        if goods is None:
            return [Item("заказ МойСклад не прочитан", None)]
        return [Item(g["name"], g["article"]) for g in goods]
    cart, composition = _field(lead, FIELD_CART)[:5000], _field(lead, FIELD_COMPOSITION)[:5000]
    names = parse_cart_items(cart) or parse_cart_items(composition)
    if not names and (cart.strip() or composition.strip()):
        return [Item("корзина не разобрана", None)]         # текст есть, а позиций из него не вышло (формат изменился): не «товаров нет»
    items: list[Item] = []
    unique = list(dict.fromkeys(names))
    for i, name in enumerate(unique):
        article = await _catalog_article(name) if i < PRODUCTS_MAX_LOOKUPS else None
        items.append(Item(name, article))
    return items


async def products_refusal(lead: dict) -> str | None:
    """Правило фильтра по товарам. Берётся, если артикул КАЖДОЙ позиции есть в целевом списке (без регистра). Сделка без товаров берётся при `allow_empty`,
    с нераспознанной позицией при `allow_unknown`. Позиция вне списка отказывает всегда. Любой сбой МойСклад это «нераспознана»."""
    conf = products_config()
    if conf is None:
        return None
    try:
        items = await asyncio.wait_for(deal_items(lead), timeout=PRODUCTS_DEADLINE_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 (в том числе TimeoutError)
        logger.warning("ai_engine_filters: товары сделки %s не определились за срок", lead.get("id"))
        items = [Item("товары не определены", None)]
    if not items:
        return None if conf["allow_empty"] else "в сделке нет товаров, «брать сделки без товаров» выключено"
    outside = [i.article for i in items if i.article is not None and _norm_sku(i.article) not in conf["skus"]]
    if outside:
        return f"в сделке товар вне целевого списка (артикул {', '.join(outside[:2])})"
    if any(i.article is None for i in items) and not conf["allow_unknown"]:
        return "в сделке нераспознанный товар, «брать сделки с нераспознанным товаром» выключено"
    return None


# =============================================================================================
# решение о взятии
# =============================================================================================

@dataclass
class TakeDecision:
    allow: bool
    reason: str = ""                 # причина отказа: одна короткая строка без личных данных
    retry_at: float | None = None    # раньше этого момента решение не пересматриваем (менеджер активен)
    reassign: dict[str, Any] | None = None   # {"to_id", "to_name", "from_id"}: перед взятием назначить ответственным пользователя ИИ
    existing: bool = False           # сделка существующая: после взятия охрана инициативы


def _refuse(reason: str, retry_at: float | None = None) -> TakeDecision:
    return TakeDecision(False, reason, retry_at)


async def decide_take(lead: dict, *, mode_id: str, chat_key: str = "", explicit: bool = False, intercept: bool = False) -> TakeDecision:
    """Решение «ИИ берёт эту сделку (в первый раз)». Вне общего замка: ходит в МойСклад. Ничего не пишет."""
    if not is_configured(mode_id):
        return TakeDecision(True)
    why = cheap_refusal(lead, mode_id=mode_id, explicit=explicit, intercept=intercept)
    if why:
        return _refuse(why)
    if mode_id in FILTER_MODES:
        why = await products_refusal(lead)
        if why:
            return _refuse(why)
    existing = False
    reassign = None
    ex = existing_config() if mode_id in EXISTING_MODES and not intercept else None
    if ex is not None and lead_is_existing(lead, _boundary(ex)):
        existing = True
        if ex["policy"] == "redistribute":
            user = ai_user()
            if user is None:
                return _refuse("«перераспределить»: пользователь ИИ в настройках не задан")
            if settings.review_enabled():
                return _refuse("«перераспределить» при включённом пилоте: смена ответственного без подтверждения человека не выполняется")
            if not REASSIGN_ENABLED:
                return _refuse("«перераспределить»: запись ответственного выключена флагом AI_REASSIGN_ENABLED")
            quiet = ex["manager_quiet_min"]
            if quiet > 0:
                last = manager_last_out(chat_key)
                if last is not None and _now() - last < quiet * 60:
                    return _refuse(f"менеджер писал клиенту менее {quiet} мин назад", retry_at=last + quiet * 60)
            try:
                current = int(lead.get("responsible_user_id") or 0)
            except (TypeError, ValueError):
                current = 0
            if current != user["id"]:
                reassign = {"to_id": user["id"], "to_name": user["name"], "from_id": current or None}
    return TakeDecision(True, existing=existing, reassign=reassign)


async def intercept_refusal(deal: dict, phones: list[Any]) -> str:
    """Для перехвата (`ai_engine_intercept._skip_reason`): причина отказа или пустая строка. Фильтры, бюджет и товары как при обычном взятии,
    `existing` не применяется."""
    mode_id = settings.active_mode_id()
    explicit = settings.contacts_restricted() and settings.contact_allowed(dedup.contact_ids_of(deal), phones)
    try:
        decision = await decide_take(deal, mode_id=mode_id, explicit=explicit, intercept=True)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_filters: сбой проверки перехвата сделки %s, сделку не берём", deal.get("id"))
        return "сбой проверки фильтров"
    return "" if decision.allow else decision.reason


# =============================================================================================
# смена ответственного (`redistribute`)
# =============================================================================================

async def _user_name(user_id: int | None) -> str:
    if not user_id:
        return "без ответственного"
    try:
        name = await asyncio.wait_for(amo_service.get_user_name(user_id), timeout=8)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        name = None
    return name or f"пользователь {user_id}"


async def reassign_responsible(deal_id: int, plan: dict[str, Any]) -> bool:
    """Назначить сделке ответственным пользователя ИИ: PATCH `responsible_user_id`, примечание, строка журнала. True только если amo приняла смену.
    Не бросает. Прежнему ответственному пинг не уходит (задач и сообщений этот код не создаёт)."""
    to_id, to_name, from_id = int(plan["to_id"]), str(plan["to_name"]), plan.get("from_id")
    try:
        result = await asyncio.wait_for(amo_service.patch_lead(int(deal_id), responsible_user_id=to_id), timeout=REASSIGN_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_filters: смена ответственного в сделке %s не вышла", deal_id)
        result = None
    ok = bool(result and result.get("ok"))
    if not ok:
        logger.warning("ai_engine: сделка %s: смена ответственного не удалась (код %s), сделку не беру", deal_id, (result or {}).get("status_code"))
        await asyncio.to_thread(_log_reassign_quiet, deal_id, from_id, to_id, False, "не удалась")
        return False
    from_name = await _user_name(from_id)
    text = f"Ответственный изменён с {from_name} на {to_name} (ИИ, режим Полный)"
    try:
        await asyncio.wait_for(amo_service.add_note(int(deal_id), text), timeout=REASSIGN_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - ответственный уже сменён, это не отказ; журнал и лог остаются
        logger.exception("ai_engine_filters: примечание о смене ответственного в сделке %s не записалось", deal_id)
    await asyncio.to_thread(_log_reassign_quiet, deal_id, from_id, to_id, True, text)
    logger.info("ai_engine: сделка %s: %s", deal_id, text)
    return True


def _log_reassign_quiet(deal_id: int, from_id: int | None, to_id: int, ok: bool, note: str) -> None:
    try:
        log_reassign(deal_id, from_id, to_id, ok, note)
    except Exception:  # noqa: BLE001 - журнал не должен отменять уже сделанную смену
        logger.exception("ai_engine_filters: строка журнала смены ответственного по сделке %s не записалась", deal_id)


def guard_for_given(lead: dict, mode_id: str) -> None:
    """Сделку отдали вручную (`give_deal`): если она «существующая», инициатива по ней ждёт первого сообщения ИИ. Фильтры не применяются."""
    try:
        if existing_for_take(lead, mode_id):
            guard_on_take(int(lead["id"]))
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_filters: не поставил охрану инициативы по переданной сделке %s", lead.get("id"))
