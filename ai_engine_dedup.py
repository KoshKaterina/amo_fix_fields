"""Выбор сделки для ИИ: правила A, C, D (контракт, раздел «Дедуп сделок»).

Решает алгоритм, не модель: у ИИ нет контекста между сделками клиента. Движок ищет открытые сделки
чата и либо выбирает одну, либо отдаёт диалог человеку. Сделки он НЕ закрывает и НЕ объединяет
(63% дублей идут в паре с проданной сделкой, закрытие меняет заказ в МойСклад).

Что считается чем:
  • «подходящая» - открытая сделка воронки текущего режима (тест: «Тест»; прод: «ОП розница», TangemShop);
  • «признак работы» - бюджет больше нуля, заказ (UUID, номер или состав), этап дальше «Взят в работу» в
    розничной воронке, либо сделкой уже владеет человек.

Первая версия, упрощённо (решение Кати 28.10.2026, правило B вынесено):
  • подходящей сделки нет                                              -> `none`, ИИ не входит;
  • одна подходящая и у клиента нет другой открытой с признаком работы -> `ai`;
  • правило A: есть ДРУГАЯ открытая сделка с признаком работы           -> `human`;
  • правило D: подходящих больше одной и признаков работы ни у кого     -> `human`, автовыбора нет
    (в разборе месяца «остаётся старшая» верно только в 64% пар);
  • правило C: то же самое, что A, но замеченное ВО ВРЕМЯ ведения (сделкой уже владеет ИИ) -> `human`,
    ИИ снимается и сделка получает заметку.

⚠️ Молчание amo (`None` из поиска) - это `error`, а не «сделок нет»: иначе сбой сети читался бы как
«можно входить» либо как «клиент никому не нужен».
"""
import logging
from dataclasses import dataclass, field
from typing import Any

import ai_engine_config as cfg
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from shared.phones import digits_only
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER_MAIN,
    STATUS_CLEVER_IN_PROGRESS,
)

logger = logging.getLogger("uvicorn")

FIELD_ORDER_NUMBER = 576697      # «№ Заказа»
FIELD_ORDER_COMPOSITION = 576703  # «Состав заказа»

K_AI = "ai"
K_HUMAN = "human"
K_NONE = "none"
K_SKIP = "skip"        # контакт вне списка режима
K_GONE = "gone"        # сделка, которую ведёт ИИ, закрыта или ушла из воронок режима
K_ERROR = "error"      # amo не ответила


@dataclass
class Resolution:
    kind: str
    deal: dict | None = None
    rule: str = ""                       # "A" | "C" | "D" для kind == human
    others: list[dict] = field(default_factory=list)   # сделки, из-за которых зовём человека
    note: str = ""


def contact_ids_of(lead: dict) -> list[int]:
    out: list[int] = []
    for c in ((lead.get("_embedded") or {}).get("contacts")) or []:
        try:
            out.append(int(c.get("id")))
        except (TypeError, ValueError):
            continue
    return out


def is_open(lead: dict) -> bool:
    try:
        return int(lead.get("status_id") or 0) not in cfg.CLOSED_STATUS_IDS
    except (TypeError, ValueError):
        return False


def has_work_signs(lead: dict) -> bool:
    """Признак работы над сделкой (см. шапку). Чистая функция, кроме чтения владельца в базе."""
    try:
        if float(lead.get("price") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    for fid in (FIELD_MOYSKLAD_ORDER_UUID, FIELD_ORDER_NUMBER, FIELD_ORDER_COMPOSITION):
        if str(amo_service.get_custom_field_value(lead, fid) or "").strip():
            return True
    try:
        pipeline = int(lead.get("pipeline_id") or 0)
        status = int(lead.get("status_id") or 0)
    except (TypeError, ValueError):
        pipeline = status = 0
    if pipeline == int(PIPELINE_CLEVER_MAIN):
        now_sort = amo_service.get_status_sort(status, pipeline)
        base_sort = amo_service.get_status_sort(int(STATUS_CLEVER_IN_PROGRESS), pipeline)
        if now_sort is not None and base_sort is not None and now_sort > base_sort:
            return True
    try:
        owner = store.get_owner(int(lead.get("id")))
    except (TypeError, ValueError):
        owner = None
    return bool(owner and owner["owner"] == store.OWNER_HUMAN)


def _queries(chat_id: str, phone: str) -> list[str]:
    seen: list[str] = []
    for raw in (chat_id, phone):
        value = digits_only(raw) if raw else ""
        # Телеграмный chat_id цифры, телефон - тоже; ищем и по тому, и по другому, но не по пустому.
        if value and value not in seen:
            seen.append(value)
    return seen


async def find_leads(chat_id: str, phone: str) -> list[dict] | None:
    """Все сделки по чату (union по chat_id и телефону). None - amo не ответила хотя бы на один запрос."""
    merged: dict[int, dict] = {}
    for q in _queries(chat_id, phone):
        # По всем страницам: у контакта с историей новая сделка не на первой (find_leads_by_query_all).
        found = await amo_service.find_leads_by_query_all(q, with_=("contacts",))
        if found is None:
            return None
        for lead in found:
            try:
                merged.setdefault(int(lead["id"]), lead)
            except (KeyError, TypeError, ValueError):
                continue
    return list(merged.values())


async def resolve(chat_id: str, phone: str, *, known_deal_id: int | None = None) -> Resolution:
    """Выбор сделки для чата. `known_deal_id` - сделка, которую ИИ уже ведёт: тогда замеченная другая
    сделка с признаком работы называется правилом C, а не A."""
    leads = await find_leads(chat_id, phone)
    if leads is None:
        return Resolution(K_ERROR, note="amoCRM не ответила на поиск сделок")

    allowed = settings.allowed_pipelines()
    open_leads = [ld for ld in leads if is_open(ld)]
    suitable = [ld for ld in open_leads if _pipeline(ld) in allowed]

    if known_deal_id is not None:
        mine = next((ld for ld in suitable if int(ld["id"]) == int(known_deal_id)), None)
        if mine is None:
            return Resolution(K_GONE, note="сделка закрыта или ушла из воронок режима")
        suitable = [mine]

    if not suitable:
        return Resolution(K_NONE, note="подходящих открытых сделок нет")

    phones = [phone, chat_id]
    permitted = [ld for ld in suitable if settings.contact_allowed(contact_ids_of(ld), phones)]
    if not permitted:
        return Resolution(K_SKIP, note="контакта нет в списке режима")
    suitable = permitted

    if len(suitable) > 1:
        # Правило D (никто не в работе) либо A (у кого-то уже есть признаки): в обоих случаях человек.
        signs = [ld for ld in suitable if has_work_signs(ld)]
        return Resolution(K_HUMAN, rule="A" if signs else "D", others=suitable,
                          note="несколько подходящих сделок")

    chosen = suitable[0]
    chosen_id = int(chosen["id"])
    parallel = [ld for ld in open_leads if int(ld["id"]) != chosen_id and has_work_signs(ld)]
    if parallel:
        rule = "C" if known_deal_id is not None else "A"
        return Resolution(K_HUMAN, deal=chosen, rule=rule, others=parallel,
                          note="у клиента есть другая открытая сделка с признаками работы")
    return Resolution(K_AI, deal=chosen)


def _pipeline(lead: dict) -> int:
    try:
        return int(lead.get("pipeline_id") or 0)
    except (TypeError, ValueError):
        return 0


def lead_summary(lead: dict) -> dict[str, Any]:
    """Сжатая сводка для заметки и алерта: без текстов клиента."""
    return {
        "id": int(lead["id"]),
        "pipeline_id": _pipeline(lead),
        "status_id": lead.get("status_id"),
        "price": lead.get("price"),
    }
