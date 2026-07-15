"""Анти-дубль сделок на создании (leads.add) — две независимые механики.

Контекст (разбор 15-16.07.2026, projects/daily-report/JOURNAL.md): вторые сделки
на одного клиента создают интеграции, которые не смотрят на уже открытые сделки.
Замер за 01.06-15.07: сайт-заказ при открытой консультации — главный живой канал
дублей (Jivo-мост починен 06.07 отдельно, JIVO_APPEND_TO_OPEN_LEAD).

1) «Заказ побеждает консультацию» (ORDER_WINS). Клиент общался (звонок/чат/квиз —
   консультационная сделка без номера заказа) и сам оформил заказ на сайте →
   родилась вторая сделка «Заказ №…». Менеджеры такую пару разгребают руками и
   часто закрывают консультацию «Пропал» (портит статистику ЗИН). Автоматика:
   на появление сайт-заказа находим у контакта более раннюю ОТКРЫТУЮ
   консультационную сделку в CLEVER и:
     - если она в ранних этапах (Неразобранное / буферы НЛ / Новый лид /
       Взят в работу) → закрываем её 143 с причиной «Дубль сделки» + примечание-
       ссылка на заказ; ответственного консультации переносим на заказ (если на
       заказе ещё автоназначение) — менеджер не теряет своего клиента;
     - если она в продвинутых этапах (Оплата запрошена и т.п.) → НЕ закрываем,
       только перекрёстные примечания + тег «возможен дубль» на заказ.

2) Пост-продажный маршрутизатор (POSTSALE). Клиент купил (сделка 142 не старше
   POSTSALE_DAYS), открытых сделок нет, и он пишет в чат → интеграция создаёт
   новую сделку. Это не новый лид, а пост-продажное общение. Автоматика: тег
   «Пост-продажа» + ответственный = менеджер успешной сделки + примечание.
   НЕ закрываем (решает менеджер).

Обе механики за флагами env (по умолчанию ВЫКЛ):
  LEAD_DEDUP_ORDER_WINS_ENABLED, LEAD_DEDUP_POSTSALE_ENABLED.
Обе работают ТОЛЬКО в CLEVER, идемпотентны (in-memory TTL), реконсиляция
дочитыванием (не доверяем полю из вебхука), задержка LEAD_DEDUP_DELAY_S —
интеграция сайта дозаполняет поля сделки в первые секунды после add.
Тест-режим LEAD_DEDUP_TEST_CONTACT_IDS: список id контактов, для которых
механики работают даже при выключенных флагах (обкатка на тест-контакте).
"""

import asyncio
import logging
import os
import time

import amo_service
from waybill_config import PIPELINE_CLEVER

logger = logging.getLogger("uvicorn")

# --- конфиг ----------------------------------------------------------------

F_SITE_ORDER = 577415   # «Номер заказа на сайте» — маркер сайт-заказа
F_ORDER_TYPE = 577671   # «Тип заявки»: Заказ / Предзаказ
F_LOSS = 577623         # «Причина ЗИН»
LOSS_DUP_ENUM = 1041141  # «Дубль сделки»

# Ранние этапы CLEVER: консультацию тут можно закрыть без риска потерять работу.
# Неразобранное, буферы НЛ1-4, Новый лид, Взят в работу.
EARLY_STATUSES = {83537710, 86794354, 86794306, 84215622, 83915186, 83537714, 83537718}

TAG_MAYBE_DUP = "возможен дубль"
TAG_POSTSALE = "Пост-продажа"


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        logger.error("%s: не целое число (%r) — дефолт %s", name, raw, default)
        return default


ORDER_WINS_ENABLED = _flag("LEAD_DEDUP_ORDER_WINS_ENABLED")
POSTSALE_ENABLED = _flag("LEAD_DEDUP_POSTSALE_ENABLED")
DELAY_S = _env_int("LEAD_DEDUP_DELAY_S", 25)
POSTSALE_DAYS = _env_int("LEAD_DEDUP_POSTSALE_DAYS", 30)
SEEN_TTL_S = _env_int("LEAD_DEDUP_SEEN_TTL_S", 3600)


def _test_contact_ids() -> set:
    raw = os.getenv("LEAD_DEDUP_TEST_CONTACT_IDS", "").strip()
    out = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                out.add(int(part))
            except ValueError:
                pass
    return out


TEST_CONTACT_IDS = _test_contact_ids()

logger.info(
    "lead_dedup: init (order_wins=%s, postsale=%s, test_contacts=%s, delay=%ss)",
    ORDER_WINS_ENABLED, POSTSALE_ENABLED, sorted(TEST_CONTACT_IDS), DELAY_S,
)

_bg_tasks: set = set()
_seen: dict[int, float] = {}


def _seen_recently(lead_id: int) -> bool:
    now = time.monotonic()
    for k in [k for k, t in _seen.items() if now - t > SEEN_TTL_S]:
        _seen.pop(k, None)
    return lead_id in _seen


def _mark_seen(lead_id: int) -> None:
    _seen[lead_id] = time.monotonic()


def is_enabled() -> bool:
    return ORDER_WINS_ENABLED or POSTSALE_ENABLED or bool(TEST_CONTACT_IDS)


# --- helpers ---------------------------------------------------------------

def _cf(lead: dict, field_id: int):
    return amo_service.get_custom_field_value(lead or {}, field_id)


def _contact_ids(lead: dict) -> list[int]:
    refs = ((lead or {}).get("_embedded") or {}).get("contacts") or []
    return [int(r["id"]) for r in refs if isinstance(r, dict) and r.get("id")]


def _is_site_order(lead: dict) -> bool:
    v = _cf(lead, F_SITE_ORDER)
    return v is not None and str(v).strip() != ""


def _is_preorder(lead: dict) -> bool:
    v = _cf(lead, F_ORDER_TYPE)
    return isinstance(v, str) and "предзаказ" in v.lower()


async def _contact_leads(contact_id: int) -> list[dict]:
    """Все сделки контакта (полные объекты, с кастом-полями)."""
    import api
    contact = await api.get_contact(contact_id, with_leads=True)
    refs = ((contact or {}).get("_embedded") or {}).get("leads") or []
    ids = [r.get("id") for r in refs if isinstance(r, dict) and r.get("id")]
    if not ids:
        return []
    return await api.get_leads_by_ids(ids)


async def _set_responsible(lead_id: int, user_id: int) -> bool:
    res = await amo_service._do_patch(  # noqa: SLF001 — точечный PATCH, хелпера нет
        f"/api/v4/leads/{lead_id}", {"responsible_user_id": int(user_id)}
    )
    return bool(res.get("ok"))


# --- главная точка входа (из webhooks.py) ----------------------------------

def maybe_process_bg(lead_id, *, source: str = "add") -> None:
    """Вызывается на leads.add. Быстрая: планирует фоновую обработку с задержкой
    (сайт-интеграция дозаполняет 577415 в первые секунды после создания)."""
    if lead_id is None or not is_enabled():
        return
    try:
        lid = int(lead_id)
    except (TypeError, ValueError):
        return
    if _seen_recently(lid):
        return
    _mark_seen(lid)
    task = asyncio.create_task(_process(lid, source))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _process(lead_id: int, source: str) -> None:
    try:
        await asyncio.sleep(DELAY_S)
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            return
        if int(lead.get("pipeline_id") or 0) != int(PIPELINE_CLEVER):
            return
        if int(lead.get("status_id") or 0) in (142, 143):
            return  # уже закрыта (напр. Jivo-триаж) — не наш случай
        cids = _contact_ids(lead)
        if not cids:
            logger.info("lead_dedup: сделка %s без контактов — пропуск", lead_id)
            return

        # тест-режим: обрабатываем только тест-контакты, пока флаги выключены
        test_only_mode = bool(TEST_CONTACT_IDS) and not (ORDER_WINS_ENABLED or POSTSALE_ENABLED)
        is_test_contact = any(c in TEST_CONTACT_IDS for c in cids)
        if test_only_mode and not is_test_contact:
            return
        logger.info(
            "lead_dedup: обрабатываю сделку %s (site_order=%s, test=%s, src=%s)",
            lead_id, _is_site_order(lead), is_test_contact, source,
        )

        siblings: list[dict] = []
        for cid in cids[:3]:
            siblings.extend(await _contact_leads(cid))
        # без себя, только CLEVER, дедуп по id
        seen_ids = set()
        others = []
        for ld in siblings:
            oid = int(ld.get("id") or 0)
            if oid == lead_id or oid in seen_ids:
                continue
            seen_ids.add(oid)
            if int(ld.get("pipeline_id") or 0) == int(PIPELINE_CLEVER):
                others.append(ld)

        if (ORDER_WINS_ENABLED or is_test_contact) and _is_site_order(lead):
            await _order_wins(lead, others)
            return
        if (POSTSALE_ENABLED or is_test_contact) and not _is_site_order(lead):
            await _postsale(lead, others)
    except Exception:
        logger.exception("lead_dedup: ошибка на сделке %s", lead_id)


# --- механика 1: заказ побеждает консультацию -------------------------------

async def _order_wins(order: dict, others: list[dict]) -> None:
    order_id = int(order["id"])
    order_no = _cf(order, F_SITE_ORDER)
    created = int(order.get("created_at") or 0)

    # кандидаты: открытые, созданы раньше заказа, НЕ сайт-заказы, НЕ предзаказы
    cands = [
        ld for ld in others
        if int(ld.get("status_id") or 0) not in (142, 143)
        and int(ld.get("created_at") or 0) < created
        and not _is_site_order(ld)
        and not _is_preorder(ld)
    ]
    if not cands:
        logger.info("lead_dedup[order]: заказ %s — открытых консультаций нет", order_id)
        return

    # самая свежая по работе
    cons = max(cands, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
    cons_id = int(cons["id"])
    cons_status = int(cons.get("status_id") or 0)
    resp = cons.get("responsible_user_id")

    if cons_status in EARLY_STATUSES:
        res = await amo_service.patch_lead(
            cons_id,
            status_id=143,
            pipeline_id=PIPELINE_CLEVER,
            custom_fields={F_LOSS: {"enum_id": LOSS_DUP_ENUM}},
        )
        if res.get("ok"):
            await amo_service.add_note(
                cons_id,
                f"🤖 Автообъединение: клиент оформил Заказ №{order_no} "
                f"(сделка #{order_id}). Эта консультация закрыта как дубль, "
                f"работа продолжается в заказе.",
            )
            note_order = (
                f"🤖 Автообъединение: у клиента была открытая сделка #{cons_id} "
                f"— закрыта как «Дубль сделки», история там."
            )
            if resp:
                moved = await _set_responsible(order_id, int(resp))
                if moved:
                    note_order += " Ответственный перенесён с консультации."
            await amo_service.add_note(order_id, note_order)
            logger.info(
                "lead_dedup[order]: заказ %s ← консультация %s закрыта (Дубль), отв=%s",
                order_id, cons_id, resp,
            )
        else:
            logger.error("lead_dedup[order]: не закрылась консультация %s: %s", cons_id, res)
    else:
        # продвинутый этап — не рискуем, только пометки
        await amo_service.add_tag(order_id, TAG_MAYBE_DUP)
        await amo_service.add_note(
            order_id,
            f"🤖 Внимание: у клиента уже есть открытая сделка #{cons_id} "
            f"в продвинутом этапе. Проверь, не дубль ли этот заказ.",
        )
        await amo_service.add_note(
            cons_id,
            f"🤖 Клиент оформил Заказ №{order_no} (сделка #{order_id}), "
            f"пока эта сделка открыта. Сверь, что это один и тот же заказ.",
        )
        logger.info(
            "lead_dedup[order]: заказ %s — консультация %s в этапе %s, только пометки",
            order_id, cons_id, cons_status,
        )


# --- механика 2: пост-продажный маршрутизатор -------------------------------

async def _postsale(lead: dict, others: list[dict]) -> None:
    lead_id = int(lead["id"])
    now = int(time.time())

    open_others = [ld for ld in others if int(ld.get("status_id") or 0) not in (142, 143)]
    if open_others:
        return  # есть открытые — не пост-продажный случай

    recent_won = [
        ld for ld in others
        if int(ld.get("status_id") or 0) == 142
        and (now - int(ld.get("closed_at") or 0)) <= POSTSALE_DAYS * 86400
    ]
    if not recent_won:
        return

    won = max(recent_won, key=lambda ld: ld.get("closed_at") or 0)
    won_id = int(won["id"])
    resp = won.get("responsible_user_id")

    await amo_service.add_tag(lead_id, TAG_POSTSALE)
    note = (
        f"🤖 Пост-продажа: клиент недавно купил (сделка #{won_id}). "
        f"Это, скорее всего, вопрос по заказу, а не новый лид."
    )
    if resp:
        moved = await _set_responsible(lead_id, int(resp))
        if moved:
            note += " Ответственный выставлен по успешной сделке."
    await amo_service.add_note(lead_id, note)
    logger.info(
        "lead_dedup[postsale]: сделка %s помечена (успешная %s, отв=%s)",
        lead_id, won_id, resp,
    )
