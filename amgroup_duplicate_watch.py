"""Сторож задвоения сделок amgroup <-> протез (amgroup_fallback).

Повод - 02.09.2026: сторонняя интеграция amgroup (МойСклад -> amoCRM) встала,
и мы включаем собственный протез, который заводит сделки вместо неё (см.
amgroup_fallback.py). Опасность: amgroup может ожить в любой момент и начать
заводить сделки поверх наших - по одному заказу склада получится ПАРА сделок,
менеджер работает с одной, а вторая живёт своей жизнью незамеченной.

Этот модуль такие пары ЛОВИТ и зовёт человека. Раз в несколько минут берёт
сделки amoCRM за последние сутки-двое и группирует их по заказу склада:
основной ключ связки - поле «ID Заказа» (UUID МойСклад, FIELD_MOYSKLAD_ORDER_UUID
из waybill_config), запасной - «№ Заказа» (FIELD_ORDER_NUMBER ниже, вида
«07182»). Сделки связываем по ОБЩЕМУ узлу графа, а не парой независимых
словарей: если у одной сделки заполнено только основное поле, а у другой -
только запасное с тем же заказом, пара всё равно должна найтись.

⚠️ Ничего не удаляет, не сливает и не двигает. Только смотрит и зовёт
человека - это жёсткое требование, обойти его нечем.

⚠️ Пустой ответ amoCRM - НЕ то же самое, что «дублей нет». Ровно на этой
путанице 03.09.2026 сгорел order_watchdog (see WORKLOG.md): ms_client.get()
вернул None из-за обрыва сети, вызывающий код прочитал None как пустой
список и объявил 19 живых заказов потерянными - ложный алерт в тех.чат.
Здесь так же: amo_service.get_leads_updated_since() вернёт None при сбое
выборки, и это НЕ читаем как «дублей нет» - молча прерываем проход, пишем в
лог, ничего не шлём.

Дедуп: о каждой найденной паре (по набору id сделок, участвующих в группе)
пишем ОДИН раз, список уже упомянутых лежит на диске (/app/var), чтобы
пересборка контейнера не запускала рассылку заново. Появилась в группе третья
сделка - набор id меняется, и это уже другая ситуация, о ней сообщаем заново.

Свои константы (поле «№ Заказа», список воронок, флаги) держим В ЭТОМ файле -
общего реестра полей и воронок в проекте нет, а над waybill_config.py,
amgroup_fallback.py и amgroup_lead_builder.py параллельно работают другие
агенты. Пометка: свести в общий конфиг на сшивке.
"""

import asyncio
import datetime
import json
import logging
import os

import amo_service
import telegram_bot
from waybill_config import (
    AMGROUP_FALLBACK_TAG,
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER,
    PIPELINE_OFFICE,
)

logger = logging.getLogger("uvicorn")

# «№ Заказа» - человекочитаемый номер заказа МойСклад вида «07182». Запасной
# ключ группировки, когда «ID Заказа» (FIELD_MOYSKLAD_ORDER_UUID) у сделки не
# заполнен. Определено локально по образцу amgroup_fallback.py
# (FIELD_MOYSKLAD_ORDER_NUMBER там же, тот же id 576697) - свести в общий
# конфиг на сшивке.
FIELD_ORDER_NUMBER = 576697

# Воронки, где могут появляться сделки по заказам МойСклада (а значит и их
# дубли): основная воронка отдела продаж и Офис. Список - сшивочный: пока
# amgroup_lead_builder не определил, в какую воронку кладёт сделки протез,
# смотрим обе актуальные воронки-получателя заказов; когда определится -
# свести сюда.
_PIPELINES = (PIPELINE_CLEVER, PIPELINE_OFFICE)

# Тег сделки-протеза (наша) - переиспользуем константу из waybill_config, она
# уже общая для всего контура amgroup_fallback, заводить свою копию смысла нет.
DUP_TAG = AMGROUP_FALLBACK_TAG

AMO_DOMAIN = "https://new5a2e8ea7b16b4.amocrm.ru"

AMGROUP_DUP_WATCH_ENABLED = os.getenv("AMGROUP_DUP_WATCH_ENABLED", "1") == "1"
AMGROUP_DUP_WATCH_INTERVAL_S = int(os.getenv("AMGROUP_DUP_WATCH_INTERVAL_S", "600"))
AMGROUP_DUP_WATCH_LOOKBACK_H = int(os.getenv("AMGROUP_DUP_WATCH_LOOKBACK_H", "48"))

_task: asyncio.Task | None = None
_REPORTED_PATH = os.getenv(
    "AMGROUP_DUP_WATCH_REPORTED_PATH", "/app/var/amgroup_duplicate_watch_reported.json")
_reported: set[str] = set()
_reported_loaded = False
_REPORTED_CAP = 2000

MSK = datetime.timezone(datetime.timedelta(hours=3))


def _load_reported() -> None:
    global _reported_loaded
    if _reported_loaded:
        return
    _reported_loaded = True
    try:
        with open(_REPORTED_PATH, encoding="utf-8") as f:
            _reported.update(str(x) for x in json.load(f))
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("Сторож дублей amgroup: не прочитался %s — начинаем с нуля", _REPORTED_PATH)


def _save_reported() -> None:
    try:
        os.makedirs(os.path.dirname(_REPORTED_PATH), exist_ok=True)
        tmp = f"{_REPORTED_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_reported)[-_REPORTED_CAP:], f)
        os.replace(tmp, _REPORTED_PATH)
    except Exception:
        logger.exception("Сторож дублей amgroup: не записался %s", _REPORTED_PATH)


def _lead_url(lead_id) -> str:
    return f"{AMO_DOMAIN}/leads/detail/{lead_id}"


def _group_duplicates(leads: list[dict]) -> list[list[dict]]:
    """Группирует сделки по заказу склада через union-find по общим значениям
    «ID Заказа» / «№ Заказа». Возвращает только группы из двух и более сделок."""
    n = len(leads)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    seen_at: dict[tuple[str, str], int] = {}  # (kind, значение) -> индекс первой сделки
    has_key = [False] * n
    for i, lead in enumerate(leads):
        for kind, field_id in (("uuid", FIELD_MOYSKLAD_ORDER_UUID), ("num", FIELD_ORDER_NUMBER)):
            value = amo_service.get_custom_field_value(lead, field_id)
            if not value:
                continue
            key = (kind, str(value).strip())
            has_key[i] = True
            if key in seen_at:
                union(i, seen_at[key])
            else:
                seen_at[key] = i

    groups: dict[int, list[dict]] = {}
    for i, lead in enumerate(leads):
        if not has_key[i]:
            continue
        groups.setdefault(find(i), []).append(lead)

    return [sorted(g, key=lambda l: l.get("created_at") or 0) for g in groups.values() if len(g) > 1]


def _order_label(leads: list[dict]) -> str:
    for lead in leads:
        number = amo_service.get_custom_field_value(lead, FIELD_ORDER_NUMBER)
        if number:
            return str(number).strip()
    for lead in leads:
        uid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
        if uid:
            return str(uid).strip()
    return "без номера"


async def _client_names(leads: list[dict]) -> dict[int, str]:
    contact_ids: list[int] = []
    for lead in leads:
        for c in (lead.get("_embedded") or {}).get("contacts") or []:
            cid = c.get("id")
            if cid:
                contact_ids.append(int(cid))
    if not contact_ids:
        return {}
    contacts = await amo_service.get_contacts_by_ids(contact_ids)
    return {cid: (c.get("name") or "").strip() for cid, c in contacts.items()}


def _lead_client_name(lead: dict, contacts: dict[int, str]) -> str:
    for c in (lead.get("_embedded") or {}).get("contacts") or []:
        cid = c.get("id")
        name = contacts.get(int(cid)) if cid else None
        if name:
            return name
    return (lead.get("name") or "").strip() or "имя не указано"


async def _creator_names(leads: list[dict]) -> dict[int, str]:
    names: dict[int, str] = {}
    for lead in leads:
        uid = lead.get("created_by")
        if not uid or uid in names:
            continue
        name = await amo_service.get_user_name(uid)
        names[uid] = name or "неизвестно"
    return names


def _fmt_dt(ts) -> str:
    try:
        dt = datetime.datetime.fromtimestamp(int(ts), MSK)
        return dt.strftime("%d.%m.%Y %H:%M")
    except (TypeError, ValueError, OSError):
        return "дата неизвестна"


def _lead_line(lead: dict, contacts: dict[int, str], creators: dict[int, str]) -> str:
    creator = creators.get(lead.get("created_by")) or "неизвестно"
    created = _fmt_dt(lead.get("created_at"))
    if amo_service.has_tag(lead, DUP_TAG):
        mark = f"тег «{DUP_TAG}» есть, это наша сделка-протез"
    else:
        mark = f"тега «{DUP_TAG}» нет, похоже, эту завёл оживший amgroup"
    return f"{_lead_url(lead.get('id'))}\nсоздал {creator}, {created}, {mark}"


async def check_once() -> dict | None:
    """Один проход. Возвращает счётчики, а при сбое выборки — None (см.
    докстринг модуля: пустой ответ amoCRM никогда не читаем как «дублей нет»)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    since_ts = int((now - datetime.timedelta(hours=AMGROUP_DUP_WATCH_LOOKBACK_H)).timestamp())

    all_leads: list[dict] = []
    for pipeline_id in _PIPELINES:
        batch = await amo_service.get_leads_updated_since(pipeline_id, since_ts, with_=("contacts",))
        if batch is None:
            logger.warning(
                "Сторож дублей amgroup: amoCRM не ответил (воронка %s) — проход пропущен, молчим",
                pipeline_id,
            )
            return None
        all_leads.extend(batch)

    dup_groups = _group_duplicates(all_leads)

    _load_reported()
    fresh: list[tuple[list[dict], str]] = []
    for group in dup_groups:
        lead_ids = sorted(int(l.get("id")) for l in group)
        dedup_key = "dup:" + "-".join(str(i) for i in lead_ids)
        if dedup_key in _reported:
            continue
        fresh.append((group, dedup_key))

    if fresh:
        await _report(fresh)
        for _, dedup_key in fresh:
            _reported.add(dedup_key)
        _save_reported()

    logger.info(
        "Сторож дублей amgroup: сделок %s, групп-дублей %s, новых пар %s",
        len(all_leads), len(dup_groups), len(fresh),
    )
    return {"leads": len(all_leads), "groups": len(dup_groups), "new": len(fresh)}


async def _report(fresh: list[tuple[list[dict], str]]) -> None:
    flat = [lead for group, _ in fresh for lead in group]
    contacts = await _client_names(flat)
    creators = await _creator_names(flat)

    head = ("Похоже, задвоилась сделка по заказу, возможно, ожил amgroup поверх нашего протеза."
            if len(fresh) == 1 else
            f"Похоже, задвоились сделки по {len(fresh)} заказам, возможно, ожил amgroup поверх нашего протеза.")
    lines = [head, ""]
    for group, _ in fresh:
        order_label = _order_label(group)
        client = _lead_client_name(group[0], contacts)
        lines.append(f"Заказ {order_label}, клиент {client}")
        for lead in group:
            lines.append(_lead_line(lead, contacts, creators))
        lines.append("")
    lines.append("Сами мы ничего не удаляли и не двигали, только посмотрели, решите, что делать, и закройте лишнюю сделку руками.")

    ok = await telegram_bot.send_alert("\n".join(lines).strip())
    logger.warning(
        "Сторож дублей amgroup: %s пар(ы), сообщение %s",
        len(fresh), "отправлено" if ok else "НЕ отправлено",
    )


async def _loop() -> None:
    # Первый проход не сразу после старта: даём сервису подняться и не шумим
    # на каждой пересборке контейнера.
    await asyncio.sleep(120)
    while True:
        try:
            await check_once()
        except Exception:
            logger.exception("Сторож дублей amgroup: проход не удался")
        await asyncio.sleep(AMGROUP_DUP_WATCH_INTERVAL_S)


async def init() -> None:
    global _task
    if not AMGROUP_DUP_WATCH_ENABLED:
        logger.info("Сторож дублей amgroup выключен (AMGROUP_DUP_WATCH_ENABLED=0)")
        return
    _task = asyncio.create_task(_loop())
    logger.info(
        "Сторож дублей amgroup запущен: раз в %s мин, окно %s ч",
        AMGROUP_DUP_WATCH_INTERVAL_S // 60, AMGROUP_DUP_WATCH_LOOKBACK_H,
    )


async def shutdown() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
        _task = None
