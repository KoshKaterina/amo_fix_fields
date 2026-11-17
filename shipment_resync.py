"""Заказ изменили после отгрузки → пересоздать отгрузку (пункт 1 плана Кати, 13.11.2026).

Повод. Отгрузка в МойСкладе списывает товар по составу заказа на момент её создания.
Если после этого менеджер поменял заказ (добавил товар, убрал, сменил количество или
цену), отгрузка остаётся старой: склад списал не то, что уехало к клиенту. Никто об этом
не узнаёт до инвентаризации.

Решение Кати 13.11: пересоздавать отгрузку ВСЕГДА, когда она есть, на любое изменение
заказа в МойСкладе; изменения полей amo не отслеживаем. И писать в чат ОП:
«Заказ №X (ссылка) изменён пользователем Y в воронке Офис, отгрузка пересоздана /
не изменена. @Катя обрати внимание на изменение заказа. Заказ до → после».

Как ловим. Не по событиям, а по СОСТОЯНИЮ: раз в `SHIPMENT_RESYNC_INTERVAL_S` берём заказы
покупателя, изменённые с прошлого прохода, и у тех, где ровно одна отгрузка, сравниваем
товары заказа с товарами отгрузки (товар + количество + цена). Совпали - всё в порядке.
Разошлись - заказ правили после отгрузки. Поэтому проход идемпотентный: после
пересоздания новая отгрузка совпадает с заказом, и следующий проход молчит; карусели нет.
Строки-услуги (доставка) не сравниваем: шаблон МойСклада добавляет в отгрузку служебную
строку доставки, которой в заказе может не быть.

Что делаем при расхождении (только для сделок воронки Офис):
  • одна отгрузка - удалить, создать новую по текущему заказу (`amgroup_shipment.recreate_shipment`),
    переписать поля сделки, уведомить «до → после»;
  • отгрузок несколько - ничего не трогаем, уведомляем: автоматике тут не разобраться;
  • старую удалили, а новая не создалась - уведомляем громко: товар вернулся на склад,
    отгрузку надо создать руками.
Кто правил - последняя запись журнала изменений заказа в МойСкладе (логин сотрудника).

Режим отчёта (`SHIPMENT_RESYNC_DRY_RUN=1`, по умолчанию): только лог «пересоздала бы»,
ничего не удаляет, не создаёт и не пишет в чат. Выключатель - `SHIPMENT_RESYNC_ENABLED=1`.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import pathlib
import time
from collections import Counter

import alerts
import amgroup_shipment
import amo_service
import ms_client
import telegram_bot
import tg_recipients
from waybill_config import FIELD_MOYSKLAD_ORDER_UUID, PIPELINE_OFFICE

logger = logging.getLogger("uvicorn")

SHIPMENT_RESYNC_ENABLED = os.getenv("SHIPMENT_RESYNC_ENABLED", "").strip() == "1"
SHIPMENT_RESYNC_DRY_RUN = os.getenv("SHIPMENT_RESYNC_DRY_RUN", "1").strip() != "0"
# Живое уведомление без действия (Катя 13.11: «живые алерты на всё» на неделю проверки):
# сообщение «до → после» уходит в чат, отгрузка НЕ трогается. Главнее DRY_RUN.
SHIPMENT_RESYNC_NOTIFY_ONLY = os.getenv("SHIPMENT_RESYNC_MODE", "").strip().lower() == "notify"
SHIPMENT_RESYNC_INTERVAL_S = int(os.getenv("SHIPMENT_RESYNC_INTERVAL_S", "300"))
SHIPMENT_RESYNC_OVERLAP_S = int(os.getenv("SHIPMENT_RESYNC_OVERLAP_S", "120"))
SHIPMENT_RESYNC_START_LOOKBACK_S = int(os.getenv("SHIPMENT_RESYNC_START_LOOKBACK_S", "600"))
SHIPMENT_RESYNC_MAX_PER_PASS = int(os.getenv("SHIPMENT_RESYNC_MAX_PER_PASS", "10"))
SHIPMENT_RESYNC_TAG = os.getenv("SHIPMENT_RESYNC_TAG", "@kathrina_bistraya")

_CURSOR_PATH = pathlib.Path(os.getenv("SHIPMENT_RESYNC_CURSOR", "/app/var/shipment_resync_cursor.json"))
EVENT_KEY = "shipment_resync"
_MSK = datetime.timezone(datetime.timedelta(hours=3))
_GOODS = {"product", "variant", "bundle", "consignment"}

_task: asyncio.Task | None = None
_last_run: dict = {}
# Заказы с несколькими отгрузками, о которых уже сказали: ключ - заказ плюс его состав.
# Иначе каждая новая правка такого заказа (смена статуса, комментарий) повторяла бы алерт.
_multi_notified: set[tuple[str, str]] = set()
_changed_notified: set[tuple[str, str]] = set()


# ─────────────────────────────── сравнение ───────────────────────────────


def _assortment(row: dict) -> tuple[str, str, str]:
    """(id, тип, имя) позиции. Имя есть, если позиции запрошены с expand=assortment."""
    a = row.get("assortment") or {}
    meta = a.get("meta") or {}
    href = meta.get("href") or ""
    return href.rsplit("/", 1)[-1].split("?")[0], meta.get("type") or "", a.get("name") or ""


def goods(rows: list[dict]) -> Counter:
    """Товары документа: (id товара, цена в копейках) → количество. Услуги не считаем."""
    c: Counter = Counter()
    for r in rows or []:
        aid, kind, _ = _assortment(r)
        if kind not in _GOODS or not aid:
            continue
        c[(aid, int(round(float(r.get("price") or 0))))] += float(r.get("quantity") or 0)
    return c


def names(*row_lists: list[dict]) -> dict[str, str]:
    out: dict[str, str] = {}
    for rows in row_lists:
        for r in rows or []:
            aid, _, name = _assortment(r)
            if aid and name:
                out[aid] = name
    return out


def _qty(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else f"{x:g}"


def diff_lines(before: Counter, after: Counter, nm: dict[str, str]) -> list[str]:
    """«До → после» по товарам человеческим языком. Цена в копейках → рубли."""
    def by_item(c: Counter) -> dict[str, tuple[float, int]]:
        out: dict[str, tuple[float, int]] = {}
        for (aid, price), q in c.items():
            q0, _ = out.get(aid, (0.0, price))
            out[aid] = (q0 + q, price)
        return out

    b, a = by_item(before), by_item(after)
    lines = []
    for aid in sorted(set(b) | set(a), key=lambda x: nm.get(x, x)):
        name = nm.get(aid, "товар")
        if aid not in a:
            lines.append(f"− {name} ×{_qty(b[aid][0])}")
        elif aid not in b:
            lines.append(f"+ {name} ×{_qty(a[aid][0])}")
        else:
            (qb, pb), (qa, pa) = b[aid], a[aid]
            parts = []
            if qb != qa:
                parts.append(f"×{_qty(qb)} → ×{_qty(qa)}")
            if pb != pa:
                parts.append(f"цена {pb / 100:g} → {pa / 100:g} ₽")
            if parts:
                lines.append(f"{name}: " + ", ".join(parts))
    return lines


# ─────────────────────────────── МойСклад и amo ───────────────────────────────


def _ms_time(ts: int) -> str:
    return datetime.datetime.fromtimestamp(ts, _MSK).strftime("%Y-%m-%d %H:%M:%S")


async def _changed_orders(since_ts: int) -> list[dict] | None:
    """Заказы, изменённые с `since_ts`, с отгрузками. None - склад не ответил."""
    out: list[dict] = []
    offset = 0
    while True:
        data = await ms_client.get("entity/customerorder", params={
            "filter": f"updated>={_ms_time(since_ts)}", "expand": "demands",
            "limit": 100, "offset": offset,
        })
        if data is None:
            return None
        rows = data.get("rows") or []
        out.extend(rows)
        if len(rows) < 100 or offset > 2000:
            return out
        offset += 100


async def _positions(kind: str, doc_id: str) -> list[dict] | None:
    data = await ms_client.get(f"entity/{kind}/{doc_id}/positions",
                               params={"expand": "assortment", "limit": 1000})
    if data is None:
        return None
    return data.get("rows") or []


# Логины МойСклада, под которыми пишут интеграции, а не люди. Правку из карточки amo
# виджет amgroup отправляет в МойСклад под «admin», а настоящего автора оставляет в сделке
# служебным примечанием «Заказ сохранен пользователем <Имя>» (разбор 17.11.2026, заказ 10151).
SHIPMENT_RESYNC_INTEGRATION_LOGINS = {
    s.strip().lower() for s in os.getenv("SHIPMENT_RESYNC_INTEGRATION_LOGINS", "admin").split(",") if s.strip()}
_AMGROUP_SAVED_PREFIX = "заказ сохранен пользователем"
_AMGROUP_NOTE_WINDOW_S = 3600


async def _amgroup_saver(lead_id) -> str | None:
    """Имя из последнего примечания amgroup «Заказ сохранен пользователем …» за последний час."""
    try:
        data = await amo_service._do_get(f"/api/v4/leads/{lead_id}/notes",
                                         [("limit", "50"), ("order[id]", "desc")])
    except Exception:
        logger.exception("Пересоздание отгрузок: примечания сделки %s не прочитались", lead_id)
        return None
    now = int(time.time())
    for n in ((data or {}).get("_embedded") or {}).get("notes") or []:
        text = str((n.get("params") or {}).get("text") or "").strip()
        if not text.casefold().startswith(_AMGROUP_SAVED_PREFIX):
            continue
        if now - int(n.get("created_at") or 0) > _AMGROUP_NOTE_WINDOW_S:
            return None
        return text[len(_AMGROUP_SAVED_PREFIX):].strip() or None
    return None


async def _who_changed(order_id: str, lead: dict | None = None) -> str:
    """Кто правил заказ: логин из журнала МойСклада, а если правка пришла через интеграцию
    (логин admin) - имя из примечания amgroup в сделке."""
    data = await ms_client.get(f"entity/customerorder/{order_id}/audit", params={"limit": 20})
    rows = (data or {}).get("rows") or []
    rows = [r for r in rows if (r.get("eventType") or "") in ("update", "create")] or rows
    rows.sort(key=lambda r: r.get("moment") or "", reverse=True)
    login = ((rows[0].get("uid") if rows else "") or "").split("@")[0]
    if (not login or login.lower() in SHIPMENT_RESYNC_INTEGRATION_LOGINS) and lead and lead.get("id"):
        name = await _amgroup_saver(lead["id"])
        if name:
            return name
    return login or "не определён"


async def _office_lead(order_id: str) -> dict | None:
    """Сделка воронки Офис с этим заказом. Поиск amo полнотекстовый - сверяем поле."""
    leads = await amo_service.find_leads_by_query(order_id, limit=10)
    for lead in leads or []:
        if str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip() != order_id:
            continue
        if int(lead.get("pipeline_id") or 0) == PIPELINE_OFFICE:
            return lead
    return None


def _demands(order: dict) -> list[dict]:
    d = order.get("demands")
    if isinstance(d, dict):
        d = d.get("rows")
    return [x for x in (d or []) if isinstance(x, dict)]


# ─────────────────────────────── уведомление ───────────────────────────────


def build_message(order: dict, lead: dict, who: str, outcome: str, lines: list[str],
                  extra: str | None = None) -> str:
    head = f"🔄 Заказ №{order.get('name') or '?'} изменён пользователем {who} в воронке Офис"
    status = {
        "recreated": "отгрузка пересоздана",
        "would": "отгрузка НЕ пересоздана (режим проверки): склад списал старый состав",
        "multi": "отгрузка НЕ изменена: у заказа несколько отгрузок, поправьте руками",
        "delete_failed": "отгрузка НЕ изменена",
        "create_failed": "⚠️ старая отгрузка удалена, новая НЕ создалась - товар вернулся на склад, "
                         "создайте отгрузку руками",
    }[outcome]
    body = [f"{head}, {status}.", f"{SHIPMENT_RESYNC_TAG} обрати внимание на изменение заказа."]
    if extra:
        body.append(extra)
    if lines:
        body.append("Заказ до → после:")
        body.extend(f"— {x}" for x in lines)
    body.append(alerts.lead_link(lead.get("id")))
    return "\n".join(body)


async def _send(text: str, lead: dict) -> None:
    try:
        d = alerts.decide(EVENT_KEY, legacy_text=text,
                          values={"теги": SHIPMENT_RESYNC_TAG, "ссылка_на_сделку": alerts.lead_link(lead.get("id"))},
                          chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID,
                          parse_mode="HTML", lead=lead)
        if d is not None:
            await telegram_bot.send_alert(d.text, **d.send_kwargs())
    except Exception:
        logger.exception("Пересоздание отгрузок: уведомление по сделке %s не ушло", lead.get("id"))


# ─────────────────────────────── проход ───────────────────────────────


def _load_cursor() -> int | None:
    try:
        return int(json.loads(_CURSOR_PATH.read_text(encoding="utf-8"))["ts"])
    except Exception:
        return None


def _save_cursor(ts: int) -> None:
    try:
        _CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _CURSOR_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps({"ts": int(ts)}), encoding="utf-8")
        tmp.replace(_CURSOR_PATH)
    except Exception:
        logger.exception("Пересоздание отгрузок: курсор не записался")


async def handle_order(order: dict) -> str:
    """Один изменённый заказ. Возвращает исход для лога и тестов."""
    demands = _demands(order)
    if not demands:
        return "no-demand"
    order_id = str(order.get("id") or "")
    order_rows = await _positions("customerorder", order_id)
    if order_rows is None:
        return "ms-silent"
    if len(demands) == 1:
        demand_rows = await _positions("demand", str(demands[0].get("id")))
        if demand_rows is None:
            return "ms-silent"
        before, after = goods(demand_rows), goods(order_rows)
        if before == after:
            return "in-sync"
    else:
        demand_rows, before, after = [], Counter(), goods(order_rows)

    lead = await _office_lead(order_id)
    if lead is None:
        return "no-office-lead"
    lines = diff_lines(before, after, names(demand_rows, order_rows))
    who = await _who_changed(order_id, lead)

    if len(demands) > 1:
        key = (order_id, json.dumps(sorted((list(k), v) for k, v in after.items())))
        if key in _multi_notified:
            return "multi-known"
        if not SHIPMENT_RESYNC_DRY_RUN or SHIPMENT_RESYNC_NOTIFY_ONLY:
            _multi_notified.add(key)
            await _send(build_message(order, lead, who, "multi", []), lead)
        return "multi"

    if SHIPMENT_RESYNC_NOTIFY_ONLY:
        # Один раз на один состав заказа: иначе любая следующая правка заказа (статус,
        # комментарий) при неизменном расхождении повторяла бы сообщение.
        key = (order_id, json.dumps(sorted((list(k), v) for k, v in after.items())))
        if key in _changed_notified:
            return "notify-known"
        _changed_notified.add(key)
        await _send(build_message(order, lead, who, "would", lines), lead)
        logger.info("Пересоздание отгрузок [уведомление]: заказ %s, сделка %s, правил %s. %s",
                    order.get("name"), lead.get("id"), who, "; ".join(lines))
        return "notified"

    if SHIPMENT_RESYNC_DRY_RUN:
        logger.info("Пересоздание отгрузок [отчёт]: заказ %s, сделка %s, правил %s - пересоздала бы. %s",
                    order.get("name"), lead.get("id"), who, "; ".join(lines))
        return "dry"

    res = await amgroup_shipment.recreate_shipment(lead, order_id, str(demands[0].get("id")))
    if res["ok"]:
        outcome, extra = "recreated", (f"Отгрузка № {demands[0].get('name') or '?'} → "
                                       f"№ {res['result']['shipment_number']}.")
    elif res["deleted"]:
        outcome, extra = "create_failed", f"Причина: {res['reason']}"
    else:
        outcome, extra = "delete_failed", f"Причина: {res['reason']}"
    await _send(build_message(order, lead, who, outcome, lines, extra), lead)
    logger.info("Пересоздание отгрузок: заказ %s, сделка %s - %s", order.get("name"), lead.get("id"), outcome)
    return outcome


async def sweep_once() -> dict:
    now = int(time.time())
    cursor = await asyncio.to_thread(_load_cursor)
    if cursor is None:
        cursor = now - SHIPMENT_RESYNC_START_LOOKBACK_S
    orders = await _changed_orders(cursor - SHIPMENT_RESYNC_OVERLAP_S)
    if orders is None:
        _last_run.update(at=now, ok=False, note="МойСклад не ответил")
        return dict(_last_run)
    outcomes: Counter = Counter()
    acted = 0
    for order in orders:
        if not _demands(order):
            continue
        if acted >= SHIPMENT_RESYNC_MAX_PER_PASS:
            outcomes["deferred"] += 1
            continue
        try:
            o = await handle_order(order)
        except Exception:
            logger.exception("Пересоздание отгрузок: заказ %s упал", order.get("name"))
            o = "error"
        outcomes[o] += 1
        if o in ("recreated", "create_failed", "delete_failed", "multi", "dry", "notified"):
            acted += 1
    # Курсор не двигаем, если что-то отложили: следующий проход доберёт.
    if not outcomes.get("deferred") and not outcomes.get("ms-silent"):
        await asyncio.to_thread(_save_cursor, now)
    _last_run.update(at=now, ok=True, orders=len(orders), outcomes=dict(outcomes))
    return dict(_last_run)


# ─────────────────────────────── жизненный цикл ───────────────────────────────


async def _loop() -> None:
    import api
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    while True:
        try:
            if not api.skip_if_congested("Пересоздание отгрузок"):
                await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Пересоздание отгрузок: проход упал")
        await asyncio.sleep(SHIPMENT_RESYNC_INTERVAL_S)


async def init() -> None:
    global _task
    if not SHIPMENT_RESYNC_ENABLED:
        logger.info("Пересоздание отгрузок: выключено (SHIPMENT_RESYNC_ENABLED=0)")
        return
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info("Пересоздание отгрузок: поднято, опрос %ss, %s", SHIPMENT_RESYNC_INTERVAL_S,
                    "только уведомления" if SHIPMENT_RESYNC_NOTIFY_ONLY
                    else "режим отчёта" if SHIPMENT_RESYNC_DRY_RUN else "боевой режим")


async def shutdown() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
        _task = None


def status() -> dict:
    if not SHIPMENT_RESYNC_ENABLED:
        return {"enabled": False}
    return {"enabled": True, "dry_run": SHIPMENT_RESYNC_DRY_RUN, "last_run": dict(_last_run)}
