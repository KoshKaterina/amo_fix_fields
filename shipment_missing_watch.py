"""Сторож «отгрузка не создалась» (пункт 3 плана Кати, 13.11.2026).

Повод. Отгрузку в МойСкладе (списание товара) делают два участника: вендорская
интеграция amgroup и наш дублёр `amgroup_shipment.py`. Если не справились оба, никто об
этом не узнаёт: дублёр только пишет в лог, amgroup молчит. Товар числится на складе,
хотя уехал к клиенту, - вылезает на инвентаризации.

Что делаем. Раз в `SHIPMENT_WATCH_INTERVAL_S` берём сделки, которые за последние
`SHIPMENT_WATCH_LOOKBACK_S` перешли на один из этапов Офиса, где интеграция МойСклада
создаёт отгрузку (настройки amgroup, скрин Кати 13.11.2026):
    Достависта МСК, Доставка наш курьер, Готова накладная, Посылка отгружена,
    В пути, Ожидает в ПВЗ, Успешно реализовано.
Прошло больше `SHIPMENT_WATCH_DELAY_S` (дублёр ждёт amgroup 5 минут, плюс запас), сделка
всё ещё на таком этапе, а «ID Отгрузки» (576691) пуст - пишем в чат ОП, топик
УВЕДОМЛЕНИЯ, с тегом Кати Зубалий (решение Кати 13.11): ссылка на сделку и причина
человеческим языком. Причину берём у дублёра (`amgroup_shipment.last_failure`): нет
заказа, МойСклад не ответил, МойСклад отказал и его текст. Не записана (пересборка
контейнера, этап дублёр не обслуживает) - называем общую.

Одна сделка - одно уведомление (отметка на диске, переживает пересборку контейнера).
Сделку увели с этапа, пока шёл проход, - молчим. Режим отчёта (`SHIPMENT_WATCH_DRY_RUN=1`,
по умолчанию) только пишет в лог и отметок не жжёт. Выключатель - `SHIPMENT_WATCH_ENABLED=1`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import time

import alerts
import amgroup_shipment
import amo_service
import telegram_bot
import tg_recipients
from waybill_config import PIPELINE_OFFICE

logger = logging.getLogger("uvicorn")

SHIPMENT_WATCH_ENABLED = os.getenv("SHIPMENT_WATCH_ENABLED", "").strip() == "1"
SHIPMENT_WATCH_DRY_RUN = os.getenv("SHIPMENT_WATCH_DRY_RUN", "1").strip() != "0"
SHIPMENT_WATCH_INTERVAL_S = int(os.getenv("SHIPMENT_WATCH_INTERVAL_S", "300"))
SHIPMENT_WATCH_DELAY_S = int(os.getenv("SHIPMENT_WATCH_DELAY_S", "900"))
SHIPMENT_WATCH_LOOKBACK_S = int(os.getenv("SHIPMENT_WATCH_LOOKBACK_S", str(6 * 3600)))
SHIPMENT_WATCH_MAX_PER_PASS = int(os.getenv("SHIPMENT_WATCH_MAX_PER_PASS", "10"))
SHIPMENT_WATCH_TAG = os.getenv("SHIPMENT_WATCH_TAG", "@kathrina_bistraya")

FIELD_SHIPMENT_ID = amgroup_shipment.FIELD_SHIPMENT_ID   # «ID Отгрузки»

# Этапы Офиса, на которых интеграция МойСклада создаёт отгрузку.
SHIPMENT_STAGES: dict[int, str] = {
    75426866: "Достависта МСК",
    75426870: "Доставка наш курьер",
    75426874: "Готова накладная",
    75426878: "Посылка отгружена",
    75426882: "В пути",
    75426886: "Ожидает в ПВЗ",
    142: "Успешно реализовано",
}

EVENT_KEY = "shipment_missing"
_STATE_PATH = pathlib.Path(os.getenv("SHIPMENT_WATCH_STATE", "/app/var/shipment_missing_alerted.json"))
_KEEP_S = 30 * 86400

_task: asyncio.Task | None = None
_last_run: dict = {}


# ─────────────────────────────── отметки ───────────────────────────────


def _load() -> dict[str, float]:
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("Сторож отгрузок: отметки не прочитались - начинаем с нуля")
        return {}


def _save(marks: dict[str, float]) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(marks), encoding="utf-8")
        tmp.replace(_STATE_PATH)
    except Exception:
        logger.exception("Сторож отгрузок: отметки не записались")


# ─────────────────────────────── решение ───────────────────────────────


def reason_for(lead: dict) -> str:
    """Причина человеческим языком: записанная дублёром или общая."""
    recorded = amgroup_shipment.human_reason(amgroup_shipment.last_failure(lead.get("id")))
    if recorded:
        return recorded
    order = str(amo_service.get_custom_field_value(lead, amgroup_shipment.FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not order:
        return "к сделке не привязан заказ МойСклада (пустое поле «ID Заказа»), отгружать не по чему"
    if int(lead.get("status_id") or 0) not in amgroup_shipment._TRIGGER_STATUSES:
        return ("интеграция МойСклада не создала отгрузку, а наш дублёр этот этап не обслуживает - "
                "создайте отгрузку в МойСкладе руками")
    return "ни интеграция МойСклада, ни наш дублёр не создали отгрузку - создайте её в МойСкладе руками"


def needs_alert(lead: dict) -> bool:
    """Сделка всё ещё на этапе отгрузки в Офисе, а «ID Отгрузки» пуст."""
    if int(lead.get("pipeline_id") or 0) != PIPELINE_OFFICE:
        return False
    if int(lead.get("status_id") or 0) not in SHIPMENT_STAGES:
        return False
    return not str(amo_service.get_custom_field_value(lead, FIELD_SHIPMENT_ID) or "").strip()


def message(lead: dict, reason: str) -> str:
    stage = SHIPMENT_STAGES.get(int(lead.get("status_id") or 0), "этап отгрузки")
    name = lead.get("name") or "Сделка"
    return (f"⚠️ {SHIPMENT_WATCH_TAG} отгрузка в МойСкладе не создалась\n"
            f"«{name}», Офис / {stage}\n"
            f"Причина: {reason}\n"
            f"{alerts.lead_link(lead.get('id'))}")


# ─────────────────────────────── проход ───────────────────────────────


async def _entered_leads(ts_from: int, ts_to: int) -> set[int]:
    import office_transfer  # ленивый: тянет за собой половину сервиса
    out: set[int] = set()
    for status_id in SHIPMENT_STAGES:
        out |= await office_transfer._entered_status_leads(PIPELINE_OFFICE, status_id, ts_from, ts_to)
    return out


async def _send(text: str, lead: dict) -> bool:
    try:
        d = alerts.decide(EVENT_KEY, legacy_text=text,
                          values={"теги": SHIPMENT_WATCH_TAG, "ссылка_на_сделку": alerts.lead_link(lead.get("id"))},
                          chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID,
                          parse_mode="HTML", lead=lead)
        if d is None:
            return True      # выключено в панели - осознанное «не слать», отметку жжём
        return bool(await telegram_bot.send_alert(d.text, **d.send_kwargs()))
    except Exception:
        logger.exception("Сторож отгрузок: уведомление по сделке %s не ушло", lead.get("id"))
        return False


async def sweep_once() -> dict:
    now = int(time.time())
    ids = await _entered_leads(now - SHIPMENT_WATCH_LOOKBACK_S, now - SHIPMENT_WATCH_DELAY_S)
    marks = await asyncio.to_thread(_load)
    marks = {k: v for k, v in marks.items() if now - v < _KEEP_S}
    fresh = sorted(i for i in ids if str(i) not in marks)
    leads = await amo_service.get_leads_by_ids(fresh) if fresh else []
    found = [lead for lead in leads if needs_alert(lead)]
    sent = 0
    for lead in found[:SHIPMENT_WATCH_MAX_PER_PASS]:
        reason = reason_for(lead)
        text = message(lead, reason)
        if SHIPMENT_WATCH_DRY_RUN:
            logger.info("Сторож отгрузок [отчёт]: написал бы по сделке %s - %s", lead.get("id"), reason)
            continue
        if await _send(text, lead):
            marks[str(lead["id"])] = now
            sent += 1
    if len(found) > SHIPMENT_WATCH_MAX_PER_PASS:
        logger.warning("Сторож отгрузок: без отгрузки %s сделок, за проход пишу о %s, остальные - следующим",
                       len(found), SHIPMENT_WATCH_MAX_PER_PASS)
    if not SHIPMENT_WATCH_DRY_RUN:
        await asyncio.to_thread(_save, marks)
    _last_run.update(at=now, entered=len(ids), checked=len(leads), missing=len(found), sent=sent)
    return dict(_last_run)


# ─────────────────────────────── жизненный цикл ───────────────────────────────


async def _loop() -> None:
    import api
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    while True:
        try:
            if not api.skip_if_congested("Сторож отгрузок"):
                await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Сторож отгрузок: проход упал")
        await asyncio.sleep(SHIPMENT_WATCH_INTERVAL_S)


async def init() -> None:
    global _task
    if not SHIPMENT_WATCH_ENABLED:
        logger.info("Сторож отгрузок: выключен (SHIPMENT_WATCH_ENABLED=0)")
        return
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info("Сторож отгрузок: поднят, опрос %ss, ждём %ss после перехода, %s",
                    SHIPMENT_WATCH_INTERVAL_S, SHIPMENT_WATCH_DELAY_S,
                    "режим отчёта" if SHIPMENT_WATCH_DRY_RUN else "пишет в чат ОП")


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
    if not SHIPMENT_WATCH_ENABLED:
        return {"enabled": False}
    return {"enabled": True, "dry_run": SHIPMENT_WATCH_DRY_RUN, "last_run": dict(_last_run)}
