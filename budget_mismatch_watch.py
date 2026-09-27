"""Сторож расхождения бюджета сделки с суммой заказа (постановка Кати 27.09.2026).

Бюджет сделки пишут ДВА источника, и они спорят. Мост amgroup зеркалит сумму заказа
МойСклада, а встроенный механизм amo «Товары» пересчитывает бюджет по списку
привязанных товаров - через секунду после каждой привязки. Мост привязки пересоздаёт,
поэтому за полминуты бюджет успевает смениться четыре-шесть раз, и в сделке остаётся
то, что записали последним.

Пока везло: мост записывал последним, итог выходил верным. Но это гонка, а не защита.
Разбор - `knowledge/budzhet-sdelki-migaet-dva-schetchika.md` в рабочей папке.

Два способа получить неверный бюджет:
  • две строки ОДНОГО товара с разными ценами (акция «второй кошелёк -30%») - в каталоге
    amo товар один элемент с одной ценой, две привязки на него не ложатся, и пересчёт
    недосчитывает вторую строку;
  • цена элемента каталога ОБЩАЯ на всю CRM, мост переписывает её под каждый новый заказ.
    27.09 цена «Tangem 2.0 WHITE (3 Карты)» за час прошла 6 990 -> 5 000 из-за чужого
    заказа. Значит сумма «по товарам» у старой сделки меняется задним числом.

Что делаем: сверяем поле «Бюджет» со строкой «Итого» из поля «Состав заказа» (его пишет
мост по заказу МойСклада, это наш источник правды) и зовём человека, когда они разошлись.
Сам бюджет модуль НЕ правит - только смотрит и сообщает.

────────────────────────── почему опрос, а не вебхук ──────────────────────────
Вебхук приходит на КАЖДУЮ из шести перезаписей бюджета, то есть ровно в те секунды,
когда расхождение законно и временно. Сторож на вебхуке кричал бы на каждую нормальную
сделку. Здесь важно противоположное: дать гонке ЗАКОНЧИТЬСЯ и посмотреть на результат.
Поэтому опрос с отстойником - сделку смотрим, только если её не трогали последние
`BUDGET_WATCH_SETTLE_MIN` минут.

──────────────────────────────── на чём стоит правильность ────────────────────────────
  • отстойник: сделка, изменённая только что, пропускается - это гонка, а не поломка;
  • порог `BUDGET_WATCH_TOLERANCE`: менеджеры округляют бюджет руками (13 500 вместо
    13 483). Замер 27.09 по 1250 сделкам: все 11 расхождений, кроме одного, были такими
    округлениями в пределах 71 рубля. Порог 100 рублей оставляет настоящие случаи;
  • перед алертом сделка ПЕРЕЧИТЫВАЕТСЯ из amo: разошлось на момент выборки, сошлось
    сейчас - молчим («ложная тревога дороже пропущенной»);
  • дедуп на ДИСКЕ (`autopilot_store.claim_notice`), иначе каждая пересборка контейнера
    звала бы заново по всем сделкам окна;
  • ключ дедупа - сделка ПЛЮС пара чисел. Бюджет поменяли, а сошлось не до конца -
    ключ другой, сторож честно позовёт снова. Ключ по одной сделке запретил бы это;
  • в режиме отчёта (`BUDGET_WATCH_ALERT_ENABLED=0`) ключи НЕ жжём - иначе сутки
    обкатки выжгли бы их и фича уехала бы в бой навсегда молчащей.
"""

import asyncio
import logging
import re
import time

import alerts
import amo_service
import autopilot_store as notices
import telegram_bot
from waybill_config import (
    BUDGET_WATCH_ALERT_CHAT_ID,
    BUDGET_WATCH_ALERT_ENABLED,
    BUDGET_WATCH_ENABLED,
    BUDGET_WATCH_INTERVAL_S,
    BUDGET_WATCH_LOOKBACK_MIN,
    BUDGET_WATCH_MAX_PER_PASS,
    BUDGET_WATCH_PIPELINES,
    BUDGET_WATCH_SETTLE_MIN,
    BUDGET_WATCH_TOLERANCE,
    FIELD_ORDER_TOTAL,
)

logger = logging.getLogger("uvicorn")

# Префикс ключа в общей таблице отметок autopilot_notified.
NOTICE_PREFIX = "budget_mismatch"

# Отметки живут 90 дней - как у соседа по таблице, чтобы уборка не съедала чужие.
_PURGE_KEEP_DAYS = 90
_PURGE_EVERY_S = 86400

# «Итого: 25 802.00 рубля» - разделитель тысяч бывает обычным пробелом, неразрывным
# и узким неразрывным, дробная часть отделяется точкой или запятой.
_TOTAL_RE = re.compile(r"Итого:\s*([\d\s  ]+[.,]\d{2})")

_task: asyncio.Task | None = None
# Единственное состояние в памяти, и оно только для статусной ручки.
_last_run: dict = {}
_last_purge_ts = 0


# ─────────────────────────────── чистые функции ───────────────────────────────


def order_total(lead: dict) -> float | None:
    """Строка «Итого» из поля «Состав заказа». Нет поля или нет строки - None.

    Поле пишет мост по заказу МойСклада. Нет его - сделка не из заказа (звонок, чат,
    ручная), сверять не с чем, и это не повод для тревоги.
    """
    raw = amo_service.get_custom_field_value(lead, FIELD_ORDER_TOTAL)
    if not raw:
        return None
    m = _TOTAL_RE.search(str(raw))
    if not m:
        return None
    digits = re.sub(r"[\s  ]", "", m.group(1)).replace(",", ".")
    try:
        return float(digits)
    except ValueError:
        return None


def notice_kind(price: float, total: float) -> str:
    """Ключ идемпотентности: сделка плюс ПАРА чисел, а не одна сделка.

    Бюджет поправили, но не до конца - пара другая, ключ другой, сторож зовёт снова.
    Числа округляем до рубля: копейки в бюджете amo не хранит.
    """
    return f"{NOTICE_PREFIX}:{int(round(price))}:{int(round(total))}"


def decide(lead: dict, now_ts: int) -> tuple[str, float | None, float | None]:
    """Решение по ОДНОЙ сделке из выборки. Без сети и без диска.

    "no-order" - сделка не из заказа · "fresh" - ещё идёт гонка, рано смотреть ·
    "ok" - сходится в пределах порога · "fire" - расхождение, зовём человека.
    """
    total = order_total(lead)
    if total is None:
        return "no-order", None, None

    try:
        updated_at = int(lead.get("updated_at") or 0)
    except (TypeError, ValueError):
        updated_at = 0
    # Отстойник. Сделку, которую только что трогали, не судим: именно в эти секунды
    # мост и пересчёт перебивают друг друга, и любое значение бюджета законно.
    if updated_at and now_ts - updated_at < BUDGET_WATCH_SETTLE_MIN * 60:
        return "fresh", None, total

    try:
        price = float(lead.get("price") or 0)
    except (TypeError, ValueError):
        return "no-order", None, total

    if abs(price - total) < BUDGET_WATCH_TOLERANCE:
        return "ok", price, total
    return "fire", price, total


def fmt_money(value: float) -> str:
    """«25 802 ₽» - неразрывные пробелы, без копеек. Читает человек, не машина."""
    return f"{int(round(value)):,}".replace(",", " ") + " ₽"


# ─────────────────────────────── работа с amo ───────────────────────────────


async def _recent_leads(now_ts: int) -> list[dict]:
    """Сделки всех сторожимых воронок, изменённые за окно просмотра.

    ⚠️ `get_leads_updated_since` возвращает None при СБОЕ выборки (сеть, 429, брейкер).
    Сбой одной воронки не должен молча превращаться в «там всё хорошо», поэтому
    считаем его отдельно и пишем в лог, а сделки остальных воронок разбираем.
    """
    since = now_ts - BUDGET_WATCH_LOOKBACK_MIN * 60
    leads: list[dict] = []
    for pipeline_id in BUDGET_WATCH_PIPELINES:
        batch = await amo_service.get_leads_updated_since(pipeline_id, since, with_=())
        if batch is None:
            logger.warning(
                "Сторож бюджета: воронка %s не прочиталась, её сделки в этот проход "
                "не проверены", pipeline_id,
            )
            continue
        leads.extend(batch)
    return leads


async def _still_mismatched(lead_id: int, price: float, total: float) -> tuple[str, dict | None]:
    """Перечитывание сделки прямо перед тревогой.

    "ok" - расхождение живо · "settled" - пока шёл проход, всё сошлось ·
    "changed" - числа стали другими, судить будем в следующий проход по свежей паре ·
    "silent" - amo не ответил. Во всех случаях кроме "ok" ключ дедупа НЕ жжём.
    """
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("Сторож бюджета: не прочиталась сделка %s", lead_id)
        return "silent", None
    if not lead:
        return "silent", None

    fresh_total = order_total(lead)
    if fresh_total is None:
        return "changed", lead
    try:
        fresh_price = float(lead.get("price") or 0)
    except (TypeError, ValueError):
        return "silent", None

    if abs(fresh_price - fresh_total) < BUDGET_WATCH_TOLERANCE:
        return "settled", lead
    if int(round(fresh_price)) != int(round(price)) or int(round(fresh_total)) != int(round(total)):
        return "changed", lead
    return "ok", lead


# ─────────────────────────────── проход ───────────────────────────────


async def sweep_once() -> dict:
    """Один проход по окну. Возвращает счётчики решений."""
    now_ts = int(time.time())
    leads = await _recent_leads(now_ts)

    decisions: dict[str, int] = {}
    suspects: list[tuple[dict, float, float]] = []
    for lead in leads:
        decision, price, total = decide(lead, now_ts)
        if decision == "fire":
            suspects.append((lead, float(price), float(total)))
            continue
        decisions[decision] = decisions.get(decision, 0) + 1

    found: list[dict] = []
    for lead, price, total in suspects:
        lead_id = int(lead.get("id") or 0)
        if not lead_id:
            continue
        if len(found) >= BUDGET_WATCH_MAX_PER_PASS:
            # Предохранитель: массовая поломка не должна вылиться пачкой сообщений.
            decisions["capped"] = decisions.get("capped", 0) + 1
            continue
        try:
            state, fresh = await _still_mismatched(lead_id, price, total)
            if state != "ok":
                decisions[state] = decisions.get(state, 0) + 1
                continue
            if BUDGET_WATCH_ALERT_ENABLED:
                claimed = await asyncio.to_thread(
                    notices.claim_notice, lead_id, notice_kind(price, total)
                )
                if not claimed:
                    decisions["already"] = decisions.get("already", 0) + 1
                    continue
            else:
                decisions["would-fire"] = decisions.get("would-fire", 0) + 1
            decisions["fire"] = decisions.get("fire", 0) + 1
            found.append({
                "lead_id": lead_id,
                "name": (fresh or lead).get("name") or "",
                "price": price,
                "total": total,
            })
        except Exception:
            logger.exception("Сторож бюджета: ошибка по сделке %s", lead_id)
            decisions["failed"] = decisions.get("failed", 0) + 1

    _last_run.clear()
    _last_run.update({"at": now_ts, "leads": len(leads), "decisions": dict(decisions)})
    logger.info(
        "Сторож бюджета: сделок в окне %s, решения %s%s",
        len(leads), decisions or "{}",
        "" if BUDGET_WATCH_ALERT_ENABLED else " (режим отчёта, в чат не писали)",
    )

    if found and BUDGET_WATCH_ALERT_ENABLED:
        await _notify(found)
    await _purge_if_due(now_ts)
    return decisions


async def report_once() -> dict:
    """Проход без тревог, чем бы ни был выставлен флаг - для обкатки и разбора.
    Ключи дедупа не жжёт."""
    global BUDGET_WATCH_ALERT_ENABLED
    saved = BUDGET_WATCH_ALERT_ENABLED
    BUDGET_WATCH_ALERT_ENABLED = False
    try:
        return await sweep_once()
    finally:
        BUDGET_WATCH_ALERT_ENABLED = saved


async def _purge_if_due(now_ts: int) -> None:
    global _last_purge_ts
    if now_ts - _last_purge_ts < _PURGE_EVERY_S:
        return
    _last_purge_ts = now_ts
    try:
        gone = await asyncio.to_thread(notices.purge_notices_older_than, _PURGE_KEEP_DAYS)
        if gone:
            logger.info("Сторож бюджета: убрано старых отметок %s", gone)
    except Exception:
        logger.exception("Сторож бюджета: уборка отметок не прошла")


# ─────────────────────────────── уведомления ───────────────────────────────


async def _notify(found: list[dict]) -> None:
    """Сообщение человеку. Без ID и без точек посередине - правила Кати."""
    head = (f"💰 Бюджет сделки разошёлся с суммой заказа: {len(found)}"
            if len(found) > 1 else "💰 Бюджет сделки разошёлся с суммой заказа")
    lines = [head]
    for f in found:
        diff = f["total"] - f["price"]
        side = "меньше заказа на" if diff > 0 else "больше заказа на"
        name = (f.get("name") or "").strip()
        title = f"{name}: " if name else ""
        lines.append(
            f"— {title}в сделке {fmt_money(f['price'])}, в заказе {fmt_money(f['total'])} "
            f"({side} {fmt_money(abs(diff))})\n  {alerts.lead_link(f['lead_id'])}"
        )
    lines.append("Верная сумма - из заказа МойСклада, поле «Состав заказа».")
    try:
        await telegram_bot.send_alert(
            "\n".join(lines),
            parse_mode="HTML",
            chat_id=int(BUDGET_WATCH_ALERT_CHAT_ID) if BUDGET_WATCH_ALERT_CHAT_ID else None,
        )
    except Exception:
        logger.exception("Сторож бюджета: не смогла отправить тревогу")


# ─────────────────────────────── жизненный цикл ───────────────────────────────


async def _loop() -> None:
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Сторож бюджета: проход упал")
        try:
            await asyncio.sleep(BUDGET_WATCH_INTERVAL_S)
        except asyncio.CancelledError:
            raise


async def init() -> None:
    global _task
    if not BUDGET_WATCH_ENABLED:
        logger.info("Сторож бюджета: выключен (BUDGET_WATCH_ENABLED=0)")
        return
    # Таблицу отметок поднимаем сами: `autopilot_store.init` зовётся только при
    # AUTOPILOT_ENABLED=1, а фича не должна зависеть от флага соседа.
    await asyncio.to_thread(notices.init)
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info(
            "Сторож бюджета: поднят (опрос %ss, окно %s мин, отстойник %s мин, "
            "порог %s руб, воронок %s, тревоги %s)",
            BUDGET_WATCH_INTERVAL_S, BUDGET_WATCH_LOOKBACK_MIN, BUDGET_WATCH_SETTLE_MIN,
            BUDGET_WATCH_TOLERANCE, len(BUDGET_WATCH_PIPELINES),
            "ВКЛ" if BUDGET_WATCH_ALERT_ENABLED else "выкл (режим отчёта)",
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


def status() -> dict:
    """Для статусной ручки сервиса: видно, что сторож жив и что он решал."""
    if not BUDGET_WATCH_ENABLED:
        return {"enabled": False}
    return {
        "enabled": True,
        "alert": BUDGET_WATCH_ALERT_ENABLED,
        "interval_s": BUDGET_WATCH_INTERVAL_S,
        "lookback_min": BUDGET_WATCH_LOOKBACK_MIN,
        "settle_min": BUDGET_WATCH_SETTLE_MIN,
        "tolerance": BUDGET_WATCH_TOLERANCE,
        "pipelines": list(BUDGET_WATCH_PIPELINES),
        "last_run": dict(_last_run),
    }
