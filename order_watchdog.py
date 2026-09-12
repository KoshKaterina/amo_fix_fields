"""Сторож заказов с сайта: доехал ли заказ WooCommerce до МойСклада.

Повод — 07.08.2026. Заказ №18287 оформили на сайте, а в МойСклад и в amoCRM он
не попал: вебхук «заказ создан» не дошёл, а периодическая сверка в
woocommerce-sklad девять дней возвращала ноль из-за формата даты. Обе линии
защиты молчали, и потерю заметила Катя глазами.

Этот модуль — третья линия, независимая от них обеих. Раз в час сверяет заказы
сайта за сутки со списком заказов покупателя в МойСкладе и пишет в технический
чат Telegram, если чего-то не хватает. Ничего не создаёт и не чинит: его дело —
не дать потере остаться незамеченной.

Почему сверяем сами, а не следим за счётчиком сверки: «создано 0, ошибок 0» в
логе неотличимо от «всё доехало» — ровно так поломка и пряталась девять дней.

Дедуп: о каждом заказе пишем ОДИН раз, список уже упомянутых лежит на диске
(/app/var), чтобы пересборка контейнера не запускала рассылку заново.
"""

import asyncio
import datetime
import json
import logging
import os

import ms_client
import telegram_bot
import alerts
import woo_client
from waybill_config import (
    MS_ATTR_ORDER_NUMBER_ID,
    ORDER_WATCHDOG_ENABLED,
    ORDER_WATCHDOG_INTERVAL_S,
    ORDER_WATCHDOG_LOOKBACK_H,
    ORDER_WATCHDOG_MIN_AGE_MIN,
    TG_ALLOWED_CHAT_ID,
    WC_URL,
)

logger = logging.getLogger("uvicorn")

_task: asyncio.Task | None = None
_REPORTED_PATH = os.getenv(
    "ORDER_WATCHDOG_REPORTED_PATH", "/app/var/order_watchdog_reported.json")
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
        logger.exception("Сторож заказов: не прочитался %s — начинаем с нуля", _REPORTED_PATH)


def _save_reported() -> None:
    try:
        os.makedirs(os.path.dirname(_REPORTED_PATH), exist_ok=True)
        tmp = f"{_REPORTED_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_reported)[-_REPORTED_CAP:], f)
        os.replace(tmp, _REPORTED_PATH)
    except Exception:
        logger.exception("Сторож заказов: не записался %s", _REPORTED_PATH)


async def _ms_order_numbers(since_utc) -> set[str]:
    """Номера заказов сайта, которые уже есть в МойСкладе (доп. поле «Номер
    заказа на сайте»). Читаем окном по дате создания заказа МС."""
    since_msk = since_utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")
    numbers: set[str] = set()
    offset = 0
    while offset <= 1000:
        data = await ms_client.get("entity/customerorder", params={
            "filter": f"created>={since_msk}", "limit": 100, "offset": offset,
        })
        rows = (data or {}).get("rows") or []
        for row in rows:
            for attr in row.get("attributes") or []:
                if attr.get("id") == MS_ATTR_ORDER_NUMBER_ID and attr.get("value"):
                    numbers.add(str(attr["value"]).strip())
        if len(rows) < 100:
            break
        offset += 100
    return numbers


def _order_summary(order: dict) -> str:
    created = (order.get("date_created") or "").replace("T", " ")[:16]
    items = ", ".join(li.get("name", "") for li in (order.get("line_items") or []))
    ship = (order.get("shipping_lines") or [{}])[0].get("method_title") or "—"
    return (f'№{order.get("id")} · {created} · {order.get("total")} ₽ · '
            f'{order.get("payment_method_title") or "—"} · {ship}\n   {items}')


async def check_once() -> dict:
    """Один проход. Возвращает счётчики — по ним же удобно тестировать."""
    now = datetime.datetime.now(datetime.timezone.utc)
    since = now - datetime.timedelta(hours=ORDER_WATCHDOG_LOOKBACK_H)

    orders = await woo_client.list_orders_created_since(since)
    in_ms = await _ms_order_numbers(since)

    _load_reported()
    fresh_limit = now - datetime.timedelta(minutes=ORDER_WATCHDOG_MIN_AGE_MIN)
    lost = []
    for order in orders:
        number = str(order.get("id"))
        if number in in_ms or number in _reported:
            continue
        # Заказ моложе порога мог ещё не доехать — это норма, не тревога.
        created_raw = (order.get("date_created_gmt") or "").replace("Z", "")
        try:
            created = datetime.datetime.fromisoformat(created_raw).replace(
                tzinfo=datetime.timezone.utc)
        except ValueError:
            continue  # без даты не судим
        if created > fresh_limit:
            continue
        lost.append(order)

    if lost:
        await _report(lost)
        for order in lost:
            _reported.add(str(order.get("id")))
        _save_reported()

    logger.info(
        "Сторож заказов: сайт %s, в МойСкладе %s, потеряно %s",
        len(orders), len(in_ms), len(lost),
    )
    return {"woo": len(orders), "ms": len(in_ms), "lost": len(lost)}


async def _report(lost: list[dict]) -> None:
    head = ("⚠️ Заказ с сайта не доехал до МойСклада"
            if len(lost) == 1 else
            f"⚠️ Заказы с сайта не доехали до МойСклада: {len(lost)}")
    lines = [head, ""]
    for order in lost[:10]:
        lines.append(_order_summary(order))
        if WC_URL:
            lines.append(f'   {WC_URL}/wp-admin/post.php?post={order.get("id")}&action=edit')
        lines.append("")
    if len(lost) > 10:
        lines.append(f"…и ещё {len(lost) - 10}")
    lines.append("Сделки в amoCRM по таким заказам тоже нет — она создаётся уже из МойСклада.")

    # Список заказов собирает код; панель решает только выключатель и чат (keep_text).
    d = alerts.decide(
        "order_watchdog_digest", legacy_text="\n".join(lines), chat_id=TG_ALLOWED_CHAT_ID,
        values={"сколько_ещё": f"…и ещё {len(lost) - 10}" if len(lost) > 10 else ""},
        keep_text=True,
    )
    if d is None:
        logger.info("Сторож заказов: сводка выключена в панели (%s потерянных)", len(lost))
        return
    ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
    logger.warning(
        "Сторож заказов: %s потерянных, сообщение %s",
        len(lost), "отправлено" if ok else "НЕ отправлено",
    )


async def _loop() -> None:
    # Первый проход — не сразу после старта: даём сервису подняться и не
    # шумим на каждой пересборке контейнера.
    await asyncio.sleep(120)
    while True:
        try:
            await check_once()
        except Exception:
            logger.exception("Сторож заказов: проход не удался")
        await asyncio.sleep(ORDER_WATCHDOG_INTERVAL_S)


async def init() -> None:
    global _task
    if not ORDER_WATCHDOG_ENABLED:
        logger.info("Сторож заказов выключен (ORDER_WATCHDOG_ENABLED=0)")
        return
    if not woo_client.is_configured():
        logger.warning("Сторож заказов: нет ключей WooCommerce — не запускаю")
        return
    _task = asyncio.create_task(_loop())
    logger.info(
        "Сторож заказов запущен: раз в %s мин, окно %s ч",
        ORDER_WATCHDOG_INTERVAL_S // 60, ORDER_WATCHDOG_LOOKBACK_H,
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
