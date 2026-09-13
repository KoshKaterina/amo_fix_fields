"""Сторож заказов с сайта: доехал ли заказ WooCommerce до МойСклада.

Повод — 07.08.2026. Заказ №18287 оформили на сайте, а в МойСклад и в amoCRM он
не попал: вебхук «заказ создан» не дошёл, а периодическая сверка в
woocommerce-sklad девять дней возвращала ноль из-за формата даты. Обе линии
защиты молчали, и потерю заметила Катя глазами.

Этот модуль — третья линия, независимая от них обеих. Раз в час сверяет заказы
сайта за сутки со списком заказов покупателя в МойСкладе и пишет в технический
чат Telegram, если чего-то не хватает.

Дописано 13.09.2026 после ложной тревоги по заказу 19003: заказ в МойСкладе БЫЛ,
но встречная интеграция (мост amgroup) затёрла у него атрибут «Номер заказа на
сайте», по которому мы ищем, — см. knowledge/zatiranie-zerkalnyh-atributov-ms.md
в папке знаний. Теперь, прежде чем объявить заказ потерянным, сторож идёт длинным
путём: сделка amo по номеру → UUID заказа МС → сам заказ. Заказ жив, а атрибут
пуст — возвращаем номер на место и сообщаем об этом, а не о «потере». Настоящую
потерю (нет ни сделки, ни заказа) сторож кричит как раньше.

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

import amo_service
import ms_client
import telegram_bot
import alerts
import woo_client
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_SITE_ORDER_NUMBER,
    MS_API_URL,
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

AMO_DOMAIN = "https://new5a2e8ea7b16b4.amocrm.ru"
MS_ORDER_URL = "https://online.moysklad.ru/app/#customerorder/edit?id={}"


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


async def _ms_order_numbers(since_utc) -> set[str] | None:
    """Номера заказов сайта, которые уже есть в МойСкладе (доп. поле «Номер
    заказа на сайте»). Читаем окном по дате создания заказа МС.

    None — МойСклад не ответил: судить некого, проход надо пропустить.
    03.09.2026 сбой сети здесь читался как «в МС пусто», и сторож разом объявил
    потерянными 22 живых заказа (см. предупреждение в ms_client.post)."""
    since_msk = since_utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")
    numbers: set[str] = set()
    offset = 0
    while offset <= 1000:
        data = await ms_client.get("entity/customerorder", params={
            "filter": f"created>={since_msk}", "limit": 100, "offset": offset,
        })
        if data is None:
            return None
        rows = data.get("rows") or []
        for row in rows:
            for attr in row.get("attributes") or []:
                if attr.get("id") == MS_ATTR_ORDER_NUMBER_ID and attr.get("value"):
                    numbers.add(str(attr["value"]).strip())
        if len(rows) < 100:
            break
        offset += 100
    return numbers


async def _put_site_number(order_uuid: str, number: str) -> bool:
    """Вернуть заказу МС затёртый «Номер заказа на сайте». PUT идемпотентен."""
    body = {"attributes": [{
        "meta": {
            "href": f"{MS_API_URL}/entity/customerorder/metadata/attributes/"
                    f"{MS_ATTR_ORDER_NUMBER_ID}",
            "type": "attributemetadata",
            "mediaType": "application/json",
        },
        "value": number,
    }]}
    res = await ms_client.put(f"entity/customerorder/{order_uuid}", body)
    return res is not None


async def _check_via_lead(number: str) -> tuple[str, dict]:
    """Заказ Woo не нашёлся в МС по атрибуту. Прежде чем кричать «потерян»,
    идём длинным путём: сделка amo по номеру (поле «Номер заказа на сайте») →
    UUID заказа МС из сделки → сам заказ.

    Вердикты:
      "lost"     — сделки нет, или она без UUID, или заказ МС не читается, или
                   в заказе стоит ЧУЖОЙ номер: судим по-старому, это тревога;
      "present"  — заказ есть и номер на месте (окна листинга разошлись) — молчим;
      "restored" — заказ жив, атрибут был пуст, номер возвращён;
      "skip"     — amoCRM не ответила: молчание не «сделки нет», не судим
                   до следующего прохода (тот же принцип, что у протеза amgroup).
    """
    leads = await amo_service.find_leads_by_query(number)
    if leads is None:
        logger.warning("Сторож заказов: amoCRM не ответила про №%s — не судим", number)
        return "skip", {}
    for lead in leads:
        site = str(amo_service.get_custom_field_value(
            lead, FIELD_SITE_ORDER_NUMBER) or "").strip()
        if site != number:
            continue  # полнотекстовый поиск цепляет и соседние сделки
        lead_id = lead.get("id")
        order_uuid = str(amo_service.get_custom_field_value(
            lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
        if not order_uuid:
            continue  # сделка без связки с МС — ищем среди остальных найденных
        ms_order = await ms_client.get(f"entity/customerorder/{order_uuid}")
        if ms_order is None:
            # 404 или сбой чтения: заказ не подтверждён. Молчать нельзя — если
            # его удалили, это ровно та потеря, ради которой сторож существует.
            return "lost", {"lead_id": lead_id}
        current = ""
        for attr in ms_order.get("attributes") or []:
            if attr.get("id") == MS_ATTR_ORDER_NUMBER_ID:
                current = str(attr.get("value") or "").strip()
        if current == number:
            logger.info(
                "Сторож заказов: №%s уже в МС (%s) — окна листинга разошлись",
                number, ms_order.get("name"),
            )
            return "present", {}
        if current:
            logger.warning(
                "Сторож заказов: у заказа МС %s чужой номер %r вместо №%s — не трогаю",
                ms_order.get("name"), current, number,
            )
            return "lost", {"lead_id": lead_id}
        if not await _put_site_number(order_uuid, number):
            return "lost", {"lead_id": lead_id}
        return "restored", {
            "uuid": order_uuid,
            "lead_id": lead_id,
            "ms_name": str(ms_order.get("name") or ""),
        }
    return "lost", {}


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
    if in_ms is None:
        logger.warning("Сторож заказов: МойСклад не ответил — проход пропущен")
        return {"woo": len(orders), "ms": -1, "lost": 0, "restored": 0}

    _load_reported()
    fresh_limit = now - datetime.timedelta(minutes=ORDER_WATCHDOG_MIN_AGE_MIN)
    lost = []
    restored = []
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
        verdict, info = await _check_via_lead(number)
        if verdict == "restored":
            restored.append((order, info))
        elif verdict == "lost":
            lost.append(order)
        # "present" и "skip" — молчим; в дедуп не пишем, следующий проход досмотрит

    for order, info in restored:
        await _report_restored(order, info)

    if lost:
        await _report(lost)
        for order in lost:
            _reported.add(str(order.get("id")))
        _save_reported()

    logger.info(
        "Сторож заказов: сайт %s, в МойСкладе %s, потеряно %s, восстановлено %s",
        len(orders), len(in_ms), len(lost), len(restored),
    )
    return {"woo": len(orders), "ms": len(in_ms),
            "lost": len(lost), "restored": len(restored)}


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


async def _report_restored(order: dict, info: dict) -> None:
    """Сообщение про возвращённый номер — это НЕ «заказ потерян», а «заказ жив,
    но кто-то затёр связку». Шлём каждый раз: повторение — сигнал, что
    затиратель ходит регулярно."""
    number = str(order.get("id"))
    lines = [
        f"🩹 Заказ №{number} нашёлся в МойСкладе с затёртым «Номером заказа "
        f"на сайте» — номер возвращён.",
        f"   Заказ МС {info.get('ms_name') or '—'}: {MS_ORDER_URL.format(info.get('uuid'))}",
    ]
    if info.get("lead_id"):
        lines.append(f"   Сделка: {AMO_DOMAIN}/leads/detail/{info['lead_id']}")
    lines.append(
        "Той же записью могли затереться трек-номер, ПВЗ и способ оплаты — "
        "гляньте заказ глазами.")

    d = alerts.decide(
        "order_watchdog_restored", legacy_text="\n".join(lines),
        chat_id=TG_ALLOWED_CHAT_ID, values={"номер": number}, keep_text=True,
    )
    if d is None:
        logger.info("Сторож заказов: №%s восстановлен, сообщение выключено в панели", number)
        return
    ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
    logger.warning(
        "Сторож заказов: №%s — номер возвращён заказу МС %s, сообщение %s",
        number, info.get("ms_name"), "отправлено" if ok else "НЕ отправлено",
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
