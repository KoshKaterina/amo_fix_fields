"""Склад шоурума: держим склад заказа в соответствии с услугой доставки.

Задача Кати 06.08.2026. Кирилл отгружает из отдельного склада «Sunscrypt Шоурум»
(заведён в МойСкладе 03.08). Правило простое и одностороннее:

    в позициях заказа есть услуга «Самовывоз из Шоурума»  → склад = Шоурум
    во всех остальных случаях                              → склад не трогаем

Правило ОДНОСТОРОННЕЕ: услуга шоурума ставит склад шоурума, обратного хода нет.

⚠️ Возврат Шоурум → Основной УБРАН 10.08.2026 по решению Кати. Изначально
(06.08) шоурум считался служебным складом под самовывоз, поэтому заказ без этой
услуги возвращался на Основной. По факту шоурум - настоящий склад с остатками:
Кирилл заводит на нём заказы с любой доставкой, а откат уводил отгрузку на
Основной. За 06-10.08 так откачено 11 заказов из 13 (последний - сделка
36531597, заказ 06139). Теперь склад, выставленный в МойСкладе руками, остаётся
как есть.

Кроме самого заказа модуль правит поле «Склад заказа» (576723) в сделке amoCRM:
по нему office_transfer решает, куда переносить сделку, и без обновления поля
смена склада для автопереноса невидима.

Работает опросом: раз в SHOWROOM_STORE_POLL_INTERVAL_S берём заказы, изменённые
за последние SHOWROOM_STORE_LOOKBACK_MIN минут. Вебхуков у МойСклада мы не
держим, а окно с запасом перекрывает интервал опроса.
"""

import asyncio
import datetime
import logging

import amo_service
import ms_client
from waybill_config import (
    FIELD_ORDER_WAREHOUSE,
    MS_SERVICE_SHOWROOM_PICKUP_ID,
    MS_STORE_SHOWROOM_ID,
    SHOWROOM_SERVICE_NAME_MARKER,
    SHOWROOM_STORE_ENABLED,
    SHOWROOM_STORE_LOOKBACK_MIN,
    SHOWROOM_STORE_POLL_INTERVAL_S,
    WAREHOUSE_SUNSCRYPT_MAIN,
    WAREHOUSE_SUNSCRYPT_SHOWROOM,
)

logger = logging.getLogger("uvicorn")

FIELD_MS_ORDER_UUID = 576689  # «Заказ МойСклад» — связка сделки с заказом
_task: asyncio.Task | None = None

# счётчики для диагностики (читает /health и ручной прогон)
stats = {"checked": 0, "moved_to_showroom": 0, "moved_back": 0, "lead_updated": 0, "errors": 0}


def _store_id(order: dict) -> str | None:
    href = ((order.get("store") or {}).get("meta") or {}).get("href") or ""
    return href.rsplit("/", 1)[-1].split("?")[0] or None


def has_showroom_service(order: dict) -> bool:
    """Есть ли в позициях заказа услуга «Самовывоз из Шоурума».

    Сверяем по id услуги (надёжно) и запасным ходом по названию — название
    правят руками, id при пересоздании услуги меняется, поэтому оба признака.
    """
    for row in ((order.get("positions") or {}).get("rows") or []):
        assortment = row.get("assortment") or {}
        href = ((assortment.get("meta") or {}).get("href") or "")
        if MS_SERVICE_SHOWROOM_PICKUP_ID and MS_SERVICE_SHOWROOM_PICKUP_ID in href:
            return True
        name = str(assortment.get("name") or "").casefold()
        if SHOWROOM_SERVICE_NAME_MARKER in name:
            return True
    return False


def target_store(order: dict) -> str | None:
    """Каким должен стать склад заказа. None — трогать не надо.

    Только вперёд: услуга шоурума → склад шоурума. Склад без этой услуги
    остаётся тем, который выставили в МойСкладе.
    """
    if not has_showroom_service(order):
        return None
    return None if _store_id(order) == MS_STORE_SHOWROOM_ID else MS_STORE_SHOWROOM_ID


async def _set_order_store(order_id: str, store_id: str) -> bool:
    body = {"store": {"meta": {
        "href": f"https://api.moysklad.ru/api/remap/1.2/entity/store/{store_id}",
        "type": "store",
        "mediaType": "application/json",
    }}}
    res = await ms_client.put(f"entity/customerorder/{order_id}", body)
    return res is not None


async def _sync_lead_field(order_uuid: str, store_id: str) -> None:
    """Проставить сделке «Склад заказа» — по нему office_transfer решает перенос."""
    enum_id = (WAREHOUSE_SUNSCRYPT_SHOWROOM if store_id == MS_STORE_SHOWROOM_ID
               else WAREHOUSE_SUNSCRYPT_MAIN)
    leads = await amo_service.find_leads_by_query(order_uuid)
    if leads is None:
        # Молчание amoCRM - не «сделок нет». Молча ничего не меняем.
        logger.warning("showroom_store: amoCRM не ответила на поиск по заказу, пропускаем")
        return
    for lead in leads:
        if str(amo_service.get_custom_field_value(lead, FIELD_MS_ORDER_UUID) or "").lower() \
                != order_uuid.lower():
            continue  # полнотекстовый поиск цепляет и соседние сделки
        if amo_service.get_custom_field_enum_id(lead, FIELD_ORDER_WAREHOUSE) == enum_id:
            continue
        # select-поле патчим ПРЯМЫМ _do_patch: patch_lead умеет только value,
        # а для выпадающих нужен enum_id, иначе amo вернёт NotSupportedChoice.
        body = {"custom_fields_values": [
            {"field_id": FIELD_ORDER_WAREHOUSE, "values": [{"enum_id": enum_id}]}]}
        res = await amo_service._do_patch(f"/api/v4/leads/{lead['id']}", body)
        if res.get("ok"):
            stats["lead_updated"] += 1
            logger.info("showroom_store: сделке %s проставлен склад %s", lead["id"], enum_id)


async def process_recent(lookback_min: int | None = None) -> dict:
    """Один проход по недавно изменённым заказам. Возвращает счётчики прохода."""
    lookback = lookback_min or SHOWROOM_STORE_LOOKBACK_MIN
    since = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))) \
        - datetime.timedelta(minutes=lookback)
    run = {"checked": 0, "moved_to_showroom": 0, "moved_back": 0}

    offset, limit = 0, 100
    while True:
        data = await ms_client.get("entity/customerorder", params={
            "filter": f"updated>={since.strftime('%Y-%m-%d %H:%M:%S')}",
            "expand": "positions.assortment,store",
            "limit": limit, "offset": offset,
        })
        rows = (data or {}).get("rows") or []
        if not rows:
            break
        for order in rows:
            run["checked"] += 1
            stats["checked"] += 1
            want = target_store(order)
            if not want:
                continue
            order_id = order.get("id")
            if not await _set_order_store(order_id, want):
                stats["errors"] += 1
                logger.warning("showroom_store: не удалось сменить склад заказа %s", order.get("name"))
                continue
            if want == MS_STORE_SHOWROOM_ID:
                run["moved_to_showroom"] += 1
                stats["moved_to_showroom"] += 1
            else:
                run["moved_back"] += 1
                stats["moved_back"] += 1
            logger.info("showroom_store: заказ %s → склад %s", order.get("name"),
                        "Шоурум" if want == MS_STORE_SHOWROOM_ID else "Основной")
            try:
                await _sync_lead_field(order_id, want)
            except Exception:
                stats["errors"] += 1
                logger.exception("showroom_store: сделка по заказу %s не обновлена", order.get("name"))
        if len(rows) < limit:
            break
        offset += limit
    return run


async def _loop() -> None:
    while True:
        try:
            run = await process_recent()
            if run["moved_to_showroom"] or run["moved_back"]:
                logger.info("showroom_store: проверено %s, в шоурум %s, обратно %s",
                            run["checked"], run["moved_to_showroom"], run["moved_back"])
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["errors"] += 1
            logger.exception("showroom_store: проход упал")
        await asyncio.sleep(SHOWROOM_STORE_POLL_INTERVAL_S)


async def init() -> None:
    global _task
    if not SHOWROOM_STORE_ENABLED:
        logger.info("showroom_store: выключен (SHOWROOM_STORE_ENABLED=0)")
        return
    if _task is None or _task.done():
        _task = asyncio.create_task(_loop())
        logger.info("showroom_store: включён, опрос каждые %s с, окно %s мин",
                    SHOWROOM_STORE_POLL_INTERVAL_S, SHOWROOM_STORE_LOOKBACK_MIN)


async def shutdown() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
        _task = None
