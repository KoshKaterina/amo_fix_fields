"""Протез сторонней интеграции amgroup (МойСклад -> amoCRM).

Повод - 02.09.2026 около 19:30: у amgroup истёк сертификат *.amgbp.ru, потом
лёг сам сервер, и новые заказы МойСклада перестали заводить сделки в amoCRM.
03.09 разобрали тройной сверкой сайт -> склад -> амо: 9 разрывов из 25 заказов
- склад в порядке, сделки нет (WORKLOG.md, запись 2026-09-03). Пока чужой мост
не починен, этот модуль сам находит такие заказы и заводит сделку вместо него.

Это СКЕЛЕТ. Модуль умеет: спросить у МойСклада заказы за окно, честно отличить
«склад не ответил» от «заказов нет», отсеять то, что в amoCRM не ездит никогда
(Озон, TangemShop), проверить по обоим полям связки, есть ли уже сделка, и
залогировать список заказов без сделки. Саму сборку полей сделки и создание
делает соседний срез - см. точку расширения create_lead_for_order ниже.

Отсеивание проверено фактами 03.09.2026 (пятнадцать заказов разобраны глазами,
WORKLOG.md): канал продаж «Маркетплейс» и контрагент ООО «ИНТЕРНЕТ РЕШЕНИЯ» -
это Озон, канал «TangemShop» - другой магазин на InSales (дыра MAG-170,
известна с 11.07.2025). Ни один из трёх в amoCRM не едет вообще, независимо от
состояния amgroup.

⚠️ Пустой ответ МойСклада - не то же самое, что «заказов нет». Именно на этой
путанице 03.09.2026 сгорел order_watchdog: ms_client.get() вернул None из-за
обрыва сети, вызывающий код прочитал None как пустой список и объявил 19
живых заказов потерянными (ложный алерт в тех.чат). Здесь склад не ответил -
проход прерывается и это видно в логе, ничего не считаем потерянным и не
создаём.

Состояние (уже залогированные заказы) - на диске в /app/var, чтобы пересборка
контейнера не повторяла один и тот же список каждые AMGROUP_FALLBACK_INTERVAL_SEC.
"""

import asyncio
import datetime
import json
import logging
import os
from typing import Awaitable, Callable

import amo_service
import ms_client
from waybill_config import (
    AMGROUP_FALLBACK_DRY_RUN,
    AMGROUP_FALLBACK_ENABLED,
    AMGROUP_FALLBACK_INTERVAL_SEC,
    AMGROUP_FALLBACK_LOOKBACK_HOURS,
    FIELD_MOYSKLAD_ORDER_UUID,
)

logger = logging.getLogger("uvicorn")

# «№ Заказа» - человекочитаемый номер заказа МойСклад вида «07182», второе
# поле связки сделки с заказом (первое - FIELD_MOYSKLAD_ORDER_UUID = 576689 из
# waybill_config). Определён локально, как и соседний FIELD_MS_ORDER_UUID в
# showroom_store.py - общего реестра полей amoCRM в проекте нет.
FIELD_MOYSKLAD_ORDER_NUMBER = 576697

MSK = datetime.timezone(datetime.timedelta(hours=3))

# Каналы продаж и контрагент, которые в amoCRM не ездят НИКОГДА (см. докстринг
# модуля). Сравнение регистронезависимое.
_EXCLUDED_CHANNEL_NAMES = {"маркетплейс", "tangemshop"}
_EXCLUDED_AGENT_MARKER = "интернет решения"  # ООО «ИНТЕРНЕТ РЕШЕНИЯ» - Озон

# Точка расширения: соседний срез подставит сюда свою async-функцию сборки и
# создания сделки. Пока не подставлена (или включён сухой режим) - модуль
# только считает и логирует, ничего не создавая.
create_lead_for_order: Callable[[dict], Awaitable[None]] | None = None

_task: asyncio.Task | None = None
_STATE_PATH = os.getenv("AMGROUP_FALLBACK_STATE_PATH", "/app/var/amgroup_fallback_logged.json")
_logged: set[str] = set()
_logged_loaded = False
_LOGGED_CAP = 2000


def _load_state() -> None:
    global _logged_loaded
    if _logged_loaded:
        return
    _logged_loaded = True
    try:
        with open(_STATE_PATH, encoding="utf-8") as f:
            _logged.update(str(x) for x in json.load(f))
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("amgroup_fallback: не прочитался %s - начинаем с нуля", _STATE_PATH)


def _save_state() -> None:
    try:
        os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
        tmp = f"{_STATE_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_logged)[-_LOGGED_CAP:], f)
        os.replace(tmp, _STATE_PATH)
    except Exception:
        logger.exception("amgroup_fallback: не записался %s", _STATE_PATH)


async def _fetch_orders(since_utc: datetime.datetime) -> list[dict] | None:
    """Заказы покупателя МойСклад, созданные после since_utc.

    Возвращает None, если склад НЕ ОТВЕТИЛ (таймаут, обрыв соединения, код
    ошибки) - вызывающий код обязан отличать это от честного пустого списка
    и никогда не читать None как «заказов нет» (см. докстринг модуля)."""
    since_msk = since_utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")
    orders: list[dict] = []
    offset, limit = 0, 100
    while True:
        data = await ms_client.get("entity/customerorder", params={
            "filter": f"created>={since_msk}",
            "expand": "agent,salesChannel",
            "limit": limit, "offset": offset,
        })
        if data is None:
            return None
        rows = data.get("rows") or []
        orders.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
        if offset > 5000:
            logger.warning("amgroup_fallback: обрыв пагинации на offset=%s", offset)
            break
    return orders


def _exclusion_reason(order: dict) -> str | None:
    """Причина исключить заказ из проверки, или None - заказ идём проверять
    дальше. Названия канала/контрагента берём из expand в _fetch_orders."""
    channel_name = str((order.get("salesChannel") or {}).get("name") or "").strip()
    if channel_name.casefold() in _EXCLUDED_CHANNEL_NAMES:
        return f"канал продаж «{channel_name}»"
    agent_name = str((order.get("agent") or {}).get("name") or "").strip()
    if _EXCLUDED_AGENT_MARKER in agent_name.casefold():
        return f"контрагент «{agent_name}»"
    return None


async def _find_existing_lead(order_uuid: str, order_number: str) -> dict | None:
    """Есть ли уже сделка по этому заказу. Ищем по обоим полям связки: «ID
    Заказа» (UUID, 576689) и «№ Заказа» (человекочитаемый номер, 576697) -
    amgroup мог успеть заполнить любое из них до своей поломки. Полнотекстовый
    поиск amo цепляет и соседние сделки (тот же приём, что в showroom_store.py
    и metrika_sync.py), поэтому найденное всегда сверяем со значением самого
    поля, а не доверяем факту попадания в выдачу."""
    candidates = (
        (order_uuid, FIELD_MOYSKLAD_ORDER_UUID),
        (order_number, FIELD_MOYSKLAD_ORDER_NUMBER),
    )
    for value, field_id in candidates:
        if not value:
            continue
        for lead in await amo_service.find_leads_by_query(value):
            found = str(amo_service.get_custom_field_value(lead, field_id) or "").strip()
            if found.casefold() == str(value).strip().casefold():
                return lead
    return None


async def check_once() -> dict:
    """Один проход. Возвращает счётчики - по ним же удобно тестировать.

    ms_answered=False значит «склад не ответил, проход пропущен» - это не то
    же самое, что missing=0 («ответил, разрывов нет»). Смешивать эти два
    случая нельзя, см. докстринг модуля."""
    now = datetime.datetime.now(datetime.timezone.utc)
    since = now - datetime.timedelta(hours=AMGROUP_FALLBACK_LOOKBACK_HOURS)

    orders = await _fetch_orders(since)
    if orders is None:
        logger.warning(
            "amgroup_fallback: МойСклад не ответил за окно %s ч - проход "
            "пропущен, ничего не считаем и не создаём (пустой ответ никогда "
            "не значит «заказов нет»)",
            AMGROUP_FALLBACK_LOOKBACK_HOURS,
        )
        return {"ms_answered": False, "total": 0, "excluded": 0, "with_deal": 0, "missing": 0}

    excluded = 0
    with_deal = 0
    missing: list[dict] = []
    for order in orders:
        reason = _exclusion_reason(order)
        if reason:
            excluded += 1
            continue
        order_uuid = str(order.get("id") or "")
        order_number = str(order.get("name") or "").strip()
        if not order_uuid:
            continue
        lead = await _find_existing_lead(order_uuid, order_number)
        if lead is not None:
            with_deal += 1
            continue
        missing.append(order)

    if missing:
        await _handle_missing(missing)

    logger.info(
        "amgroup_fallback: заказов МС %s, отсеяно %s, сделка уже есть %s, без сделки %s",
        len(orders), excluded, with_deal, len(missing),
    )
    return {
        "ms_answered": True, "total": len(orders), "excluded": excluded,
        "with_deal": with_deal, "missing": len(missing),
    }


async def _handle_missing(missing: list[dict]) -> None:
    """Заказы без сделки: логируем (раз на заказ, дедуп на диске) и, если не
    сухой режим и точка расширения подключена, зовём создание сделки."""
    _load_state()
    fresh = [o for o in missing if str(o.get("id")) not in _logged]
    if not fresh:
        return

    mode = "сухой режим" if AMGROUP_FALLBACK_DRY_RUN else "боевой режим"
    shown = ", ".join(f'{o.get("name")} ({o.get("id")})' for o in fresh[:20])
    tail = "" if len(fresh) <= 20 else f", …и ещё {len(fresh) - 20}"
    logger.warning(
        "amgroup_fallback [%s]: заказов МойСклада без сделки в amoCRM - %s: %s%s",
        mode, len(fresh), shown, tail,
    )

    if AMGROUP_FALLBACK_DRY_RUN:
        logger.info("amgroup_fallback: сухой режим - сделки не создаём, только считаем")
    elif create_lead_for_order is None:
        logger.info(
            "amgroup_fallback: боевой режим включён, но создание сделки делает "
            "соседний срез - точка расширения create_lead_for_order ещё не подключена"
        )
    else:
        for order in fresh:
            try:
                await create_lead_for_order(order)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "amgroup_fallback: не удалось создать сделку по заказу %s",
                    order.get("name"),
                )

    for order in fresh:
        _logged.add(str(order.get("id")))
    _save_state()


async def _loop() -> None:
    # Первый проход - не сразу после старта: даём сервису подняться.
    await asyncio.sleep(30)
    while True:
        try:
            await check_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("amgroup_fallback: проход не удался")
        await asyncio.sleep(AMGROUP_FALLBACK_INTERVAL_SEC)


async def init() -> None:
    global _task
    if not AMGROUP_FALLBACK_ENABLED:
        logger.info("amgroup_fallback выключен (AMGROUP_FALLBACK_ENABLED=0)")
        return
    _task = asyncio.create_task(_loop())
    logger.info(
        "amgroup_fallback запущен: раз в %s с, окно %s ч, режим %s",
        AMGROUP_FALLBACK_INTERVAL_SEC, AMGROUP_FALLBACK_LOOKBACK_HOURS,
        "сухой" if AMGROUP_FALLBACK_DRY_RUN else "боевой",
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
