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

⚠️ Та же путаница на пути ЗАПИСИ дороже, чем на пути чтения: пустой ответ
amoCRM при поиске сделки - не то же самое, что «сделки нет». Молчание amoCRM
(сеть/429/5xx/открытый брейкер) отличаем от честного нуля так же, как и
молчание склада выше - см. amo_service.find_leads_by_query/find_contacts_by_query
и _AmoSearchFailed ниже. Цена ошибки здесь другая: не ложный алерт, а дубль
сделки и дубль контакта у живого клиента.

Состояние (уже залогированные заказы, а также заказы с подтверждённой
сделкой - _known_with_deal) - на диске в /app/var, чтобы пересборка
контейнера не повторяла одну и ту же работу каждые AMGROUP_FALLBACK_INTERVAL_SEC.
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
    AMGROUP_FALLBACK_MIN_AGE_MIN,
    FIELD_MOYSKLAD_ORDER_UUID,
)

logger = logging.getLogger("uvicorn")


class _AmoSearchFailed(Exception):
    """amoCRM не ответил на поиск сделки - вызывающий код обязан прервать
    проход, а не читать сбой как «сделки нет» (см. докстринг модуля)."""


# «№ Заказа» - человекочитаемый номер заказа МойСклад вида «07182», второе
# поле связки сделки с заказом (первое - FIELD_MOYSKLAD_ORDER_UUID = 576689 из
# waybill_config). Определён локально, как и соседний FIELD_MS_ORDER_UUID в
# showroom_store.py - общего реестра полей amoCRM в проекте нет.
FIELD_MOYSKLAD_ORDER_NUMBER = 576697

# Лимит полнотекстового поиска amo при проверке дубля. Больше дефолтных 50:
# короткий номер заказа цепляет много постороннего, без запаса нужная сделка
# может не попасть в первую страницу выдачи (находка приёмки безопасности
# 03.09.2026, тот же приём в amgroup_lead_builder._LEAD_SEARCH_LIMIT).
_LEAD_SEARCH_LIMIT = 250

MSK = datetime.timezone(datetime.timedelta(hours=3))

# Каналы продаж и контрагент, которые в amoCRM не ездят НИКОГДА (см. докстринг
# модуля). Сравнение регистронезависимое.
_EXCLUDED_CHANNEL_NAMES = {"маркетплейс", "tangemshop"}
_EXCLUDED_AGENT_MARKER = "интернет решения"  # ООО «ИНТЕРНЕТ РЕШЕНИЯ» - Озон

# Точка расширения: соседний срез подставит сюда свою async-функцию сборки и
# создания сделки. Пока не подставлена (или включён сухой режим) - модуль
# только считает и логирует, ничего не создавая. Возвращает id сделки (успех,
# новая или уже существующая) или None (неудача) - _handle_missing обязан
# читать этот результат, а не считать любой вызов успешным (см. её докстринг).
create_lead_for_order: Callable[[dict], Awaitable[int | None]] | None = None

_task: asyncio.Task | None = None
_STATE_PATH = os.getenv("AMGROUP_FALLBACK_STATE_PATH", "/app/var/amgroup_fallback_logged.json")
_logged: set[str] = set()
_logged_loaded = False
_LOGGED_CAP = 2000
# Потолок на ОДИН проход и пауза между заказами (03.09.2026, по итогам приёмки
# безопасности). Без них первый боевой проход заводит сделку на каждый заказ
# двухсуточного окна разом - сотня сделок за минуту, менеджеры получают вал,
# а очередь запросов в amoCRM забивается и отодвигает накладные СДЭК и
# распределение лидов. Остаток окна разбирается следующими проходами.
_CREATE_PER_PASS_CAP = int(os.getenv("AMGROUP_FALLBACK_PER_PASS_CAP", "25"))
_CREATE_PAUSE_SEC = float(os.getenv("AMGROUP_FALLBACK_PAUSE_SEC", "1"))

# Отдельная память «по этому заказу сделка уже подтверждена» (не путать с
# _logged выше - тот про заказы БЕЗ сделки). Без неё каждый заказ окна
# (по умолчанию 48ч) перепроверяется в amoCRM на КАЖДОМ проходе (по умолчанию
# раз в 3 мин) пожизненно, включая давно найденные - лишняя нагрузка на amo,
# которая сама способна выбить предохранитель (см. докстринг модуля).
_DEAL_STATE_PATH = os.getenv(
    "AMGROUP_FALLBACK_DEAL_STATE_PATH", "/app/var/amgroup_fallback_has_deal.json",
)
_known_with_deal: set[str] = set()
_deal_loaded = False
_DEAL_CAP = 20000


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


def _load_deal_state() -> None:
    global _deal_loaded
    if _deal_loaded:
        return
    _deal_loaded = True
    try:
        with open(_DEAL_STATE_PATH, encoding="utf-8") as f:
            _known_with_deal.update(str(x) for x in json.load(f))
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("amgroup_fallback: не прочитался %s - начинаем с нуля", _DEAL_STATE_PATH)


def _save_deal_state() -> None:
    try:
        os.makedirs(os.path.dirname(_DEAL_STATE_PATH), exist_ok=True)
        tmp = f"{_DEAL_STATE_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_known_with_deal)[-_DEAL_CAP:], f)
        os.replace(tmp, _DEAL_STATE_PATH)
    except Exception:
        logger.exception("amgroup_fallback: не записался %s", _DEAL_STATE_PATH)


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


def _order_age_min(order: dict, now_utc: datetime.datetime) -> float | None:
    """Возраст заказа в минутах по полю created. МойСклад отдаёт московское
    время без зоны, вид «2026-09-03 17:32:00.138». None - поле пустое или не
    разобралось: такой заказ порог не держит (лучше лишняя проверка в amo, чем
    заказ, который порог прячет вечно)."""
    raw = str(order.get("created") or "").strip()
    if not raw:
        return None
    try:
        created = datetime.datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
    except ValueError:
        return None
    return (now_utc - created).total_seconds() / 60


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
    поля, а не доверяем факту попадания в выдачу.

    Может raise _AmoSearchFailed, если amoCRM не ответил ни на один запрос -
    вызывающий код (check_once) обязан прервать проход, а не читать сбой как
    «сделки нет» - иначе на каждый сбой поиска протез заведёт дубль."""
    candidates = (
        (order_uuid, FIELD_MOYSKLAD_ORDER_UUID),
        (order_number, FIELD_MOYSKLAD_ORDER_NUMBER),
    )
    for value, field_id in candidates:
        if not value:
            continue
        leads = await amo_service.find_leads_by_query(value, limit=_LEAD_SEARCH_LIMIT)
        if leads is None:
            raise _AmoSearchFailed(value)
        for lead in leads:
            found = str(amo_service.get_custom_field_value(lead, field_id) or "").strip()
            if found.casefold() == str(value).strip().casefold():
                return lead
    return None


async def check_once() -> dict:
    """Один проход. Возвращает счётчики - по ним же удобно тестировать.

    ms_answered=False значит «склад не ответил, проход пропущен» - это не то
    же самое, что missing=0 («ответил, разрывов нет»). amo_answered=False -
    тот же смысл, но про amoCRM: поиск сделки сорвался хотя бы на одном
    заказе, проход прерван БЕЗ создания (см. _AmoSearchFailed выше) - вместо
    неполного/сомнительного missing лучше повтор на следующем проходе, чем
    риск дубля. Смешивать все три случая с честным «ничего не найдено»
    нельзя, см. докстринг модуля."""
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
        return {"ms_answered": False, "amo_answered": True, "total": 0, "excluded": 0, "too_young": 0, "with_deal": 0, "missing": 0}

    _load_deal_state()
    excluded = 0
    too_young = 0
    with_deal = 0
    missing: list[dict] = []
    newly_confirmed: list[str] = []
    amo_answered = True
    for order in orders:
        reason = _exclusion_reason(order)
        if reason:
            excluded += 1
            continue
        order_uuid = str(order.get("id") or "")
        order_number = str(order.get("name") or "").strip()
        if not order_uuid:
            continue
        # Порог возраста: живому amgroup хватает секунд, чтобы завести сделку.
        # Заказ моложе порога ещё не «без сделки» - смотрим на следующем
        # проходе, в amo за ним сейчас даже не ходим.
        age = _order_age_min(order, now)
        if age is not None and age < AMGROUP_FALLBACK_MIN_AGE_MIN:
            too_young += 1
            continue
        # Память «сделка уже подтверждена» - не спрашиваем amo повторно про
        # заказ, который на прошлом проходе уже нашёлся (см. докстринг
        # _known_with_deal выше): лишняя нагрузка способна сама выбить
        # предохранитель и обрушить весь проход.
        if order_uuid in _known_with_deal:
            with_deal += 1
            continue
        try:
            lead = await _find_existing_lead(order_uuid, order_number)
        except _AmoSearchFailed:
            logger.warning(
                "amgroup_fallback: amoCRM не ответил при поиске сделки по заказу "
                "%s - проход прерван, чтобы не завести дубль; остаток окна "
                "проверим на следующем проходе",
                order.get("name"),
            )
            amo_answered = False
            break
        if lead is not None:
            with_deal += 1
            newly_confirmed.append(order_uuid)
            continue
        missing.append(order)

    if newly_confirmed:
        _known_with_deal.update(newly_confirmed)
        _save_deal_state()

    if missing and amo_answered:
        await _handle_missing(missing)

    logger.info(
        "amgroup_fallback: заказов МС %s, отсеяно %s, моложе %s мин %s, сделка уже есть %s, без сделки %s%s",
        len(orders), excluded, AMGROUP_FALLBACK_MIN_AGE_MIN, too_young, with_deal, len(missing),
        "" if amo_answered else " (проход прерван сбоем amoCRM, не полный)",
    )
    return {
        "ms_answered": True, "amo_answered": amo_answered, "total": len(orders), "excluded": excluded,
        "too_young": too_young, "with_deal": with_deal, "missing": len(missing),
    }


async def _handle_missing(missing: list[dict]) -> None:
    """Заказы без сделки: логируем (раз на заказ, дедуп на диске) и, если не
    сухой режим и точка расширения подключена, зовём создание сделки.

    Обработанным (_logged) заказ помечаем ТОЛЬКО после подтверждённого
    успеха create_lead_for_order (вернул truthy id) - не после исключения и
    не после тихого None, и НЕ в сухом режиме (там ничего не создавалось).
    Раньше это было не так: заказ попадал в _logged безусловно, включая
    сухой прогон и сбой создания - находка приёмки безопасности 03.09.2026,
    цена бага двойная:
    - сухой режим отравлял состояние - весь разрыв, накопленный за время
      наблюдения, не закрывался при переходе в боевой режим никогда, только
      руками по одному;
    - одна сетевая икота на одном заказе (например склад не ответил на
      карточку заказа - самый частый отказ create_lead_for_order) навсегда
      выводила этот заказ из-под защиты модуля - он существует именно затем,
      чтобы ловить такие заказы, и молча терял ровно те, на которых
      споткнулся."""
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
        logger.info(
            "amgroup_fallback: сухой режим - сделки не создаём, только считаем; "
            "заказы НЕ помечаем обработанными, чтобы боевой режим потом их подхватил"
        )
        return

    if create_lead_for_order is None:
        logger.info(
            "amgroup_fallback: боевой режим включён, но создание сделки делает "
            "соседний срез - точка расширения create_lead_for_order ещё не подключена"
        )
        return

    created: list[str] = []
    batch = fresh[:_CREATE_PER_PASS_CAP]
    if len(fresh) > len(batch):
        logger.warning(
            "amgroup_fallback: за проход берём %s заказов из %s - остальные "
            "разберём следующими проходами, чтобы не завалить менеджеров и "
            "очередь запросов в amoCRM",
            len(batch), len(fresh),
        )
    for idx, order in enumerate(batch):
        if idx:
            await asyncio.sleep(_CREATE_PAUSE_SEC)
        order_id = str(order.get("id"))
        try:
            result = await create_lead_for_order(order)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "amgroup_fallback: не удалось создать сделку по заказу %s - "
                "заказ останется в очереди на следующий проход",
                order.get("name"),
            )
            continue
        if not result:
            logger.warning(
                "amgroup_fallback: create_lead_for_order не подтвердил успех по "
                "заказу %s (вернул пусто) - заказ останется в очереди на "
                "следующий проход",
                order.get("name"),
            )
            continue
        created.append(order_id)

    if created:
        _logged.update(created)
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
