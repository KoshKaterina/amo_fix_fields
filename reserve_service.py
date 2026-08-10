"""Резерв товара в МойСклад по статусам сделки — перенос функции с amGroup (04.08.2026).

Раньше резерв/снятие резерва по статусам сделки ставил виджет amGroup, но он
не умеет «сделка простояла N дней без оплаты — снять резерв самостоятельно»,
из-за чего товар мог простаивать под зависшей сделкой. amGroup НЕ отключаем
полностью (создание заказа МС на сайте, синк состава/сумм и т.п. остаются за
ним) — переносим только функцию резерва.

Механизм резерва в МойСклад — НЕ отдельный флаг на документе, а количество на
позиции заказа (`position.reserve`, у услуг/доставки этого поля нет). Чекбокс
«Резерв» в интерфейсе МС — просто UI-обёртка, массово проставляющая это поле.
Проверено живым тестом на заказе 05950 (04.08.2026, с разрешения Тианы):
PUT entity/customerorder/{uuid}/positions/{id} {"reserve": qty} реально
резервирует склад — подтверждено независимым отчётом
report/stock/all/current?stockType=reserve; {"reserve": 0} снимает.

Работаем ТОЛЬКО со сделками, где заполнено FIELD_MOYSKLAD_ORDER_UUID — заказ
МС должен уже существовать (создан amGroup/виджетом сайта). Свой заказ не
создаём.

Требует уже инициализированный ms_client: в lifespan (webhooks.py) ms_client.init()
стоит строкой выше reserve_service.init() — клиент один на процесс, повторно
его не поднимаем.

PIPELINE_TEST («Тест», 8642414) — песочница для живого сквозного теста этого
механизма (см. JOURNAL.md/PR): резерв ставим на «В работе», снимаем на ЗНР.
Не пересекается ни с одной другой автоматикой проекта — все прочие модули
(office_transfer/ozon_invoice/waybill_service) гейтятся на другие pipeline_id.

Мастер-флаг RESERVE_SERVICE_ENABLED — рубильник на случай, если сервис начнёт
спорить с виджетом amGroup за те же строки: выключается одной переменной
в .env без выкатки кода. Выключенный сервис НИЧЕГО не пишет в МойСклад и не
гоняет фоновый цикл тайм-аута; уже стоящие резервы остаются как есть.
"""

import asyncio
import datetime
import logging

import amo_service
import ms_client
import reserve_store
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OFFICE,
    PIPELINE_TANGEMSHOP,
    PIPELINE_TEST,
    RESERVE_SERVICE_ENABLED,
    RESERVE_TIMEOUT_DAYS,
    RESERVE_TIMEOUT_POLL_INTERVAL_S,
    STATUS_CLEVER_IN_PROGRESS,
    STATUS_CLEVER_NEW_LEAD,
    STATUS_CLEVER_OFFICE_RECORD,
    STATUS_CLEVER_PRECLOSED,
    STATUS_CLEVER_QUALIFIED,
    STATUS_CLEVER_TERMS_AGREED,
    STATUS_CLEVER_UPSELL_DONE,
    STATUS_CLEVER_WALLET_PICKED,
    STATUS_CLOSED_LOST,
    STATUS_LINK_SENT,
    STATUS_OFFICE_AWAITING_PICKUP,
    STATUS_OFFICE_COURIER_MSK,
    STATUS_OFFICE_COURIER_OWN,
    STATUS_OFFICE_DEFERRED_RESERVE,
    STATUS_OFFICE_IN_TRANSIT,
    STATUS_OFFICE_PREORDER_PAID,
    STATUS_OFFICE_SHIPPED,
    STATUS_PAYMENT_RECEIVED,
    STATUS_PAYMENT_REQUESTED,
    STATUS_SUCCESS,
    STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED,
    STATUS_TANGEM_IN_PROGRESS,
    STATUS_TANGEM_NEW_ORDER,
    STATUS_TANGEM_UPSELL_DONE,
    STATUS_TEST_IN_PROGRESS,
    STATUS_WAYBILL_READY,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
_timeout_task: asyncio.Task | None = None

# Статусы, где резерв ставим/держим. Успешно реализовано (142) в Основной и
# TangemShop входит сюда — резерв ДЕРЖИМ, не снимаем (сделка едет в Офис,
# жизненный цикл ещё не закончен).
_RESERVE_ON: dict[int, set[int]] = {
    PIPELINE_CLEVER_MAIN: {
        STATUS_CLEVER_NEW_LEAD,
        STATUS_CLEVER_IN_PROGRESS,
        STATUS_CLEVER_OFFICE_RECORD,
        STATUS_CLEVER_QUALIFIED,
        STATUS_CLEVER_WALLET_PICKED,
        STATUS_CLEVER_UPSELL_DONE,
        STATUS_CLEVER_TERMS_AGREED,
        STATUS_PAYMENT_REQUESTED,
        STATUS_LINK_SENT,
        STATUS_PAYMENT_RECEIVED,
        STATUS_SUCCESS,
    },
    PIPELINE_TANGEMSHOP: {
        STATUS_TANGEM_NEW_ORDER,
        STATUS_TANGEM_IN_PROGRESS,
        STATUS_TANGEM_UPSELL_DONE,
        STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED,
        STATUS_SUCCESS,
    },
    # Офис: товар отложен под клиента осознанно — резерв ставим и держим бессрочно.
    PIPELINE_OFFICE: {STATUS_OFFICE_PREORDER_PAID, STATUS_OFFICE_DEFERRED_RESERVE},
    # Песочница для живого теста механизма резерва (см. docstring модуля).
    PIPELINE_TEST: {STATUS_TEST_IN_PROGRESS},
}

# Статусы, где резерв снимаем. STATUS_SUCCESS (142) в Офисе — реально
# закрывающий статус (в отличие от УР в Основной/TangemShop выше).
_RESERVE_OFF: dict[int, set[int]] = {
    PIPELINE_CLEVER_MAIN: {STATUS_CLEVER_PRECLOSED, STATUS_CLOSED_LOST},
    PIPELINE_TANGEMSHOP: {STATUS_CLOSED_LOST},
    PIPELINE_OFFICE: {
        STATUS_OFFICE_COURIER_MSK,
        STATUS_OFFICE_COURIER_OWN,
        STATUS_WAYBILL_READY,
        STATUS_OFFICE_SHIPPED,
        STATUS_OFFICE_IN_TRANSIT,
        STATUS_OFFICE_AWAITING_PICKUP,
        STATUS_SUCCESS,
    },
    PIPELINE_TEST: {STATUS_CLOSED_LOST},
}

# Где тайм-аут трёх дней НЕ действует и резерв держим бессрочно — до статуса
# из _RESERVE_OFF либо до фактической отгрузки. Два случая: деньги пришли
# либо товар отложен под клиента осознанно (решение встречи 04.08.2026).
_TIMEOUT_EXEMPT: dict[int, set[int]] = {
    PIPELINE_CLEVER_MAIN: {STATUS_PAYMENT_RECEIVED, STATUS_SUCCESS},
    PIPELINE_TANGEMSHOP: {STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED, STATUS_SUCCESS},
    PIPELINE_OFFICE: {STATUS_OFFICE_PREORDER_PAID, STATUS_OFFICE_DEFERRED_RESERVE},
}

_TRACKED_PIPELINES = (PIPELINE_CLEVER_MAIN, PIPELINE_TANGEMSHOP, PIPELINE_OFFICE, PIPELINE_TEST)


def maybe_apply_bg(lead_id, pipeline_hint=None) -> None:
    """Вызывается из webhooks.py при смене статуса сделки ИЛИ при изменении
    поля «Состав заказа» (FIELD_ORDER_COMPOSITION). pipeline_hint — pipeline_id
    из вебхука, если он в нём есть — для быстрого отсева воронок вне зоны
    действия без похода в API. Если None (поле изменилось без смены
    воронки/этапа в этом вебхуке), решение всё равно примет _apply() по
    свежим данным сделки."""
    if not RESERVE_SERVICE_ENABLED:
        return
    if lead_id is None:
        return
    if pipeline_hint is not None and int(pipeline_hint) not in _TRACKED_PIPELINES:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            return
        pipeline_id = lead.get("pipeline_id")
        status_id = lead.get("status_id")
        order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
        order_uuid = str(order_uuid or "").strip()
        if not order_uuid:
            return  # заказа МС нет (не создан виджетом) — не наш случай

        if status_id in _RESERVE_OFF.get(pipeline_id, ()):
            await _set_reserve(order_uuid, on=False)
            await asyncio.to_thread(reserve_store.clear, lead_id)
        elif status_id in _TIMEOUT_EXEMPT.get(pipeline_id, ()):
            # Деньги пришли либо товар отложен осознанно — резерв держим бессрочно.
            # Запись В ХРАНИЛИЩЕ ОСТАВЛЯЕМ с пометкой exempt: таймер по ней не
            # сработает, но фоновая сверка должна видеть заказ и снять резерв,
            # когда товар отгрузят — отгрузка статус сделки не меняет.
            left = await _set_reserve(order_uuid, on=True)
            if left == 0:
                await asyncio.to_thread(reserve_store.clear, lead_id)
            else:
                await asyncio.to_thread(
                    reserve_store.mark_reserved, lead_id, order_uuid, pipeline_id, True
                )
        elif status_id in _RESERVE_ON.get(pipeline_id, ()):
            left = await _set_reserve(order_uuid, on=True)
            if left == 0:
                await asyncio.to_thread(reserve_store.clear, lead_id)
            else:
                await asyncio.to_thread(
                    reserve_store.mark_reserved, lead_id, order_uuid, pipeline_id, False
                )
        # иначе — статус вне зоны действия схемы, не трогаем
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Резерв: ошибка обработки сделки %s", lead_id)


async def _set_reserve(order_uuid: str, *, on: bool) -> int:
    """Ставит или снимает резерв по всем позициям заказа. Возвращает, сколько
    единиц осталось в резерве после правки — ноль значит, что держать больше
    нечего и запись в хранилище можно чистить.

    Отгрузка резерв съедает: держим только то, что ещё НЕ ушло со склада
    (quantity минус shipped). МойСклад сам отгрузку и резерв не связывает — это
    решение встречи 04.08.2026 «снимать безусловно по факту отгрузки,
    независимо от статуса». Слепок 10.08.2026 показал, что без этого правила
    82 единицы резерва висели на полностью отгруженных позициях.
    """
    positions = await ms_client.get(f"entity/customerorder/{order_uuid}/positions")
    if not positions:
        logger.warning("Резерв: не удалось получить позиции заказа МС %s", order_uuid)
        return -1  # состояние неизвестно — запись не чистим, переспросим в следующий раз
    left = 0
    for row in positions.get("rows", []):
        if "reserve" not in row:
            continue  # услуги/доставка — резерв не применим
        if on:
            target = max((row.get("quantity") or 0) - (row.get("shipped") or 0), 0)
        else:
            target = 0
        left += target
        if row.get("reserve") == target:
            continue  # уже в нужном состоянии — не дёргаем API зря
        result = await ms_client.put(
            f"entity/customerorder/{order_uuid}/positions/{row['id']}",
            {"reserve": target},
        )
        if result is None:
            logger.error(
                "Резерв: не удалось PUT reserve=%s на позицию %s заказа %s",
                target, row["id"], order_uuid,
            )
        else:
            logger.info(
                "Резерв: заказ %s, позиция %s → reserve=%s", order_uuid, row["id"], target,
            )
    return left


async def _timeout_once() -> None:
    cutoff = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=RESERVE_TIMEOUT_DAYS)
    ).isoformat()
    expired = await asyncio.to_thread(reserve_store.list_expired, cutoff)
    for row in expired:
        lead_id = row["lead_id"]
        try:
            lead = await amo_service.get_lead_full(lead_id, with_=())
            if not lead:
                continue
            pipeline_id = lead.get("pipeline_id")
            status_id = lead.get("status_id")
            if status_id in _TIMEOUT_EXEMPT.get(pipeline_id, ()) or status_id in _RESERVE_OFF.get(pipeline_id, ()):
                # Уже не наш случай (оплатили/уже сняли обычным путём) —
                # штатно должно было очиститься в _apply, подчищаем хвост.
                await asyncio.to_thread(reserve_store.clear, lead_id)
                continue
            logger.info(
                "Резерв: тайм-аут %s дней — снимаю резерв по сделке %s (заказ %s)",
                RESERVE_TIMEOUT_DAYS, lead_id, row["ms_order_uuid"],
            )
            await _set_reserve(row["ms_order_uuid"], on=False)
            await asyncio.to_thread(reserve_store.clear, lead_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Резерв: ошибка тайм-аута по сделке %s", lead_id)


async def _shipment_once() -> None:
    """Снятие резерва по ФАКТУ ОТГРУЗКИ, независимо от статуса сделки
    (решение встречи 04.08.2026).

    Отгрузка — отдельный документ в МойСкладе, статус сделки она не меняет —
    значит вебхука по ней не придёт и через _apply мы о ней не узнаем никогда.
    Поэтому ходим по своим же записям и пересчитываем резерв по quantity
    минус shipped. Только МойСклад, amo не дёргаем — очередь вебхуков amo
    общая на весь аккаунт, её бережём.
    """
    rows = await asyncio.to_thread(reserve_store.list_active)
    for row in rows:
        try:
            left = await _set_reserve(row["ms_order_uuid"], on=True)
            if left == 0:
                logger.info(
                    "Резерв: заказ %s отгружен целиком — резерв снят, сделка %s",
                    row["ms_order_uuid"], row["lead_id"],
                )
                await asyncio.to_thread(reserve_store.clear, row["lead_id"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Резерв: ошибка сверки отгрузки по сделке %s", row["lead_id"])


async def _timeout_loop() -> None:
    while True:
        try:
            # Сначала отгрузки: они снимают резерв безусловно и чистят часть
            # записей — тайм-ауту останется меньше работы и меньше запросов в amo.
            await _shipment_once()
            await _timeout_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Резерв: ошибка фонового опроса")
        await asyncio.sleep(RESERVE_TIMEOUT_POLL_INTERVAL_S)


async def init() -> None:
    global _timeout_task
    if not RESERVE_SERVICE_ENABLED:
        logger.warning("Резерв: сервис ВЫКЛЮЧЕН флагом RESERVE_SERVICE_ENABLED — не стартую")
        return
    await asyncio.to_thread(reserve_store.init)
    _timeout_task = asyncio.create_task(_timeout_loop())
    logger.info(
        "Резерв: сервис включён, тайм-аут %s дн., опрос раз в %s с",
        RESERVE_TIMEOUT_DAYS, RESERVE_TIMEOUT_POLL_INTERVAL_S,
    )


async def shutdown() -> None:
    global _timeout_task
    if _timeout_task is not None:
        _timeout_task.cancel()
        try:
            await _timeout_task
        except asyncio.CancelledError:
            pass
        _timeout_task = None
