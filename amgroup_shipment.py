"""Протез отгрузок amgroup (amoCRM -> МойСклад).

Повод - 02.09.2026 у amgroup истёк сертификат *.amgbp.ru и лёг сервер (разбор
в amgroup_fallback.py и WORKLOG.md, запись 2026-09-03). Кроме создания сделок
amgroup ещё и создавала отгрузку в МойСкладе при попадании сделки на один из
целевых этапов воронки «Офис» - сейчас этого не делает никто, товар не
списывается со склада. Этот модуль - протез именно отгрузки: создание сделки
(другой разрыв) закрывает соседний срез amgroup_fallback.py.

Когда создаём отгрузку - пара «воронка + этап», ТРИ варианта (разведка
03.09.2026, живые сделки):
    Офис + «Готова накладная» (СДЭК)        - STATUS_WAYBILL_READY
    Офис + «Доставка наш курьер»            - STATUS_OFFICE_COURIER_OWN
    Офис + «Успешно реализовано»            - STATUS_SUCCESS
Все три константы уже есть в waybill_config.py (модуль не трогаем, только
импортируем). ⚠️ STATUS_SUCCESS=142 - системный статус, общий для ВСЕХ
воронок аккаунта, поэтому пара всегда проверяется ЦЕЛИКОМ (pipeline_id ==
PIPELINE_OFFICE И status_id в целевом наборе) - иначе поймаем закрытие сделки
в любой другой воронке и спишем товар зря.

Три локальных поля сделки, которые amgroup дописывала следом (576691/576699/
576675) - общего реестра полей amoCRM в проекте нет (тот же приём, что
FIELD_MOYSKLAD_ORDER_NUMBER в amgroup_fallback.py и FIELD_MS_ORDER_UUID в
showroom_store.py). ⚠️ Свести в общий конфиг на сшивке с соседними срезами.

Отгрузку собираем ШАБЛОНОМ МойСклада, а не руками (проверено по официальной
документации JSON API 1.2, git-репозиторий moysklad/api-remap-1.2-doc,
md/documents/_common_info.md и _demand.md, раздел «Шаблон Отгрузки на
основе»): PUT entity/demand/new с телом {"customerOrder": {meta}} возвращает
предзаполненный JSON отгрузки - позиции (товары ЗАКАЗА плюс служебная строка
доставки), организация и склад копируются автоматически той же логикой, что
и кнопка «Отгрузить» в интерфейсе МойСклада. PUT на /new ничего не создаёт
(это "болванка"), реальный документ появляется только когда этот же JSON
отправлен POST'ом на entity/demand. Раз шаблон рабочий и задокументированный
- ручную сборку позиций (перебор состава заказа + подбор услуги доставки по
способу) в этом срезе НЕ делаем: это отдельный пласт логики, не нужный, пока
шаблон отвечает.

⚠️ Как и остальной проект - пустой ответ МойСклада (None от ms_client) НИКОГДА
не читаем как «отгрузки нет» или «шаблон пуст». Не уверены, что склад ответил
- не создаём ничего (03.09.2026 на этой путанице уже горел order_watchdog).

Защита от двойного списания - ТРОЙНАЯ, все три гейта обязательны и идут по
возрастанию цены проверки:
    1. поле сделки «ID Отгрузки» уже заполнено -> выходим молча;
    2. МойСклад уже знает отгрузку по этому заказу покупателя -> выходим молча;
    3. состояние на диске (/app/var) - эта сделка уже отгружена в прошлом
       проходе -> выходим молча.

⚠️ Все три гейта выше только ЧИТАЮТ состояние - между чтением и финальной
записью в МойСклад идут два сетевых вызова, и без дополнительной защиты
конкурентный вебхук по той же сделке успевает пройти те же гейты, пока первый
ждёт сеть (авария на разборе 03.09.2026: гонка двух вебхуков создавала две
отгрузки). Закрыто ДВУМЯ слоями:
    - замок на lead_id (_lock_for) - конкурентный вызов create_shipment_for_lead
      по той же сделке ждёт, пока первый пройдёт всё целиком;
    - синхронное занятие слота на гейте 3 (диск) - тем же приёмом, что
      showroom_alert._is_new: проверка и запись идут ОДНИМ действием, без
      await между ними, поэтому гонка невозможна даже без замка.
Не состоялось создание - слот освобождается (см. create_shipment_for_lead),
иначе одна сетевая икота навсегда заблокирует отгрузку по этой сделке.

Диск пишем СРАЗУ после получения id и номера документа от МойСклада - до
запроса имени склада (_resolve_store_name, тоже сетевой) и ДО записи полей в
amoCRM: отгрузка необратимо списывает товар, а обрыв на любом из двух
следующих сетевых вызовов не должен терять память о том, что документ уже
реален. Провал записи полей лечится ручной допиской, а не риском пересоздать
отгрузку.
"""

import asyncio
import datetime
import json
import logging
import os

import amo_service
import ms_client
from waybill_config import (
    AMGROUP_SHIPMENT_DRY_RUN,
    AMGROUP_SHIPMENT_ENABLED,
    AMGROUP_SHIPMENT_GRACE_SEC,
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_API_URL,
    PIPELINE_OFFICE,
    STATUS_OFFICE_COURIER_OWN,
    STATUS_SUCCESS,
    STATUS_WAYBILL_READY,
)

logger = logging.getLogger("uvicorn")

# Поля сделки amoCRM (свести в общий конфиг на сшивке с соседними срезами -
# см. докстринг модуля).
FIELD_SHIPMENT_ID = 576691          # «ID Отгрузки»
FIELD_SHIPMENT_NUMBER = 576699      # «№ Отгрузки»
FIELD_SHIPMENT_WAREHOUSE = 576675   # «Склад отгрузки» - ВЫПАДАЮЩИЙ СПИСОК, не текст

# Варианты выпадающего списка «Склад отгрузки» - сверено с amoCRM 03.09.2026.
# Поле select, поэтому в него пишется НЕ имя склада, а идентификатор варианта:
# на обычный value amo отвечает отказом NotSupportedChoice.
WAREHOUSE_ENUMS = {
    "Sunscrypt Основной": 1040157,
    "Sunscrypt Брак": 1040155,
    "Sunscrypt Вскрытые": 1040163,
    "Sunscrypt временный": 1041515,
    "Tangem Russia Основной": 1040161,
    "Tangem Russia Брак": 1040159,
    "Tangem Russia Вскрытые": 1040167,
    "OZON ДаркСтор Казань": 1040153,
    "OZON ДаркСтор СПБ": 1040165,
    "OZON ДаркСтор Краснодар": 1040171,
    "ЭРМС_Основной": 1041665,
    "корректировка": 1040169,
}

# Целевые этапы воронки «Офис» - см. докстринг модуля. Все три уже определены
# в waybill_config.py, здесь только собираем набор для проверки.
_TRIGGER_STATUSES = {STATUS_WAYBILL_READY, STATUS_OFFICE_COURIER_OWN, STATUS_SUCCESS}

# Разведка 03.09.2026: во всех трёх случаях отгрузка идёт с этого склада и от
# этого юрлица (ИП Перфилов - в поля сделки не пишем, туда просят только
# идентификатор/номер/склад отгрузки). Имя склада читаем ЖИВЬЁМ у МойСклада
# (_resolve_store_name) - молчание склада НЕ читаем как «Основной»: правка по
# итогам ревью 03.09.2026, раньше здесь был запасной ход DEFAULT_STORE_NAME =
# "Sunscrypt Основной", и он молча проходил проверку по вариантам списка
# незамеченным. Незнакомое или неопределённое имя склада - это пустое поле
# сделки и предупреждение в лог, а не знакомое имя наугад.

# Состояние на диске - третий, самый дешёвый по цепочке, но обязательный гейт
# от повторного создания отгрузки (см. докстринг модуля). Каталог /app/var -
# как и у amgroup_fallback.py, чтобы пересборка контейнера не теряла память.
_STATE_PATH = os.getenv("AMGROUP_SHIPMENT_STATE_PATH", "/app/var/amgroup_shipment_created.json")
_created: dict[str, dict] = {}
_state_loaded = False
_STATE_CAP = 5000

# Фоновые задачи вебхука (handle_lead_status_change_bg) - ссылку держим, иначе
# event loop хранит только слабую ссылку и сборщик мусора может срезать задачу
# на любом await (тем же приёмом, что showroom_tag/unmiss_tag/reserve_service).
_bg_tasks: set = set()

# Замок на lead_id - защита от гонки двух вебхуков по одной сделке (см.
# докстринг модуля). Создаётся лениво, на первое обращение к сделке.
_lead_locks: dict[str, asyncio.Lock] = {}


def _lock_for(lead_id) -> asyncio.Lock:
    key = str(lead_id)
    lock = _lead_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _lead_locks[key] = lock
    return lock


def _load_state() -> None:
    global _state_loaded
    if _state_loaded:
        return
    _state_loaded = True
    try:
        with open(_STATE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _created.update(data)
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("amgroup_shipment: не прочитался %s - начинаем с нуля", _STATE_PATH)


def _save_state() -> None:
    try:
        os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
        tmp = f"{_STATE_PATH}.tmp"
        # Кап по числу записей (как в amgroup_fallback.py) - файл не растёт
        # бесконечно, при этом самые свежие записи (порядок вставки в dict)
        # остаются последними.
        trimmed = dict(list(_created.items())[-_STATE_CAP:])
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(trimmed, f, ensure_ascii=False)
        os.replace(tmp, _STATE_PATH)
    except Exception:
        logger.exception("amgroup_shipment: не записался %s", _STATE_PATH)


def _as_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _customerorder_meta(order_uuid: str) -> dict:
    href = f"{MS_API_URL}/entity/customerorder/{order_uuid}"
    return {
        "meta": {
            "href": href,
            "metadataHref": f"{MS_API_URL}/entity/customerorder/metadata",
            "type": "customerorder",
            "mediaType": "application/json",
        }
    }


async def _find_existing_demand(order_uuid: str) -> list[dict] | None:
    """Отгрузки МойСклада, уже привязанные к этому заказу покупателя.

    None - склад НЕ ОТВЕТИЛ (см. докстринг модуля), пустой список - ответил
    честно и отгрузки нет. Смешивать эти два случая нельзя."""
    href = f"{MS_API_URL}/entity/customerorder/{order_uuid}"
    data = await ms_client.get("entity/demand", params={"filter": f"customerOrder={href}", "limit": 1})
    if data is None:
        return None
    return data.get("rows") or []


async def _resolve_store_name(store_ref: dict | None) -> str | None:
    """Человекочитаемое имя склада отгрузки - читаем ЖИВЬЁМ у МойСклада.

    Склад не ответил или в шаблоне вовсе нет ссылки на склад - возвращаем
    None. Молча подставлять знакомое имя («Sunscrypt Основной») опаснее, чем
    оставить поле сделки пустым: незнакомое имя не пишем вовсе (правка по
    итогам ревью 03.09.2026, см. докстринг модуля)."""
    href = ((store_ref or {}).get("meta") or {}).get("href") or ""
    store_id = href.rsplit("/", 1)[-1].split("?")[0]
    if not store_id:
        logger.warning(
            "amgroup_shipment: в шаблоне отгрузки нет ссылки на склад - "
            "поле «Склад отгрузки» в сделке останется пустым",
        )
        return None
    store = await ms_client.get(f"entity/store/{store_id}")
    if store and store.get("name"):
        return str(store["name"])
    logger.warning(
        "amgroup_shipment: МойСклад не ответил на запрос имени склада %s - "
        "поле «Склад отгрузки» в сделке останется пустым, «Sunscrypt Основной» "
        "молча не подставляем",
        store_id,
    )
    return None


async def write_shipment_fields_to_lead(lead_id: int | str, result: dict) -> bool:
    """Дописывает в сделку три поля отгрузки одним PATCH через amo_service -
    свой HTTP-клиент не заводим. Ошибка записи НЕ откатывает отгрузку в
    МойСкладе (см. докстринг модуля): товар уже списан, это необратимо."""
    # Два текстовых поля идут обычным путём.
    patched = await amo_service.patch_lead(
        lead_id,
        custom_fields={
            FIELD_SHIPMENT_ID: result["shipment_id"],
            FIELD_SHIPMENT_NUMBER: result["shipment_number"],
        },
    )
    ok = bool(patched.get("ok"))

    # Склад отгрузки - выпадающий список, ему нужен идентификатор варианта.
    # patch_lead умеет только value, поэтому патчим напрямую, тем же приёмом,
    # что и showroom_store.py. Незнакомое имя склада не пишем вовсе: молча
    # подставить "Основной" опаснее, чем оставить поле пустым.
    enum_id = WAREHOUSE_ENUMS.get(result.get("warehouse"))
    if enum_id is None:
        if result.get("warehouse") is None:
            logger.warning(
                "amgroup_shipment: склад отгрузки не определён (МойСклад не ответил "
                "или в шаблоне нет ссылки на склад) - поле сделки %s не заполняем",
                lead_id,
            )
        else:
            logger.warning(
                "amgroup_shipment: склад %r нет в списке вариантов поля сделки, "
                "поле не заполнено (сделка %s)", result.get("warehouse"), lead_id,
            )
    else:
        body = {"custom_fields_values": [
            {"field_id": FIELD_SHIPMENT_WAREHOUSE, "values": [{"enum_id": enum_id}]}]}
        res = await amo_service._do_patch(f"/api/v4/leads/{lead_id}", body)
        if not res.get("ok"):
            ok = False
            logger.error(
                "amgroup_shipment: склад отгрузки не записался в сделку %s (%r)",
                lead_id, res,
            )
    if not ok:
        logger.error(
            "amgroup_shipment: отгрузка создана в МойСкладе (ID %s, № %s, склад %s), "
            "но поля сделки %s НЕ записались (%r) - допишите вручную",
            result["shipment_id"], result["shipment_number"], result["warehouse"],
            lead_id, patched,
        )
    return ok


async def create_shipment_for_lead(lead: dict) -> dict | None:
    """Создаёт отгрузку в МойСкладе по сделке amoCRM, если сделка на нужном
    этапе и защита от повтора это разрешает. При успехе сразу же дописывает
    три поля сделки (write_shipment_fields_to_lead) и возвращает
    {"shipment_id", "shipment_number", "warehouse"}. При любом отказе -
    None, и в МойСкладе ничего не создано (кроме самого последнего шага -
    после успешного POST отгрузка уже реальна и назад не откатывается).

    Гонка двух конкурентных вызовов по одной сделке (см. докстринг модуля)
    закрыта замком на lead_id (_lock_for) - второй вызов ждёт, пока первый
    пройдёт всё целиком, и видит уже занятый или закрытый гейт 3."""
    lead_id = lead.get("id")
    if lead_id is None:
        logger.error("amgroup_shipment: у сделки нет id, отгрузку не создаём")
        return None

    # Флаг проверяем и здесь, а не только в двух точках входа
    # (handle_lead_status_change / handle_lead_status_change_bg) - функция
    # публичная, вызов мимо них не должен обходить выключатель (находка
    # приёмки безопасности 03.09.2026).
    if not AMGROUP_SHIPMENT_ENABLED:
        logger.info(
            "amgroup_shipment: модуль выключен флагом AMGROUP_SHIPMENT_ENABLED, "
            "отгрузку по сделке %s не создаём",
            lead_id,
        )
        return None

    pipeline_id = _as_int(lead.get("pipeline_id"))
    status_id = _as_int(lead.get("status_id"))
    if pipeline_id != PIPELINE_OFFICE or status_id not in _TRIGGER_STATUSES:
        return None  # не наш этап - тихо выходим, это штатный путь для 99% вебхуков

    # Гейт 1: поле «ID Отгрузки» уже заполнено.
    if str(amo_service.get_custom_field_value(lead, FIELD_SHIPMENT_ID) or "").strip():
        logger.info(
            "amgroup_shipment: сделка %s - поле «ID Отгрузки» уже заполнено, отгрузку не создаём",
            lead_id,
        )
        return None

    order_uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not order_uuid:
        logger.error(
            "amgroup_shipment: сделка %s на целевом этапе, но пусто поле «ID Заказа» "
            "(576689) - отгрузку делать не по чему",
            lead_id,
        )
        return None

    async with _lock_for(lead_id):
        key = str(lead_id)

        # Гейт 3 (диск) + занятие слота. Раньше гейт только ЧИТАЛ _created -
        # конкурентный вызов читал «пусто» и тоже ехал в сеть, пока первый ждал
        # ответ (авария, см. докстринг модуля). Проверка и запись идут ОДНИМ
        # синхронным действием, без await между ними - под замком это
        # дополнительный, а не единственный слой защиты.
        _load_state()
        if key in _created:
            logger.info(
                "amgroup_shipment: сделка %s уже отмечена отгруженной локально, повторно не создаём",
                lead_id,
            )
            return None
        _created[key] = {"pending": True, "order_uuid": order_uuid}
        _save_state()

        try:
            # Гейт 2: спрашиваем склад, нет ли уже отгрузки по этому заказу покупателя.
            existing = await _find_existing_demand(order_uuid)
            if existing is None:
                logger.warning(
                    "amgroup_shipment: МойСклад не ответил на проверку существующих отгрузок по "
                    "заказу %s (сделка %s) - ничего не создаём (пустой ответ никогда не значит "
                    "«отгрузки нет»)",
                    order_uuid, lead_id,
                )
                return None
            if existing:
                logger.info(
                    "amgroup_shipment: по заказу %s уже есть отгрузка в МойСкладе (%s) - выходим, сделка %s",
                    order_uuid, existing[0].get("name"), lead_id,
                )
                return None

            # Шаблон отгрузки на основе заказа (см. докстринг модуля) - PUT ничего не
            # создаёт, только возвращает предзаполненный JSON.
            template = await ms_client.put(
                "entity/demand/new", {"customerOrder": _customerorder_meta(order_uuid)},
            )
            if template is None:
                logger.error(
                    "amgroup_shipment: МойСклад не отдал шаблон отгрузки по заказу %s (сделка %s)",
                    order_uuid, lead_id,
                )
                return None

            if AMGROUP_SHIPMENT_DRY_RUN:
                # Сухой режим (находка приёмки безопасности 03.09.2026): все гейты
                # пройдены, шаблон у МойСклада собран, но саму запись (POST в
                # МойСклад, PATCH в amoCRM) не делаем - только лог, что создали бы.
                positions = ((template.get("positions") or {}).get("rows") or [])
                store_href = ((template.get("store") or {}).get("meta") or {}).get("href") or "?"
                logger.info(
                    "amgroup_shipment: СУХОЙ РЕЖИМ - создал бы отгрузку по заказу %s "
                    "(сделка %s), позиций в шаблоне %s, склад в шаблоне %s - в МойСклад "
                    "и amoCRM не пишем",
                    order_uuid, lead_id, len(positions), store_href,
                )
                return None

            # Сам документ появляется только здесь - реальное списание товара.
            demand = await ms_client.post("entity/demand", template)
            if not demand or not demand.get("id"):
                logger.error(
                    "amgroup_shipment: МойСклад не создал отгрузку по заказу %s (сделка %s): %r",
                    order_uuid, lead_id, demand,
                )
                return None

            shipment_id = str(demand["id"])
            shipment_number = str(demand.get("name") or "")

            # Диск - СРАЗУ после успешного создания, ДО запроса имени склада и
            # ДО записи полей в amo (см. докстринг модуля): обрыв на любом из
            # двух следующих сетевых вызовов не должен терять память о том,
            # что документ в МойСкладе уже реален.
            _created[key] = {
                "shipment_id": shipment_id,
                "shipment_number": shipment_number,
                "order_uuid": order_uuid,
                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            _save_state()

            warehouse_name = await _resolve_store_name(demand.get("store"))
            result = {"shipment_id": shipment_id, "shipment_number": shipment_number, "warehouse": warehouse_name}
            await write_shipment_fields_to_lead(lead_id, result)
            return result
        finally:
            # Слот освобождаем ТОЛЬКО если реальная отгрузка не состоялась -
            # запись выше (после успешного POST) уже без "pending", её не
            # трогаем. Не освободить при отказе - одна сетевая икота навсегда
            # заблокирует отгрузку по этой сделке (требование ревью 03.09.2026).
            if _created.get(key, {}).get("pending"):
                del _created[key]
                _save_state()


async def handle_lead_status_change(lead_id: int | str, status_id, pipeline_id) -> dict | None:
    """Точка входа из вебхука смены этапа. Сперва отсекает по дешёвым признакам
    (флаг, воронка, этап) и только потом лезет в сеть за сделкой: вебхук
    приходит на каждое изменение любой сделки, и лишний запрос в amoCRM отсюда
    стоил бы дорого - тем же соображением живёт new_lead_watch."""
    if not AMGROUP_SHIPMENT_ENABLED:
        return None
    if _as_int(pipeline_id) != PIPELINE_OFFICE or _as_int(status_id) not in _TRIGGER_STATUSES:
        return None

    # Пауза - ДО чтения сделки (03.09.2026): живой amgroup делает отгрузку сам
    # за 7-15 секунд после перехода на этап. Даём ему фору, потом читаем
    # сделку заново - если он успел, гейт «ID Отгрузки уже заполнено» и
    # проверка отгрузок в МойСкладе нас остановят. Прочитать сделку до паузы
    # нельзя: снимок был бы сделан до того, как amgroup дописал поля.
    if AMGROUP_SHIPMENT_GRACE_SEC > 0:
        logger.info(
            "amgroup_shipment: сделка %s на целевом этапе - ждём %s с, даём amgroup "
            "сделать отгрузку самому",
            lead_id, AMGROUP_SHIPMENT_GRACE_SEC,
        )
        await asyncio.sleep(AMGROUP_SHIPMENT_GRACE_SEC)

    lead = await amo_service.get_lead_full(lead_id)
    if not lead:
        logger.warning(
            "amgroup_shipment: сделку %s не удалось прочитать, отгрузку не создаём", lead_id,
        )
        return None
    return await create_shipment_for_lead(lead)


def handle_lead_status_change_bg(lead_id, status_id, pipeline_id) -> None:
    """Быстрая обёртка для вебхука: планирует фон и сразу возвращает - тем же
    приёмом, что unmiss_tag.maybe_remove_bg. Вебхук не должен ждать ни склад,
    ни amoCRM: ответ на вебхук держит соединение amo.

    Ссылку на задачу держим в _bg_tasks (как showroom_tag/unmiss_tag/
    reserve_service) - event loop иначе хранит только слабую ссылку, и
    сборщик мусора может срезать задачу на любом await. Хуже всего срез
    между созданием отгрузки и записью состояния (см. докстринг модуля) -
    товар уже списан, памяти об этом нет."""
    if lead_id is None or not AMGROUP_SHIPMENT_ENABLED:
        return
    if _as_int(pipeline_id) != PIPELINE_OFFICE or _as_int(status_id) not in _TRIGGER_STATUSES:
        return
    task = asyncio.create_task(_handle_lead_status_change_bg(lead_id, status_id, pipeline_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _handle_lead_status_change_bg(lead_id, status_id, pipeline_id) -> None:
    """Тело фоновой задачи - обёрнуто в try/except, иначе исключение всплывёт
    безымянной строкой при сборке мусора (как у showroom_tag._apply)."""
    try:
        await handle_lead_status_change(lead_id, status_id, pipeline_id)
    except Exception:
        logger.exception(
            "amgroup_shipment: фоновая отгрузка упала на сделке %s", lead_id,
        )


async def init() -> None:
    """Регистрация в жизненном цикле (lifespan, webhooks.py). Отдельного
    состояния поднимать не нужно (клиент МойСклада и amo уже живут к этому
    моменту) - только сообщаем в лог, в каком режиме модуль стартовал."""
    logger.info(
        "amgroup_shipment: подключён к жизненному циклу - AMGROUP_SHIPMENT_ENABLED=%s, "
        "AMGROUP_SHIPMENT_DRY_RUN=%s",
        AMGROUP_SHIPMENT_ENABLED, AMGROUP_SHIPMENT_DRY_RUN,
    )


async def shutdown() -> None:
    """Дождаться незавершённых фоновых отгрузок перед остановкой (образец -
    unmiss_tag.shutdown, строки 88-101). Срез между созданием отгрузки в
    МойСкладе и записью состояния на диск - самый опасный момент модуля (см.
    докстринг): товар уже списан, а память об этом ещё не сохранена."""
    pending = [t for t in _bg_tasks if not t.done()]
    if not pending:
        return
    _done, still_pending = await asyncio.wait(pending, timeout=15)
    if still_pending:
        logger.warning(
            "amgroup_shipment: %d фоновых отгрузок не успели на shutdown - "
            "возможна отгрузка без полей в сделке, проверьте вручную",
            len(still_pending),
        )
        for t in still_pending:
            t.cancel()
