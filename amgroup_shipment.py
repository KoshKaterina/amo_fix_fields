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
Диск пишем СРАЗУ после успешного создания в МойСкладе, ДО записи полей в
amoCRM - отгрузка необратимо списывает товар, а провал последующей записи в
amo лечится ручной допиской полей, а не риском пересоздать отгрузку.
"""

import datetime
import json
import logging
import os

import amo_service
import ms_client
from waybill_config import (
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
FIELD_SHIPMENT_WAREHOUSE = 576675   # «Склад отгрузки»

# Целевые этапы воронки «Офис» - см. докстринг модуля. Все три уже определены
# в waybill_config.py, здесь только собираем набор для проверки.
_TRIGGER_STATUSES = {STATUS_WAYBILL_READY, STATUS_OFFICE_COURIER_OWN, STATUS_SUCCESS}

# Разведка 03.09.2026: во всех трёх случаях отгрузка идёт с этого склада и от
# этого юрлица (ИП Перфилов - в поля сделки не пишем, туда просят только
# идентификатор/номер/склад отгрузки). Имя склада читаем живьём у МойСклада
# (_resolve_store_name) - эта строка только запасной ход, если склад не
# ответил на уточняющий запрос имени.
DEFAULT_STORE_NAME = "Sunscrypt Основной"

# Состояние на диске - третий, самый дешёвый по цепочке, но обязательный гейт
# от повторного создания отгрузки (см. докстринг модуля). Каталог /app/var -
# как и у amgroup_fallback.py, чтобы пересборка контейнера не теряла память.
_STATE_PATH = os.getenv("AMGROUP_SHIPMENT_STATE_PATH", "/app/var/amgroup_shipment_created.json")
_created: dict[str, dict] = {}
_state_loaded = False
_STATE_CAP = 5000


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


async def _resolve_store_name(store_ref: dict | None) -> str:
    """Человекочитаемое имя склада отгрузки - читаем живьём у МойСклада, а не
    жёстко кодируем: DEFAULT_STORE_NAME только запасной ход, если сам склад
    не ответил на уточняющий запрос (см. факт разведки у константы)."""
    href = ((store_ref or {}).get("meta") or {}).get("href") or ""
    store_id = href.rsplit("/", 1)[-1].split("?")[0]
    if not store_id:
        return DEFAULT_STORE_NAME
    store = await ms_client.get(f"entity/store/{store_id}")
    if store and store.get("name"):
        return str(store["name"])
    logger.warning(
        "amgroup_shipment: не удалось получить имя склада %s у МойСклада - "
        "в поле сделки пишем название по умолчанию (%s)",
        store_id, DEFAULT_STORE_NAME,
    )
    return DEFAULT_STORE_NAME


async def write_shipment_fields_to_lead(lead_id: int | str, result: dict) -> bool:
    """Дописывает в сделку три поля отгрузки одним PATCH через amo_service -
    свой HTTP-клиент не заводим. Ошибка записи НЕ откатывает отгрузку в
    МойСкладе (см. докстринг модуля): товар уже списан, это необратимо."""
    patched = await amo_service.patch_lead(
        lead_id,
        custom_fields={
            FIELD_SHIPMENT_ID: result["shipment_id"],
            FIELD_SHIPMENT_NUMBER: result["shipment_number"],
            FIELD_SHIPMENT_WAREHOUSE: result["warehouse"],
        },
    )
    ok = bool(patched.get("ok"))
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
    после успешного POST отгрузка уже реальна и назад не откатывается)."""
    lead_id = lead.get("id")
    if lead_id is None:
        logger.error("amgroup_shipment: у сделки нет id, отгрузку не создаём")
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

    # Гейт 3 (диск): эту сделку уже отгружали в прошлом проходе. Дешевле сети,
    # поэтому спрашиваем раньше склада.
    _load_state()
    if str(lead_id) in _created:
        logger.info(
            "amgroup_shipment: сделка %s уже отмечена отгруженной локально, повторно не создаём",
            lead_id,
        )
        return None

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
    warehouse_name = await _resolve_store_name(demand.get("store"))

    # Диск - до записи в amo, см. докстринг модуля.
    _created[str(lead_id)] = {
        "shipment_id": shipment_id,
        "shipment_number": shipment_number,
        "order_uuid": order_uuid,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _save_state()

    result = {"shipment_id": shipment_id, "shipment_number": shipment_number, "warehouse": warehouse_name}
    await write_shipment_fields_to_lead(lead_id, result)
    return result
