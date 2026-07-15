import datetime
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response
from starlette.status import HTTP_200_OK

import amo_service
import cdek_client
import cdek_status_sync
import dup_autoclose
import jivo_service
import lead_dedup
import metrika_sync
import ms_status_sync
import showroom_tag
import telegram_bot
import uis_missed_call
import unmiss_tag
import urgency_tag
import wazzup_sla
import woo_status_sync
from api import init_api_pipeline, shutdown_api_pipeline
from help_function import (
    get_nested,
    parse_the_cart_field,
    parse_the_cart_field_2,
)
from queue_manager import (
    enqueue_jivo,
    enqueue_kontrol,
    enqueue_new,
    enqueue_waybill,
    init_queue,
    queue_stats,
    shutdown_queue,
)
from waybill_config import (
    PIPELINE_FULFILLMENT,
    STATUS_CREATE_WAYBILL,
    STATUS_FF_KONTROL,
    UIS_WEBHOOK_SECRET,
    WAZZUP_WEBHOOK_SECRET,
    looks_like_uuid,
)


@asynccontextmanager
async def lifespan(app):
    msk = datetime.timezone(datetime.timedelta(hours=3))

    class MskFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.datetime.fromtimestamp(record.created, tz=msk)
            return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

    formatter = MskFormatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        for handler in logging.getLogger(name).handlers:
            handler.setFormatter(formatter)

    init_api_pipeline()
    init_queue()
    await amo_service.warm_pipeline_cache()
    await cdek_client.init()
    await telegram_bot.init_telegram_bot()
    await cdek_status_sync.init()
    await metrika_sync.init()
    await woo_status_sync.init()
    await ms_status_sync.init()
    await wazzup_sla.init()
    yield
    # Первым — досверка хвостов unmiss (спящие дебаунс-задачи), пока API-пайплайн жив.
    await wazzup_sla.shutdown()
    await unmiss_tag.shutdown()
    await ms_status_sync.shutdown()
    await woo_status_sync.shutdown()
    await metrika_sync.shutdown()
    await cdek_status_sync.shutdown()
    await telegram_bot.shutdown_telegram_bot()
    await cdek_client.aclose()
    await shutdown_queue()
    await shutdown_api_pipeline()


app = FastAPI(lifespan=lifespan)

logger = logging.getLogger("uvicorn")


@app.get("/")
async def health():
    """Здоровье + срез очередей (глубина дорожек, api_queue, последнее ожидание) —
    чтобы «очередь огромная» была проверяема одним запросом, без чтения логов."""
    return {"status": "ok", **queue_stats()}


def insert_nested(data, keys, value):
    cur = data
    for key in keys[:-1]:
        if key not in cur:
            cur[key] = {}
        cur = cur[key]
    cur[keys[-1]] = value


@app.get("/barcode/{ident}")
async def barcode(ident: str):
    """Проксирует штрихкод СДЭК: принимает cdek_number или UUID заказа,
    ходит в СДЭК с токеном и отдаёт PDF. Ссылку кладём в примечание сделки."""
    try:
        if looks_like_uuid(ident):
            uuid = ident
        else:
            uuid = await cdek_client.find_uuid_by_cdek_number(ident)
        if not uuid:
            return Response("Заказ СДЭК не найден", status_code=404)
        pdf = await cdek_client.get_barcodes_batch_pdf([uuid])
    except cdek_client.CdekError as exc:
        logger.warning("barcode %s: ошибка СДЭК: %s", ident, exc)
        return Response(f"Штрихкод недоступен: {exc}", status_code=502)
    except Exception:
        logger.exception("barcode %s: неожиданная ошибка", ident)
        return Response("Внутренняя ошибка", status_code=500)
    return Response(
        pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="barcode_{ident}.pdf"'},
    )


@app.post("/cdek_status")
async def cdek_status(request: Request):
    """Вебхук СДЭК ORDER_STATUS. Отвечаем 200 всегда и быстро —
    обработка идёт через очередь с низшим приоритетом."""
    try:
        payload = await request.json()
    except Exception:
        logger.warning("CDEK webhook: невалидный JSON")
        return {"ok": False}
    try:
        cdek_status_sync.handle_webhook_event(payload)
    except Exception:
        logger.exception("CDEK webhook: ошибка обработки события")
    return {"ok": True}


@app.post("/jivo/{token}")
async def jivo_webhook(token: str, request: Request):
    """Вебхук Jivo (канал website → Integration Settings for Developers →
    Webhooks API). Без сегмента сайта → дефолтный сайт (Sunscrypt)."""
    return await _handle_jivo(token, None, request)


@app.post("/jivo/{token}/{site}")
async def jivo_webhook_site(token: str, site: str, request: Request):
    """Тот же вебхук с явным сайтом-источником в пути (/jivo/<secret>/<site>).
    У каждого канала Jivo (Sunscrypt, Tangemshop) — свой URL → сделка метится
    источником. Воронка и операторы общие."""
    return await _handle_jivo(token, site, request)


async def _handle_jivo(token: str, site: str | None, request: Request):
    """На завершение чата с контактом создаём контакт+сделку+примечание в amo —
    замена связки через Albato. Секрет в пути заменяет отсутствующую у Jivo
    подпись. Отвечаем быстро {"result":"ok"} (этого Jivo и ждёт), реальная
    работа — фоном через очередь."""
    if not jivo_service.secret_ok(token):
        logger.warning("Jivo webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)

    try:
        event = await request.json()
    except Exception:
        logger.warning("Jivo webhook: невалидный JSON")
        return {"result": "ok"}

    jivo_service.log_payload(event)
    event_name = event.get("event_name") if isinstance(event, dict) else None

    if not jivo_service.is_enabled():
        logger.info("Jivo webhook: получено '%s', обработка выключена (JIVO_WEBHOOK_ENABLED)", event_name)
        return {"result": "ok"}

    parsed = jivo_service.parse_event(event, site)
    if parsed is None:
        logger.info("Jivo webhook: '%s' пропущено (не наш тип события / нет контакта)", event_name)
        return {"result": "ok"}

    logger.info("Jivo webhook: '%s' принято, сайт=%s", event_name, parsed.get("site"))
    enqueue_jivo(parsed)
    return {"result": "ok"}


@app.get("/uis/{secret}")
async def uis_missed_call_webhook(secret: str, request: Request):
    """UIS HTTP-уведомление «Потерянный звонок» → алерт в ТГ отделу продаж.
    Секрет в пути (простая защита). Отвечаем 200 сразу — UIS ждёт быстрый ответ
    (иначе ретраит); реальная работа (поиск сделки + отправка) идёт в фоне."""
    if not UIS_WEBHOOK_SECRET or secret != UIS_WEBHOOK_SECRET:
        logger.warning("UIS webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)
    uis_missed_call.notify_bg(dict(request.query_params))
    return {"ok": True}


@app.post("/wazzup/{secret}")
async def wazzup_webhook(secret: str, request: Request):
    """Вебхук Wazzup (messages/statuses) → SLA-таймер «клиент без ответа N мин».
    Секрет в пути — простая защита. Отвечаем 200 сразу и всегда (Wazzup при
    ошибке/таймауте ретраит и может отключить вебхук). Тело messages[] обновляет
    состояние ожиданий; statuses[] (доставка/ошибки) игнорируем. При установке
    подписки Wazzup шлёт тестовый запрос — на него тоже отвечаем 200."""
    if not WAZZUP_WEBHOOK_SECRET or secret != WAZZUP_WEBHOOK_SECRET:
        logger.warning("Wazzup webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)
    try:
        payload = await request.json()
    except Exception:
        logger.warning("Wazzup webhook: невалидный JSON")
        return {"ok": True}
    if isinstance(payload, dict) and payload.get("test") is True:
        logger.info("Wazzup webhook: тестовый запрос — отвечаю 200")
        return {"ok": True}
    try:
        wazzup_sla.handle_webhook(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка обработки")
    return {"ok": True}


@app.post("/lead_change")
async def lead_change(request: Request):
    form = await request.form()

    nested = {}
    for raw_key, value in form.items():
        keys = re.findall(r"([^\[\]]+)", raw_key)
        insert_nested(nested, keys, value)

    goods = None
    delivery_type = None
    delivery_address = None
    lead_name = None
    promo_type = None
    comment = None

    lead_id = await get_nested(nested, ["leads", "update", "0", "id"])
    if lead_id is None:
        lead_id = await get_nested(nested, ["leads", "add", "0", "id"])

    modified_by = await get_nested(nested, ["leads", "update", "0", "updated_by"])
    logger.info(f"lead_id: {lead_id}, modified_by: {modified_by}")

    # Анти-дубль сделок (заказ побеждает консультацию / пост-продажный
    # маршрутизатор). Вызывается на ЛЮБОМ вебхуке сделки: amo шлёт add и update
    # в непредсказуемом порядке, а идемпотентность (TTL-set) и реконсиляция
    # дочитыванием живут внутри. За флагами LEAD_DEDUP_* (см. lead_dedup.py).
    lead_dedup.maybe_process_bg(lead_id, source="webhook")

    # Автоснятие «пропущенный» при дозвоне: реконсиляция по дочитыванию (amo не шлёт
    # теги в вебхук). На любом изменении сделки в фоне сверяем теги: если есть
    # «Успешный звонок» И «пропущенный» — снимаем «пропущенный» (сделка + контакты).
    unmiss_tag.maybe_remove_bg(lead_id)

    status_update = await get_nested(nested, ["leads", "update", "0", "status_id"])
    status_add = await get_nested(nested, ["leads", "add", "0", "status_id"])
    incoming_status = status_update if status_update is not None else status_add
    if lead_id is not None and incoming_status is not None and str(incoming_status) == str(STATUS_CREATE_WAYBILL):
        logger.info("Lead %s entered STATUS_CREATE_WAYBILL — enqueue waybill", lead_id)
        enqueue_waybill(lead_id, source="webhook")

    # Метрика+Woo вебхуком БОЛЬШЕ НЕ триггерятся (08.07.2026): аналитике реальное
    # время не нужно, а вебхучный путь давал больше половины задач очереди.
    # Синк идёт сверкой по расписанию — см. metrika_sync (интрадей + ночная).
    pipeline_update = await get_nested(nested, ["leads", "update", "0", "pipeline_id"])
    pipeline_add = await get_nested(nested, ["leads", "add", "0", "pipeline_id"])
    incoming_pipeline = pipeline_update if pipeline_update is not None else pipeline_add

    # Гейт КОНТРОЛЬ: ФФ-сделка зашла на этап «КОНТРОЛЬ» → автопроверка заказа
    # (подгон полей МС + стоп-поля + наличие) → релиз в «00» или удержание с тегом
    # «ошибка передачи» и причиной в примечании. Тяжёлая работа — в очереди (LANE_AMO).
    if (
        lead_id is not None
        and incoming_status is not None
        and str(incoming_status) == str(STATUS_FF_KONTROL)
        and (incoming_pipeline is None or str(incoming_pipeline) == str(PIPELINE_FULFILLMENT))
    ):
        logger.info("Lead %s entered STATUS_FF_KONTROL — enqueue kontrol gate", lead_id)
        enqueue_kontrol(lead_id, source="webhook")

    # Обратная синхронизация amo→МС: ТОЛЬКО при заходе ФФ-сделки на «00. Обрабатывается»
    # (ручной выпуск из КОНТРОЛЯ / создание копии там). Дальше склад ведёт amo (МС→amo).
    if (
        lead_id is not None
        and incoming_status is not None
        and ms_status_sync.is_enabled()
        and (incoming_pipeline is None or str(incoming_pipeline) == str(PIPELINE_FULFILLMENT))
    ):
        ms_status_sync.push_processing_bg(lead_id, incoming_status)

    updates = await get_nested(nested, ["leads", "update", "0", "custom_fields"])
    if updates:
        # Автотег «Срочно»: Срочность → «Срочно» → вешаем тег (в фоне, не блокирует).
        urgency_tag.maybe_apply_bg(updates, lead_id)
        # Авто-перенос дубля в ЗИН: если «Причина отказа» стала «Дубль сделки» →
        # в фоне переводим сделку в 143 (её воронка). Идемпотентно.
        dup_autoclose.maybe_close_bg(updates, lead_id)
        for updated_field in updates:
            info = updates[updated_field]
            if info["id"] == "576703":
                order_summary = info["values"]["0"]["value"]
                goods, delivery_type = await parse_the_cart_field(order_summary)
            if info["id"] == "576711":
                comment_summary = info["values"]["0"]["value"]
                promo_type, comment = await parse_the_cart_field_2(comment_summary)
            if info["id"] == "576719":
                delivery_address = info["values"]["0"]["value"]
            if info["id"] == "577415":
                lead_name = f'Заказ №{info["values"]["0"]["value"]}'
                logger.info(f"lead_name: {lead_name}")

        # Автотег «Запись в шоурум»: тип доставки (577315) = самовывоз из офиса
        # Sunscrypt → вешаем тег (в фоне, идемпотентно). «CDEK: Самовывоз» не триггерит.
        showroom_tag.maybe_apply_bg(delivery_type, lead_id)

        if (
            goods is not None
            or delivery_type is not None
            or delivery_address is not None
            or lead_name is not None
            or promo_type is not None
            or comment is not None
        ):
            if lead_id is None:
                logger.warning("Skipping update because lead_id is missing in payload")
                return HTTP_200_OK

            enqueue_new({
                "lead_id": lead_id,
                "goods": goods,
                "delivery_type": delivery_type,
                "delivery_address": delivery_address,
                "lead_name": lead_name,
                "promo_type": promo_type,
                "comment": comment,
            })
            return HTTP_200_OK

        logger.info(f"lead_id {lead_id}, nothing to update")
    else:
        logger.info(f"lead_id: {lead_id}, no Updates")

    return HTTP_200_OK
