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
import metrika_sync
import telegram_bot
from api import init_api_pipeline, shutdown_api_pipeline
from help_function import (
    get_nested,
    parse_the_cart_field,
    parse_the_cart_field_2,
)
from queue_manager import (
    enqueue_metrika_sync,
    enqueue_new,
    enqueue_waybill,
    init_queue,
    shutdown_queue,
)
from waybill_config import (
    PIPELINE_CLEVER,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    STATUS_CREATE_WAYBILL,
    looks_like_uuid,
)

METRIKA_PIPELINES = {str(PIPELINE_CLEVER), str(PIPELINE_OFFICE), str(PIPELINE_FULFILLMENT)}


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
    yield
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
    return {"status": "ok"}


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

    status_update = await get_nested(nested, ["leads", "update", "0", "status_id"])
    status_add = await get_nested(nested, ["leads", "add", "0", "status_id"])
    incoming_status = status_update if status_update is not None else status_add
    if lead_id is not None and incoming_status is not None and str(incoming_status) == str(STATUS_CREATE_WAYBILL):
        logger.info("Lead %s entered STATUS_CREATE_WAYBILL — enqueue waybill", lead_id)
        enqueue_waybill(lead_id, source="webhook")

    # Смена статуса в воронках сквозного потока (CLEVER/Офис/Фулфилмент) →
    # задача для Яндекс.Метрики (низший приоритет). Прочие воронки не трогаем.
    pipeline_update = await get_nested(nested, ["leads", "update", "0", "pipeline_id"])
    pipeline_add = await get_nested(nested, ["leads", "add", "0", "pipeline_id"])
    incoming_pipeline = pipeline_update if pipeline_update is not None else pipeline_add
    if (
        lead_id is not None
        and incoming_status is not None
        and metrika_sync.is_enabled()
        and (incoming_pipeline is None or str(incoming_pipeline) in METRIKA_PIPELINES)
    ):
        enqueue_metrika_sync(lead_id, incoming_status)

    updates = await get_nested(nested, ["leads", "update", "0", "custom_fields"])
    if updates:
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
