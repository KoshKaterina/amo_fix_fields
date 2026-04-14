import datetime
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from starlette.status import HTTP_200_OK

from api import init_api_pipeline, shutdown_api_pipeline
from help_function import (
    get_nested,
    parse_the_cart_field,
    parse_the_cart_field_2,
)
from queue_manager import enqueue_new, init_queue, shutdown_queue


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
    yield
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
