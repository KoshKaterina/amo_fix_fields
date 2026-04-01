import asyncio
import datetime
import logging
import re

from fastapi import BackgroundTasks, FastAPI, Request
from starlette.status import HTTP_200_OK

from api import add_info_from_ms, get_lead_by_id
from help_function import (
    get_custom_field_value,
    get_nested,
    normalize_text,
    parse_the_cart_field,
    parse_the_cart_field_2,
)
from memory import update_info_later

app = FastAPI()

logger = logging.getLogger("uvicorn")

lead_last_processed = {}
RATE_LIMIT_SECONDS = 3
lead_processing_locks = {}
lead_processing_locks_guard = asyncio.Lock()


def insert_nested(data, keys, value):
    cur = data
    for key in keys[:-1]:
        if key not in cur:
            cur[key] = {}
        cur = cur[key]
    cur[keys[-1]] = value


async def _get_lead_processing_lock(lead_id: str) -> asyncio.Lock:
    async with lead_processing_locks_guard:
        lock = lead_processing_locks.get(lead_id)
        if lock is None:
            lock = asyncio.Lock()
            lead_processing_locks[lead_id] = lock
        return lock


async def _process_lead_update(
    *,
    lead_id,
    goods,
    delivery_type,
    delivery_address,
    lead_name,
    promo_type,
    comment,
):
    lead_lock = await _get_lead_processing_lock(str(lead_id))
    if lead_lock.locked():
        logger.info("Lead %s is already being processed, waiting for lock", lead_id)
    async with lead_lock:
        current_info = await get_lead_by_id(lead_id)
        if not isinstance(current_info, dict):
            logger.warning(
                "Skipping update for lead %s because current lead fetch failed",
                lead_id,
            )
            return

        current_goods = await get_custom_field_value(current_info, 577313)
        current_delivery_type = await get_custom_field_value(current_info, 577315)
        current_delivery_address = await get_custom_field_value(current_info, 577311)
        current_promo_type = await get_custom_field_value(current_info, 570661)
        current_comment = await get_custom_field_value(current_info, 577753)

        if goods:
            is_goods_match = await normalize_text(current_goods) == await normalize_text(goods)
        else:
            is_goods_match = True

        if delivery_type:
            is_delivery_match = await normalize_text(current_delivery_type) == await normalize_text(delivery_type)
        else:
            is_delivery_match = True

        if delivery_address:
            is_address_match = await normalize_text(current_delivery_address) == await normalize_text(delivery_address)
        else:
            is_address_match = True

        if lead_name:
            normalized_name = await normalize_text(lead_name)
            current_name = current_info.get("name") if isinstance(current_info, dict) else None
            normalized_current_name = await normalize_text(current_name)
            is_name_match = normalized_name == normalized_current_name
            logger.info(f"normalized_name: {normalized_name}, normalized_current_name: {normalized_current_name}")
        else:
            is_name_match = True

        if promo_type:
            is_promo_match = await normalize_text(current_promo_type) == await normalize_text(promo_type)
        else:
            is_promo_match = True

        if comment:
            is_comment_match = await normalize_text(current_comment) == await normalize_text(comment)
        else:
            is_comment_match = True

        if is_goods_match and is_delivery_match and is_address_match and is_name_match and is_promo_match and is_comment_match:
            logger.info("MATCH: Data is identical (ignoring whitespace).")
            return

        current_time = datetime.datetime.now()
        if lead_id in lead_last_processed:
            elapsed_seconds = (current_time - lead_last_processed[lead_id]).total_seconds()
            if elapsed_seconds < RATE_LIMIT_SECONDS:
                logger.info(f"Rate limit hit for lead {lead_id}")
                execute_after_seconds = RATE_LIMIT_SECONDS - elapsed_seconds
                logger.info(f"Will handle in {execute_after_seconds} seconds")
                await update_info_later(
                    goods,
                    delivery_type,
                    delivery_address,
                    lead_id,
                    lead_name,
                    execute_after_seconds,
                    lead_last_processed,
                    promo_type,
                    comment,
                )
                return

        lead_last_processed[lead_id] = current_time
        logger.info("MISMATCH: Updating info...")
        await add_info_from_ms(
            goods=goods,
            delivery_type=delivery_type,
            delivery_address=delivery_address,
            lead_id=lead_id,
            name=lead_name,
            promo_type=promo_type,
            comment=comment,
        )
        logger.info(
            f"UPDATING:\n goods: {goods}\n delivery_type: {delivery_type}\n delivery_address: {delivery_address}\n lead_name: {lead_name}\n promo: {promo_type}\n comment: {comment}"
        )


@app.post("/lead_change")
async def lead_change(request: Request, background_tasks: BackgroundTasks):
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
            or lead_id is not None
            or promo_type is not None
            or comment is not None
        ):
            if lead_id is None:
                logger.warning("Skipping update because lead_id is missing in payload")
                return HTTP_200_OK

            background_tasks.add_task(
                _process_lead_update,
                lead_id=lead_id,
                goods=goods,
                delivery_type=delivery_type,
                delivery_address=delivery_address,
                lead_name=lead_name,
                promo_type=promo_type,
                comment=comment,
            )
            return HTTP_200_OK

        logger.info(f"lead_id {lead_id}, nothing to update")
    else:
        logger.info(f"lead_id: {lead_id}, no Updates")

    return HTTP_200_OK
