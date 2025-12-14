import datetime
import logging
import re
from pprint import pprint

from fastapi import FastAPI, Request
from starlette.status import HTTP_200_OK

from api import add_info_from_ms, get_lead_by_id
from help_function import parse_the_cart_field, get_nested, get_custom_field_value, normalize_text

app = FastAPI()

logger = logging.getLogger("uvicorn")

lead_last_processed = {}
RATE_LIMIT_SECONDS = 5


def insert_nested(data, keys, value):
    cur = data
    for k in keys[:-1]:
        if k not in cur:
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value

@app.post("/lead_change")
async def lead_change(request: Request):
    form = await request.form()

    nested = {}
    for raw_key, value in form.items():
        # 'leads[update][0][id]' -> ['leads', 'update', '0', 'id']
        keys = re.findall(r'([^\[\]]+)', raw_key)
        insert_nested(nested, keys, value)

    ## parsing the specific lead_id#



    goods = None
    delivery_type = None
    delivery_address = None

    lead_id = await get_nested(nested, ["leads", "update", "0", "id"])
    if lead_id is None:
        lead_id = await get_nested(nested, ["leads", "add", "0", "id"])

    modified_by = await get_nested(nested, ["leads", "update", "0", "updated_by"])

    logger.info(f'lead_id: {lead_id}, modified_by: {modified_by}')

    current_time = datetime.datetime.now()
    if lead_id in lead_last_processed:
        if current_time - lead_last_processed[lead_id] < RATE_LIMIT_SECONDS:
            logger.info(f"Rate limit hit for lead {lead_id}, skipping.")
            return HTTP_200_OK

    lead_last_processed[lead_id] = current_time

    updates = await get_nested(nested, ["leads", "update", "0", "custom_fields"])

    if updates:
        for updated_field in updates:
            info = updates[updated_field]
            if info['id'] == '576703':
                order_summary = info["values"]['0']['value']
                goods, delivery_type = await parse_the_cart_field(order_summary)
            if info['id'] == '576719':
                delivery_address = info["values"]['0']['value']
        if goods is not None or delivery_type is not None or delivery_address is not None:
            # check if info is already correct
            current_info = await get_lead_by_id(lead_id)
            current_goods = await get_custom_field_value(current_info, 577313)
            current_delivery_type = await get_custom_field_value(current_info, 577315)
            current_delivery_address = await get_custom_field_value(current_info, 577311)

            ## matching ignoring the spaces
            is_goods_match = await normalize_text(current_goods) == await normalize_text(goods)
            is_delivery_match = await normalize_text(current_delivery_type) == await normalize_text(delivery_type)
            is_address_match = await normalize_text(current_delivery_address) == await normalize_text(delivery_address)

            if is_goods_match and is_delivery_match and is_address_match:
                logger.info("MATCH: Data is identical (ignoring whitespace).")
                return HTTP_200_OK
            else:
                await add_info_from_ms(goods=goods, delivery_type=delivery_type, delivery_address=delivery_address, lead_id=lead_id)
                logger.info("MISMATCH: Updating info...")
                return HTTP_200_OK
        else:
            logger.info(f'lead_id {lead_id}, nothing to update')
    else:
        logger.info(f'lead_id: {lead_id}, no Updates')
    return HTTP_200_OK