import datetime
import logging
import re
from pprint import pprint

from fastapi import FastAPI, Request, BackgroundTasks
from starlette.status import HTTP_200_OK

from api import add_info_from_ms, get_lead_by_id
from help_function import parse_the_cart_field, get_nested, get_custom_field_value, normalize_text
from memory import update_info_later

app = FastAPI()

logger = logging.getLogger("uvicorn")

lead_last_processed = {}
RATE_LIMIT_SECONDS = 3


def insert_nested(data, keys, value):
    cur = data
    for k in keys[:-1]:
        if k not in cur:
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value

@app.post("/lead_change")
async def lead_change(request: Request, background_tasks: BackgroundTasks):
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
    lead_name = None

    lead_id = await get_nested(nested, ["leads", "update", "0", "id"])
    if lead_id is None:
        lead_id = await get_nested(nested, ["leads", "add", "0", "id"])

    modified_by = await get_nested(nested, ["leads", "update", "0", "updated_by"])

    logger.info(f'lead_id: {lead_id}, modified_by: {modified_by}')


    updates = await get_nested(nested, ["leads", "update", "0", "custom_fields"])

    if updates:
        for updated_field in updates:
            info = updates[updated_field]
            if info['id'] == '576703':
                order_summary = info["values"]['0']['value']
                goods, delivery_type = await parse_the_cart_field(order_summary)
            if info['id'] == '576719':
                delivery_address = info["values"]['0']['value']
            if info['id'] == '576720':
                lead_name = info["values"]['0']['value']
        if goods is not None or delivery_type is not None or delivery_address is not None or lead_id is not None:
            # check if info is already correct
            current_info = await get_lead_by_id(lead_id)
            current_goods = await get_custom_field_value(current_info, 577313)
            current_delivery_type = await get_custom_field_value(current_info, 577315)
            current_delivery_address = await get_custom_field_value(current_info, 577311)
            current_lead_name = await get_custom_field_value(current_info, 576720)

            ## matching ignoring the spaces
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
                is_lead_match = await normalize_text(current_lead_name) == await normalize_text(lead_name)
            else:
                is_lead_match = True

            if is_goods_match and is_delivery_match and is_address_match and is_lead_match:
                logger.info("MATCH: Data is identical (ignoring whitespace).")
                return HTTP_200_OK
            else:
                current_time = datetime.datetime.now()
                if lead_id in lead_last_processed:
                    if (current_time - lead_last_processed[lead_id]).seconds < RATE_LIMIT_SECONDS:
                        logger.info(f"Rate limit hit for lead {lead_id}")
                        execute_after_seconds = (current_time - lead_last_processed[lead_id]).seconds
                        logger.info(f"Will handle in {execute_after_seconds} seconds")
                        background_tasks.add_task(update_info_later, goods, delivery_type, delivery_address, lead_id, lead_name, execute_after_seconds, lead_last_processed)
                        return HTTP_200_OK
                    else:
                        logger.info(f'No limits are hit, updating lead {lead_id}')
                        lead_last_processed[lead_id] = current_time
                        await add_info_from_ms(goods=goods, delivery_type=delivery_type,
                                               delivery_address=delivery_address, lead_id=lead_id, name=lead_name)
                        logger.info("MISMATCH: Updating info...")
                else:
                    lead_last_processed[lead_id] = current_time
                    await add_info_from_ms(goods=goods, delivery_type=delivery_type, delivery_address=delivery_address, lead_id=lead_id, name=lead_name)
                    logger.info("MISMATCH: Updating info...")
                    return HTTP_200_OK
        else:
            logger.info(f'lead_id {lead_id}, nothing to update')
    else:
        logger.info(f'lead_id: {lead_id}, no Updates')
    return HTTP_200_OK