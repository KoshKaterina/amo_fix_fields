import asyncio
import logging
from datetime import datetime

from api import add_info_from_ms

logger = logging.getLogger("uvicorn")


async def update_info_later(goods, delivery_type, delivery_address, lead_id, execute_after, lead_last_processed):
    try:
        await asyncio.sleep(execute_after)
        logger.info(f"Patching scheduled changes in {lead_id} now...")
        await add_info_from_ms(goods=goods, delivery_type=delivery_type, delivery_address=delivery_address, lead_id=lead_id)
        lead_last_processed[lead_id] = datetime.now()
    except Exception as e:
        logger.error(e)