import asyncio
import logging
from datetime import datetime

from api import add_info_from_ms

logger = logging.getLogger("uvicorn")


async def update_info_later(goods, delivery_type, delivery_address, lead_id, name, execute_after, lead_last_processed, promo_type=None, comment=None):
    max_retries = 5
    base_delay = max(1, int(execute_after))

    for attempt in range(max_retries + 1):
        delay = base_delay if attempt == 0 else base_delay * (2 ** attempt)
        if delay > 0:
            await asyncio.sleep(delay)

        try:
            logger.info(f"Patching scheduled changes in {lead_id} now... attempt {attempt + 1}/{max_retries + 1}")
            is_ok = await add_info_from_ms(
                goods=goods,
                delivery_type=delivery_type,
                delivery_address=delivery_address,
                lead_id=lead_id,
                name=name,
                promo_type=promo_type,
                comment=comment,
            )
            if is_ok:
                lead_last_processed[lead_id] = datetime.now()
                return
        except Exception as e:
            logger.error(f"Queued update failed for lead {lead_id} on attempt {attempt + 1}: {e}")

    logger.error(f"Queued update failed for lead {lead_id} after {max_retries + 1} attempts")
