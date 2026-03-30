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
            result = await add_info_from_ms(
                goods=goods,
                delivery_type=delivery_type,
                delivery_address=delivery_address,
                lead_id=lead_id,
                name=name,
                promo_type=promo_type,
                comment=comment,
            )
            is_ok = bool(result.get("ok")) if isinstance(result, dict) else bool(result)
            retryable = bool(result.get("retryable", not is_ok)) if isinstance(result, dict) else (not is_ok)

            if is_ok:
                lead_last_processed[lead_id] = datetime.now()
                return

            if not retryable:
                status_code = result.get("status_code") if isinstance(result, dict) else None
                logger.error(
                    "Queued update aborted for lead %s on attempt %s due to non-retryable result (status=%s)",
                    lead_id,
                    attempt + 1,
                    status_code,
                )
                return
        except Exception:
            logger.exception(
                "Queued update failed for lead %s on attempt %s",
                lead_id,
                attempt + 1,
            )

    logger.error(f"Queued update failed for lead {lead_id} after {max_retries + 1} attempts")
