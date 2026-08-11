"""Отправка лога решений распределения лидов в team-panel (11.08.2026).

Одна POST на каждое успешное автораспределение — team-panel хранит долговременную
историю в Postgres (см. app/routers/ingest.py POST /lead-distribution/log), здесь
только отправка. Потеря отдельной строки лога не критична (сама сделка уже
корректно назначена в amoCRM независимо от этого запроса) — поэтому без
retry/бэклога, как в team_panel_client.py, просто best-effort POST: любая
ошибка логируется и проглатывается, никогда не поднимается вызывающему.
"""

import asyncio
import logging

import httpx

from waybill_config import TEAM_PANEL_BASE_URL, TEAM_PANEL_INGEST_TOKEN

logger = logging.getLogger("uvicorn")


async def send(payload: dict) -> None:
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return
    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/lead-distribution/log"
    headers = {"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            logger.warning("lead_distribution_log_client: HTTP %s на /lead-distribution/log", resp.status_code)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("lead_distribution_log_client: сбой отправки лога")
