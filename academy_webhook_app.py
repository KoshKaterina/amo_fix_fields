"""Isolated zero-downtime entrypoint for the BotHelp Academy webhook."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

import academy_bothelp_upsert
import academy_invite_delivery
from api import init_api_pipeline, shutdown_api_pipeline


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_api_pipeline()
    academy_invite_delivery.start()
    try:
        yield
    finally:
        await academy_invite_delivery.stop()
        await shutdown_api_pipeline()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True, "service": "academy-bothelp-upsert"}


@app.post("/bothelp/academy/{secret}")
async def bothelp_academy(secret: str, request: Request):
    if not academy_bothelp_upsert.authorized(secret):
        raise HTTPException(status_code=404, detail="Not found")
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    result = await academy_bothelp_upsert.process(payload if isinstance(payload, dict) else {})
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result.get("reason", "upsert_failed"))
    return result
