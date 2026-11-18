"""Коллтрекинг → Яндекс Метрика: факт звонка с сайта как цель «Звонок с сайта».

Решение Кати 18.11.2026: в Метрике должны быть видны источники звонков, как у целей «Заказ» и
лидов. Метрика привязывает звонок к последнему визиту посетителя до звонка, дальше звонки видны в
отчёте «Источники, сводка» (группа «Переходы по рекламе» и разбивка по кампаниям, метки UTM) при
выборе цели «Звонок с сайта». Цель Метрика создаёт сама при первой загрузке (new_goal_name).

Что отправляем: только факт звонка - ClientID посетителя и время начала звонка. Длительность,
отвечен ли и номер не нужны («отслеживаем факт звонка, а не принятие»).

Кого отправляем:
  * только однозначные склейки клика по номеру со звонком (спорные - нет: неясно, чей ClientID);
  * только с ClientID (без него Метрика звонок к визиту не привяжет);
  * уникальные клиенты: тот же ClientID по тому же источнику повторно не отправляем 3 дня
    (CALL_CLICK_METRIKA_REPEAT_DAYS). Другой источник - отправляем: клиент засчитывается
    по каждому источнику.

ClientID храним не дольше 3 дней (call_click_store.metrika_calls чистится по сроку повтора).

Env:
    CALL_CLICK_METRIKA=1                   включатель, по умолчанию выключено
    METRIKA_TOKEN, METRIKA_COUNTER_ID      те же, что у выгрузки заказов (metrika_sync)
    CALL_CLICK_METRIKA_GOAL="Звонок с сайта"
    CALL_CLICK_METRIKA_REPEAT_DAYS=3
"""

import asyncio
import logging
import os
import time
from urllib.parse import urlsplit

import httpx

import call_click_store as store

logger = logging.getLogger("uvicorn.error")

ENABLED = os.getenv("CALL_CLICK_METRIKA", "").strip().lower() in ("1", "true", "yes", "on")
TOKEN = os.getenv("METRIKA_TOKEN", "").strip()
COUNTER_ID = os.getenv("METRIKA_COUNTER_ID", "").strip()
GOAL_NAME = os.getenv("CALL_CLICK_METRIKA_GOAL", "Звонок с сайта").strip() or "Звонок с сайта"
try:
    REPEAT_S = int(os.getenv("CALL_CLICK_METRIKA_REPEAT_DAYS", "3")) * 86400
except ValueError:
    REPEAT_S = 3 * 86400
API = "https://api-metrika.yandex.net"


def is_enabled() -> bool:
    return ENABLED and bool(TOKEN) and COUNTER_ID.isdigit()


def source_key(click: dict) -> str:
    """Источник для правила уникальности: метки рекламы, иначе сайт-источник, иначе прямой заход."""
    utm = click.get("utm") or {}
    if any(utm.get(k) for k in ("utm_source", "utm_medium", "utm_campaign")):
        return "utm:" + "|".join(utm.get(k, "") for k in ("utm_source", "utm_medium", "utm_campaign"))
    ref = click.get("referrer") or ""
    host = (urlsplit(ref).hostname or "").lower() if ref else ""
    if host.startswith("www."):
        host = host[4:]
    return "ref:" + host if host else "direct"


def build_csv(client_id: str, call_ts: int) -> bytes:
    return f"ClientId,DateTime\n{client_id},{int(call_ts)}\n".encode()


async def _post(csv_body: bytes, comment: str) -> dict:
    """Загрузка звонков Метрики. Вынесено отдельно, чтобы тесты подменяли сеть."""
    url = f"{API}/management/v1/counter/{COUNTER_ID}/offline_conversions/upload_calls"
    params = {"client_id_type": "CLIENT_ID", "new_goal_name": GOAL_NAME, "comment": comment[:250]}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, params=params, headers={"Authorization": f"OAuth {TOKEN}"},
                                 files={"file": ("calls.csv", csv_body, "text/csv")})
    if resp.status_code != 200:
        raise RuntimeError(f"http {resp.status_code}: {resp.text[:300]}")
    return (resp.json() or {}).get("uploading") or {}


async def report_call(click: dict, call_id: str, call_ts: float) -> str:
    """Решает и отправляет. Возвращает итог для журнала и тестов:
    off / no-client-id / repeat / sent / error."""
    if not is_enabled():
        return "off"
    client_id = click.get("client_id") or ""
    if not client_id:
        logger.info("call_click: Метрика - у клика к звонку %s нет ClientID, не отправляю", call_id)
        return "no-client-id"
    key = source_key(click)
    now = time.time()
    if not await asyncio.to_thread(store.metrika_claim, call_id, client_id, key, float(call_ts), REPEAT_S, now):
        logger.info("call_click: Метрика - тот же клиент по тому же источнику (%s) за %s дн., звонок %s "
                    "не засчитываю", key, REPEAT_S // 86400, call_id)
        return "repeat"
    try:
        uploading = await _post(build_csv(client_id, int(call_ts)), f"call {call_id} {key}")
    except Exception as exc:
        logger.warning("call_click: Метрика - загрузка звонка %s не удалась: %s", call_id, exc)
        await asyncio.to_thread(store.metrika_release, call_id)
        return "error"
    await asyncio.to_thread(store.metrika_mark, call_id, str(uploading.get("id") or ""), str(uploading.get("status") or ""))
    logger.info("call_click: Метрика - звонок %s отправлен (источник %s, загрузка %s, статус %s)",
                call_id, key, uploading.get("id"), uploading.get("status"))
    return "sent"
