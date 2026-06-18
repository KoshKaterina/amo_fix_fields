"""Однократная сверка amoCRM → Яндекс.Метрика CDP (запуск ЛОКАЛЬНО из терминала).

Что делает:
  - проходит сделки 3 воронок (CLEVER / Офис / Фулфилмент), изменённые за окно
    в `days` дней, той же логикой, что и realtime (metrika_sync.process_sync);
  - пересобирает корректную строку каждого заказа и грузит её в CDP с
    merge_mode=UPDATE → ПЕРЕЗАПИСЫВАЕТ существующие записи по id заказа.

Что чинит в уже накопленных данных CDP:
  - дубли «оригинал + дубликат под своим id» — схлопываются на id оригинала CLEVER;
  - неверный/залипший статус и «прилипшую»/обнулённую выручку (revenue шлётся явно);
  - подтягивает client_id/email/phone, если они появились на сделке.

Чего НЕ делает (ограничение API):
  - не удаляет «лишние» исторические заказы — поштучного удаления в CDP нет.
    От нового мусора защищает гард по дате: задай METRIKA_SINCE=YYYY-MM-DD в .env,
    тогда сверка не трогает заказы, созданные до старта интеграции.

Требует в .env (локально уже есть): TOKEN (amo), METRIKA_TOKEN, METRIKA_COUNTER_ID.
Пишет в БОЕВОЙ счётчик Метрики — это не dry-run.

Запуск:
    python3 metrika_backfill.py          # окно 30 дней
    python3 metrika_backfill.py 14        # окно 14 дней
"""

import asyncio
import logging
import sys

import metrika_sync
from api import init_api_pipeline, shutdown_api_pipeline
from waybill_config import METRIKA_COUNTER_ID, METRIKA_SINCE_TS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("uvicorn")


async def main(days: int) -> None:
    init_api_pipeline()
    try:
        await metrika_sync.init()
        if not metrika_sync.is_enabled():
            logger.error(
                "Metrika sync ВЫКЛЮЧЕН (нет METRIKA_TOKEN / counter в .env) — прерываю."
            )
            return
        logger.info(
            "Backfill: counter=%s, окно=%s дн., гард по дате=%s",
            METRIKA_COUNTER_ID, days,
            "выкл" if METRIKA_SINCE_TS is None else f"с unix {METRIKA_SINCE_TS}",
        )
        await metrika_sync.reconcile_window(days)
    finally:
        await metrika_sync.shutdown()
        await shutdown_api_pipeline()


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    asyncio.run(main(days))
