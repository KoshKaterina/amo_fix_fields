"""Dry-run / backfill сверка amoCRM → WooCommerce (запуск ЛОКАЛЬНО из терминала).

Показывает (а с --apply — выполняет) простановку статуса 'completed' в WC для всех
заказов, которые по логике Метрики считаются оплаченными (PAID), за окно с
WOO_STATUS_SINCE. Использует ТОТ ЖЕ код, что и боевой синк (woo_status_sync.resolve_target
+ woo_client), поэтому отчёт точно отражает, что сделает сервис.

По умолчанию DRY-RUN: только читает статусы заказов в WC и печатает отчёт; в WC
ничего не пишет. Флаг WOO_STATUS_SYNC_ENABLED для этого скрипта не нужен.

Запуск:
    python3 woo_status_backfill.py            # полный отчёт с WOO_STATUS_SINCE
    python3 woo_status_backfill.py --limit 50  # быстрый сэмпл: 50 сделок на воронку
    python3 woo_status_backfill.py --apply     # РЕАЛЬНО проставить completed
"""

import argparse
import asyncio
import logging
from collections import Counter

import amo_service
import woo_client
import woo_status_sync
from api import init_api_pipeline, shutdown_api_pipeline
from waybill_config import (
    PIPELINE_CLEVER,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    WOO_COMPLETED_STATUS,
    WOO_STATUS_SINCE_TS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("uvicorn")


_RISKY = woo_client.RISKY_STATUSES  # единый источник: cancelled/refunded/failed


async def main(limit: int | None, apply: bool, skip_cancelled: bool) -> None:
    init_api_pipeline()
    if not woo_client.is_configured():
        logger.error("WC_URL/WC_CONSUMER_* не заданы в .env — прерываю.")
        await shutdown_api_pipeline()
        return
    if WOO_STATUS_SINCE_TS is None:
        logger.error("WOO_STATUS_SINCE не задан в .env — прерываю.")
        await shutdown_api_pipeline()
        return

    await woo_client.init()
    # Глушим пер-сделочные INFO из resolve_target (без номера на сайте и т.п.) —
    # для чистого отчёта. Итоги печатаем через print().
    logger.setLevel(logging.WARNING)

    try:
        since = WOO_STATUS_SINCE_TS
        leads_by_id: dict[int, dict] = {}
        for pipeline in (PIPELINE_CLEVER, PIPELINE_OFFICE, PIPELINE_FULFILLMENT):
            batch = await amo_service.get_leads_updated_since(pipeline, since, with_=())
            if limit:
                batch = batch[:limit]
            for ld in batch:
                if ld.get("id") is not None:
                    leads_by_id[ld["id"]] = ld

        print(f"\nСканирую {len(leads_by_id)} сделок (since unix {since}, "
              f"{'apply' if apply else 'DRY-RUN'})...\n")

        seen_sites: dict[str, int] = {}
        buckets: Counter = Counter()
        attention: list[tuple] = []   # cancelled/refunded/failed → would complete
        actions: list[tuple] = []     # (site, lead_id, current_status)
        scanned = 0

        for lid, ld in leads_by_id.items():
            scanned += 1
            if scanned % 200 == 0:
                print(f"  ...{scanned}/{len(leads_by_id)}")
            target = await woo_status_sync.resolve_target({"lead_id": lid}, ld)
            if not target:
                continue
            buckets["paid_with_site"] += 1
            site = target["site"]
            cid = target["canonical"].get("id")
            if site in seen_sites:
                buckets["dup_site"] += 1
                continue
            seen_sites[site] = cid
            try:
                cur = await woo_client.get_order_status(site)
            except woo_client.WooError as exc:
                buckets["wc_error"] += 1
                print(f"  ! WC error site={site}: {exc}")
                continue
            if cur is None:
                buckets["not_found"] += 1
                continue
            if cur == WOO_COMPLETED_STATUS:
                buckets["already_completed"] += 1
                continue
            if cur in _RISKY:
                attention.append((site, cid, cur))
                if skip_cancelled:
                    buckets["skipped_cancelled"] += 1
                    continue
            buckets["would_complete"] += 1
            buckets[f"from::{cur}"] += 1
            actions.append((site, cid, cur))

        # ---- Отчёт ----
        print("\n================ ИТОГО (dry-run) ================")
        print(f"Сделок просканировано:            {len(leads_by_id)}")
        print(f"PAID со номером на сайте:          {buckets['paid_with_site']}")
        print(f"  дубль-заказы (один WC на неск.):  {buckets['dup_site']}")
        print(f"Уникальных заказов WC проверено:   {len(seen_sites)}")
        print(f"  уже completed (пропуск):          {buckets['already_completed']}")
        print(f"  не найдено в WC (удалён):         {buckets['not_found']}")
        print(f"  ошибки WC:                        {buckets['wc_error']}")
        print(f"  БУДЕТ переведено в completed:      {buckets['would_complete']}")
        if buckets["skipped_cancelled"]:
            print(f"  ПРОПУЩЕНО cancelled/refunded/failed: {buckets['skipped_cancelled']} (--skip-cancelled)")
        froms = sorted((k, v) for k, v in buckets.items() if k.startswith("from::"))
        if froms:
            print("    из статусов:")
            for k, v in froms:
                print(f"      {k.split('::',1)[1]:<14} {v}")
        if attention:
            verb = "ПРОПУЩЕНЫ (не трогаем)" if skip_cancelled else "будут переведены в completed"
            print(f"\n  ⚠ ВНИМАНИЕ: {len(attention)} заказов в cancelled/refunded/failed {verb}:")
            for site, cid, cur in attention[:50]:
                print(f"      site={site:<8} lead={cid} (сейчас {cur})")
            if len(attention) > 50:
                print(f"      ... ещё {len(attention) - 50}")
        print("=================================================\n")

        if apply:
            print(f"APPLY: проставляю completed для {len(actions)} заказов...")
            done = Counter()
            for site, cid, cur in actions:
                try:
                    res = await woo_client.complete_order(site)
                    done[res] += 1
                    print(f"  site={site} lead={cid}: {cur} → {res}")
                except woo_client.WooError as exc:
                    done["error"] += 1
                    print(f"  ! site={site} lead={cid}: {exc}")
            print(f"\nГотово: {dict(done)}")
    finally:
        await woo_client.aclose()
        await shutdown_api_pipeline()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="макс. сделок на воронку (сэмпл)")
    ap.add_argument("--apply", action="store_true", help="реально проставить completed в WC")
    ap.add_argument("--skip-cancelled", action="store_true",
                    help="не трогать заказы в cancelled/refunded/failed")
    args = ap.parse_args()
    asyncio.run(main(args.limit, args.apply, args.skip_cancelled))
