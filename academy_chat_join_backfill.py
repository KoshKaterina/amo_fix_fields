"""Разовый догон: кто УЖЕ в чате практикума, а сделка на этап не переведена.

Событие `chat_member` приходит только на новые вступления, а на 29.09.2026 в чате уже
сидели 92 человека, из них 68 опознались по полю контакта 571839. Этот прогон догоняет
их один раз; дальше работает academy_chat_join сам.

Запуск ВНУТРИ боевого контейнера:

    docker exec -w /app -e PYTHONPATH=/app amo-fix-fields \\
        python3 academy_chat_join_backfill.py               # холостой ход
    docker exec -w /app -e PYTHONPATH=/app amo-fix-fields \\
        python3 academy_chat_join_backfill.py --apply       # боевой прогон
    ... --apply --limit 3                                   # первая волна

Холостой ход - по умолчанию, и это осознанно: массовый перевод сделок трогает воронку,
по которой работают люди.

⚠️ Между ЛЮБЫМИ запросами к amo держим паузу `--amo-pause` (по умолчанию 5 секунд,
требование Кати 29.09.2026). Пауза навешена на транспорт `amo_service`, а не на цикл:
так под неё попадают и запросы, которые делает сам модуль автоперехода внутри.

⚠️ Перевод сделки между этапами в amo способен запускать автоматику этапа и отправлять
сообщения живым клиентам. Поэтому прогон идёт волнами: `--limit 3`, проверка карточек,
и только потом остальные.
"""

import argparse
import asyncio
import collections
import sys
import time

import academy_chat_join as join
import amo_service
import api
import waybill_config as wc

PAGE_LIMIT = 250
CONTACT_CHUNK = 100


def install_amo_pause(seconds: float) -> None:
    """Повесить паузу на транспорт amo: и на чтение, и на запись.

    Дешевле и надёжнее, чем расставлять sleep по циклам: внутренние запросы
    academy_chat_join (поиск контакта, чтение сделок, patch) тоже замедляются.
    """
    state = {"last": 0.0}
    original_get = amo_service._do_get
    original_patch = amo_service._do_patch

    async def wait() -> None:
        left = seconds - (time.monotonic() - state["last"])
        if left > 0:
            await asyncio.sleep(left)
        state["last"] = time.monotonic()

    async def slow_get(*args, **kwargs):
        await wait()
        return await original_get(*args, **kwargs)

    async def slow_patch(*args, **kwargs):
        await wait()
        return await original_patch(*args, **kwargs)

    amo_service._do_get = slow_get
    amo_service._do_patch = slow_patch


async def collect_academy(verbose: bool = True):
    """Один проход по воронке: сделки со статусами И карта контакт → его сделки.

    Сделки читаем с `with=contacts`, поэтому карта строится без отдельного запроса
    на каждого человека - при паузе 5 секунд это разница между семью минутами и часом.
    """
    leads_of_contact: dict[int, list[dict]] = collections.defaultdict(list)
    leads_total = 0
    page = 1
    while page <= 20:
        data = await amo_service._do_get(
            "/api/v4/leads?filter[pipeline_id]=%s&with=contacts&limit=%s&page=%s"
            % (wc.PIPELINE_ACADEMY, PAGE_LIMIT, page))
        leads = ((data or {}).get("_embedded") or {}).get("leads") or []
        if not leads:
            break
        leads_total += len(leads)
        for lead in leads:
            for contact in ((lead.get("_embedded") or {}).get("contacts") or []):
                try:
                    leads_of_contact[int(contact["id"])].append(lead)
                except (KeyError, TypeError, ValueError):
                    continue
        if len(leads) < PAGE_LIMIT:
            break
        page += 1
    if verbose:
        print("  сделок в воронке: %s, контактов при них: %s"
              % (leads_total, len(leads_of_contact)))
    return leads_of_contact


async def telegram_ids(contact_ids: list[int]) -> dict[int, str]:
    """Telegram id по контактам, пачками по 100."""
    out: dict[int, str] = {}
    for i in range(0, len(contact_ids), CONTACT_CHUNK):
        chunk = contact_ids[i:i + CONTACT_CHUNK]
        query = "&".join("filter[id][]=%s" % c for c in chunk)
        data = await amo_service._do_get("/api/v4/contacts?%s&limit=250" % query)
        for contact in ((data or {}).get("_embedded") or {}).get("contacts") or []:
            raw = amo_service.get_custom_field_value(
                contact, join.FIELD_WAZZUP_TELEGRAM_ID)
            digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
            if digits:
                out[int(contact["id"])] = digits
    return out


async def in_chat(user_id: str) -> bool:
    """Человек сейчас в чате практикума? Telegram - не amo, пауза тут своя."""
    try:
        member = await join._telegram(
            "getChatMember",
            {"chat_id": wc.ACADEMY_PRACTICUM_CHAT_ID, "user_id": int(user_id)})
    except join.TelegramRefused:
        # «Нет такого участника» Telegram отдаёт ошибкой, а не статусом left.
        return False
    return join.is_member_status(member)


async def main(apply: bool, limit: int, pause: float) -> int:
    if not join.configured():
        print("Выключено: нужен ACADEMY_CHAT_JOIN_ENABLED=1, токен бота и чат практикума.")
        return 1

    # ⚠️ В боевом прогоне холостой ход снимаем ЯВНО: в боевом .env стоит DRY_RUN=1,
    # и без этой строки --apply молча ничего бы не двинул.
    join.ACADEMY_CHAT_JOIN_DRY_RUN = not apply
    print("режим: %s, пауза между запросами amo: %s с, предел: %s"
          % ("БОЕВОЙ (сделки двигаем)" if apply else "холостой ход",
             pause, limit or "без предела"))

    install_amo_pause(pause)
    api.init_api_pipeline()
    await amo_service.warm_pipeline_cache()
    try:
        leads_of_contact = await collect_academy()
        tg_by_contact = await telegram_ids(sorted(leads_of_contact))
        print("  из них с Telegram id: %s" % len(tg_by_contact))

        # Кого проверять в Telegram: только тех, у кого ЕСТЬ что двигать. Так мы не
        # дёргаем Telegram по людям, чья сделка всё равно на защищённом этапе.
        candidates = []
        for contact_id, user_id in sorted(tg_by_contact.items()):
            lead = join.pick_lead(leads_of_contact.get(contact_id) or [])
            if lead is not None:
                candidates.append((contact_id, user_id, lead))
        print("  у кого сделка на разрешённом этапе: %s\n" % len(candidates))

        outcomes: collections.Counter = collections.Counter()
        members = 0
        moved_log = []
        for contact_id, user_id, lead in candidates:
            if not await in_chat(user_id):
                outcomes["в чате нет"] += 1
                continue
            members += 1
            was = int(lead.get("status_id") or 0)
            outcome = await join.process_join(
                user_id, display="контакт %s" % contact_id)
            outcomes[outcome] += 1
            mark = ""
            if outcome == "moved":
                moved_log.append((lead.get("id"), was))
                mark = "  было %s -> стало %s" % (was, join.STATUS_ACADEMY_JOINED_CHAT)
            print("  контакт %-9s сделка %-9s %-16s%s"
                  % (contact_id, lead.get("id"), outcome, mark))
            if limit and len(moved_log) >= limit:
                print("\n  предел %s достигнут - останавливаюсь" % limit)
                break
            await asyncio.sleep(0.15)

        print("\n=== итог ===")
        print("  из проверенных в чате: %s" % members)
        for name, count in outcomes.most_common():
            print("  %-18s %s" % (name, count))
        if moved_log:
            print("\n=== для отката: сделка -> прежний этап ===")
            for lead_id, was in moved_log:
                print("  %s -> %s" % (lead_id, was))
        if not apply:
            print("\nЭто был холостой ход. Боевой прогон - с --apply.")
    finally:
        await api.shutdown_api_pipeline()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="двигать сделки (без флага - только отчёт)")
    parser.add_argument("--limit", type=int, default=0,
                        help="остановиться после N переведённых сделок (волна)")
    parser.add_argument("--amo-pause", type=float, default=5.0,
                        help="пауза между запросами к amo, секунды")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.apply, args.limit, args.amo_pause)))
