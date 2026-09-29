"""Разовый догон: кто УЖЕ в чате практикума, а сделка на этап не переведена.

Событие `chat_member` приходит только на новые вступления, а на 29.09.2026 в чате
уже сидели 92 человека, из них 68 опознались по полю контакта 571839. Этот прогон
догоняет их один раз; дальше работает academy_chat_join сам.

Запуск ВНУТРИ боевого контейнера:

    docker exec -w /app -e PYTHONPATH=/app amo-fix-fields \\
        python3 academy_chat_join_backfill.py            # холостой ход, ничего не двигает
    docker exec -w /app -e PYTHONPATH=/app amo-fix-fields \\
        python3 academy_chat_join_backfill.py --apply    # боевой прогон

Холостой ход - по умолчанию, и это осознанно: массовый перевод сделок трогает
воронку, по которой работают люди.
"""

import argparse
import asyncio
import sys

import academy_chat_join as join
import amo_service
import api
import waybill_config as wc

PAGE_LIMIT = 250
CONTACT_CHUNK = 100


def _cf(entity: dict, field_id: int):
    return amo_service.get_custom_field_value(entity or {}, field_id)


async def _academy_contact_ids() -> list[int]:
    """Контакты, привязанные к сделкам воронки Академия."""
    ids: set[int] = set()
    page = 1
    while page <= 20:
        data = await amo_service._do_get(
            "/api/v4/leads?filter[pipeline_id]=%s&with=contacts&limit=%s&page=%s"
            % (wc.PIPELINE_ACADEMY, PAGE_LIMIT, page))
        leads = ((data or {}).get("_embedded") or {}).get("leads") or []
        if not leads:
            break
        for lead in leads:
            for contact in ((lead.get("_embedded") or {}).get("contacts") or []):
                try:
                    ids.add(int(contact["id"]))
                except (KeyError, TypeError, ValueError):
                    continue
        if len(leads) < PAGE_LIMIT:
            break
        page += 1
        await asyncio.sleep(0.4)
    return sorted(ids)


async def _telegram_ids(contact_ids: list[int]) -> dict[int, str]:
    """Telegram id по контактам, пачками: 800 отдельных чтений тут излишни."""
    out: dict[int, str] = {}
    for i in range(0, len(contact_ids), CONTACT_CHUNK):
        chunk = contact_ids[i:i + CONTACT_CHUNK]
        query = "&".join("filter[id][]=%s" % c for c in chunk)
        data = await amo_service._do_get("/api/v4/contacts?%s&limit=250" % query)
        for contact in ((data or {}).get("_embedded") or {}).get("contacts") or []:
            raw = _cf(contact, join.FIELD_WAZZUP_TELEGRAM_ID)
            digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
            if digits:
                out[int(contact["id"])] = digits
        await asyncio.sleep(0.4)
    return out


async def main(apply: bool) -> int:
    if not join.configured():
        print("Выключено: нужен ACADEMY_CHAT_JOIN_ENABLED=1, токен бота и чат практикума.")
        return 1
    if not apply:
        # Холостой ход поверх боевых настроек: решение считаем, сделки не трогаем.
        join.ACADEMY_CHAT_JOIN_DRY_RUN = True
    print("режим: %s" % ("БОЕВОЙ (сделки двигаем)" if apply else "холостой ход"))

    api.init_api_pipeline()
    await amo_service.warm_pipeline_cache()
    try:
        contact_ids = await _academy_contact_ids()
        tg_by_contact = await _telegram_ids(contact_ids)
        print("контактов Академии: %s, с Telegram id: %s"
              % (len(contact_ids), len(tg_by_contact)))

        outcomes: dict[str, int] = {}
        members = 0
        for contact_id, user_id in sorted(tg_by_contact.items()):
            try:
                member = await join._telegram(
                    "getChatMember",
                    {"chat_id": wc.ACADEMY_PRACTICUM_CHAT_ID, "user_id": int(user_id)})
            except join.TelegramRefused:
                # «Нет такого участника» Telegram отдаёт ошибкой, а не статусом left.
                continue
            except join.TelegramUnavailable as exc:
                print("  Telegram недоступен (%s) - прерываю прогон" % exc)
                break
            if not join.is_member_status(member):
                continue
            members += 1
            outcome = await join.process_join(user_id, display="контакт %s" % contact_id)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
            if outcome not in ("already_handled", "dry_run"):
                print("  контакт %-9s -> %s" % (contact_id, outcome))
            await asyncio.sleep(0.12)

        print("\nв чате из проверенных: %s" % members)
        for name, count in sorted(outcomes.items(), key=lambda kv: -kv[1]):
            print("  %-18s %s" % (name, count))
        if not apply:
            print("\nЭто был холостой ход. Боевой прогон - с --apply.")
    finally:
        await api.shutdown_api_pipeline()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="двигать сделки (без флага - только отчёт)")
    sys.exit(asyncio.run(main(parser.parse_args().apply)))
