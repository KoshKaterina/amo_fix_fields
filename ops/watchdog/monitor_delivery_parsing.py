"""Сторож после хотфикса имён доставки (23.09.2026). Только чтение amoCRM.

Смотрит сделки, обновлённые за последние N часов, и отвечает на три вопроса:
  1. корзина и доставка разделились - «Тип доставки» (577315) заполнен там, где
     в «Корзине» (576703) есть доставочная строка;
  2. МойСклад доехал - у сделки есть «ID Заказа» (576689);
  3. распределение отработало - ответственный проставлен, самовывоз не ушёл в пул.

Запуск: python ops/watchdog/monitor_delivery_parsing.py [часов] [--since-ts UNIX]
Токен берётся из .env корня проекта (переопределяется AMO_FIX_FIELDS_ROOT).
"""
import os
import re
import sys
import time
import collections

import httpx

ROOT = os.environ.get("AMO_FIX_FIELDS_ROOT") or os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE = "https://new5a2e8ea7b16b4.amocrm.ru"
LEAD_URL = BASE + "/leads/detail/"

FIELD_CART = 576703
FIELD_DELIVERY = 577315
FIELD_MS_ORDER = 576689
FIELD_COMPOSITION = 577313

# как на проде после хотфикса
PREFIXES = ("cdek", "сдэк", "доставка", "курьер", "самовывоз",
            "наценка за наложенный платеж", "почта россии")
TARIFF_PATTERNS = ("CDEK: Самовывоз", "Самовывоз СДЭК", "Посылка склад-склад",
                   "CDEK: Посылка склад-постамат", "СДЭК: Доставка в постамат",
                   "Посылка склад-дверь", "СДЭК: Курьерская доставка", "Курьер СДЭК")
PICKUP_MARKERS = ("самовывоз из офиса", "самовывоз из шоурума")
DLV_RX = re.compile("(cdek|сдэк|достав|самовывоз|почт[аы]|курьер|постамат|пвз|boxberry|dpd)", re.I)

ZUBALIY = 13963494
# Поля заполняются не мгновенно: вебхук → очередь → PATCH. На живой сделке
# 36562137 (23.09) путь занял 76 секунд. Сделки моложе этого порога показываем
# отдельной строкой «ещё в обработке», а не как поломку.
GRACE_S = 300


def env_token():
    with open(os.path.join(ROOT, ".env"), encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("TOKEN="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("TOKEN не найден в .env")


def cf(lead, fid):
    for f in lead.get("custom_fields_values") or []:
        if f.get("field_id") == fid:
            vals = f.get("values") or []
            return vals[0].get("value") if vals else None
    return None


def fetch(since_ts, token):
    rows = []
    with httpx.Client(timeout=30) as c:
        for page in range(1, 11):
            r = c.get(BASE + "/api/v4/leads", headers={"Authorization": "Bearer " + token},
                      params={"page": page, "limit": 250,
                              "filter[updated_at][from]": since_ts,
                              "with": "tags"})
            if r.status_code == 204:
                break
            r.raise_for_status()
            leads = (r.json().get("_embedded") or {}).get("leads") or []
            rows.extend(leads)
            if len(leads) < 250:
                break
            time.sleep(1)
    return rows


def main():
    hours = 2.0
    since = None
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--since-ts" and i + 1 < len(args):
            since = int(args[i + 1])
        elif not a.startswith("--"):
            hours = float(a)
    if since is None:
        since = int(time.time() - hours * 3600)

    rows = fetch(since, env_token())
    with_cart = [l for l in rows if (cf(l, FIELD_CART) or "").strip()]

    unparsed = []        # доставка в корзине есть, «Тип доставки» пуст
    in_flight = []       # то же, но сделка совсем свежая — ждём очередь
    no_tariff = []       # СДЭК в корзине, тариф не определится
    no_ms = []           # нет «ID Заказа» МойСклада
    no_responsible = []  # ответственного нет вовсе
    pickup = []          # наш самовывоз: кто ответственный
    names = collections.Counter()

    for l in with_cart:
        cart = str(cf(l, FIELD_CART))
        dtype = str(cf(l, FIELD_DELIVERY) or "").strip()
        lid = l["id"]

        has_delivery_line = False
        for it in re.findall(r"^\s*\d+\.\s*(.+)$", cart, flags=re.MULTILINE):
            line = it.strip()
            if DLV_RX.search(line):
                has_delivery_line = True
                names[line.split(",")[0].strip()] += 1
                if not line.casefold().startswith(PREFIXES):
                    unparsed.append((lid, line.split(",")[0].strip()))

        if has_delivery_line and not dtype:
            if time.time() - l.get("created_at", 0) < GRACE_S:
                in_flight.append((lid, "создана %s" % time.strftime("%H:%M:%S", time.localtime(l["created_at"]))))
            else:
                unparsed.append((lid, "«Тип доставки» пуст"))

        low = cart.lower()
        if ("сдэк" in low or "cdek" in low) and not any(p in cart for p in TARIFF_PATTERNS):
            no_tariff.append(lid)

        if not str(cf(l, FIELD_MS_ORDER) or "").strip():
            no_ms.append(lid)

        if not l.get("responsible_user_id"):
            no_responsible.append(lid)

        if any(m in dtype.casefold() for m in PICKUP_MARKERS):
            pickup.append((lid, l.get("responsible_user_id")))

    print("окно: с %s, сделок обновлено %d, из них с корзиной %d"
          % (time.strftime("%d.%m %H:%M", time.localtime(since)), len(rows), len(with_cart)))
    print()
    print("имена услуг доставки в окне:")
    for name, n in names.most_common(15):
        mark = "ок" if name.casefold().startswith(PREFIXES) else "НЕ ЛОВИТСЯ"
        print("  %4d  %-11s %s" % (n, mark, name))
    if not names:
        print("  (доставочных строк не было)")

    def block(title, items, fmt=lambda x: str(x)):
        print()
        if not items:
            print("%s: 0 ✓" % title)
            return
        print("%s: %d ⚠️" % (title, len(items)))
        for x in items[:10]:
            print("   " + fmt(x))

    block("ещё в обработке (свежие, поля вот-вот встанут)", in_flight,
          lambda x: "%s%s  — %s" % (LEAD_URL, x[0], x[1]))
    block("разбор корзины не сработал", unparsed,
          lambda x: "%s%s  — %s" % (LEAD_URL, x[0], x[1]))
    block("СДЭК без тарифа (накладная не создастся)", no_tariff,
          lambda x: LEAD_URL + str(x))
    block("нет «ID Заказа» МойСклада", no_ms, lambda x: LEAD_URL + str(x))
    block("без ответственного", no_responsible, lambda x: LEAD_URL + str(x))

    print()
    if pickup:
        print("наш самовывоз в окне: %d" % len(pickup))
        for lid, uid in pickup[:10]:
            who = "Зубалий" if uid == ZUBALIY else ("user %s" % uid if uid else "НЕТ")
            print("   %s%s  ответственный: %s" % (LEAD_URL, lid, who))
    else:
        print("наш самовывоз в окне: 0")

    bad = len(unparsed) + len(no_tariff) + len(no_responsible)
    print()
    print("ИТОГ: " + ("всё чисто" if bad == 0 else "ЕСТЬ ЧТО СМОТРЕТЬ (%d)" % bad))


if __name__ == "__main__":
    main()
