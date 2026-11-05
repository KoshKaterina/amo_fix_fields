"""Оплата Ozon -> входящий платёж в заказе МойСклад (признак - `payedSum`).

Зачем. Менеджер выставляет ссылку Ozon из амо (ozon_invoice), клиент платит, сделка
уезжает в «Оплата получена», а мост amgroup через секунду переводит заказ МС в одноимённый
статус. Деньги при этом в заказ НЕ попадают: у МС нет поля «статус оплаты», есть вычисляемая
сумма `payedSum`, и она растёт только от входящих платежей (`paymentin`), привязанных к заказу.
Без платежа `payedSum` остаётся прежним, и поле «Статус оплаты» (576669) в амо навсегда
«Частично оплачен». Живые случаи 05.11.2026: сделки 36591997 (заказ 09386, доплата 1500 ₽)
и 36592853 (заказ 09458, доплата 1200 ₽) - сайт записал предоплату, доплату через ссылку
Ozon не записал никто.

Что делает. Одну вещь: заводит `paymentin` на сумму Ozon и привязывает к заказу. Статус
заказа не трогает (его ведёт мост), поле 576669 в амо не трогает (его выставит мост из
`payedSum`). Формат платежа повторяет сайтовый, их в МС 4334: организация и контрагент
берутся из заказа, расчётного счёта нет, `operations[{meta, linkedSum}]`.

Решение «писать или нет» принимается по `payedSum` и сумме заказа:
  - платёж с нашим externalCode уже есть - ничего не делаем (идемпотентность);
  - заказ уже оплачен целиком (`payedSum >= sum`) - не пишем: либо офис уже занёс платёж
    руками, либо клиент заплатил дважды, и разбирать это должен человек;
  - сумма Ozon больше остатка (`sum - payedSum`) - не пишем, нужен человек;
  - иначе - пишем платёж ровно на сумму Ozon и сверяем, что `payedSum` вырос.

⚠️ Модуль не знает ни про флаги, ни про амо: флаг и режим «сухого прогона» передаёт
вызывающий (`ozon_invoice`), результат возвращается строкой-исходом и пояснением. Так
его можно проверять отдельно от боевого пути.

⚠️ Пустой ответ МС и «МС не ответил» (None) - разные вещи (03.09.2026 на этом сгорел
order_watchdog). None даёт исход `failed-*` и ничего не пишет.

⚠️ `ms_client.post` неидемпотентен и сам не повторяется. Если он вернул None, платёж МОГ
создаться (таймаут после исполнения), поэтому дальше смотрим по externalCode, а не шлём
второй раз.
"""

import logging

import ms_client
from waybill_config import looks_like_uuid

logger = logging.getLogger("uvicorn")

# Исходы, о которых должен узнать человек (в боевом режиме). Остальные - штатные пропуски.
ATTENTION = frozenset({
    "failed-ms-read", "failed-ms-lookup", "failed-ms-post", "failed-order-shape", "skipped-overpay",
})

# Платежи, которые прямо сейчас создаются. Вебхук и сверка могут принести один и тот же
# платёж одновременно: поиск по externalCode ещё ничего не нашёл, а второй POST уже летит.
# Событийный цикл один, между проверкой и добавлением нет await - гонки нет.
_inflight: set[str] = set()


def payment_code(key: str) -> str:
    """externalCode платежа МС: по нему же ищем дубль. `key` - устойчивый ключ счёта,
    одинаковый для вебхука и сверки (см. ozon_invoice._ms_payment_key)."""
    return f"ozon-{key}"[:255]


def decide(total: int, payed: int, kopecks: int) -> str:
    """Чистое решение по суммам (копейки): create / skipped-zero-sum / skipped-fully-paid /
    skipped-overpay."""
    if total <= 0:
        return "skipped-zero-sum"
    remaining = total - payed
    if remaining <= 0:
        return "skipped-fully-paid"
    if kopecks > remaining:
        return "skipped-overpay"
    return "create"


def _kop(value) -> int:
    try:
        return int(round(float(value or 0)))
    except (TypeError, ValueError):
        return 0


async def _find_payments(code: str) -> list | None:
    """Платежи с таким externalCode. None - МС не ответил (это НЕ «платежа нет»)."""
    data = await ms_client.get("entity/paymentin", params={"filter": f"externalCode={code}"})
    if data is None:
        return None
    return data.get("rows") or []


def _build_body(order: dict, kopecks: int, code: str, key: str) -> dict | None:
    org = (order.get("organization") or {}).get("meta")
    agent = (order.get("agent") or {}).get("meta")
    meta = order.get("meta")
    if not (org and agent and meta):
        return None
    return {
        "organization": {"meta": org},
        "agent": {"meta": agent},
        "sum": kopecks,
        "externalCode": code,
        "description": f"Оплата Ozon Pay (автоматически, amo_fix_fields), счёт {key}",
        "operations": [{"meta": meta, "linkedSum": kopecks}],
    }


async def record_ozon_payment(
    order_uuid, kopecks, key: str, *, dry_run: bool = True,
) -> tuple[str, str]:
    """Записать оплату Ozon в заказ МС. Возвращает (исход, пояснение).

    Исходы: created, dry-run, already-recorded, skipped-inflight, skipped-no-order,
    skipped-no-amount, skipped-zero-sum, skipped-fully-paid, skipped-overpay,
    failed-ms-read, failed-ms-lookup, failed-ms-post, failed-order-shape.

    dry_run=True (умолчание): все чтения МС выполняются, запись - нет. Исход `dry-run`
    значит «платёж был бы создан».
    """
    uuid = str(order_uuid or "").strip()
    if not looks_like_uuid(uuid):
        return "skipped-no-order", "в сделке нет заказа МС (поле 576689 пусто или не UUID)"
    try:
        kopecks = int(kopecks)
    except (TypeError, ValueError):
        kopecks = 0
    if kopecks <= 0:
        return "skipped-no-amount", "сумма оплаты неизвестна"
    code = payment_code(key)
    if code in _inflight:
        return "skipped-inflight", f"платёж {code} уже создаётся параллельно"

    _inflight.add(code)
    try:
        order = await ms_client.get(f"entity/customerorder/{uuid}")
        if not order:
            return "failed-ms-read", f"МС не отдал заказ {uuid}"
        total, payed = _kop(order.get("sum")), _kop(order.get("payedSum"))
        name = order.get("name") or uuid

        existing = await _find_payments(code)
        if existing is None:
            return "failed-ms-lookup", f"МС не ответил на поиск платежа {code}"
        if existing:
            return "already-recorded", f"платёж {code} уже есть в МС (заказ {name})"

        verdict = decide(total, payed, kopecks)
        facts = (f"заказ {name}: сумма {total / 100:.2f}, оплачено {payed / 100:.2f}, "
                 f"оплата Ozon {kopecks / 100:.2f} ₽")
        if verdict != "create":
            logger.info("ms_payment: %s - %s (%s)", code, verdict, facts)
            return verdict, facts

        body = _build_body(order, kopecks, code, key)
        if body is None:
            return "failed-order-shape", f"в заказе {name} нет organization/agent/meta"

        if dry_run:
            logger.info("ms_payment DRY-RUN: создал бы paymentin %s на %.2f ₽ (%s); payedSum был бы %.2f -> %.2f",
                        code, kopecks / 100, facts, payed / 100, (payed + kopecks) / 100)
            return "dry-run", f"{facts}; payedSum {payed / 100:.2f} -> {(payed + kopecks) / 100:.2f}"

        created = await ms_client.post("entity/paymentin", body)
        if created is None:
            # Ответа нет, но документ мог создаться (таймаут после исполнения). Не шлём
            # второй раз - смотрим по externalCode.
            again = await _find_payments(code)
            if again is None:
                return "failed-ms-lookup", f"POST платежа {code} без ответа и проверить не удалось - сверьте заказ {name} вручную"
            if not again:
                return "failed-ms-post", f"МС отклонил или не принял платёж {code} ({facts})"
        # Контроль по признаку payedSum: вырос ли он на сумму платежа.
        after = await ms_client.get(f"entity/customerorder/{uuid}")
        payed_after = _kop((after or {}).get("payedSum"))
        note = f"{facts}; payedSum {payed / 100:.2f} -> {payed_after / 100:.2f}"
        if after is not None and payed_after != payed + kopecks:
            logger.warning("ms_payment: %s записан, но payedSum не сошёлся (ждали %.2f, стало %.2f)",
                           code, (payed + kopecks) / 100, payed_after / 100)
            note += " (ВНИМАНИЕ: payedSum не сошёлся с ожидаемым)"
        logger.info("ms_payment: создан paymentin %s (%s)", code, note)
        return "created", note
    finally:
        _inflight.discard(code)
