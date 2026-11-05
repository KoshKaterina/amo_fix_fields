"""Оплата -> входящий платёж в заказе МойСклад (признак - `payedSum`).

Зачем. Менеджер выставляет ссылку Ozon из амо (ozon_invoice), клиент платит, сделка
уезжает в «Оплата получена», а мост amgroup через секунду переводит заказ МС в одноимённый
статус. Деньги при этом в заказ НЕ попадают: у МС нет поля «статус оплаты», есть вычисляемая
сумма `payedSum`, и она растёт только от входящих платежей (`paymentin`), привязанных к заказу.
Без платежа `payedSum` остаётся прежним, и поле «Статус оплаты» (576669) в амо навсегда
«Частично оплачен». Живые случаи 05.11.2026: сделки 36591997 (заказ 09386, доплата 1500 ₽)
и 36592853 (заказ 09458, доплата 1200 ₽) - сайт записал предоплату, доплату через ссылку
Ozon не записал никто. Второй случай - оплата при выдаче (самовывоз из офиса, наложка):
на 30 свежих сделках Офиса в «Успешно реализовано» `payedSum` равен нулю у 29.

Что делает. Одну вещь: заводит `paymentin` и привязывает к заказу. Статус заказа не трогает
(его ведёт мост), поле 576669 в амо не трогает (его выставит мост из `payedSum`). Формат
платежа повторяет сайтовый, их в МС 4334: организация и контрагент берутся из заказа,
расчётного счёта нет, `operations[{meta, linkedSum}]`. Приходных ордеров (`cashin`) в МС ноль,
поэтому и наличные при выдаче идут тем же `paymentin`.

Два режима, у каждого свой externalCode (по нему же ищем дубль):

  Ozon (`record_ozon_payment`): пишем СТРОГО ту сумму, что оплатил клиент. Решение - по
  накопленному итогу `payedSum + оплата` против суммы заказа (замер 05.11.2026 по 600 заказам:
  `payedSum` до оплаты - ноль или предоплата сайта, отрицательным не бывает ни разу):
    - заказ уже оплачен целиком (`payedSum >= sum`) - НЕ пишем (возможный дубль), но зовём человека;
    - итог меньше суммы заказа - пишем, исход `created-short` (недоплата, алерт логический);
    - итог равен сумме заказа - пишем, исход `created` (идеальный случай, тихо);
    - итог больше суммы заказа - пишем, исход `created-over` (переплата, предупреждение).

  Выдача (`record_office_payment`): сумму считаем сами - остаток `sum - payedSum`, чтобы
  заказ, уже оплаченный онлайн, не получил второй платёж. Остатка нет - молча пропускаем.

⚠️ Модуль не знает ни про флаги, ни про амо, ни про Телеграм: флаг и «сухой прогон»
передаёт вызывающий (`ms_payment_flow`), результат возвращается структурой `Result`.

⚠️ Пустой ответ МС и «МС не ответил» (None) - разные вещи (03.09.2026 на этом сгорел
order_watchdog). None даёт исход `failed-*` и ничего не пишет.

⚠️ `ms_client.post` неидемпотентен и сам не повторяется. Если он вернул None, платёж МОГ
создаться (таймаут после исполнения), поэтому дальше смотрим по externalCode, а не шлём
второй раз.
"""

import logging
from typing import NamedTuple

import ms_client
from waybill_config import looks_like_uuid

logger = logging.getLogger("uvicorn")

# Сбои МС: имеет смысл повторить позже (ms_payment_flow ставит такой платёж в очередь).
RETRIABLE = frozenset({"failed-ms-read", "failed-ms-lookup", "failed-ms-post", "failed-exception"})

# Платежи, которые прямо сейчас создаются. Вебхук и сверка могут принести один и тот же
# платёж одновременно: поиск по externalCode ещё ничего не нашёл, а второй POST уже летит.
# Событийный цикл один, между проверкой и добавлением нет await - гонки нет.
_inflight: set[str] = set()


class Result(NamedTuple):
    """Итог попытки. outcome - исход (см. docstring функций), detail - человеческое
    пояснение, dry_run - платёж НЕ создавался (сухой прогон: outcome показывает, что было
    бы), facts - цифры для уведомления: order, sum, payed, paid, after (в рублях)."""
    outcome: str
    detail: str
    dry_run: bool = False
    facts: dict = {}


def payment_code(key: str) -> str:
    """externalCode платежа МС Ozon: по нему же ищем дубль. `key` - устойчивый ключ счёта,
    одинаковый для вебхука и сверки (см. ozon_invoice._ms_payment_key)."""
    return f"ozon-{key}"[:255]


def office_code(lead_id) -> str:
    """externalCode платежа при выдаче: один на сделку."""
    return f"office-{lead_id}"[:255]


def decide(total: int, payed: int, kopecks: int) -> str:
    """Чистое решение для оплаты Ozon по суммам (копейки): skipped-zero-sum /
    skipped-fully-paid / created-short / created / created-over."""
    if total <= 0:
        return "skipped-zero-sum"
    if payed >= total:
        return "skipped-fully-paid"
    after = payed + kopecks
    if after < total:
        return "created-short"
    if after == total:
        return "created"
    return "created-over"


def _kop(value) -> int:
    try:
        return int(round(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _rub(kop: int) -> float:
    return round(kop / 100, 2)


async def _find_payments(code: str) -> list | None:
    """Платежи с таким externalCode. None - МС не ответил (это НЕ «платежа нет»)."""
    data = await ms_client.get("entity/paymentin", params={"filter": f"externalCode={code}"})
    if data is None:
        return None
    return data.get("rows") or []


def _build_body(order: dict, kopecks: int, code: str, description: str) -> dict | None:
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
        "description": description,
        "operations": [{"meta": meta, "linkedSum": kopecks}],
    }


async def _write(
    uuid: str, order: dict, kopecks: int, code: str, description: str, success: str,
    facts: dict, dry_run: bool,
) -> Result:
    """Общий хвост обоих режимов: собрать платёж, (не) создать, сверить рост payedSum."""
    total, payed = _kop(order.get("sum")), _kop(order.get("payedSum"))
    name = order.get("name") or uuid
    note = (f"заказ {name}: сумма {_rub(total):.2f}, оплачено {_rub(payed):.2f}, "
            f"платёж {_rub(kopecks):.2f} ₽, итог {_rub(payed + kopecks):.2f}")
    body = _build_body(order, kopecks, code, description)
    if body is None:
        return Result("failed-order-shape", f"в заказе {name} нет organization/agent/meta", False, facts)
    if dry_run:
        logger.info("ms_payment DRY-RUN: создал бы paymentin %s: %s -> %s", code, note, success)
        return Result(success, note, True, facts)

    created = await ms_client.post("entity/paymentin", body)
    if created is None:
        # Ответа нет, но документ мог создаться (таймаут после исполнения). Не шлём второй
        # раз - смотрим по externalCode.
        again = await _find_payments(code)
        if again is None:
            return Result("failed-ms-lookup", f"POST платежа {code} без ответа и проверить не удалось "
                          f"- сверьте заказ {name} вручную", False, facts)
        if not again:
            return Result("failed-ms-post", f"МС не принял платёж {code} ({note})", False, facts)
    # Контроль по признаку payedSum: вырос ли он на сумму платежа.
    after = await ms_client.get(f"entity/customerorder/{uuid}")
    payed_after = _kop((after or {}).get("payedSum"))
    if after is not None and payed_after != payed + kopecks:
        logger.warning("ms_payment: %s записан, но payedSum не сошёлся (ждали %.2f, стало %.2f)",
                       code, _rub(payed + kopecks), _rub(payed_after))
        note += " (ВНИМАНИЕ: payedSum не сошёлся с ожидаемым)"
    logger.info("ms_payment: создан paymentin %s: %s -> %s", code, note, success)
    return Result(success, note, False, facts)


async def _prepare(order_uuid, code: str):
    """Общий вход (код платежа уже занят в _inflight вызывающим): чтение заказа и поиск
    платежа с этим кодом. Возвращает (Result | None, uuid, order). Result - досрочный исход."""
    uuid = str(order_uuid or "").strip()
    order = await ms_client.get(f"entity/customerorder/{uuid}")
    if not order:
        return Result("failed-ms-read", f"МС не отдал заказ {uuid}"), uuid, None
    existing = await _find_payments(code)
    name = order.get("name") or uuid
    if existing is None:
        return Result("failed-ms-lookup", f"МС не ответил на поиск платежа {code}"), uuid, order
    if existing:
        return Result("already-recorded", f"платёж {code} уже есть в МС (заказ {name})"), uuid, order
    return None, uuid, order


async def record_ozon_payment(
    order_uuid, kopecks, key: str, *, dry_run: bool = True,
) -> Result:
    """Записать оплату Ozon в заказ МС строго на сумму, которую заплатил клиент.

    Исходы: created, created-short, created-over (платёж записан), already-recorded,
    skipped-inflight, skipped-no-order, skipped-no-amount, skipped-zero-sum,
    skipped-fully-paid (платёж НЕ записан), failed-ms-read, failed-ms-lookup,
    failed-ms-post, failed-order-shape.

    dry_run=True (умолчание): все чтения МС выполняются, запись - нет. Исход показывает, что
    было бы, `Result.dry_run` = True.
    """
    try:
        kopecks = int(kopecks)
    except (TypeError, ValueError):
        kopecks = 0
    if not looks_like_uuid(str(order_uuid or "").strip()):
        return Result("skipped-no-order", "в сделке нет заказа МС (поле 576689 пусто или не UUID)")
    if kopecks <= 0:
        return Result("skipped-no-amount", "сумма оплаты неизвестна")
    code = payment_code(key)
    if code in _inflight:
        return Result("skipped-inflight", f"платёж {code} уже создаётся параллельно")

    _inflight.add(code)
    try:
        early, uuid, order = await _prepare(order_uuid, code)
        if early is not None:
            return early
        total, payed = _kop(order.get("sum")), _kop(order.get("payedSum"))
        facts = {"order": order.get("name") or uuid, "sum": _rub(total), "payed": _rub(payed),
                 "paid": _rub(kopecks), "after": _rub(payed + kopecks)}
        verdict = decide(total, payed, kopecks)
        if verdict in ("skipped-zero-sum", "skipped-fully-paid"):
            detail = (f"заказ {facts['order']}: сумма {facts['sum']:.2f}, оплачено {facts['payed']:.2f}, "
                      f"оплата Ozon {facts['paid']:.2f} ₽")
            logger.info("ms_payment: %s - %s (%s)", code, verdict, detail)
            return Result(verdict, detail, dry_run, facts)
        return await _write(
            uuid, order, kopecks, code,
            f"Оплата Ozon Pay (автоматически, amo_fix_fields), счёт {key}", verdict, facts, dry_run,
        )
    finally:
        _inflight.discard(code)


async def record_office_payment(
    order_uuid, key, *, label: str = "оплата при выдаче", dry_run: bool = True,
) -> Result:
    """Записать оплату при выдаче (самовывоз из офиса, наложка): платёж на ОСТАТОК
    `sum - payedSum`, чтобы после него `payedSum == sum`.

    Исходы: created (платёж записан), already-recorded, skipped-inflight, skipped-no-order,
    skipped-zero-sum, skipped-fully-paid (остатка нет - заказ уже оплачен, это норма),
    failed-ms-read, failed-ms-lookup, failed-ms-post, failed-order-shape.
    """
    code = office_code(key)
    if not looks_like_uuid(str(order_uuid or "").strip()):
        return Result("skipped-no-order", "в сделке нет заказа МС (поле 576689 пусто или не UUID)")
    if code in _inflight:
        return Result("skipped-inflight", f"платёж {code} уже создаётся параллельно")

    _inflight.add(code)
    try:
        early, uuid, order = await _prepare(order_uuid, code)
        if early is not None:
            return early
        total, payed = _kop(order.get("sum")), _kop(order.get("payedSum"))
        name = order.get("name") or uuid
        facts = {"order": name, "sum": _rub(total), "payed": _rub(payed)}
        if total <= 0:
            return Result("skipped-zero-sum", f"заказ {name}: сумма {_rub(total):.2f}", dry_run, facts)
        remainder = total - payed
        facts.update(paid=_rub(max(remainder, 0)), after=_rub(max(total, payed)))
        if remainder <= 0:
            return Result("skipped-fully-paid",
                          f"заказ {name}: оплачено {_rub(payed):.2f} из {_rub(total):.2f} - остатка нет",
                          dry_run, facts)
        return await _write(
            uuid, order, remainder, code,
            f"Оплата при выдаче ({label}), автоматически, amo_fix_fields, сделка {key}",
            "created", facts, dry_run,
        )
    finally:
        _inflight.discard(code)
