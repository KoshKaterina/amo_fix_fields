"""Имена способов доставки: тариф СДЭК и маркеры правил.

Переименование 23.09.2026 (вопрос Кати): «CDEK:» → «СДЭК:», «Доставка курьером по
Москве» → «Курьерская доставка», «Самовывоз из офиса» → «Самовывоз из шоурума».
Старые имена продолжают приходить в старых заказах, поэтому проверяем ОБА набора.
Карта мест, где зашиты имена, - knowledge/imena-dostavki-gde-zashity.md.
"""

from waybill_config import (
    DELIVERY_CDEK_MARKERS,
    DELIVERY_COURIER_OWN_MARKERS,
    DELIVERY_PICKUP_MARKERS,
    parse_tariff,
)

TARIFF_PVZ, TARIFF_POSTAMAT, TARIFF_DOOR = 136, 368, 137

GOODS_LINE = "1. Keystone 3 Pro, 1 шт, 14 990.00 рублей"


def _cart(delivery_name: str, price: str = "390.00") -> str:
    return GOODS_LINE + "\n" + "2. " + delivery_name + ", 1 шт, " + price + " рублей"


# ── тариф СДЭК определяется по строке «Корзины» ────────────────────────────
CASES = {
    # старые имена
    "CDEK: Самовывоз": TARIFF_PVZ,
    "Самовывоз СДЭК": TARIFF_PVZ,
    "CDEK: Посылка склад-постамат": TARIFF_POSTAMAT,
    "CDEK: Посылка склад-дверь": TARIFF_DOOR,
    # новые имена
    "CDEK: Самовывоз из ПВЗ": TARIFF_PVZ,
    "CDEK: Посылка склад-склад": TARIFF_PVZ,
    "СДЭК: Доставка в постамат": TARIFF_POSTAMAT,
    "СДЭК: Курьерская доставка": TARIFF_DOOR,
    "Курьер СДЭК": TARIFF_DOOR,
}
for name, expected in CASES.items():
    got = parse_tariff(_cart(name))
    assert got == expected, "%s: ждали тариф %s, получили %s" % (name, expected, got)

# наша курьерка и самовывоз — не СДЭК, накладной у них нет
for name in ("Доставка курьером по Москве", "Курьерская доставка",
             "Самовывоз из шоурума Sunscrypt"):
    assert parse_tariff(_cart(name, "0.00")) is None, "%s: тариф СДЭК не должен определяться" % name

print("✓ тарифы СДЭК: старые и новые имена")

# ── маркеры правил ─────────────────────────────────────────────────────────
for name in ("Самовывоз из офиса Sunscrypt", "Самовывоз из шоурума Sunscrypt",
             "Самовывоз из Шоурума"):
    assert any(m in name.casefold() for m in DELIVERY_PICKUP_MARKERS), \
        "%s: наш самовывоз не опознан" % name

for name in ("CDEK: Самовывоз", "CDEK: Самовывоз из ПВЗ"):
    assert not any(m in name.casefold() for m in DELIVERY_PICKUP_MARKERS), \
        "%s: ПВЗ перевозчика — не наш самовывоз" % name

for name in ("Доставка курьером по Москве", "Курьерская доставка"):
    assert any(m in name.casefold() for m in DELIVERY_COURIER_OWN_MARKERS), \
        "%s: своя курьерка не опознана" % name

# ⚠️ ключевая мина: имя курьерки СДЭК содержит подстроку нашей курьерки
sdek_courier = "СДЭК: Курьерская доставка".casefold()
assert any(m in sdek_courier for m in DELIVERY_COURIER_OWN_MARKERS), \
    "подстрока пересекается — так и задумано"
assert any(m in sdek_courier for m in DELIVERY_CDEK_MARKERS), \
    "перевозчик должен опознаваться и отсеиваться первым"

print("✓ маркеры правил: самовывоз и курьерка")
