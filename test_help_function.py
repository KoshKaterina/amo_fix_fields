"""Юнит-тест разбора состава заказа (МойСклад → «Корзина»/«Тип доставки»).

Регресс сделки 36545673 (31.08.2026, вопрос Кати): позиция доставки «Почта
России» не входила в DELIVERY_PREFIXES → startswith не срабатывал, строка
доставки оставалась в товарах («Корзина»), а «Тип доставки» (577315) вообще
не патчился (api.add_info_from_ms пропускает пустой delivery_type).
"""

import asyncio

from help_function import parse_the_cart_field


def _parse(data: str):
    return asyncio.run(parse_the_cart_field(data))

# сделка 36545673 — «Почта России» раньше не распознавалась как доставка
goods, delivery = _parse(
    "1. Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля\n"
    "2. Почта России, 1 шт, 390.00 рублей"
)
assert goods == "Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля"
assert delivery == "Почта России, 390.00 рублей"  # «1 шт» — количество, вырезается

# уже работавшие варианты не сломались
goods, delivery = _parse(
    "1. Keystone 3 Pro, 1 шт, 14 990.00 рублей\n"
    "2. Самовывоз из офиса Sunscrypt, 1 шт, 0.00 рублей"
)
assert goods == "Keystone 3 Pro, 1 шт, 14 990.00 рублей"
assert delivery == "Самовывоз из офиса Sunscrypt, 0.00 рублей"

goods, delivery = _parse("1. Trezor Safe 3 Bitcoin-only, 1 шт, 7 890.00 рублей\n2. CDEK, 1 шт, 500.00 рублей")
assert goods == "Trezor Safe 3 Bitcoin-only, 1 шт, 7 890.00 рублей"
assert delivery == "CDEK, 500.00 рублей"

# без доставки в составе — только товары
goods, delivery = _parse("1. YubiKey 5 NFC, 4 шт, 24 360.00 рублей")
assert goods == "YubiKey 5 NFC, 4 шт, 24 360.00 рублей"
assert delivery == ""
