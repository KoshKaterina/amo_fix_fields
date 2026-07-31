"""Юнит-тест наименования товара в накладной СДЭК (без сети/прода).

СДЭК требует, чтобы в накладной было описано, что едет (Катя, 31.07.2026, MAG-303):
в items[].name уходит «Аппаратный кошелёк <наименования из состава 577313>», а не имя
сделки amo. Строки состава ниже — дословно из живых сделок воронки Офис (срез 31.07.2026).
"""

from waybill_config import (
    CDEK_ITEM_NAME_MAX_LEN,
    CDEK_ITEM_NAME_PREFIX,
    build_cdek_item_name,
    parse_composition_items,
)

# --- парс состава ----------------------------------------------------------

# сделка 36525181
assert parse_composition_items("Trezor Safe 3 Bitcoin-only, 1 шт, 7 890.00 рублей") == [
    ("Trezor Safe 3 Bitcoin-only", 1)
]
# сделка 36520265 — количество больше одного
assert parse_composition_items("YubiKey 5 NFC, 4 шт, 24 360.00 рублей") == [("YubiKey 5 NFC", 4)]
# сделка 36398241 — вариант БЕЗ «шт» (24 из 699 живых сделок), строгий регэксп листа сборки его не видит
assert parse_composition_items("YubiKey 5 NFC, 1 , 6 990.00 рублей") == [("YubiKey 5 NFC", 1)]
# сделка 36524723 — несколько позиций, каждая со своей строки
assert parse_composition_items(
    "Keystone 3 Pro, 1 шт, 13 491.00 рубль\n"
    "Чехол Keystone Wallet Pouch 2, 1 шт, 1 490.00 рублей\n"
    "Keystone Tablet, 1 шт, 4 990.00 рублей"
) == [("Keystone 3 Pro", 1), ("Чехол Keystone Wallet Pouch 2", 1), ("Keystone Tablet", 1)]
# неразрывный пробел в цене: в живых составах не встретился, но parse_total его чистит — держим
assert parse_composition_items("Tangem 2.0 (3 карты), 1 шт, 7 590.00 рублей") == [
    ("Tangem 2.0 (3 карты)", 1)
]
assert parse_composition_items(None) == []
assert parse_composition_items("") == []

# --- сборка наименования ---------------------------------------------------

assert build_cdek_item_name("Keystone 3 Pro, 1 шт, 13 491.00 рубль") == "Аппаратный кошелёк Keystone 3 Pro"
assert build_cdek_item_name("YubiKey 5 NFC, 4 шт, 24 360.00 рублей") == "Аппаратный кошелёк YubiKey 5 NFC, 4 шт"
assert build_cdek_item_name(
    "Tangem 2.0 WHITE (3 Карты), 2 шт, 13 980.00 рублей\nKeystone Tablet, 1 шт, 4 990.00 рублей"
) == "Аппаратный кошелёк Tangem 2.0 WHITE (3 Карты), 2 шт; Keystone Tablet"

# состав пуст или не распознан — только категория, имя сделки НЕ подставляем
assert build_cdek_item_name(None) == CDEK_ITEM_NAME_PREFIX
assert build_cdek_item_name("Заказ №18180") == CDEK_ITEM_NAME_PREFIX

# длинный состав режется по границе позиции и не превышает лимит СДЭК
long_composition = "".join(f"Tangem 2.0 Stealth (3 карты) вариант {i}, 2 шт, 7 590.00 рублей\n" for i in range(20))
long_name = build_cdek_item_name(long_composition)
assert len(long_name) <= CDEK_ITEM_NAME_MAX_LEN, len(long_name)
assert long_name.startswith(CDEK_ITEM_NAME_PREFIX + " ")
assert long_name.endswith("…")
assert "вариант 0, 2 шт" in long_name          # начало состава сохранено
assert not long_name.rstrip("…").endswith(",")  # наименование не разорвано посередине

# --- лист сборки ------------------------------------------------------------
# Раньше у листа сборки был свой строгий регэксп: он требовал «шт» и терял 24 заказа
# из 699. Теперь парсер общий — проверяем, что «Итого к сборке» их видит.
import picking_pdf

assert picking_pdf.parse_items("YubiKey 5 NFC, 1 , 6 990.00 рублей") == [("YubiKey 5 NFC", 1)]
assert picking_pdf.parse_items(
    "Keystone 3 Pro, 1 шт, 13 491.00 рубль\nKeystone Tablet, 2 шт, 9 980.00 рублей"
) == [("Keystone 3 Pro", 1), ("Keystone Tablet", 2)]

print("cdek_item_name: тесты наименования товара и листа сборки прошли")
