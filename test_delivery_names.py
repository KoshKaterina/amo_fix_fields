"""Имена способов доставки: тариф СДЭК и маркеры правил.

Переименование 23.09.2026 (вопрос Кати): «CDEK:» → «СДЭК:», «Доставка курьером по
Москве» → «Курьерская доставка», «Самовывоз из офиса» → «Самовывоз из шоурума».
Старые имена продолжают приходить в старых заказах, поэтому проверяем ОБА набора.
Карта мест, где зашиты имена, - knowledge/imena-dostavki-gde-zashity.md.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде проверки шли циклами с `assert` на уровне
модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА и ронял сбор всего
репозитория, а в сводке файл давал ноль тестов.

Циклы превращены в `parametrize`, а не в один тест с циклом внутри, и это не формальность:
при падении видно, КАКОЕ имя доставки сломалось, прямо в названии теста. Раньше цикл
останавливался на первом же несовпадении и про остальные имена не говорил ничего.
"""

import pytest
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


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        # старые имена
        ("CDEK: Самовывоз", TARIFF_PVZ),
        ("Самовывоз СДЭК", TARIFF_PVZ),
        ("CDEK: Посылка склад-постамат", TARIFF_POSTAMAT),
        ("CDEK: Посылка склад-дверь", TARIFF_DOOR),
        # новые имена
        ("CDEK: Самовывоз из ПВЗ", TARIFF_PVZ),
        ("CDEK: Посылка склад-склад", TARIFF_PVZ),
        ("СДЭК: Доставка в постамат", TARIFF_POSTAMAT),
        ("СДЭК: Курьерская доставка", TARIFF_DOOR),
        ("Курьер СДЭК", TARIFF_DOOR),
    ],
)
def test_tarif_sdek_po_imeni_dostavki(name, expected):
    got = parse_tariff(_cart(name))
    assert got == expected, f"{name}: ждали тариф {expected}, получили {got}"


@pytest.mark.parametrize(
    "name",
    ["Доставка курьером по Москве", "Курьерская доставка", "Самовывоз из шоурума Sunscrypt"],
)
def test_nasha_dostavka_ne_sdek_i_nakladnoy_ne_trebuet(name):
    assert parse_tariff(_cart(name, "0.00")) is None, f"{name}: тариф СДЭК не должен определяться"


# ── маркеры правил ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name",
    ["Самовывоз из офиса Sunscrypt", "Самовывоз из шоурума Sunscrypt", "Самовывоз из Шоурума"],
)
def test_nash_samovyvoz_opoznaetsya(name):
    assert any(m in name.casefold() for m in DELIVERY_PICKUP_MARKERS), \
        f"{name}: наш самовывоз не опознан"


@pytest.mark.parametrize("name", ["CDEK: Самовывоз", "CDEK: Самовывоз из ПВЗ"])
def test_pvz_perevozchika_ne_nash_samovyvoz(name):
    assert not any(m in name.casefold() for m in DELIVERY_PICKUP_MARKERS), \
        f"{name}: ПВЗ перевозчика — не наш самовывоз"


@pytest.mark.parametrize("name", ["Доставка курьером по Москве", "Курьерская доставка"])
def test_svoya_kurerka_opoznaetsya(name):
    assert any(m in name.casefold() for m in DELIVERY_COURIER_OWN_MARKERS), \
        f"{name}: своя курьерка не опознана"


def test_imya_kurerki_sdek_peresekaetsya_s_nashey_i_perevozchik_otseivaetsya_pervym():
    """⚠️ Ключевая мина: имя курьерки СДЭК содержит подстроку нашей курьерки."""
    sdek_courier = "СДЭК: Курьерская доставка".casefold()
    assert any(m in sdek_courier for m in DELIVERY_COURIER_OWN_MARKERS), \
        "подстрока пересекается — так и задумано"
    assert any(m in sdek_courier for m in DELIVERY_CDEK_MARKERS), \
        "перевозчик должен опознаваться и отсеиваться первым"
