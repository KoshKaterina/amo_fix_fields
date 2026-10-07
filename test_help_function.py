"""Юнит-тест разбора состава заказа (МойСклад → «Корзина»/«Тип доставки»).

Второй заход 23.09.2026 (вопрос Кати): способы доставки переименовали - латинское
«CDEK:» стало кириллическим «СДЭК:», «Доставка курьером по Москве» - «Курьерской
доставкой». Замер по 4 498 живым сделкам за 30 дней: 11 строк доставки прошли мимо
парсера, у всех «Тип доставки» остался пустым.

Регресс сделки 36545673 (31.08.2026, вопрос Кати): позиция доставки «Почта
России» не входила в DELIVERY_PREFIXES → startswith не срабатывал, строка
доставки оставалась в товарах («Корзина»), а «Тип доставки» (577315) вообще
не патчился (api.add_info_from_ms пропускает пустой delivery_type).

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде проверки стояли голыми `assert` на уровне
модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА и ронял сбор всего
репозитория, а в сводке файл давал ноль тестов. Перебор имён доставки стал `parametrize` -
теперь при падении видно, какое именно имя сломалось.
"""

import asyncio

import pytest
from help_function import parse_the_cart_field


def _parse(data: str):
    return asyncio.run(parse_the_cart_field(data))


def test_pochta_rossii_opoznaetsya_kak_dostavka():
    """Сделка 36545673: раньше «Почта России» уезжала в товары, а «Тип доставки» пустел."""
    goods, delivery = _parse(
        "1. Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля\n"
        "2. Почта России, 1 шт, 390.00 рублей"
    )
    assert goods == "Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля"
    assert delivery == "Почта России, 390.00 рублей"      # «1 шт» — количество, вырезается


def test_samovyvoz_iz_ofisa_ne_slomalsya():
    goods, delivery = _parse(
        "1. Keystone 3 Pro, 1 шт, 14 990.00 рублей\n"
        "2. Самовывоз из офиса Sunscrypt, 1 шт, 0.00 рублей"
    )
    assert goods == "Keystone 3 Pro, 1 шт, 14 990.00 рублей"
    assert delivery == "Самовывоз из офиса Sunscrypt, 0.00 рублей"


def test_korotkoe_imya_cdek_ne_slomalos():
    goods, delivery = _parse(
        "1. Trezor Safe 3 Bitcoin-only, 1 шт, 7 890.00 рублей\n2. CDEK, 1 шт, 500.00 рублей"
    )
    assert goods == "Trezor Safe 3 Bitcoin-only, 1 шт, 7 890.00 рублей"
    assert delivery == "CDEK, 500.00 рублей"


def test_bez_dostavki_v_sostave_tolko_tovary():
    goods, delivery = _parse("1. YubiKey 5 NFC, 4 шт, 24 360.00 рублей")
    assert goods == "YubiKey 5 NFC, 4 шт, 24 360.00 рублей"
    assert delivery == ""


# ── новые имена способов доставки (переименование 23.09.2026) ──────────────


@pytest.mark.parametrize(
    "name",
    [
        "Самовывоз из шоурума Sunscrypt",
        "Курьерская доставка",
        "СДЭК: Курьерская доставка",
        "CDEK: Самовывоз из ПВЗ",
        "СДЭК: Доставка в постамат",
        "Курьер СДЭК",
        "Курьерская доставка Яндекс",
        "Доставка (уточняет менеджер)",
    ],
)
def test_novye_imena_dostavki_razbirayutsya(name):
    goods, delivery = _parse(
        "1. Keystone 3 Pro, 1 шт, 14 990.00 рублей\n"
        f"2. {name}, 1 шт, 0.00 рублей"
    )
    assert goods == "Keystone 3 Pro, 1 шт, 14 990.00 рублей", f"{name}: товар уехал не туда"
    assert delivery == f"{name}, 0.00 рублей", f"{name}: строка доставки не опознана"


def test_registr_bolshe_ne_reshaet():
    goods, delivery = _parse(
        "1. YubiKey 5 NFC, 1 шт, 6 090.00 рублей\n"
        "2. самовывоз из шоурума, 1 шт, 0.00 рублей"
    )
    assert delivery == "самовывоз из шоурума, 0.00 рублей"


def test_tovar_s_dostavochnym_slovom_v_nazvanii_ne_schitaetsya_dostavkoy():
    """Опознание идёт по ПРЕФИКСУ строки, а не по подстроке в любом месте."""
    goods, delivery = _parse("1. Кейс для курьера Tangem, 1 шт, 1 500.00 рублей")
    assert goods == "Кейс для курьера Tangem, 1 шт, 1 500.00 рублей"
    assert delivery == ""
