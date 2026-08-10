"""Тесты правила «склад идёт за услугой доставки» (showroom_store).

Проверяем правило в редакции 10.08.2026: услуга «Самовывоз из Шоурума»
задаёт склад шоурума, а обратного хода нет: без этой услуги склад заказа
не трогаем вообще - шоурум это рабочий склад с остатками, а не служебная
метка самовывоза.
"""

import showroom_store
from waybill_config import MS_STORE_MAIN_ID, MS_STORE_SHOWROOM_ID

SERVICE_SHOWROOM = "15ff040c-529c-11f1-0a80-0d0c00781fe6"
SERVICE_CDEK = "1446e4d4-529c-11f1-0a80-00e800766d91"
STORE_OPENED = "3c333a53-4e54-11ef-0a80-15a2000c93fc"
STORE_ERMS = "ee6f138f-5ce7-11f1-0a80-17a900213ca9"


def _order(store_id, services=(), names=()):
    rows = [{"assortment": {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/service/{s}"},
                            "name": "услуга"}} for s in services]
    rows += [{"assortment": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/service/xxx"},
                             "name": n}} for n in names]
    return {
        "id": "order-1", "name": "00001",
        "store": {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/store/{store_id}"}},
        "positions": {"rows": rows},
    }


def test_uslugа_showrooma_stavit_sklad_showrooma():
    order = _order(MS_STORE_MAIN_ID, services=[SERVICE_SHOWROOM])
    assert showroom_store.target_store(order) == MS_STORE_SHOWROOM_ID


def test_uslugа_uznayotsya_po_nazvaniyu_esli_id_drugoy():
    order = _order(MS_STORE_MAIN_ID, names=["Самовывоз из Шоурума"])
    assert showroom_store.target_store(order) == MS_STORE_SHOWROOM_ID


def test_sklad_uzhe_verniy_nichego_ne_menyaem():
    order = _order(MS_STORE_SHOWROOM_ID, services=[SERVICE_SHOWROOM])
    assert showroom_store.target_store(order) is None


def test_bez_uslugi_shourooma_sklad_ne_trogaem():
    """Заказ на складе шоурума с любой другой доставкой остаётся на шоуруме.

    До 10.08 здесь был откат на Основной - он уводил отгрузки Кирилла
    с его склада (11 заказов за 06-10.08).
    """
    order = _order(MS_STORE_SHOWROOM_ID, services=[SERVICE_CDEK])
    assert showroom_store.target_store(order) is None


def test_sdek_s_osnovnogo_sklada_tozhe_ne_trogaem():
    order = _order(MS_STORE_MAIN_ID, services=[SERVICE_CDEK])
    assert showroom_store.target_store(order) is None


def test_samovyvoz_sdeka_ne_schitaetsya_shouroomom():
    """«Самовывоз СДЭК» — обычная доставка, склад остаётся основным."""
    order = _order(MS_STORE_MAIN_ID, names=["Самовывоз СДЭК"])
    assert showroom_store.target_store(order) is None


def test_chuzhie_sklady_ne_trogaem():
    """Вскрытые и ЭРМС живут по своим правилам — без услуги шоурума не трогаем."""
    assert showroom_store.target_store(_order(STORE_OPENED, services=[SERVICE_CDEK])) is None
    assert showroom_store.target_store(_order(STORE_ERMS, services=[SERVICE_CDEK])) is None


def test_zakaz_bez_pozitsiy_ne_lomaet():
    order = {"id": "x", "store": {"meta": {"href": f".../store/{MS_STORE_MAIN_ID}"}}}
    assert showroom_store.target_store(order) is None


def test_pravilo_perenosa_prinimaet_shourum():
    """Сделка с новым складом обязана подходить под автоперенос в Офис."""
    from waybill_config import OFFICE_TRANSFER_WAREHOUSES, WAREHOUSE_SUNSCRYPT_SHOWROOM
    assert WAREHOUSE_SUNSCRYPT_SHOWROOM in OFFICE_TRANSFER_WAREHOUSES


def test_marker_samovyvoza_iz_shourooma_est():
    from waybill_config import DELIVERY_PICKUP_MARKERS
    text = "самовывоз из шоурума, 1 шт, 0.00".casefold()
    assert any(m in text for m in DELIVERY_PICKUP_MARKERS)
    text_office = "самовывоз из офиса sunscrypt, 1 шт, 0.00".casefold()
    assert any(m in text_office for m in DELIVERY_PICKUP_MARKERS)
