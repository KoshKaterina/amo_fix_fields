"""Юнит-тесты имени сделки для заявок с формы предзаказа (без сети и прода).

Проверяем три решающих места: узнаём ли заявку именно с формы предзаказа,
не переименовываем ли уже переименованное, заводится ли фоновая задача только
при включённом флаге.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде проверки стояли голыми `assert` на уровне
модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА и ронял сбор всего
репозитория, а в сводке файл давал ноль тестов.

⚠️ Заодно убрана утечка состояния. Скрипт в конце переключал флаг
`PREORDER_LEAD_NAME_ENABLED` в False и так его и оставлял. Пока файл был скриптом, это
никому не мешало - процесс завершался. В pytest такой остаток достался бы соседним тестам
и всему, что импортируется позже, а порядок файлов в прогоне не обещан. Теперь флаг
переключается фикстурой и возвращается на место.
"""

import os

os.environ["PREORDER_LEAD_NAME_ENABLED"] = "1"

import preorder_lead_name as pln                                            # noqa: E402
import pytest                                                              # noqa: E402
from waybill_config import (                                                # noqa: E402
    APPLICATION_TYPE_ORDER,
    APPLICATION_TYPE_PREORDER,
    FIELD_APPLICATION_TYPE,
    LEAD_SOURCE_SITE_CONTACT_FORM,
)

LEAD_ID = 36559797


def _lead(*, source_id=LEAD_SOURCE_SITE_CONTACT_FORM, type_enum=APPLICATION_TYPE_PREORDER,
          name="Эльдар", source_place="top") -> dict:
    """source_place: где лежит источник. `top` - полем `source_id` (так отвечает amo
    на `with=source_id`, проверено на боевых сделках), `embedded` - вложенным."""
    lead = {"id": LEAD_ID, "name": name, "_embedded": {}}
    if source_id is not None:
        if source_place == "top":
            lead["source_id"] = source_id
        else:
            lead["_embedded"]["source"] = {"id": source_id}
    if type_enum is not None:
        lead["custom_fields_values"] = [
            {"field_id": FIELD_APPLICATION_TYPE, "values": [{"enum_id": type_enum}]}
        ]
    return lead


# ── узнаём заявку с формы предзаказа ───────────────────────────────────────


def test_zayavka_s_formy_predzakaza_uznaetsya():
    assert pln.is_preorder_form_lead(_lead()) is True


def test_drugoy_istochnik_ne_nasha_zayavka():
    """Звонок, чат, маркетплейс."""
    assert pln.is_preorder_form_lead(_lead(source_id=23478413)) is False


def test_istochnika_net_vovse_ne_nasha():
    assert pln.is_preorder_form_lead(_lead(source_id=None)) is False


def test_ta_zhe_forma_no_obychnyy_zakaz_ne_pereimenovyvaem():
    assert pln.is_preorder_form_lead(_lead(type_enum=APPLICATION_TYPE_ORDER)) is False


def test_tip_zayavki_ne_prostavlen_zhdem_povtora():
    """Переименовывать вслепую нельзя - ждём, пока amo проставит тип."""
    assert pln.is_preorder_form_lead(_lead(type_enum=None)) is False


def test_istochnik_vlozhennym_obektom_tozhe_uznaetsya():
    """⚠️ Источник приезжает полем верхнего уровня - на этом модуль спотыкался 21.09.2026.

    Оба формата ответа amo должны узнаваться.
    """
    assert pln.is_preorder_form_lead(_lead(source_place="embedded")) is True
    assert pln.is_preorder_form_lead(_lead(source_id=23478413, source_place="embedded")) is False


# ── идемпотентность и формат имени ─────────────────────────────────────────


def test_uzhe_pereimenovannuyu_ne_pereimenovyvaem_povtorno():
    assert pln.needs_rename(_lead(name="Эльдар")) is True
    assert pln.needs_rename(_lead(name=f"Заказ №{LEAD_ID}")) is False


def test_pustoe_imya_pereimenovyvaem():
    assert pln.needs_rename(_lead(name="")) is True


def test_format_imeni():
    assert pln.build_name(LEAD_ID) == f"Заказ №{LEAD_ID}"


# ── планировщик: флаг и пустой id ──────────────────────────────────────────

_scheduled: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _apply — в нём сеть
    _scheduled.append(True)
    return _FakeTask()


@pytest.fixture(autouse=True)
def _no_real_background_tasks():
    """⚠️ `pln.asyncio` - это сам модуль `asyncio`, общий на процесс, поэтому возвращаем."""
    was = pln.asyncio.create_task
    pln.asyncio.create_task = _fake_create_task
    try:
        yield
    finally:
        pln.asyncio.create_task = was


def _fires(lead_id) -> bool:
    _scheduled.clear()
    pln.rename_bg(lead_id)
    return len(_scheduled) == 1


@pytest.fixture()
def flag():
    """Переключатель флага, который ВОЗВРАЩАЕТ прежнее значение.

    Без возврата остаток состояния достался бы соседним тестам, а порядок файлов в прогоне
    не обещан - такие связи ловятся потом сутками.
    """
    was = pln.PREORDER_LEAD_NAME_ENABLED
    try:
        yield lambda value: setattr(pln, "PREORDER_LEAD_NAME_ENABLED", value)
    finally:
        pln.PREORDER_LEAD_NAME_ENABLED = was


def test_s_flagom_zadacha_zavoditsya(flag):
    flag(True)
    assert _fires(LEAD_ID) is True


def test_bez_sdelki_ne_planiruem(flag):
    flag(True)
    assert _fires(None) is False


def test_flag_vyklyuchen_molchim(flag):
    flag(False)
    assert _fires(LEAD_ID) is False
