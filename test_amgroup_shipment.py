"""Тесты протеза отгрузок amgroup (amgroup_shipment) - создание отгрузки в
МойСкладе по сделке amoCRM на нужном этапе воронки «Офис» и тройная защита
от двойного списания.

Написаны по инциденту 02-03.09.2026 (см. докстринг amgroup_shipment.py):
amgroup встала, отгрузки в МойСкладе перестал создавать кто бы то ни было.

Запуск: python3 -m pytest test_amgroup_shipment.py -q
"""

import asyncio
import os
import sys

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amgroup_shipment  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER,
    PIPELINE_OFFICE,
    STATUS_OFFICE_COURIER_OWN,
    STATUS_SUCCESS,
    STATUS_WAYBILL_READY,
)

ORDER_UUID = "order-uuid-0001"


def _lead(pipeline_id, status_id, *, order_uuid=ORDER_UUID, shipment_id=None, lead_id=1001):
    fields = []
    if order_uuid:
        fields.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order_uuid}]})
    if shipment_id:
        fields.append({"field_id": amgroup_shipment.FIELD_SHIPMENT_ID, "values": [{"value": shipment_id}]})
    return {
        "id": lead_id,
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "custom_fields_values": fields,
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    monkeypatch.setattr(amgroup_shipment, "_STATE_PATH", str(tmp_path / "created.json"), raising=False)
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)
    # По умолчанию тесты проверяют ВКЛЮЧЁННЫЙ модуль в БОЕВОМ режиме - в .env
    # для тестов оба флага не заданы и берут дефолт (ENABLED=0, DRY_RUN=1),
    # который создавать отгрузку не даст. Кто хочет проверить выключенный
    # модуль или сухой режим - переопределяет флаг в своём тесте явно.
    monkeypatch.setattr(amgroup_shipment, "AMGROUP_SHIPMENT_ENABLED", True, raising=False)
    monkeypatch.setattr(amgroup_shipment, "AMGROUP_SHIPMENT_DRY_RUN", False, raising=False)
    amgroup_shipment._created.clear()
    amgroup_shipment._lead_locks.clear()
    amgroup_shipment._bg_tasks.clear()
    yield
    amgroup_shipment._created.clear()
    amgroup_shipment._lead_locks.clear()
    amgroup_shipment._bg_tasks.clear()


def _wire(
    monkeypatch,
    *,
    demand_search_rows=(),
    template=None,
    created_demand=None,
    patch_ok=True,
    store_name="Sunscrypt Основной",
):
    """Стабит ms_client (get/put/post) и amo_service.patch_lead. Возвращает
    списки вызовов для проверок."""
    calls = {"get": [], "put": [], "post": [], "patch": [], "patch_enum": []}

    async def fake_get(path, params=None):
        calls["get"].append((path, params))
        if path == "entity/demand":
            return {"rows": list(demand_search_rows)}
        if path.startswith("entity/store/"):
            return {"name": store_name} if store_name else None
        return None

    async def fake_put(path, body):
        calls["put"].append((path, body))
        return template

    async def fake_post(path, body):
        calls["post"].append((path, body))
        return created_demand

    async def fake_patch_lead(lead_id, *, custom_fields=None, **kwargs):
        calls["patch"].append((lead_id, custom_fields))
        return {"ok": patch_ok}

    # Склад отгрузки - выпадающий список, он идёт прямым патчем мимо patch_lead.
    async def fake_do_patch(path, body):
        calls["patch_enum"].append((path, body))
        return {"ok": patch_ok}

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_shipment.ms_client, "put", fake_put)
    monkeypatch.setattr(amgroup_shipment.ms_client, "post", fake_post)
    monkeypatch.setattr(amgroup_shipment.amo_service, "patch_lead", fake_patch_lead)
    monkeypatch.setattr(amgroup_shipment.amo_service, "_do_patch", fake_do_patch)
    return calls


_DEMAND_TEMPLATE = {
    "customerOrder": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/order-uuid-0001"}},
    "store": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/store/store-uuid-0001"}},
    "positions": {"rows": [{"quantity": 1}]},
}
_CREATED_DEMAND = {**_DEMAND_TEMPLATE, "id": "demand-uuid-0001", "name": "00007"}


def test_sozdaetsya_na_gotova_nakladnaya(monkeypatch):
    """Офис + «Готова накладная» (СДЭК) - отгрузка создаётся."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result == {"shipment_id": "demand-uuid-0001", "shipment_number": "00007", "warehouse": "Sunscrypt Основной"}
    assert len(calls["post"]) == 1
    assert calls["post"][0][0] == "entity/demand"
    # Два текстовых поля - обычным патчем.
    assert len(calls["patch"]) == 1
    _, fields = calls["patch"][0]
    assert fields == {
        amgroup_shipment.FIELD_SHIPMENT_ID: "demand-uuid-0001",
        amgroup_shipment.FIELD_SHIPMENT_NUMBER: "00007",
    }
    # Склад отгрузки - выпадающий список, ему нужен идентификатор варианта,
    # на голое имя склада amo отвечает отказом. Сверено с полем 03.09.2026.
    assert len(calls["patch_enum"]) == 1
    _, body = calls["patch_enum"][0]
    assert body == {"custom_fields_values": [
        {"field_id": amgroup_shipment.FIELD_SHIPMENT_WAREHOUSE,
         "values": [{"enum_id": 1040157}]}]}


def test_sozdaetsya_na_dostavka_nash_kurer(monkeypatch):
    """Офис + «Доставка наш курьер» - отгрузка создаётся."""
    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_OFFICE_COURIER_OWN)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None
    assert result["shipment_id"] == "demand-uuid-0001"


def test_sozdaetsya_na_uspeshno_realizovano_v_ofise(monkeypatch):
    """Офис + «Успешно реализовано» (142) - отгрузка создаётся."""
    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_SUCCESS)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None


def test_status_142_v_chuzhoy_voronke_ne_sozdaet(monkeypatch):
    """142 - системный статус на все воронки. Сделка закрыта в [CLEVER]
    Основная (не Офис) - отгрузку создавать НЕЛЬЗЯ, иначе спишем товар зря
    на закрытие сделки в чужой воронке."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_CLEVER, STATUS_SUCCESS)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []
    assert calls["patch"] == []


def test_ne_celevoy_etap_v_ofise_ne_sozdaet(monkeypatch):
    """Воронка Офис, но этап не из целевых трёх - не наша сделка."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, 999999)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["get"] == []
    assert calls["put"] == []


def test_pole_id_otgruzki_uzhe_zapolneno_ne_sozdaet(monkeypatch):
    """«ID Отгрузки» уже заполнено в сделке - выходим молча, до склада не
    достаём вовсе (гейт 1 - самый дешёвый, идёт первым)."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY, shipment_id="already-created")

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["get"] == []
    assert calls["put"] == []
    assert calls["post"] == []


def test_pustoe_pole_zakaza_ne_sozdaet(monkeypatch):
    """Нет «ID Заказа» (576689) - создавать отгрузку не по чему."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY, order_uuid=None)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []


def test_sklad_ne_otvetil_na_proverku_ne_sozdaet(monkeypatch):
    """entity/demand (проверка существующих) вернул None - склад НЕ ОТВЕТИЛ,
    это не значит «отгрузки нет». Ничего не создаём."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)

    async def fake_get_none(path, params=None):
        calls["get"].append((path, params))
        return None

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get_none)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["put"] == []
    assert calls["post"] == []


def test_otgruzka_uzhe_est_v_mojskladom_ne_sozdaet_vtoruyu(monkeypatch):
    """Проверка entity/demand по заказу вернула существующую отгрузку -
    выходим молча, вторую не создаём."""
    calls = _wire(
        monkeypatch,
        demand_search_rows=[{"id": "already-there", "name": "00003"}],
        template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND,
    )
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["put"] == []
    assert calls["post"] == []


def test_shablon_ne_otvetil_nichego_ne_sozdaet(monkeypatch):
    """PUT entity/demand/new вернул None - шаблон недоступен, POST не зовём."""
    calls = _wire(monkeypatch, template=None, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []


def test_povtornyy_vyzov_ne_sozdaet_vtoruyu_otgruzku(monkeypatch):
    """Диск (гейт 3): вторая сделка того же лида по амо не приходит дважды в
    реальности, но состояние на диске обязано пережить процесс. Здесь -
    прямая проверка: второй вызов той же функции для той же сделки не должен
    создавать вторую отгрузку, даже если поле «ID Отгрузки» почему-то не
    записалось (симулируем неудачный PATCH на первом вызове)."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND, patch_ok=False)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    first = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))
    second = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert first is not None
    assert second is None
    assert len(calls["post"]) == 1  # POST на создание отгрузки был только один раз


def test_diskovoe_sostoyanie_perezhivaet_perezagruzku_modulya(monkeypatch, tmp_path):
    """Состояние - на диске, а не только в памяти процесса: имитируем
    «перезапуск» явной очисткой in-memory кэша и повторной загрузкой файла."""
    state_path = tmp_path / "created.json"
    monkeypatch.setattr(amgroup_shipment, "_STATE_PATH", str(state_path), raising=False)
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)
    amgroup_shipment._created.clear()

    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)
    first = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))
    assert first is not None
    assert state_path.exists()

    # «Перезапуск процесса»: чистим память, состояние читаем заново с диска.
    amgroup_shipment._created.clear()
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)

    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    second = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert second is None
    assert calls["post"] == []


def test_molchanie_sklada_pole_ostaetsya_pustym(monkeypatch):
    """Имя склада не удалось получить (entity/store/... молчит, вернул None) -
    поле «Склад отгрузки» НЕ заполняем вовсе (правка по итогам ревью
    03.09.2026: раньше сюда молча подставлялось «Sunscrypt Основной», и оно
    проходило проверку по вариантам списка незамеченным). Отгрузка при этом
    всё равно считается созданной - товар уже списан, откатывать нельзя."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND, store_name=None)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None
    assert result["warehouse"] is None
    # Два текстовых поля пишутся как обычно, а вот прямой PATCH на выпадающий
    # список «Склад отгрузки» звать не должны - писать в него нечего.
    assert len(calls["patch"]) == 1
    assert calls["patch_enum"] == []


def test_v_shablone_net_sklada_pole_ostaetsya_pustym(monkeypatch):
    """В созданной отгрузке вовсе нет ссылки на склад (store_ref пуст) - тот
    же результат: поле не заполняем, к МойСкладу за именем даже не обращаемся."""
    demand_bez_sklada = {**_CREATED_DEMAND, "store": {}}
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=demand_bez_sklada)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None
    assert result["warehouse"] is None
    assert calls["patch_enum"] == []
    assert not any(path.startswith("entity/store/") for path, _ in calls["get"])


# ---------------------------------------------------------------------------
# Гонка двух вызовов, обрыв между созданием и записью состояния, флаг и
# сухой режим - правки по итогам ревью 03.09.2026 (см. докстринг
# amgroup_shipment.py, раздел про замок и слот).
# ---------------------------------------------------------------------------

def test_parallelnyy_vyzov_sozdaet_odnu_otgruzku(monkeypatch):
    """Гонка двух конкурентных вызовов по одной сделке (авария 02-03.09.2026) -
    раньше оба проходили гейты, пока первый ждал сеть, и создавались ДВЕ
    отгрузки. Настоящая конкурентность эмулируется реальной точкой
    переключения (asyncio.sleep(0)) внутри сетевых стабов - без неё обе
    корутины прошли бы все гейты подряд в одном "ходе" event loop, и тест
    ничего бы не проверял."""
    calls = {"get": [], "put": [], "post": [], "patch": [], "patch_enum": []}

    async def fake_get(path, params=None):
        await asyncio.sleep(0)  # реальная точка переключения - как в бою
        calls["get"].append((path, params))
        if path == "entity/demand":
            return {"rows": []}
        return None  # склад молчит - в этом тесте не важно

    async def fake_put(path, body):
        await asyncio.sleep(0)
        calls["put"].append((path, body))
        return _DEMAND_TEMPLATE

    async def fake_post(path, body):
        await asyncio.sleep(0)
        calls["post"].append((path, body))
        return {**_CREATED_DEMAND, "id": f"demand-uuid-{len(calls['post']):04d}"}

    async def fake_patch_lead(lead_id, *, custom_fields=None, **kwargs):
        calls["patch"].append((lead_id, custom_fields))
        return {"ok": True}

    async def fake_do_patch(path, body):
        calls["patch_enum"].append((path, body))
        return {"ok": True}

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_shipment.ms_client, "put", fake_put)
    monkeypatch.setattr(amgroup_shipment.ms_client, "post", fake_post)
    monkeypatch.setattr(amgroup_shipment.amo_service, "patch_lead", fake_patch_lead)
    monkeypatch.setattr(amgroup_shipment.amo_service, "_do_patch", fake_do_patch)

    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    async def _run_both():
        return await asyncio.gather(
            amgroup_shipment.create_shipment_for_lead(lead),
            amgroup_shipment.create_shipment_for_lead(lead),
        )

    results = asyncio.run(_run_both())

    assert len(calls["post"]) == 1  # POST на создание отгрузки был только один раз
    non_none = [r for r in results if r is not None]
    assert len(non_none) == 1
    assert amgroup_shipment._created["1001"].get("pending") is not True


def test_obryv_mezhdu_sozdaniem_i_zaprosom_sklada_ne_sozdaet_vtoruyu(monkeypatch):
    """Обрыв МЕЖДУ успешным созданием отгрузки в МойСкладе и следующим сетевым
    вызовом (запрос имени склада, _resolve_store_name) не должен терять
    память о том, что документ уже реален - иначе повторный проход создаёт
    вторую отгрузку. Раньше _resolve_store_name вызывался ДО записи состояния
    на диск - здесь проверяем, что порядок починен."""
    calls = {"get": [], "put": [], "post": []}

    async def fake_get_boom(path, params=None):
        calls["get"].append((path, params))
        if path == "entity/demand":
            return {"rows": []}
        if path.startswith("entity/store/"):
            raise RuntimeError("МойСклад оборвал соединение на середине")
        return None

    async def fake_put(path, body):
        calls["put"].append((path, body))
        return _DEMAND_TEMPLATE

    async def fake_post(path, body):
        calls["post"].append((path, body))
        return _CREATED_DEMAND

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get_boom)
    monkeypatch.setattr(amgroup_shipment.ms_client, "put", fake_put)
    monkeypatch.setattr(amgroup_shipment.ms_client, "post", fake_post)

    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    with pytest.raises(RuntimeError):
        asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    # Отгрузка в МойСкладе уже реальна (POST прошёл) - state должен быть
    # записан ДО сбоя на запросе склада, а не потерян вместе с исключением.
    assert amgroup_shipment._created["1001"].get("pending") is not True
    assert amgroup_shipment._created["1001"]["shipment_id"] == "demand-uuid-0001"

    # Повторный проход (даже с рабочим складом) вторую отгрузку создавать не должен.
    calls2 = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    second = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert second is None
    assert calls2["post"] == []
    assert len(calls["post"]) == 1


def test_suhoy_rezhim_nichego_ne_pishet(monkeypatch):
    """Сухой режим (AMGROUP_SHIPMENT_DRY_RUN=1): все гейты пройдены, шаблон
    собран у МойСклада (PUT), а вот создание документа (POST) и запись полей
    в amoCRM - нет."""
    monkeypatch.setattr(amgroup_shipment, "AMGROUP_SHIPMENT_DRY_RUN", True)
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert len(calls["put"]) == 1  # шаблон собрали - это ничего не создаёт
    assert calls["post"] == []      # а создавать документ - нет
    assert calls["patch"] == []
    assert calls["patch_enum"] == []


def test_flag_vyklyuchen_vnutri_funktsii_ne_sozdaet(monkeypatch):
    """AMGROUP_SHIPMENT_ENABLED=False - проверка стоит и ВНУТРИ самой функции
    создания отгрузки, не только в двух точках входа (находка приёмки
    безопасности 03.09.2026): вызов мимо handle_lead_status_change_bg не
    должен обходить выключатель."""
    monkeypatch.setattr(amgroup_shipment, "AMGROUP_SHIPMENT_ENABLED", False)
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["get"] == []
    assert calls["put"] == []
    assert calls["post"] == []


def test_bg_task_derzhit_ssylku_i_osvobozhdaetsya(monkeypatch):
    """handle_lead_status_change_bg держит фоновую задачу в _bg_tasks (защита
    от среза сборщиком мусора, event loop иначе хранит только слабую ссылку)
    и убирает её оттуда по завершении."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)

    async def fake_get_lead_full(lead_id, **kwargs):
        return _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY, lead_id=lead_id)

    monkeypatch.setattr(amgroup_shipment.amo_service, "get_lead_full", fake_get_lead_full)

    async def _run():
        amgroup_shipment.handle_lead_status_change_bg(1001, STATUS_WAYBILL_READY, PIPELINE_OFFICE)
        assert len(amgroup_shipment._bg_tasks) == 1
        pending = list(amgroup_shipment._bg_tasks)
        await asyncio.gather(*pending)

    asyncio.run(_run())

    assert amgroup_shipment._bg_tasks == set()
    assert len(calls["post"]) == 1


def test_bg_task_lovit_isklyuchenie(monkeypatch):
    """Исключение внутри фоновой отгрузки не должно всплывать безымянной
    строкой при сборке мусора - оно поймано и залогировано (как у
    showroom_tag._apply / unmiss_tag._apply)."""
    async def fake_get_lead_full_boom(lead_id, **kwargs):
        raise RuntimeError("amo недоступен")

    monkeypatch.setattr(amgroup_shipment.amo_service, "get_lead_full", fake_get_lead_full_boom)

    async def _run():
        amgroup_shipment.handle_lead_status_change_bg(1001, STATUS_WAYBILL_READY, PIPELINE_OFFICE)
        pending = list(amgroup_shipment._bg_tasks)
        await asyncio.gather(*pending)  # не должно бросить исключение наружу

    asyncio.run(_run())

    assert amgroup_shipment._bg_tasks == set()
