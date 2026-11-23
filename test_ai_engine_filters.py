"""Взятие сделки ИИ: фильтры, бюджет, товары по артикулам, «существующие сделки» (этап 2 плана режимов, 20.11.2026, `ai_engine_filters`).

Что проверяем:
  • сопоставление значений полей amo с кодами на РЕАЛЬНЫХ значениях (сверка 23.11.2026 по 2266 свежим сделкам четырёх воронок): тип заявки, способ
    оплаты (правило cod и on_receipt), статус оплаты, тип доставки; золотой список кодов;
  • чтение блоков настроек: нет блока значит без ограничения, повреждённая подгруппа не действует, версия 1 и режим Тест новых блоков не читают;
  • бюджет (граница включительно, ноль и пусто без лимита, сделки с бюджетом 0 берём);
  • товары: три пути артикулов (заказ МойСклад, название из каталога, нераспознанная), сбой и таймаут МойСклад, кэш, правило «каждая позиция»;
  • движок: отказ не пишет ничего ни клиенту, ни в amo, ни людям, причина одной строкой в лог; правила A и D не зовут людей из-за сделок, которых ИИ не
    возьмёт; уже взятую сделку изменения полей не снимают; ручное «Дать сделку» и возврат диалога фильтров не знают; перехват их применяет;
  • существующие сделки: leave и redistribute, граница `since`, активный менеджер, пилот, флаг записи, сбой смены ответственного, охрана инициативы.

Сети нет: amo, МойСклад, сервис ИИ подставные. Запуск: python -m pytest test_ai_engine_filters.py -q
"""
import asyncio
import datetime
import logging

import pytest

import ai_engine as E
import ai_engine_api as API
import ai_engine_config as cfg
import ai_engine_filters as F
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from test_ai_engine import CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_followup import HOUR, drain, pending, scheduled
from test_ai_engine_give import give
from test_ai_engine_intercept import client as intercept_client
from test_ai_engine_intercept import igor_deal, intercepts, setup as intercept_setup, taken_notes
from test_ai_engine_ur import at, clock, waves

PIPE_TANGEM = 9822330
DEAL2 = DEAL + 1
OPEN_FULL = {"scope": "full", "restricted": False, "items": []}
PILOT_LIST = {"scope": "full", "restricted": True, "items": [{"contact_id": CONTACT, "phone_norm": PHONE, "name": "Пилот"}]}
UUID = "78245ce7-dfda-11f1-0a80-09520056c39a"
AI_USER = {"id": 11513202, "name": "Гладков Александер"}
OLD_MANAGER = 7777777
UTC = datetime.timezone.utc

FIELDS = {"type": F.FIELD_REQUEST_TYPE, "pay": F.FIELD_PAYMENT_METHOD, "status": F.FIELD_PAYMENT_STATUS, "delivery": F.FIELD_DELIVERY,
          "cart": F.FIELD_CART, "comp": F.FIELD_COMPOSITION, "order": F.FIELD_MS_ORDER}


# =============================================================================================
# подставной мир
# =============================================================================================

class FakeMS:
    """Подставной МойСклад: позиции заказов покупателей и каталог по названию. Сети нет, вызовы записываются."""

    def __init__(self):
        self.orders: dict[str, list[dict]] = {}
        self.catalog: dict[str, list[dict]] = {}
        self.calls: list[tuple[str, dict | None]] = []
        self.down = False
        self.delay = 0.0
        self.assortment_down = False

    async def get(self, path, params=None):
        self.calls.append((path, params))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.down:
            return None
        if path.startswith("entity/customerorder/") and path.endswith("/positions"):
            rows = self.orders.get(path.split("/")[2])
            return None if rows is None else {"rows": rows, "meta": {"size": len(rows)}}
        if path in ("entity/assortment", "entity/product"):
            if path == "entity/assortment" and self.assortment_down:
                return None
            return {"rows": list(self.catalog.get((params or {})["filter"][len("name="):], []))}
        raise AssertionError(f"неожиданный запрос {path}")


def row(name, article, typ="product"):
    return {"assortment": {"name": name, "article": article, "meta": {"type": typ}}, "quantity": 1}


def item(name, article, typ="product"):
    return {"name": name, "article": article, "meta": {"type": typ}}


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    F._cache.clear()
    F._inflight.clear()
    F._warned.clear()
    settings.set_document(None)
    monkeypatch.setattr(F, "REASSIGN_ENABLED", False)
    yield
    F._cache.clear()
    F._inflight.clear()


@pytest.fixture
def ms(monkeypatch):
    fake = FakeMS()
    monkeypatch.setattr(F, "_ms_get", fake.get)
    return fake


def full_doc(**blocks):
    """Эффективный документ панели версии 2 для Полного режима (пилот выключен, список контактов не нужен)."""
    doc = dict(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, settings_version=2, mode_id="full",
               review={"enabled": False}, deals={"items": []})
    doc.update(blocks)
    return doc


def use(**blocks):
    """Положить документ в настройки без стенда (для проверок без движка)."""
    settings.set_document(full_doc(**blocks))


def lead(price=0, created_at=None, responsible=None, lead_id=501, **named):
    """Сделка как её отдаёт amo: поля по коротким именам (`type`, `pay`, `status`, `delivery`, `cart`, `comp`, `order`)."""
    out = {"id": lead_id, "pipeline_id": PIPE_RETAIL, "status_id": 1000, "price": price,
           "custom_fields_values": [{"field_id": FIELDS[k], "values": [{"value": v}]} for k, v in named.items() if v is not None],
           "_embedded": {"contacts": [{"id": CONTACT}]}}
    if created_at is not None:
        out["created_at"] = created_at
    if responsible is not None:
        out["responsible_user_id"] = responsible
    return out


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, UTC).isoformat()


def set_full(env, **blocks):
    env.set_settings(**full_doc(**blocks))
    env.mp.setattr(F, "_now", lambda: env.t)


def fields(**named):
    return {FIELDS[k]: v for k, v in named.items()}


def put_lead(env, lead_id=DEAL, price=0, created_at=None, responsible=None, pipeline=PIPE_RETAIL, **named):
    ld = env.lead(lead_id=lead_id, pipeline=pipeline, price=price, fields=fields(**named))
    if created_at is not None:
        ld["created_at"] = created_at
    if responsible is not None:
        ld["responsible_user_id"] = responsible
    return ld


async def client_writes(env, text="Здравствуйте", mid=None):
    await env.say(env.msg(text, mid=mid))
    await env.advance(61)


def quiet(env):
    """Ничего не ушло ни людям, ни в amo, ни сервису ИИ."""
    return env.notes == [] and env.alerts == [] and env.tasks_open == [] and env.turn_requests == []


# =============================================================================================
# сопоставление значений с кодами (реальные значения amo, 23.11.2026)
# =============================================================================================

def test_the_golden_list_of_codes_is_the_one_in_the_schema():
    assert F.CODES == {
        "request_type": ("order", "preorder", "reserve", "other", "unset"),
        "payment_method": ("online", "on_receipt", "cod", "cash", "crypto", "invoice", "other_method", "other", "unset"),
        "payment_status": ("paid", "unpaid", "partial", "other", "unset"),
        "delivery_type": ("cdek", "showroom", "office", "courier_msk", "other", "unset"),
    }


@pytest.mark.parametrize("value, code", [
    ("Заказ", "order"), ("Предзаказ", "preorder"), ("Резерв", "reserve"), ("  заказ ", "order"), ("ПРЕДЗАКАЗ", "preorder"),
    ("Подарок", "other"), ("", "unset"), ("   ", "unset"), (None, "unset"),
])
def test_request_type_codes(value, code):
    assert F.request_type_code(lead(type=value)) == code


@pytest.mark.parametrize("value, code", [
    ("Оплачен", "paid"), ("Не оплачен", "unpaid"), ("Частично оплачен", "partial"), ("оплачен", "paid"), ("  Не   оплачен ", "unpaid"),
    ("Возврат", "other"), ("", "unset"), (None, "unset"),
])
def test_payment_status_codes(value, code):
    assert F.payment_status_code(lead(status=value)) == code


CDEK_ONLINE = "CDEK: Самовывоз, (1-2 дней), 238.00 рублей"
CDEK_COD = "CDEK: Самовывоз из ПВЗ, (4-5 дней), 447.00 рублей Наценка за наложенный платеж, 168.00 рублей"
SHOWROOM = "Самовывоз из шоурума Sunscrypt, 0.00 рублей"
OFFICE = "Самовывоз из офиса Sunscrypt, 0.00 рублей"
COURIER = "Курьерская доставка, 1 000.00 рублей"
MSK_COURIER = "Доставка курьером по Москве, 1 500.00 рублей"
MANAGER_COD = "Доставка через менеджера, 0.00 рублей Наценка за наложенный платеж, 349.50 рублей"


@pytest.mark.parametrize("pay, delivery, code", [
    # онлайн: все реальные написания
    ("Онлайн-оплата", CDEK_ONLINE, "online"), ("Онлайн оплата", CDEK_ONLINE, "online"), ("онлайн-оплата", SHOWROOM, "online"),
    ("Онлайн по ссылке", COURIER, "online"), ("СБП", None, "online"),
    # «При получении»: СДЭК это наложка, всё остальное (включая пустую доставку) просто при получении
    ("При получении", CDEK_ONLINE, "cod"), ("Оплата при получении", "СДЭК: Доставка в постамат, (1-2 дней), 227.00 рублей", "cod"),
    ("При получении", "СДЭК: Курьерская доставка, (1-2 дней), 477.00 рублей", "cod"), ("При получении", "Самовывоз СДЭК, 195.00 рублей", "cod"),
    ("При получении", CDEK_COD, "cod"),
    ("При получении", SHOWROOM, "on_receipt"), ("Оплата при получении", OFFICE, "on_receipt"), ("При получении", COURIER, "on_receipt"),
    ("Оплата при получении", MSK_COURIER, "on_receipt"), ("При получении", "Самовывоз со склада, 0.00 рублей", "on_receipt"),
    ("При получении", "", "on_receipt"), ("При получении", None, "on_receipt"),
    ("При получении", MANAGER_COD, "cod"),                       # запасной признак: в строке доставки слова про наложенный платёж
    # наличные
    ("Наличные менеджер", SHOWROOM, "cash"), ("наличные менеджер", SHOWROOM, "cash"), ("Эвотор", SHOWROOM, "cash"), ("Наличные", OFFICE, "cash"),
    ("Эвотор + наличные менеджер", SHOWROOM, "cash"),
    # остальное
    ("Крипта", None, "crypto"), ("Расчётный счёт", None, "invoice"), ("Счёт юр лицо", None, "invoice"), ("Счёт", None, "invoice"),
    ("Банковский перевод", None, "invoice"), ("Другой способ", None, "other_method"), ("другой способ", None, "other_method"),
    # незнакомое и составное из разных способов: other
    ("картой", None, "other"), ("на карту", None, "other"), ("Наличные менеджер + онлайн оплата", None, "other"),
    ("Онлайн оплата + наличные менеджер", None, "other"),
    # пусто
    ("", None, "unset"), ("   ", None, "unset"), (None, None, "unset"),
])
def test_payment_method_codes(pay, delivery, code):
    assert F.payment_method_code(lead(pay=pay, delivery=delivery)) == code


@pytest.mark.parametrize("value, code", [
    (CDEK_ONLINE, "cdek"), ("CDEK: Самовывоз из ПВЗ, (3-4 дней), 399.00 рублей", "cdek"), ("CDEK: Самовывоз (10 дней), 398.00 рублей", "cdek"),
    ("СДЭК: Доставка в постамат, (1-2 дней), 227.00 рублей", "cdek"), ("СДЭК: Курьерская доставка, (2-3 дней), 563.00 рубля", "cdek"),
    ("CDEK: Посылка склад-дверь, (1-2 дней), 470.00 рублей", "cdek"), ("Самовывоз СДЭК, 195.00 рублей", "cdek"),
    ("СДЭК: Пункт выдачи, 350.00 рублей", "cdek"), (CDEK_COD, "cdek"),
    (SHOWROOM, "showroom"), ("Самовывоз из Шоурума, 0.00 рублей", "showroom"), ("Самовывоз из шоурума Tangem", "showroom"),
    (OFFICE, "office"),
    (COURIER, "courier_msk"), (MSK_COURIER, "courier_msk"), ("Доставка курьером по Москве, 282.00 рубля", "courier_msk"),
    ("Доставка, 0.00 рублей", "courier_msk"), ("Доставка", "courier_msk"),
    ("Доставка (уточняет менеджер), 0.00 рублей", "other"), ("Доставка через менеджера, 0.00 рублей", "other"),
    ("Самовывоз со склада, 0.00 рублей", "other"), ("Почта России, 1 146.00 рублей", "other"), ("другое", "other"),
    ("", "unset"), ("  ", "unset"), (None, "unset"),
])
def test_delivery_type_codes(value, code):
    assert F.delivery_type_code(lead(delivery=value)) == code


def test_comparison_ignores_case_yo_hyphens_and_extra_spaces():
    assert F.norm("  Расчётный   СЧЁТ ") == "расчетный счет"
    assert F.norm("Онлайн-оплата") == F.norm("Онлайн оплата") == "онлайн оплата"
    assert F.norm(None) == ""


# =============================================================================================
# чтение настроек: нет блока, повреждённая подгруппа, версия 1, Тест
# =============================================================================================

NEW_BLOCKS = dict(filters={"payment_method": ["online"], "products": {"enabled": True}}, budget={"skip_from_rub": 100}, target_skus=["HW-26"],
                  existing={"policy": "leave", "since": None, "manager_quiet_min": 15})


def test_version_2_without_the_new_blocks_restricts_nothing():
    use()
    assert (F.filter_groups(), F.products_config(), F.budget_limit(), F.existing_config()) == ({}, None, None, None)
    assert not F.is_configured("full") and not F.is_configured("help") and not F.is_configured("intercept")
    assert F.make_eligibility("full", ["1"]) is None


def test_a_version_1_document_ignores_the_new_blocks():
    settings.set_document(dict(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, **NEW_BLOCKS))
    assert settings.settings_version() == 1
    assert (F.filter_groups(), F.products_config(), F.budget_limit(), F.existing_config()) == ({}, None, None, None)
    assert not F.is_configured("full")


def test_the_test_mode_has_no_filters_and_existing_is_for_the_full_mode_only():
    use(**NEW_BLOCKS)
    assert F.is_configured("full") and F.is_configured("help") and F.is_configured("intercept")
    assert not F.is_configured("test")                                     # Тест ограничивается списком разрешённых
    use(existing={"policy": "leave"})
    assert F.is_configured("full") and not F.is_configured("help") and not F.is_configured("intercept")


def test_filter_groups_read_checked_codes_and_skip_empty_or_broken_groups(caplog):
    caplog.set_level(logging.ERROR, logger="uvicorn")
    use(filters={"request_type": ["order", "preorder"], "payment_method": [], "payment_status": "paid", "delivery_type": ["cdek", "teleport", 5, None]})
    assert F.filter_groups() == {"request_type": frozenset({"order", "preorder"}), "delivery_type": frozenset({"cdek"})}
    assert any("payment_status" in r.message for r in caplog.records)           # не список: подгруппа не действует, причина в логе
    assert any("delivery_type" in r.message for r in caplog.records)           # незнакомые коды отброшены
    n = len(caplog.records)
    F.filter_groups()
    assert len(caplog.records) == n                                            # одна и та же поломка в лог один раз


def test_a_group_of_only_unknown_codes_or_a_broken_filters_block_restricts_nothing():
    use(filters={"payment_method": ["teleport"]})
    assert F.filter_groups() == {}
    use(filters="order")
    assert F.filter_groups() == {} and not F.is_configured("full")


@pytest.mark.parametrize("raw, limit", [
    (None, None), (0, None), ("", None), ("  ", None), (50000, 50000), ("50000", 50000), (50000.0, 50000), (1, 1),
    (True, None), (False, None), (-5, None), ("abc", None), ([1], None), ({}, None), (float("nan"), None), (0.4, None),
])
def test_budget_limit_values(raw, limit):
    use(budget={"skip_from_rub": raw})
    assert F.budget_limit() == limit


def test_budget_block_of_the_wrong_type_or_missing_key_means_no_limit():
    use(budget=[1])
    assert F.budget_limit() is None
    use(budget={})
    assert F.budget_limit() is None


@pytest.mark.parametrize("raw, since", [
    (None, None), ("", None), ("мусор", None), (True, None), (0, None),
    (1_700_000_000, 1_700_000_000.0), ("1700000000", 1_700_000_000.0),
    ("2026-11-22T10:00:00Z", datetime.datetime(2026, 11, 22, 10, 0, tzinfo=UTC).timestamp()),
    ("2026-11-22T13:00:00+03:00", datetime.datetime(2026, 11, 22, 10, 0, tzinfo=UTC).timestamp()),
    ("2026-11-22T10:00:00", datetime.datetime(2026, 11, 22, 10, 0, tzinfo=UTC).timestamp()),       # без пояса значит UTC
])
def test_existing_since_formats(raw, since):
    use(existing={"policy": "leave", "since": raw})
    assert F.existing_config() == {"policy": "leave", "since": since, "manager_quiet_min": 15}


def test_existing_block_values_and_defaults(caplog):
    caplog.set_level(logging.ERROR, logger="uvicorn")
    use(existing={"policy": "redistribute", "manager_quiet_min": 0})
    assert F.existing_config()["manager_quiet_min"] == 0 and F.existing_config()["policy"] == "redistribute"
    use(existing={"policy": "leave", "manager_quiet_min": 999})
    assert F.existing_config()["manager_quiet_min"] == 15                       # вне границ 0-240: значение по умолчанию
    use(existing={"policy": "leave", "manager_quiet_min": "15"})
    assert F.existing_config()["manager_quiet_min"] == 15
    for broken in ({"policy": "delete_all"}, {"policy": None}, {}, "leave", [1]):
        use(existing=broken)
        assert F.existing_config() is None                                      # повреждённая подгруппа не действует (как сегодня)
    assert any("existing.policy" in r.message for r in caplog.records)


def test_product_filter_config_forms_and_defaults():
    use(filters={"products": {"enabled": False}}, target_skus=["HW-26"])
    assert F.products_config() is None
    use(filters={"products": {"enabled": "yes"}}, target_skus=["HW-26"])
    assert F.products_config() is None                                          # включает только настоящее true
    use(filters={"products": {"enabled": True}}, target_skus=[" hw-26 ", {"article": "TG-1", "name": "Tangem"}, "HW  26", 5, None, {"x": 1}])
    conf = F.products_config()
    assert conf == {"allow_empty": True, "allow_unknown": False, "skus": frozenset({"hw-26", "tg-1", "hw 26"})}
    use(filters={"products": {"enabled": True, "allow_empty": False, "allow_unknown": True}}, target_skus=[])
    assert F.products_config() == {"allow_empty": False, "allow_unknown": True, "skus": frozenset()}   # пустой список при включённом фильтре допустим
    use(filters={"products": {"enabled": True, "allow_empty": "нет", "allow_unknown": 1}}, target_skus=[])
    assert F.products_config() == {"allow_empty": True, "allow_unknown": False, "skus": frozenset()}   # не bool: значения по умолчанию


def test_product_filter_without_the_target_list_or_with_a_broken_block_does_not_act(caplog):
    caplog.set_level(logging.ERROR, logger="uvicorn")
    use(filters={"products": {"enabled": True}})
    assert F.products_config() is None and any("target_skus" in r.message for r in caplog.records)
    use(filters={"products": "on"}, target_skus=["A"])
    assert F.products_config() is None


def test_target_list_is_read_only_while_the_filter_is_on():
    use(target_skus=["HW-26"])
    assert F.products_config() is None and not F.is_configured("full")


def test_ai_user_default_valid_and_broken():
    use()
    assert F.ai_user() == AI_USER
    use(ai_user={"id": 42, "name": "Робот"})
    assert F.ai_user() == {"id": 42, "name": "Робот"}
    use(ai_user={"id": 42})
    assert F.ai_user() == {"id": 42, "name": "ИИ-менеджер"}
    for broken in ({"id": "x"}, {"id": 0}, {"id": True}, [1], "Гладков"):
        use(ai_user=broken)
        assert F.ai_user() is None


# =============================================================================================
# проверки по полям сделки: бюджет, группы фильтров, существующие сделки
# =============================================================================================

def test_budget_boundary_is_inclusive_and_zero_budget_is_taken():
    use(budget={"skip_from_rub": 50000})
    assert F.cheap_refusal(lead(price=49999), mode_id="full") is None
    assert "50000" in F.cheap_refusal(lead(price=50000), mode_id="full")
    assert F.cheap_refusal(lead(price=120000), mode_id="full")
    assert F.cheap_refusal(lead(price=0), mode_id="full") is None              # сделки с бюджетом 0 берём
    assert F.cheap_refusal(lead(price=None), mode_id="full") is None
    assert F.cheap_refusal({"id": 1}, mode_id="full") is None


@pytest.mark.parametrize("raw", [None, 0, ""])
def test_budget_zero_or_empty_means_no_limit(raw):
    use(budget={"skip_from_rub": raw})
    assert F.cheap_refusal(lead(price=10 ** 9), mode_id="full") is None


def test_budget_and_filters_do_not_apply_in_the_test_mode_and_apply_in_help_and_intercept():
    use(budget={"skip_from_rub": 100}, filters={"payment_status": ["paid"]})
    big = lead(price=500, status="Не оплачен")
    assert F.cheap_refusal(big, mode_id="test") is None
    assert F.cheap_refusal(big, mode_id="help") and F.cheap_refusal(big, mode_id="intercept") and F.cheap_refusal(big, mode_id="full")


def test_a_group_passes_when_the_deal_value_is_among_the_checked_codes():
    use(filters={"payment_method": ["online", "cod"]})
    assert F.cheap_refusal(lead(pay="Онлайн-оплата"), mode_id="full") is None
    assert F.cheap_refusal(lead(pay="При получении", delivery=CDEK_ONLINE), mode_id="full") is None          # наложка отмечена
    why = F.cheap_refusal(lead(pay="При получении", delivery=SHOWROOM), mode_id="full")                       # при получении без СДЭК не отмечено
    assert why and "on_receipt" in why and "способ оплаты" in why
    assert F.cheap_refusal(lead(pay="Крипта"), mode_id="full")


def test_unset_and_other_are_ordinary_codes_of_a_group():
    use(filters={"payment_status": ["paid"]})
    assert "unset" in F.cheap_refusal(lead(), mode_id="full")                  # поле пустое, а «Не указано» не отмечено
    assert "other" in F.cheap_refusal(lead(status="Возврат"), mode_id="full")
    use(filters={"payment_status": ["paid", "unset", "other"]})
    assert F.cheap_refusal(lead(), mode_id="full") is None and F.cheap_refusal(lead(status="Возврат"), mode_id="full") is None
    assert F.cheap_refusal(lead(status="Не оплачен"), mode_id="full")


def test_groups_combine_with_and_and_an_empty_group_does_not_act():
    use(filters={"request_type": ["order"], "payment_status": ["paid"], "delivery_type": []})
    assert F.cheap_refusal(lead(type="Заказ", status="Оплачен", delivery=COURIER), mode_id="full") is None
    assert "тип заявки" in F.cheap_refusal(lead(type="Предзаказ", status="Оплачен"), mode_id="full")
    assert "статус оплаты" in F.cheap_refusal(lead(type="Заказ", status="Не оплачен"), mode_id="full")


def test_existing_deals_under_leave(env):
    now = env.t
    use(existing={"policy": "leave", "since": now - 100})
    old, new = lead(created_at=now - 3600), lead(created_at=now + 5)
    assert "существующая" in F.cheap_refusal(old, mode_id="full")
    assert F.cheap_refusal(new, mode_id="full") is None
    assert F.cheap_refusal(lead(created_at=now - 100), mode_id="full") is None        # граница: созданная ровно в `since` уже новая
    assert F.cheap_refusal(lead(created_at=now - 101), mode_id="full")
    assert F.cheap_refusal(lead(), mode_id="full")                                    # возраст неизвестен: считается существующей
    assert F.cheap_refusal(old, mode_id="full", explicit=True) is None               # явное разрешение сильнее переключателя
    assert F.cheap_refusal(old, mode_id="full", given=True) is None                  # отданная вручную
    assert F.cheap_refusal(old, mode_id="full", intercept=True) is None              # перехват существующих не касается
    assert F.cheap_refusal(old, mode_id="help") is None and F.cheap_refusal(old, mode_id="test") is None     # `existing` только у Полного
    use(existing={"policy": "redistribute", "since": now - 100})
    assert F.cheap_refusal(old, mode_id="full") is None                              # redistribute берёт и существующие


def test_without_since_the_boundary_is_the_first_time_it_was_needed_and_it_survives(env):
    t0 = env.t
    env.mp.setattr(F, "_now", lambda: t0)
    use(existing={"policy": "leave", "manager_quiet_min": 15})
    assert F.cheap_refusal(lead(created_at=t0 - 10), mode_id="full")                 # создана до первой проверки: существующая
    assert F.cheap_refusal(lead(created_at=t0 + 10), mode_id="full") is None
    env.mp.setattr(F, "_now", lambda: t0 + 5000)
    assert F.cheap_refusal(lead(created_at=t0 + 10), mode_id="full") is None         # граница осталась прежней, не уехала на «сейчас»
    assert F.cheap_refusal(lead(created_at=t0 - 10), mode_id="full")


# =============================================================================================
# разбор корзины
# =============================================================================================

@pytest.mark.parametrize("text, names", [
    ("Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля", ["Tangem 2.0 WHITE (3 Карты)"]),
    ("Tangem 2.0 (3 карты), 1 шт, 5 390.00 рублей Keystone Tablet, 1 шт, 3 490.00 рублей", ["Tangem 2.0 (3 карты)", "Keystone Tablet"]),
    ("Ledger Stax, 1 шт, 35 615.50 рублей Keystone Tablet, 2 шт, 6 980.00 рублей", ["Ledger Stax", "Keystone Tablet"]),
    ("Tangem 2.0 White, 3 карты, 1 шт, 7 590.00 рублей", ["Tangem 2.0 White, 3 карты"]),                       # запятая внутри названия
    ("Tangem 2.0 WHITE (3 Карты), 1 шт, 4 990.00 рублей Tangem 2.0 WHITE (3 Карты), 1 шт, 6 990.00 рублей",
     ["Tangem 2.0 WHITE (3 Карты)", "Tangem 2.0 WHITE (3 Карты)"]),
    ("Заказ № 09971 от 14.11.2026:\n1. Tangem 2.0 WHITE (3 Карты), 1 шт, 4 893.00 рубля\n2. CDEK: Самовывоз, (1-2 дней), 1 шт, 238.00 рублей\n"
     "НДС: 0.00 рублей\nИтого: 5 131.00 рубль", ["Tangem 2.0 WHITE (3 Карты)"]),
    ("Заказ № 09055 от 27.10.2026:\n1. Trezor Safe 3 Galactic Rose, 1 шт, 7 890.00 рублей\n2. CDEK: Самовывоз из ПВЗ, (1-2 дней), 1 , 285.00 рублей\n"
     "3. Наценка за наложенный платеж, 1 , 316.00 рублей\nИтого: 8 491.00 рубль", ["Trezor Safe 3 Galactic Rose"]),
    ("Заказ № 09046 от 26.10.2026:\n1. SafePal S1, 1 шт, 4 190.00 рублей\n2. Самовывоз из Шоурума, 1 , 0.00 рублей\nНДС: 0.00 рублей\nИтого: 4 190.00 рублей",
     ["SafePal S1"]),
    ("1. Tangem 2.0 White, 3 карты, 1 шт, 7 590.00 рублей\n2. СДЭК: Пункт выдачи, 1 шт, 350.00 рублей\nИтого: 7 940.00 рублей", ["Tangem 2.0 White, 3 карты"]),
    ("1. Keystone 3 Pro, 1 шт, 13 491.00 рубль\n2) Промокод SUN10, 1 шт, 100.00 рублей\n3. Скидка 5%, 1 шт, 50.00 рублей\n4. Доставка курьером по Москве, 1 , 1 000.00 рублей",
     ["Keystone 3 Pro"]),
    ("Заказ № 1 от 01.01.2026:\n1. 2.0 Wallet, 2 шт, 100.00 рублей", ["2.0 Wallet"]),                      # «2.0» в начале названия не нумерация
    ("", []), (None, []), ("просто текст без позиций", []), ("Заказ № 1:\nИтого: 10.00 рублей", []),
])
def test_cart_items_from_real_shapes(text, names):
    assert F.parse_cart_items(text) == names


# =============================================================================================
# товары: артикулы позиций и правило «каждая позиция»
# =============================================================================================

PRODUCTS_ON = {"products": {"enabled": True, "allow_empty": True, "allow_unknown": False}}
TARGET = ["HW-26", "tg-1"]
CART = "Tangem 2.0 (3 карты), 1 шт, 7 590.00 рублей"


def products(*, empty=True, unknown=False, skus=TARGET):
    use(filters={"products": {"enabled": True, "allow_empty": empty, "allow_unknown": unknown}}, target_skus=list(skus))


def refusal(ld):
    return asyncio.run(F.products_refusal(ld))


def test_products_filter_off_asks_moysklad_nothing(ms):
    use(target_skus=TARGET)
    assert refusal(lead(order=UUID, cart=CART)) is None and ms.calls == []
    use(filters={"products": {"enabled": False}}, target_skus=TARGET)
    assert refusal(lead(order=UUID, cart=CART)) is None and ms.calls == []


def test_order_path_every_position_must_be_in_the_list_case_insensitive(ms):
    products()
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1"), row("Keystone", "hw-26")]
    assert refusal(lead(order=UUID)) is None
    assert ms.calls[0][0] == f"entity/customerorder/{UUID}/positions" and ms.calls[0][1]["expand"] == "assortment"
    F._cache.clear()
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1"), row("Ledger Stax", "LD-9")]
    assert "LD-9" in refusal(lead(order=UUID))                                  # одна позиция вне списка: не берём


def test_order_path_skips_services_such_as_delivery(ms):
    products()
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1"), row("CDEK доставка", None, typ="service")]
    assert refusal(lead(order=UUID)) is None


def test_order_with_only_services_has_no_goods_and_follows_allow_empty(ms):
    ms.orders[UUID] = [row("Доставка", None, typ="service")]
    products(empty=True)
    assert refusal(lead(order=UUID)) is None
    F._cache.clear()
    products(empty=False)
    assert "нет товаров" in refusal(lead(order=UUID))


def test_order_that_cannot_be_read_is_unrecognized_and_follows_allow_unknown(ms):
    products(unknown=False)
    assert "нераспознанный" in refusal(lead(order=UUID))                         # заказа нет в МойСклад
    F._cache.clear()
    products(unknown=True)
    assert refusal(lead(order=UUID)) is None
    F._cache.clear()
    ms.down = True
    products(unknown=False)
    assert "нераспознанный" in refusal(lead(order=UUID))                         # сбой МойСклад: безопасный отказ


def test_order_with_truncated_rows_is_unrecognized(ms, monkeypatch):
    products()
    ms.orders[UUID] = [row("A", "TG-1")]
    real = ms.get

    async def truncated(path, params=None):
        data = await real(path, params)
        data["meta"]["size"] = 150
        return data

    monkeypatch.setattr(F, "_ms_get", truncated)
    assert "нераспознанный" in refusal(lead(order=UUID))


def test_position_without_article_is_unrecognized_and_a_known_outsider_is_refused_even_when_unknown_is_allowed(ms):
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1"), row("Что-то без артикула", None)]
    products(unknown=False)
    assert "нераспознанный" in refusal(lead(order=UUID))
    F._cache.clear()
    products(unknown=True)
    assert refusal(lead(order=UUID)) is None                                     # нераспознанное разрешено, известные в списке
    F._cache.clear()
    ms.orders[UUID] = [row("Что-то без артикула", None), row("Ledger Stax", "LD-9")]
    assert "LD-9" in refusal(lead(order=UUID))                                   # известный вне списка отказывает всегда


def test_catalog_path_matches_the_cart_name_to_an_article(ms):
    products()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1")]
    assert refusal(lead(cart=CART)) is None
    assert [c[0] for c in ms.calls] == ["entity/assortment"] and ms.calls[0][1]["filter"] == "name=Tangem 2.0 (3 карты)"


def test_catalog_path_with_several_positions_and_a_composition_fallback(ms):
    products()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1")]
    ms.catalog["Keystone Tablet"] = [item("Keystone Tablet", "HW-26")]
    glued = "Tangem 2.0 (3 карты), 1 шт, 5 390.00 рублей Keystone Tablet, 1 шт, 3 490.00 рублей"
    assert refusal(lead(cart=glued)) is None
    composition = "Заказ № 1 от 01.01.2026:\n1. Keystone Tablet, 1 шт, 3 490.00 рублей\n2. CDEK: Самовывоз, (1-2 дней), 1 шт, 238.00 рублей\nИтого: 3 728.00 рубля"
    assert refusal(lead(lead_id=502, comp=composition)) is None                  # «Корзина» пуста, берём «Состав заказа»


def test_catalog_path_not_found_ambiguous_or_mismatching_names_are_unrecognized(ms):
    products()
    assert "нераспознанный" in refusal(lead(cart=CART))                          # нет в каталоге
    F._cache.clear()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1"), item("Tangem 2.0 (3 карты)", "TG-2")]
    assert "нераспознанный" in refusal(lead(cart=CART))                          # два разных артикула под одним названием
    F._cache.clear()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1"), item("tangem 2.0  (3 карты)", "tg-1")]
    assert refusal(lead(cart=CART)) is None                                      # один артикул под разными написаниями: однозначно
    F._cache.clear()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (2 карты)", "TG-1")]
    assert "нераспознанный" in refusal(lead(cart=CART))                          # фильтр МойСклад вернул похожее, но другое название
    F._cache.clear()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1", typ="service")]
    assert "нераспознанный" in refusal(lead(cart=CART))                          # услуги в каталоге товаров не считаются
    F._cache.clear()
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", None)]
    assert "нераспознанный" in refusal(lead(cart=CART))                          # у товара нет артикула


def test_catalog_path_falls_back_to_the_product_list_when_assortment_does_not_answer(ms):
    products()
    ms.assortment_down = True
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1")]
    assert refusal(lead(cart=CART)) is None
    assert [c[0] for c in ms.calls] == ["entity/assortment", "entity/product"]


def test_catalog_path_when_moysklad_is_down_is_unrecognized(ms):
    ms.down = True
    products()
    assert "нераспознанный" in refusal(lead(cart=CART))
    products(unknown=True)
    F._cache.clear()
    assert refusal(lead(cart=CART)) is None


def test_name_with_a_semicolon_is_never_sent_to_the_catalog_filter(ms):
    products()
    assert "нераспознанный" in refusal(lead(cart="Кошелёк; особый, 1 шт, 100.00 рублей"))
    assert ms.calls == []


def test_a_deal_without_goods_follows_allow_empty(ms):
    products(empty=True)
    assert refusal(lead()) is None and ms.calls == []
    products(empty=False)
    assert "нет товаров" in refusal(lead())


def test_text_that_cannot_be_parsed_is_unrecognized_not_empty(ms):
    products(empty=True, unknown=False)
    assert "нераспознанный" in refusal(lead(cart="что-то в новом формате без цен"))
    assert ms.calls == []


def test_empty_target_list_takes_only_deals_without_goods(ms):
    products(skus=[], empty=True)
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1")]
    assert refusal(lead()) is None
    assert "TG-1" in refusal(lead(order=UUID))


def test_results_are_cached_so_a_repeat_check_does_not_call_moysklad(ms):
    products()
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1")]
    ms.catalog["Tangem 2.0 (3 карты)"] = [item("Tangem 2.0 (3 карты)", "TG-1")]
    assert refusal(lead(order=UUID)) is None and refusal(lead(cart=CART)) is None
    calls = len(ms.calls)
    assert calls == 2
    assert refusal(lead(order=UUID)) is None and refusal(lead(cart=CART)) is None and len(ms.calls) == calls


def test_failures_are_cached_briefly_too(ms):
    products()
    ms.down = True
    assert refusal(lead(order=UUID))
    calls = len(ms.calls)
    ms.down = False
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1")]
    assert refusal(lead(order=UUID)) and len(ms.calls) == calls                 # неудача помнится недолго: не долбим МойСклад на каждом сообщении
    F._cache.clear()
    assert refusal(lead(order=UUID)) is None


def test_a_slow_moysklad_means_unrecognized_but_the_answer_still_fills_the_cache(ms, monkeypatch):
    monkeypatch.setattr(F, "MS_WAIT_S", 0.05)
    products()
    ms.delay = 0.3
    ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1")]

    async def go():
        assert "нераспознанный" in await F.products_refusal(lead(order=UUID))   # не дождались: безопасный отказ, обработка сообщений не стоит
        await asyncio.sleep(0.4)                                                # запрос дошёл в фоне и лёг в кэш
        calls = len(ms.calls)
        assert await F.products_refusal(lead(order=UUID)) is None and len(ms.calls) == calls
    run(go())


def test_the_whole_product_check_has_a_deadline(ms, monkeypatch):
    monkeypatch.setattr(F, "PRODUCTS_DEADLINE_S", 0.05)
    monkeypatch.setattr(F, "MS_WAIT_S", 5)
    products()
    ms.delay = 0.5
    t0 = asyncio.run(asyncio.wait_for(F.products_refusal(lead(order=UUID)), 3))
    assert "нераспознанный" in t0


def test_only_a_few_names_of_one_cart_are_looked_up(ms, monkeypatch):
    monkeypatch.setattr(F, "PRODUCTS_MAX_LOOKUPS", 1)
    products()
    ms.catalog["A"] = [item("A", "TG-1")]
    ms.catalog["B"] = [item("B", "TG-1")]
    assert "нераспознанный" in refusal(lead(cart="A, 1 шт, 1.00 рубль B, 1 шт, 1.00 рубль"))
    assert [c[1]["filter"] for c in ms.calls] == ["name=A"]


def test_the_real_ms_wrapper_returns_none_on_errors_and_timeouts(monkeypatch):
    async def boom(path, params=None, retries=3):
        raise RuntimeError("нет связи")

    async def slow(path, params=None, retries=3):
        await asyncio.sleep(5)

    async def ok(path, params=None, retries=3):
        assert retries == 1                                                      # повторов нет: каждый запрос ограничен таймаутом
        return {"rows": []}

    monkeypatch.setattr(F, "MS_CALL_TIMEOUT_S", 0.05)
    monkeypatch.setattr(F.ms_client, "get", boom)
    assert asyncio.run(F._ms_get("entity/product")) is None
    monkeypatch.setattr(F.ms_client, "get", slow)
    assert asyncio.run(F._ms_get("entity/product")) is None
    monkeypatch.setattr(F.ms_client, "get", ok)
    assert asyncio.run(F._ms_get("entity/product")) == {"rows": []}


# =============================================================================================
# охрана инициативы: чистая функция и таблица
# =============================================================================================

def test_initiative_guard_function(env):
    assert F.initiative_allowed_for_deal({"deal_id": 5}, guard=None) is True
    assert F.initiative_allowed_for_deal({"deal_id": 5}, guard={"first_msg_at": None}) is False
    assert F.initiative_allowed_for_deal({"deal_id": 5}, guard={"first_msg_at": 10.0}) is True
    assert F.initiative_allowed_for_deal({"deal_id": 5, "closed": 1}, guard={"first_msg_at": None}) is True       # закрытая: охраны нет
    assert F.initiative_allowed_for_deal({"deal_id": 5, "owner": "human"}, guard={"first_msg_at": None}) is True
    assert F.initiative_allowed_for_deal({"deal_id": 5, "owner": "ai"}, guard={"first_msg_at": None}) is False
    for broken in (None, {}, {"deal_id": "x"}):
        assert F.initiative_allowed_for_deal(broken) is True
    F.guard_on_take(7)
    assert F.initiative_allowed_for_deal({"deal_id": 7}) is False and F.initiative_allowed_for_deal({"deal_id": 8}) is True
    F.mark_first_ai_message(8)                                                   # охраны нет: ничего не делает
    assert F.guard_row(8) is None
    F.mark_first_ai_message(7)
    assert F.initiative_allowed_for_deal({"deal_id": 7}) is True
    first = F.guard_row(7)["first_msg_at"]
    F.mark_first_ai_message(7, now=first + 1000)
    assert F.guard_row(7)["first_msg_at"] == first                               # первое сообщение не переписывается
    F.guard_on_take(7)                                                           # новое взятие: охрана снова
    assert F.initiative_allowed_for_deal({"deal_id": 7}) is False


def test_guard_answers_allowed_when_the_database_fails(monkeypatch, caplog):
    caplog.set_level(logging.ERROR, logger="uvicorn")

    def boom(deal_id):
        raise RuntimeError("база недоступна")

    monkeypatch.setattr(F, "guard_row", boom)
    assert F.initiative_allowed_for_deal({"deal_id": 5}) is True                 # сегодняшнее поведение, а не зависшая инициатива


# =============================================================================================
# движок: фильтры при взятии сделки
# =============================================================================================

def test_a_document_without_the_new_blocks_takes_deals_as_before(env):
    async def go():
        set_full(env)
        put_lead(env, pay="Крипта", status="Не оплачен", type="Предзаказ", price=10 ** 7)
        await client_writes(env)
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai" and owner["mode"] == "full" and len(env.turn_requests) == 1
        assert F.guard_row(DEAL) is None                                         # охраны нет, как сегодня
    run(go())


def test_a_version_1_document_with_the_new_blocks_takes_deals_as_before(env):
    async def go():
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL, PIPE_TANGEM], contacts=OPEN_FULL, **NEW_BLOCKS)
        put_lead(env, pay="Крипта", price=10 ** 7, created_at=int(env.t) - 10 ** 6)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_the_test_mode_ignores_filters(env):
    async def go():
        env.set_settings(settings_version=2, mode_id="test", review={"enabled": True}, filters={"payment_method": ["online"]}, budget={"skip_from_rub": 5})
        env.lead(price=100, fields=fields(pay="Крипта"))
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_refused_deal_changes_nothing_anywhere_and_the_reason_goes_to_the_log(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        set_full(env, filters={"payment_method": ["online"]})
        put_lead(env, pay="Наличные менеджер")
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)                      # ни клиенту, ни в amo, ни людям, ни сервису
        session = store.get_session(CHAT_KEY)
        assert session["state"] == "skip" and session["deal_id"] is None
        lines = [r.getMessage() for r in caplog.records if "не взята" in r.getMessage()]
        assert len(lines) == 1 and str(DEAL) in lines[0] and "способ оплаты" in lines[0] and "cash" in lines[0]
        assert PHONE not in lines[0] and "Тиана" not in lines[0]                 # личных данных нет
    run(go())


def test_a_refused_deal_is_reconsidered_after_the_skip_period_when_the_deal_changed(env):
    async def go():
        set_full(env, filters={"payment_method": ["online"]})
        ld = put_lead(env, pay="Наличные менеджер")
        await client_writes(env, mid="a")
        await env.say(env.msg("ещё", mid="b"))                                   # в окне пропуска сообщение не разбирается
        await env.advance(61)
        assert store.get_owner(DEAL) is None and env.turn_requests == []
        ld["custom_fields_values"] = [{"field_id": F.FIELD_PAYMENT_METHOD, "values": [{"value": "Онлайн-оплата"}]}]
        await env.advance(cfg.SKIP_TTL_S + 1)
        await client_writes(env, mid="c")
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
    run(go())


def test_a_deal_that_passes_the_filters_is_taken(env):
    async def go():
        set_full(env, filters={"payment_method": ["online", "cod"], "request_type": ["order"], "delivery_type": ["cdek"]})
        put_lead(env, pay="Онлайн-оплата", type="Заказ", delivery=CDEK_ONLINE)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
    run(go())


@pytest.mark.parametrize("price, taken", [(49999, True), (50000, False), (50001, False), (0, True)])
def test_budget_through_the_engine(env, price, taken):
    async def go():
        set_full(env, budget={"skip_from_rub": 50000})
        put_lead(env, price=price)
        await client_writes(env)
        assert (store.get_owner(DEAL) is not None) is taken
        assert (len(env.turn_requests) == 1) is taken
    run(go())


def test_products_through_the_engine_with_an_order_in_moysklad(env, ms):
    async def go():
        set_full(env, filters=PRODUCTS_ON, target_skus=TARGET)
        ms.orders[UUID] = [row("Tangem 2.0 (3 карты)", "TG-1")]
        put_lead(env, order=UUID)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_products_through_the_engine_with_a_foreign_article_and_with_moysklad_down(env, ms, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        set_full(env, filters=PRODUCTS_ON, target_skus=TARGET)
        ms.orders[UUID] = [row("Ledger Stax", "LD-9")]
        put_lead(env, order=UUID)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)
        assert any("LD-9" in r.getMessage() and "не взята" in r.getMessage() for r in caplog.records)
        ms.down = True
        F._cache.clear()
        await env.advance(cfg.SKIP_TTL_S + 1)
        await client_writes(env, mid="again")
        assert store.get_owner(DEAL) is None and quiet(env)                      # сбой МойСклад: безопасный отказ
    run(go())


def test_a_deal_with_a_deleted_cart_and_no_order_is_taken_when_empty_deals_are_allowed(env, ms):
    async def go():
        set_full(env, filters=PRODUCTS_ON, target_skus=TARGET)
        put_lead(env)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai" and ms.calls == []
    run(go())


def test_filters_are_checked_once_a_deal_the_ai_already_leads_is_not_taken_back(env):
    async def go():
        set_full(env, filters={"payment_status": ["unpaid"]})
        ld = put_lead(env, status="Не оплачен")
        await client_writes(env)
        turn = env.turn_requests[-1]
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        env.turn_requests.clear()
        ld["custom_fields_values"] = [{"field_id": F.FIELD_PAYMENT_STATUS, "values": [{"value": "Оплачен"}]}]       # клиент оплатил, бюджет изменился
        ld["price"] = 10 ** 6
        set_full(env, filters={"payment_status": ["unpaid"]}, budget={"skip_from_rub": 100})
        await client_writes(env, "спасибо, оплатил", mid="next")
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1        # ход пошёл, сделку не сняли
    run(go())


def test_a_deal_the_engine_or_a_human_already_owns_is_not_checked_again(env):
    async def go():
        set_full(env, filters={"payment_status": ["unpaid"]})
        put_lead(env, status="Оплачен")
        store.set_owner(DEAL, store.OWNER_ENGINE, reason="released", pipeline_id=PIPE_RETAIL, mode="full")
        await client_writes(env)
        assert store.get_session(CHAT_KEY)["state"] == "released"                 # прежний путь, не «пропуск по фильтру»
    run(go())


def test_refused_deals_are_not_suitable_so_rules_a_and_d_do_not_call_people(env):
    async def go():
        set_full(env, filters={"payment_status": ["paid"]})
        put_lead(env, lead_id=DEAL, status="Не оплачен")
        put_lead(env, lead_id=DEAL2, status="Не оплачен")
        await client_writes(env)
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL2) is None and quiet(env)     # без фильтров это правило D и алерт
        assert store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_with_one_deal_refused_the_other_is_the_only_suitable_one(env):
    async def go():
        set_full(env, filters={"payment_status": ["paid"]})
        put_lead(env, lead_id=DEAL, status="Не оплачен")
        put_lead(env, lead_id=DEAL2, status="Оплачен")
        await client_writes(env)
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL2)["owner"] == "ai" and env.alerts == []
    run(go())


def test_two_deals_that_both_pass_are_still_a_duplicate_for_a_human(env):
    async def go():
        set_full(env, filters={"payment_status": ["paid"]})
        put_lead(env, lead_id=DEAL, status="Оплачен")
        put_lead(env, lead_id=DEAL2, status="Оплачен")
        await client_writes(env)
        assert store.get_owner(DEAL2) is None and len(env.alerts) == 1           # правило D остаётся правилом D
    run(go())


def test_a_refused_deal_with_work_signs_still_makes_the_other_deal_a_rule_a_case(env):
    async def go():
        set_full(env, filters={"payment_status": ["paid"]})
        put_lead(env, lead_id=DEAL, status="Не оплачен", price=5000)            # другая открытая сделка клиента с признаком работы
        put_lead(env, lead_id=DEAL2, status="Оплачен")
        await client_writes(env)
        assert len(env.alerts) == 1 and store.get_owner(DEAL2)["owner"] == "human"        # правило A: у клиента уже есть работа, ИИ не входит
    run(go())


def test_the_gate_failure_is_a_refusal_not_a_crash(env, monkeypatch):
    async def go():
        set_full(env, filters={"payment_method": ["online"]})
        put_lead(env, pay="Онлайн-оплата")

        async def boom(*a, **k):
            raise RuntimeError("сбой")

        monkeypatch.setattr(F, "decide_take", boom)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)
    run(go())


# =============================================================================================
# ручная передача, возврат диалога, перехват
# =============================================================================================

def test_give_deal_does_not_apply_filters_budget_or_products(env, ms):
    async def go():
        set_full(env, filters={"payment_method": ["online"], **PRODUCTS_ON}, target_skus=TARGET, budget={"skip_from_rub": 100})
        put_lead(env, price=10 ** 6, pay="Крипта", order=UUID)
        code, out = await give()
        assert (code, out["ok"]) == (200, True)
        assert store.get_owner(DEAL)["owner"] == "ai" and ms.calls == []
    run(go())


def test_give_deal_of_an_existing_deal_puts_the_guard_and_does_not_change_the_responsible(env, monkeypatch):
    async def go():
        patched = []

        async def patch_lead(*a, **k):
            patched.append((a, k))
            return {"ok": True}

        monkeypatch.setattr(amo_service, "patch_lead", patch_lead)
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 10 ** 5, responsible=OLD_MANAGER)
        code, _ = await give()
        assert code == 200 and patched == []
        assert F.guard_row(DEAL)["first_msg_at"] is None
        assert F.initiative_allowed_for_deal(store.get_owner(DEAL)) is False
    run(go())


def test_give_deal_of_a_new_deal_has_no_guard(env):
    async def go():
        set_full(env, existing={"policy": "leave", "since": env.t - 100})
        put_lead(env, created_at=int(env.t))
        code, _ = await give()
        assert code == 200 and F.guard_row(DEAL) is None
    run(go())


def test_return_dialog_does_not_apply_filters(env):
    async def go():
        set_full(env, filters={"payment_method": ["online"]}, budget={"skip_from_rub": 1})
        put_lead(env, price=10 ** 6, pay="Крипта")
        store.set_owner(DEAL, store.OWNER_HUMAN, reason="rule_A", pipeline_id=PIPE_RETAIL, mode="full")
        code, out = await E.return_dialog(DEAL)
        assert (code, out["ok"]) == (200, True) and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def intercept_full(env, **blocks):
    """Перехват при Полном режиме версии 2 (воронки перехвата любые)."""
    intercept_setup(env, **full_doc(**blocks))
    env.mp.setattr(F, "_now", lambda: env.t)


def test_intercept_applies_filters_and_budget_like_a_regular_take(env):
    async def go():
        intercept_full(env, filters={"payment_method": ["online"]}, budget={"skip_from_rub": 5000})
        igor_deal(env)
        env.leads[0]["custom_fields_values"] = [{"field_id": F.FIELD_PAYMENT_METHOD, "values": [{"value": "Крипта"}]}]
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert taken_notes(env) == [] and intercepts(env) == [] and store.get_owner(DEAL) is None       # не по фильтру: не вмешиваемся
        env.notes.clear()
        env.leads[0]["custom_fields_values"] = [{"field_id": F.FIELD_PAYMENT_METHOD, "values": [{"value": "Онлайн-оплата"}]}]
        env.leads[0]["price"] = 5000
        await env.say(intercept_client(env, text="Есть кто?"))
        await env.advance(16 * 60)
        await env.advance(5)
        assert taken_notes(env) == []                                            # бюджет от лимита: не берём
        env.leads[0]["price"] = 4999
        await env.say(intercept_client(env, text="Алло"))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_intercept_asks_moysklad_for_products_and_refuses_on_a_foreign_article(env, ms):
    async def go():
        intercept_full(env, filters=PRODUCTS_ON, target_skus=TARGET)
        igor_deal(env)
        env.leads[0]["custom_fields_values"] = [{"field_id": F.FIELD_MS_ORDER, "values": [{"value": UUID}]}]
        ms.orders[UUID] = [row("Ledger Stax", "LD-9")]
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert taken_notes(env) == [] and store.get_owner(DEAL) is None
    run(go())


def test_intercept_is_not_governed_by_the_existing_deals_policy(env):
    async def go():
        intercept_full(env, existing={"policy": "leave", "since": env.t + 10 ** 6})
        igor_deal(env)
        await env.say(intercept_client(env))
        await env.advance(16 * 60)
        await env.advance(5)
        assert len(taken_notes(env)) == 1 and store.get_owner(DEAL)["owner"] == "ai"
        assert F.guard_row(DEAL) is None
    run(go())


# =============================================================================================
# существующие сделки: leave
# =============================================================================================

def test_leave_does_not_touch_an_existing_deal_even_when_the_client_writes(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        set_full(env, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)
        assert any("существующая сделка" in r.getMessage() and "не взята" in r.getMessage() for r in caplog.records)
    run(go())


def test_leave_takes_a_deal_created_after_since_without_the_guard(env):
    async def go():
        set_full(env, existing={"policy": "leave", "since": iso(env.t - 100), "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 10)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai" and F.guard_row(DEAL) is None
    run(go())


def test_leave_takes_an_existing_deal_of_an_allowed_contact_and_puts_the_guard(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai"                             # явное разрешение сильнее переключателя
        assert F.guard_row(DEAL)["first_msg_at"] is None
    run(go())


def test_filters_still_apply_to_an_allowed_contacts_deal(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, filters={"payment_status": ["paid"]}, existing={"policy": "leave", "since": env.t - 100})
        put_lead(env, created_at=int(env.t) - 3600, status="Не оплачен")
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)
    run(go())


def test_the_boundary_without_since_is_the_first_check(env):
    async def go():
        set_full(env, existing={"policy": "leave", "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and quiet(env)                       # старая сделка: существующая
        boundary = F._first_seen()
        env.leads.clear()
        put_lead(env, created_at=int(boundary) + 30)
        await env.advance(cfg.SKIP_TTL_S + 1)
        await client_writes(env, mid="fresh")
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


# =============================================================================================
# существующие сделки: redistribute
# =============================================================================================

class Amo:
    """Подставные запись ответственного и имена пользователей amo."""

    def __init__(self, env, ok=True):
        self.patches: list[tuple[int, int | None]] = []
        self.ok = ok
        self.status = 200 if ok else 403

        async def patch_lead(lead_id, **kw):
            self.patches.append((int(lead_id), kw.get("responsible_user_id")))
            return {"ok": self.ok, "status_code": self.status, "retryable": False}

        async def user_name(user_id):
            return {OLD_MANAGER: "Иванова Мария"}.get(int(user_id))

        env.mp.setattr(amo_service, "patch_lead", patch_lead)
        env.mp.setattr(amo_service, "get_user_name", user_name)


def redistribute(env, **over):
    block = {"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}
    block.update(over)
    set_full(env, existing=block, ai_user=AI_USER)
    env.mp.setattr(F, "REASSIGN_ENABLED", True)


def test_redistribute_takes_an_existing_deal_and_assigns_the_ai_user(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [(DEAL, AI_USER["id"])]
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
        notes = [t for d, t in env.notes if d == DEAL and "Ответственный изменён" in t]
        assert notes == ["Ответственный изменён с Иванова Мария на Гладков Александер (ИИ, режим Полный)"]
        journal = F.reassign_journal()
        assert len(journal) == 1 and (journal[0]["deal_id"], journal[0]["from_id"], journal[0]["to_id"], journal[0]["ok"]) == (DEAL, OLD_MANAGER, 11513202, 1)
        assert env.alerts == [] and env.tasks_open == []                          # прежнему ответственному пинга и задачи нет
        assert F.guard_row(DEAL)["first_msg_at"] is None                          # инициатива ждёт первого сообщения ИИ
    run(go())


def test_the_assignment_goes_through_before_the_deal_is_taken(env):
    async def go():
        order = []
        amo = Amo(env)
        real = amo_service.patch_lead

        async def patch_lead(lead_id, **kw):
            order.append(("patch", store.get_owner(DEAL)))
            return await real(lead_id, **kw)

        env.mp.setattr(amo_service, "patch_lead", patch_lead)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert order == [("patch", None)] and store.get_owner(DEAL)["owner"] == "ai"        # владельца в базе ещё не было, когда писали в amo
    run(go())


def test_redistribute_leaves_the_responsible_alone_when_it_is_already_the_ai_user(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=AI_USER["id"])
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL)["owner"] == "ai"
        assert not any("Ответственный изменён" in t for _, t in env.notes) and F.reassign_journal() == []
        assert F.guard_row(DEAL) is not None                                      # сделка существующая: охрана всё равно
    run(go())


def test_redistribute_takes_a_new_deal_without_touching_the_responsible(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 10, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL)["owner"] == "ai" and F.guard_row(DEAL) is None
    run(go())


def test_redistribute_with_the_pilot_on_does_not_take_the_deal_and_does_not_assign(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        amo = Amo(env)
        redistribute(env)
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, review={"enabled": True})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL) is None and quiet(env)
        assert any("пилот" in r.getMessage() and "не взята" in r.getMessage() for r in caplog.records)
    run(go())


def test_redistribute_without_the_write_flag_does_not_take_the_deal(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        amo = Amo(env)
        redistribute(env)
        env.mp.setattr(F, "REASSIGN_ENABLED", False)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL) is None and quiet(env)
        assert any("AI_REASSIGN_ENABLED" in r.getMessage() for r in caplog.records)
    run(go())


def test_redistribute_with_a_broken_ai_user_block_does_not_take_the_deal(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100}, ai_user={"id": "x"})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL) is None and quiet(env)
    run(go())


def test_a_failed_assignment_means_the_deal_is_not_taken(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        amo = Amo(env, ok=False)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert amo.patches == [(DEAL, 11513202)]
        assert store.get_owner(DEAL) is None and env.turn_requests == [] and env.notes == [] and env.alerts == []
        assert store.get_session(CHAT_KEY)["state"] == "skip"
        assert F.guard_row(DEAL) is None
        journal = F.reassign_journal()
        assert len(journal) == 1 and journal[0]["ok"] == 0
        assert any("смена ответственного не удалась" in r.getMessage() for r in caplog.records)
    run(go())


def test_an_exception_during_the_assignment_means_the_deal_is_not_taken(env):
    async def go():
        redistribute(env)

        async def boom(*a, **k):
            raise RuntimeError("amo не отвечает")

        env.mp.setattr(amo_service, "patch_lead", boom)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and env.turn_requests == []
    run(go())


def test_a_failed_note_does_not_undo_a_successful_assignment(env):
    async def go():
        Amo(env)
        redistribute(env)

        async def broken_note(lead_id, text):
            raise RuntimeError("не записалось")

        env.mp.setattr(amo_service, "add_note", broken_note)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai" and F.reassign_journal()[0]["ok"] == 1
    run(go())


def test_the_assignment_is_not_made_when_the_ai_service_is_down(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        env.mp.setattr(E, "_ai_ok_at", env.t - 10 ** 6)
        env.health_ok = False
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL) is None                # сервис лежит: ИИ не входит, ответственного не трогаем
    run(go())


def test_redistribute_refused_deals_still_pass_the_filters_first(env):
    async def go():
        amo = Amo(env)
        redistribute(env)
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100}, ai_user=AI_USER, filters={"payment_status": ["paid"]})
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER, status="Не оплачен")
        await client_writes(env)
        assert amo.patches == [] and store.get_owner(DEAL) is None
    run(go())


# --- менеджер активен ---

async def manager_writes(env):
    await env.say(env.msg("Добрый день, это менеджер", echo=True, author="Игорь Оанча", mid=f"mgr-{env.t}"))


def test_an_active_manager_blocks_the_takeover_until_the_quiet_time_has_passed(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        amo = Amo(env)
        redistribute(env)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        t_manager = env.t
        await manager_writes(env)
        assert F.manager_last_out(CHAT_KEY) == t_manager
        env.t += 300                                                                # менеджер писал 5 минут назад
        await client_writes(env, mid="c1")
        assert amo.patches == [] and store.get_owner(DEAL) is None and quiet(env)
        assert any("менеджер писал клиенту менее 15 мин назад" in r.getMessage() for r in caplog.records)
        session = store.get_session(CHAT_KEY)
        assert session["state"] == "skip" and session["skip_until"] == pytest.approx(t_manager + 900)      # пересмотр, когда пройдёт тишина менеджера
        env.t = t_manager + 16 * 60                                                 # менеджер молчит больше 15 минут
        await client_writes(env, "Алло", mid="c2")
        assert amo.patches == [(DEAL, 11513202)] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_zero_quiet_minutes_means_the_manager_is_not_checked(env):
    async def go():
        amo = Amo(env)
        redistribute(env, manager_quiet_min=0)
        put_lead(env, created_at=int(env.t) - 3600, responsible=OLD_MANAGER)
        await manager_writes(env)
        env.t += 10
        await client_writes(env)
        assert amo.patches == [(DEAL, 11513202)] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_robot_or_ai_echo_does_not_count_as_an_active_manager(env):
    async def go():
        await env.say(env.msg("Здравствуйте, я ИИ", echo=True, author="", mid="robot-1"))
        await env.say(env.msg("Автоответ", echo=True, author="Admin", mid="robot-2"))
        assert F.manager_last_out(CHAT_KEY) is None
        await env.say(env.msg("Это человек", echo=True, author="Игорь Оанча", mid="human-1"))
        assert F.manager_last_out(CHAT_KEY) == env.t
    run(go())


def test_the_manager_quiet_time_is_not_checked_under_leave(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        await manager_writes(env)
        env.t += 10
        await client_writes(env)
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


# =============================================================================================
# охрана инициативы по взятой существующей сделке
# =============================================================================================

async def take_existing(env):
    set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
    put_lead(env, created_at=int(env.t) - 3600)
    await client_writes(env)
    turn = env.turn_requests[-1]
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    env.turn_requests.clear()
    assert F.guard_row(DEAL)["first_msg_at"] is None


def test_the_silence_ladder_waits_for_the_first_ai_message(env):
    async def go():
        await take_existing(env)
        await E.silence_arm(DEAL)                                                   # отсчёт взведён, а первого разрешения на отправку не было
        await env.advance(3 * HOUR)
        await env.advance(5)
        assert [r for r in env.turn_requests if r.get("event", {}).get("type") == "silent_alt"] == []
        assert env.alerts == []
        F.mark_first_ai_message(DEAL)
        await env.advance(5)
        await env.advance(5)
        assert [r for r in env.turn_requests if r.get("event", {}).get("type") == "silent_alt"]
    run(go())


def test_the_ur_windows_skip_a_guarded_deal_and_run_after_the_first_message(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": at(13), "manager_quiet_min": 15})
        clock(env, at(14))
        put_lead(env, created_at=int(at(12)))
        env.mp.setattr(F, "_now", lambda: env.t)
        await client_writes(env)
        turn = env.turn_requests[-1]
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        env.turn_requests.clear()
        assert F.guard_row(DEAL)["first_msg_at"] is None
        clock(env, at(17, 55))
        for _ in range(3):
            await E.tick_once()
            await env.advance(5)
        assert waves(env) == []                                                      # окно 17:50 по старой сделке не прошло
        F.mark_first_ai_message(DEAL)
        for _ in range(3):
            await E.tick_once()
            await env.advance(5)
        assert len(waves(env)) == 1                                                  # после первого сообщения ИИ окно срабатывает, как обычно
    run(go())


def test_a_deferred_touch_of_a_guarded_deal_is_cancelled_with_a_reason_and_a_normal_one_fires(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        turn = await scheduled(env, hours=24)                                       # ИИ зарегистрировал касание в ходе
        assert pending() is not None and F.guard_row(DEAL) is not None
        await env.advance(25 * HOUR)
        await drain()
        assert pending() is None
        assert [r for r in env.turn_requests if r.get("event", {}).get("type") == "followup_due"] == []
        assert any("ИИ ещё не писал клиенту" in t for _, t in env.notes) or any("ИИ ещё не писал клиенту" in a for a in env.alerts)
    run(go())


def test_a_deferred_touch_fires_once_the_first_message_was_sent(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        await scheduled(env, hours=24)
        F.mark_first_ai_message(DEAL)
        await env.advance(25 * HOUR)
        await env.advance(5)
        assert [r for r in env.turn_requests if r.get("event", {}).get("type") == "followup_due"]
    run(go())


def test_the_permit_endpoint_lifts_the_guard_only_when_it_allows_the_message(env):
    async def go():
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15})
        put_lead(env, created_at=int(env.t) - 3600)
        await client_writes(env)
        turn = env.turn_requests[-1]
        stale = API.PermitIn(deal_id=DEAL, epoch=turn["epoch"] + 5, turn_id=turn["turn_id"], up_to_message_id=turn["up_to_message_id"])
        resp = await API.permit(stale, "engine-token", "2")
        assert resp.status_code == 409 and F.guard_row(DEAL)["first_msg_at"] is None       # отказ: сообщения не будет, охрана остаётся
        good = API.PermitIn(deal_id=DEAL, epoch=turn["epoch"], turn_id=turn["turn_id"], up_to_message_id=turn["up_to_message_id"])
        resp = await API.permit(good, "engine-token", "2")
        assert resp.status_code == 200 and F.guard_row(DEAL)["first_msg_at"] is not None
        assert F.initiative_allowed_for_deal(store.get_owner(DEAL)) is True
    run(go())


def test_no_existing_block_means_no_guard_and_the_old_behaviour_of_the_ladder(env):
    async def go():
        set_full(env)
        put_lead(env, created_at=int(env.t) - 3600)
        await client_writes(env)
        turn = env.turn_requests[-1]
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        env.turn_requests.clear()
        await E.silence_arm(DEAL)
        await env.advance(HOUR + 60)
        await env.advance(5)
        await env.advance(5)
        assert [r for r in env.turn_requests if r.get("event", {}).get("type") == "silent_alt"]
    run(go())
