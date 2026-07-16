"""Юнит-тест lead_dedup (без сети/прода): стабы amo_service/api.

Кейсы:
  ORDER_WINS:
    1. заказ + открытая консультация в раннем этапе → консультация 143 «Дубль
       сделки», перекрёстные примечания, ответственный консультации → на заказ;
    2. консультация в продвинутом этапе → НЕ закрываем, тег+примечания;
    3. кандидат-предзаказ → не трогаем;
    4. кандидат-«другой сайт-заказ» → не трогаем (второй заказ клиента легален);
  POSTSALE:
    5. чатовая сделка + недавняя 142 и нет открытых → тег + ответственный + note;
    6. есть другая открытая → скип;
  общие:
    7. не-CLEVER → скип.
"""

import asyncio

import lead_dedup

CLEVER = lead_dedup.PIPELINE_CLEVER
lead_dedup.ORDER_WINS_ENABLED = True
lead_dedup.POSTSALE_ENABLED = True
lead_dedup.DELAY_S = 0

calls = {"patch": [], "notes": [], "tags": [], "resp": []}


def _reset():
    for v in calls.values():
        v.clear()


def _lead(lid, status, *, pipeline=CLEVER, site_order=None, order_type=None,
          created=1000, updated=1000, closed=None, resp=111, contacts=(9001,)):
    cfv = []
    if site_order is not None:
        cfv.append({"field_id": lead_dedup.F_SITE_ORDER, "values": [{"value": site_order}]})
    if order_type is not None:
        cfv.append({"field_id": lead_dedup.F_ORDER_TYPE, "values": [{"value": order_type}]})
    return {
        "id": lid, "status_id": status, "pipeline_id": pipeline,
        "created_at": created, "updated_at": updated, "closed_at": closed,
        "responsible_user_id": resp,
        "custom_fields_values": cfv,
        "_embedded": {"contacts": [{"id": c} for c in contacts]},
    }


_main_lead = None
_siblings = []


async def _fake_get_lead_full(lead_id, with_=()):
    return _main_lead


async def _fake_patch_lead(lead_id, **kw):
    calls["patch"].append((lead_id, kw))
    return {"ok": True}


async def _fake_add_note(lead_id, text):
    calls["notes"].append((lead_id, text))
    return {"ok": True}


async def _fake_add_tag(lead_id, tag):
    calls["tags"].append((lead_id, tag))
    return {"ok": True}


async def _fake_do_patch(path, body):
    calls["resp"].append((path, body))
    return {"ok": True}


class _FakeApi:
    @staticmethod
    async def get_contact(cid, with_leads=False):
        return {"_embedded": {"leads": [{"id": ld["id"]} for ld in _siblings]}}

    @staticmethod
    async def get_leads_by_ids(ids):
        return list(_siblings)


lead_dedup.amo_service.get_lead_full = _fake_get_lead_full
lead_dedup.amo_service.patch_lead = _fake_patch_lead
lead_dedup.amo_service.add_note = _fake_add_note
lead_dedup.amo_service.add_tag = _fake_add_tag
lead_dedup.amo_service._do_patch = _fake_do_patch
import sys
sys.modules["api"] = _FakeApi  # lead_dedup импортирует api лениво внутри функции


def _run(main, siblings):
    global _main_lead, _siblings
    _main_lead, _siblings = main, siblings
    _reset()
    asyncio.run(lead_dedup._process(main["id"], "test"))


# 1. заказ побеждает раннюю консультацию
order = _lead(2, 83537714, site_order="17999", created=2000)
cons = _lead(1, 83537714, created=1000, resp=222)
_run(order, [cons])
assert any(lid == 1 and kw.get("status_id") == 143 for lid, kw in calls["patch"]), "консультация должна закрыться 143"
assert any(lid == 1 for lid, _ in calls["notes"]) and any(lid == 2 for lid, _ in calls["notes"]), "перекрёстные примечания"
assert any("/leads/2" in p and b.get("responsible_user_id") == 222 for p, b in calls["resp"]), "ответственный переносится на заказ"

# 2. консультация в продвинутом этапе — не закрываем
cons_adv = _lead(3, 85000000, created=1000)  # статус вне EARLY_STATUSES
_run(order, [cons_adv])
assert not calls["patch"], "продвинутую консультацию не закрываем"
assert any(t == lead_dedup.TAG_MAYBE_DUP for _, t in calls["tags"]), "тег «возможен дубль»"

# 3. предзаказ не трогаем
pre = _lead(4, 83537714, created=1000, order_type="Предзаказ")
_run(order, [pre])
assert not calls["patch"] and not calls["notes"], "предзаказ игнорируется"

# 4. другой сайт-заказ не трогаем
order_old = _lead(5, 83537714, created=1000, site_order="17555")
_run(order, [order_old])
assert not calls["patch"] and not calls["notes"], "второй заказ клиента легален"

# 5. пост-продажа: недавняя 142, открытых нет
import time
chat = _lead(6, 83537714)  # без site_order → «не заказ»
won = _lead(7, 142, closed=int(time.time()) - 3 * 86400, resp=333)
_run(chat, [won])
assert any(t == lead_dedup.TAG_POSTSALE for _, t in calls["tags"]), "тег Пост-продажа"
assert any(b.get("responsible_user_id") == 333 for _, b in calls["resp"]), "ответственный из успешной"
assert any(lid == 6 for lid, _ in calls["notes"]), "примечание на новой"

# 6. есть открытая другая → скип пост-продажи
open_other = _lead(8, 83537718)
_run(chat, [won, open_other])
assert not calls["tags"] and not calls["resp"], "открытая сделка есть — пост-продажа не наша"

# 7. не-CLEVER → скип
foreign = _lead(9, 83537714, pipeline=999, site_order="18000")
_run(foreign, [cons])
assert not calls["patch"] and not calls["notes"] and not calls["tags"], "чужая воронка — скип"

print("lead_dedup: все тесты прошли (order_wins + postsale + гейты)")
