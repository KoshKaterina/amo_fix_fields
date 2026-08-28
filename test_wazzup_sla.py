"""Юнит-тесты чистой логики wazzup_sla (без сети/amo/telegram).

Запуск: python3 -m pytest test_wazzup_sla.py -q
        (или python3 test_wazzup_sla.py — свой мини-раннер ниже)
"""
import asyncio
import datetime
import sys
import types

# --- стабы тяжёлых зависимостей (aiogram/amo/httpx) — тест только про логику ---
def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)
_stub("telegram_bot", send_alert=None)


def _cf_value(entity, field_id):
    """Копия чистого хелпера amo_service.get_custom_field_value: сам модуль застаблен
    целиком (тянет httpx), а функция нужна is_pickup_lead для чтения типа доставки."""
    for f in (entity or {}).get("custom_fields_values") or []:
        if f.get("field_id") == field_id:
            values = f.get("values") or []
            if values:
                return values[0].get("value")
    return None


_stub("amo_service", find_leads_by_query=None,
      find_contacts_by_query=None, get_talks_by_contact=None,
      get_custom_field_value=_cf_value)
_stub("api", BASE_URL="https://amo.example")
_stub("httpx", AsyncClient=object)
# tg_recipients НЕ стабим — он тянет только waybill_config (реальную карту хендлов),
# чтобы тесты тега работали против настоящей логики.

import tg_recipients as T  # noqa: E402
import wazzup_sla as W  # noqa: E402


def _msg(chat_id="79990000000", is_echo=False, status=None, text="привет",
         chat_type="whatsapp", channel="ch1", name="Иван"):
    m = {"channelId": channel, "chatId": chat_id, "chatType": chat_type,
         "text": text, "contact": {"name": name}}
    if is_echo is not None:
        m["isEcho"] = is_echo
    if status is not None:
        m["status"] = status
    return m


_ORIG_IN_WINDOW = W._in_window
_ORIG_RESOLVE = W._resolve_lead_safe
_ORIG_TALK_CLOSED = W._talk_closed_safe
_ORIG_ROP_CHAT = W.ROP_CHAT_ID


async def _resolve_nothing(chat_id):
    """Дефолт для тестов, где сделка не важна: не найдена → общий порог, тег смены.
    Без него _fill_lead_info полез бы в застабленный amo_service."""
    return None, None, False


def setup_function(_=None):
    W._pending.clear()
    # восстановить всё, что sweep-тесты могли подменить
    W._in_window = _ORIG_IN_WINDOW
    W._resolve_lead_safe = _resolve_nothing
    W._talk_closed_safe = _ORIG_TALK_CLOSED
    W.ROP_CHAT_ID = _ORIG_ROP_CHAT


def test_inbound_starts_timer():
    W._pending.clear()
    W.handle_webhook({"messages": [_msg(is_echo=False)]})
    assert len(W._pending) == 1
    st = next(iter(W._pending.values()))
    assert st["alerted"] is False
    assert st["text"] == "привет"


def test_skipped_channel_never_starts_timer():
    """Решение Кати 06.08.2026: партнёрский телеграм Саши (обменники, боты,
    блогеры) SLA не сторожит — там пишут не клиенты."""
    W._pending.clear()
    skipped = next(iter(W.WAZZUP_SLA_SKIP_CHANNELS))
    W.handle_webhook({"messages": [_msg(channel=skipped, text="Добрый день!")]})
    assert W._pending == {}
    # клиентский канал в том же вебхуке продолжает работать
    W.handle_webhook({"messages": [_msg(channel="ch1", text="Добрый день!")]})
    assert len(W._pending) == 1


def test_repeated_inbound_does_not_reset_timer():
    """Таймер считаем от первого неотвеченного сообщения: повторное входящее
    не сдвигает waiting_since (иначе частые сообщения = вечное молчание алерта)."""
    W._pending.clear()
    W.handle_webhook({"messages": [_msg(is_echo=False, text="раз")]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 25 * 60  # 25 мин уже ждём
    first_since = st["waiting_since"]
    W.handle_webhook({"messages": [_msg(is_echo=False, text="два")]})
    st2 = next(iter(W._pending.values()))
    assert st2["waiting_since"] == first_since, "waiting_since не должен сброситься"
    assert st2["text"] == "два", "сниппет обновляется на последний"


def test_outbound_resets_timer():
    W._pending.clear()
    W.handle_webhook({"messages": [_msg(is_echo=False)]})
    assert len(W._pending) == 1
    # ответ менеджера (исходящее) — снимает ожидание
    W.handle_webhook({"messages": [_msg(is_echo=True, status="sent")]})
    assert len(W._pending) == 0


def test_status_delivery_is_not_a_message():
    """SYSTEM-WZ / статусы доставки приходят в statuses[], НЕ messages[] —
    не должны стартовать таймер."""
    W._pending.clear()
    W.handle_webhook({"statuses": [{"messageId": "x", "status": "error",
                                    "error": {"description": "template marketing limit"}}]})
    assert len(W._pending) == 0


def test_outbound_by_status_without_isecho():
    """Если isEcho не пришёл, но status=sent/delivered — это исходящее (сброс)."""
    W._pending.clear()
    W.handle_webhook({"messages": [_msg(is_echo=None, status="inbound")]})
    assert len(W._pending) == 1
    W.handle_webhook({"messages": [_msg(is_echo=None, status="delivered")]})
    assert len(W._pending) == 0


def test_window_check():
    mk = lambda h: datetime.datetime(2026, 7, 9, h, 0, tzinfo=W._MSK)
    assert W._in_window(mk(12)) is True
    assert W._in_window(mk(18)) is True
    assert W._in_window(mk(11)) is False
    assert W._in_window(mk(19)) is False   # 19:00 не включаем
    assert W._in_window(mk(9)) is False


def test_sweep_marks_alerted_and_dedups(monkeypatch=None):
    """В окне, возраст ≥ порога → один алерт, повторный проход не дублирует."""
    W._pending.clear()
    sent = []

    async def fake_send(text, **kw):
        sent.append(text)
        return True

    async def fake_resolve(chat_id):
        return 12345, 13929334, False  # сделка + ответственный Егор, доставка не самовывоз

    W.telegram_bot.send_alert = fake_send
    W._resolve_lead_safe = fake_resolve
    W._in_window = lambda now=None: True  # форсим окно

    async def fake_talk_open(st):
        return False  # беседа не закрыта — обычный путь алерта

    W._talk_closed_safe = fake_talk_open

    # клиент написал «давно» (сдвигаем waiting_since назад на 40 мин)
    W.handle_webhook({"messages": [_msg(is_echo=False, text="где заказ?")]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 40 * 60

    asyncio.run(W._sweep(threshold_s=30 * 60))
    assert len(sent) == 1, "должен быть ровно один алерт"
    assert "где заказ?" in sent[0]
    assert "@egorkonsss" in sent[0], "тег ответственного"
    assert "@gladkov_369" not in sent[0], "Саша в отпуске — не тегаем (13.08.2026)"
    assert st["alerted"] is True

    # повторный проход — без нового алерта
    asyncio.run(W._sweep(threshold_s=30 * 60))
    assert len(sent) == 1, "повторно слать нельзя"


def test_sweep_holds_outside_window():
    W._pending.clear()
    sent = []

    async def fake_send(text, **kw):
        sent.append(text)
        return True

    W.telegram_bot.send_alert = fake_send
    W._in_window = lambda now=None: False  # вне окна

    W.handle_webhook({"messages": [_msg(is_echo=False)]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 40 * 60
    asyncio.run(W._sweep(threshold_s=30 * 60))
    assert len(sent) == 0, "вне окна не досылаем"
    assert st["alerted"] is False


def test_mentions_responsible_only():
    # Надзорный тег снят 13.08.2026 (отпуск Саши) — тегаем только ответственного.
    m = T.mentions_for(13929334)  # Егор
    assert m == "@egorkonsss"
    assert "@gladkov_369" not in m


def test_mentions_gladkov_falls_back_to_shift():
    # Саша убран из карты ответственных → его сделки уходят всей смене,
    # а сам он не тегается нигде.
    m = T.mentions_for(11513202)
    assert m == T.MANAGERS_ON_SHIFT
    assert "@gladkov_369" not in m


def test_mentions_igor_and_kirill():
    assert T.mentions_for(9291546) == "@thebarsa1"    # Игорь
    assert T.mentions_for(13946318) == "@offf1cer"   # Кирилл


def test_mentions_artem_b2b():
    # ОПТ-сделки не должны падать в фолбэк «вся розничная смена» (MAG-жалоба
    # Тианы 31.07.2026: пропуск на сделке Артёма тегал офицера/Егора/Катю).
    assert T.mentions_for(13822630) == "@sunscryptb2b"


def test_mentions_unknown_falls_back_to_shift():
    assert T.mentions_for(None) == T.MANAGERS_ON_SHIFT
    assert T.mentions_for(999999) == T.MANAGERS_ON_SHIFT  # не наш МОП → вся смена



# --- самовывоз: порог 3 минуты и один адресат (Катя 13.08.2026) ---------------

def _sweep_stubs(pickup=False, lead=12345, responsible=13929334):
    """Общие подмены: окно открыто, беседа в amo не закрыта, сделка нашлась.
    Возвращает список отправленных текстов."""
    sent = []

    async def fake_send(text, **kw):
        sent.append(text)
        return True

    async def fake_resolve(chat_id):
        return lead, responsible, pickup

    async def fake_talk_open(st):
        return False

    W.telegram_bot.send_alert = fake_send
    W._resolve_lead_safe = fake_resolve
    W._talk_closed_safe = fake_talk_open
    W._in_window = lambda now=None: True
    return sent


def test_sweep_pickup_alerts_after_three_minutes():
    """Тип доставки — наш самовывоз → алерт на 4-й минуте, хотя общий порог 15,
    и тег ОДИН: Катя-офис, смену не будим."""
    W._pending.clear()
    sent = _sweep_stubs(pickup=True)

    W.handle_webhook({"messages": [_msg(is_echo=False, text="я подъезжаю")]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 4 * 60

    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert len(sent) == 1, "самовывоз должен алертить на 4-й минуте"
    assert "@kathrina_bistraya" in sent[0]
    assert "@offf1cer" not in sent[0], "смену тут не тегаем"
    assert "@egorkonsss" not in sent[0], "ответственного тоже не тегаем"
    assert "самовывоз" in sent[0], "в тексте видно, почему разбудили так быстро"
    assert f"{W.WAZZUP_SLA_PICKUP_MINUTES}+ мин" in sent[0], "порог в тексте — фактический"
    assert st["alerted"] is True


def test_sweep_non_pickup_still_waits_full_threshold():
    """Все остальные ждут общий порог: на 4-й минуте молчим, на 16-й алертим
    как раньше — с тегом ответственного и Гладкова."""
    W._pending.clear()
    sent = _sweep_stubs(pickup=False)

    W.handle_webhook({"messages": [_msg(is_echo=False, text="сколько стоит?")]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 4 * 60
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert sent == [], "не самовывоз — на 4-й минуте рано"
    assert st["alerted"] is False

    st["waiting_since"] -= 12 * 60  # итого 16 минут
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert len(sent) == 1
    assert "@egorkonsss" in sent[0], "обычный путь — тег ответственного"
    assert "@kathrina_bistraya" not in sent[0], "Катю-офис тут не тегаем — это не самовывоз"
    assert "самовывоз" not in sent[0]
    assert "15+ мин" in sent[0]


def test_sweep_reads_lead_once_per_waiting():
    """Сделку читаем один раз на ожидание, а не на каждый проход цикла."""
    W._pending.clear()
    sent = _sweep_stubs(pickup=False)
    calls = []
    orig = W._resolve_lead_safe

    async def counting_resolve(chat_id):
        calls.append(chat_id)
        return await orig(chat_id)

    W._resolve_lead_safe = counting_resolve

    W.handle_webhook({"messages": [_msg(is_echo=False)]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 5 * 60
    asyncio.run(W._sweep(threshold_s=15 * 60))   # прочитали сделку, ждём дальше
    asyncio.run(W._sweep(threshold_s=15 * 60))   # второй проход — повторно не читаем
    assert len(calls) == 1, f"amo должен быть опрошен один раз, а не {len(calls)}"
    assert sent == []


def test_fill_lead_info_retries_when_lead_not_found():
    """Сделки ещё нет (клиент написал раньше, чем она создалась) — пробуем снова,
    но не чаще _RESOLVE_RETRY_S, иначе будем дёргать amo каждую минуту."""
    calls = []

    async def resolve_empty(chat_id):
        calls.append(chat_id)
        return None, None, False

    W._resolve_lead_safe = resolve_empty
    st = {"chat_id": "79990000000"}

    asyncio.run(W._fill_lead_info(st, 1000.0))
    asyncio.run(W._fill_lead_info(st, 1000.0 + 60))          # минуту спустя — рано
    assert len(calls) == 1
    asyncio.run(W._fill_lead_info(st, 1000.0 + W._RESOLVE_RETRY_S + 1))
    assert len(calls) == 2, "после паузы попытка повторяется"


def _lead(status_id=83537718, delivery=None, updated_at=100, lead_id=1, responsible=13929334):
    ld = {"id": lead_id, "status_id": status_id, "updated_at": updated_at,
          "responsible_user_id": responsible}
    if delivery is not None:
        ld["custom_fields_values"] = [
            {"field_id": W.FIELD_DELIVERY_TYPE, "values": [{"value": delivery}]}]
    return ld


def test_is_pickup_lead_on_live_values():
    """Значения взяты из живых сделок (срез 14 дней, 13.08.2026)."""
    assert W.is_pickup_lead(_lead(delivery="Самовывоз из офиса Sunscrypt, 1 шт, 0.00 рублей")) is True
    assert W.is_pickup_lead(_lead(delivery="Самовывоз из Шоурума, 1 , 0.00 рублей")) is True
    # ПВЗ СДЭК — не наш самовывоз, порог остаётся общим
    assert W.is_pickup_lead(_lead(delivery="CDEK: Самовывоз, (2-3 дней), 1 шт, 339.00 рублей")) is False
    assert W.is_pickup_lead(_lead(delivery="Доставка курьером по Москве, 1 шт, 1 000.00 рублей")) is False
    assert W.is_pickup_lead(_lead(delivery=None)) is False
    assert W.is_pickup_lead({}) is False


def test_resolve_lead_prefers_pickup_over_fresher():
    """У клиента две открытые сделки: свежая курьерская и старая самовывозная.
    Берём самовывозную — по ней и порог, и ссылка."""
    async def fake_find(query, with_=()):
        return [
            _lead(lead_id=10, delivery="Доставка курьером по Москве", updated_at=900),
            _lead(lead_id=20, delivery="Самовывоз из офиса Sunscrypt", updated_at=100,
                  responsible=9291546),
        ]

    W.amo_service.find_leads_by_query = fake_find
    lead_id, responsible, pickup = asyncio.run(W._resolve_lead("79990000000"))
    assert (lead_id, responsible, pickup) == (20, 9291546, True)


def test_resolve_lead_ignores_closed_pickup():
    """Закрытая самовывозная сделка не ускоряет порог: клиент за ней не едет."""
    async def fake_find(query, with_=()):
        return [
            _lead(lead_id=30, status_id=142, delivery="Самовывоз из офиса Sunscrypt", updated_at=900),
            _lead(lead_id=40, delivery="CDEK: Самовывоз, (1-2 дней)", updated_at=500),
        ]

    W.amo_service.find_leads_by_query = fake_find
    lead_id, _, pickup = asyncio.run(W._resolve_lead("79990000000"))
    assert (lead_id, pickup) == (40, False)

# --- «Ответ не требуется»: беседа закрыта в amo → алерт не нужен -------------

def _talk(status="closed", origin="com.wazzup24.wz", updated_at=0):
    return {"talk_id": 1, "status": status, "is_in_work": status == "in_work",
            "origin": origin, "updated_at": updated_at}


def _st_waiting():
    return {"chat_id": "79990000000", "wall_since": W._now_msk()}


def _set_amo(contacts, talks):
    async def fake_contacts(q, limit=10):
        return contacts

    async def fake_talks(cid):
        return talks

    W.amo_service.find_contacts_by_query = fake_contacts
    W.amo_service.get_talks_by_contact = fake_talks


def test_sweep_skips_alert_when_talk_closed():
    """Менеджер нажал «Ответ не требуется» → алерта нет, ожидание снято."""
    W._pending.clear()
    sent = []

    async def fake_send(text, **kw):
        sent.append(text)
        return True

    async def fake_closed(st):
        return True

    W.telegram_bot.send_alert = fake_send
    W._talk_closed_safe = fake_closed
    W._in_window = lambda now=None: True

    W.handle_webhook({"messages": [_msg(is_echo=False)]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= 40 * 60
    asyncio.run(W._sweep(threshold_s=30 * 60))
    assert len(sent) == 0, "беседа закрыта — алерт не шлём"
    assert len(W._pending) == 0, "ожидание должно сняться"


def test_talk_closed_true_when_recent_wazzup_talk_closed():
    now_ts = int(W._now_msk().timestamp())
    _set_amo([{"id": 1}], [_talk(status="closed", updated_at=now_ts)])
    assert asyncio.run(W._talk_closed(_st_waiting())) is True


def test_talk_in_work_means_alert():
    now_ts = int(W._now_msk().timestamp())
    _set_amo([{"id": 1}], [_talk(status="in_work", updated_at=now_ts),
                           _talk(status="closed", updated_at=now_ts)])
    assert asyncio.run(W._talk_closed(_st_waiting())) is False


def test_talk_closed_ignores_old_and_foreign_talks():
    """Старые беседы (до клиентского сообщения) и не-Wazzup origin не считаются:
    актуальных Wazzup-бесед нет → fail-open, алерт идёт."""
    now_ts = int(W._now_msk().timestamp())
    old = now_ts - 3 * 3600
    _set_amo([{"id": 1}], [_talk(status="closed", updated_at=old),
                           _talk(status="closed", origin="com.amocrm.mail", updated_at=now_ts)])
    assert asyncio.run(W._talk_closed(_st_waiting())) is False


def test_talk_closed_no_contacts_fail_open():
    _set_amo([], [])
    assert asyncio.run(W._talk_closed(_st_waiting())) is False


def test_talk_closed_safe_swallows_errors():
    async def boom(q, limit=10):
        raise RuntimeError("amo down")

    W.amo_service.find_contacts_by_query = boom
    assert asyncio.run(W._talk_closed_safe(_st_waiting())) is False



# ─────────────────── эскалация к руководству (Катя 28.08.2026) ───────────────────
# Второй порог: менеджерам уже написали, клиенту так и не ответили. Такой алерт уходит в
# отдельную группу «ОП срочные уведомления», и цена ошибки здесь выше, чем в чате ОП:
# пара ложных сообщений — и руководство перестанет читать чат целиком.


def _wait_state(minutes_ago: int, *, alerted=True, responsible=13929334):
    """Ожидание, которое уже провисело столько-то минут и по которому менеджерам
    (обычно) уже написали."""
    W._pending.clear()
    W.handle_webhook({"messages": [_msg(is_echo=False, text="когда привезёте?")]})
    st = next(iter(W._pending.values()))
    st["waiting_since"] -= minutes_ago * 60
    st["alerted"] = alerted
    st["responsible_id"] = responsible
    st["lead_resolved"] = True
    return st


def _catch_sends():
    sent = []

    async def fake_send(text, **kw):
        sent.append((text, kw.get("chat_id")))
        return True

    W.telegram_bot.send_alert = fake_send
    W._in_window = lambda now=None: True
    return sent


def test_escalation_goes_to_leadership_chat_after_second_threshold():
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(31)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert len(sent) == 1, "после второго порога руководству уходит ровно одно сообщение"
    text, chat = sent[0]
    assert chat == -5358037627, "эскалация обязана уйти в чат руководства, а не в чат ОП"
    assert "не ответили" in text


def test_escalation_names_the_manager_instead_of_tagging_him():
    """В чате руководства менеджеров нет: @ник там мусор, нужно имя человека."""
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(31)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    text = sent[0][0]
    assert "Егор Константинов" in text
    assert "@" not in text, "тегов в чате руководства быть не должно"
    assert "·" not in text, "точка посередине запрещена (правило Кати 26.08.2026)"


def test_escalation_is_sent_once():
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(31)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert len(sent) == 1, "повторять эскалацию по тому же ожиданию нельзя"


def test_no_escalation_before_second_threshold():
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(20)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert sent == []


def test_no_escalation_without_first_alert():
    """Порядок контуров: сперва шанс менеджеру, и только потом руководство. Ожидание,
    по которому первый алерт не уходил (например, оно родилось вне окна), эскалации не
    получает - иначе руководитель узнаёт о проблеме раньше исполнителя."""
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(31, alerted=False)
    W._resolve_lead_safe = _resolve_nothing
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert len(sent) == 1, "это первый алерт менеджерам, а не эскалация"
    assert sent[0][1] == W.NOTIFY_CHAT_ID


def test_escalation_silent_when_chat_not_configured():
    """Выключатель: пустой ROP_CHAT_ID означает «эскалации нет», а не «шлём куда-нибудь»."""
    sent = _catch_sends()
    W.ROP_CHAT_ID = None
    _wait_state(31)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert sent == []


def test_closed_talk_cancels_escalation():
    """«Ответ не требуется» снимает и эскалацию: менеджер закрыл беседу в amo, дёргать
    руководство не за что."""
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627

    async def closed(st):
        return True

    W._talk_closed_safe = closed
    _wait_state(31)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    assert sent == []
    assert W._pending == {}, "закрытая беседа снимается с ожидания"


def test_unknown_manager_is_named_in_words_not_by_id():
    sent = _catch_sends()
    W.ROP_CHAT_ID = -5358037627
    _wait_state(31, responsible=999999)
    asyncio.run(W._sweep(threshold_s=15 * 60))
    text = sent[0][0]
    assert "менеджер не определён" in text
    assert "999999" not in text


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    ok = 0
    for fn in fns:
        setup_function()
        try:
            fn()
            print(f"✅ {fn.__name__}")
            ok += 1
        except Exception:
            print(f"❌ {fn.__name__}")
            traceback.print_exc()
    print(f"\n{ok}/{len(fns)} прошли")
