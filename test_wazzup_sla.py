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
_stub("amo_service", find_leads_by_query=None,
      find_contacts_by_query=None, get_talks_by_contact=None)
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


def setup_function(_=None):
    W._pending.clear()
    # восстановить всё, что sweep-тесты могли подменить
    W._in_window = _ORIG_IN_WINDOW
    W._resolve_lead_safe = _ORIG_RESOLVE
    W._talk_closed_safe = _ORIG_TALK_CLOSED


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
        return 12345, 13929334  # сделка + ответственный Егор

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
