"""Сообщение менеджера из другого чата того же клиента (`ai_engine_same_client`, просьба Кати 25.11.2026).

Проверяем то, ошибка в чём значит ИИ, пишущего клиенту, которому менеджер уже ответил в другом канале, или снятую ЧУЖУЮ сделку из-за случайно совпавших цифр:

  • тот же телефон (10 цифр) или тот же Telegram ID по чатам сделки и по карточкам контактов: ИИ снимается с той же заметкой;
  • анонимный Telegram ID никогда не телефон; другой номер, группы, роботы, неслушаемые каналы: ничего;
  • amo молчит: совпадения по базе работают, остальное по уже прочитанному;
  • перехват, «Выключено» и возврат после него видят то же правило.

Сети нет: amo, сервис и Telegram подставные, время ручное. Запуск: python -m pytest test_ai_engine_same_client.py -q
"""
import asyncio

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_filters as F
import ai_engine_intercept as intercept
import ai_engine_mode_return as MR
import ai_engine_prompts as prompts
import ai_engine_same_client as SC
import ai_engine_store as store
import amo_service
from test_ai_engine import CHAN, CHAT, CHAT_KEY, CONTACT, DEAL, PHONE, PIPE_TEST, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_calls import TEXT_MESSAGE, drain
from test_ai_engine_mode_return import World, webhook

MANAGER = "Иванов Иван"
WA = "wa-channel"
OTHER = "other-channel"
TG_ID = "555000111"


class Cards:
    """Подставные карточки контактов amo: `GET /api/v4/contacts?filter[id][]`."""

    def __init__(self, mp):
        self.cards: dict[int, dict] = {}
        self.requests: list[list[int]] = []
        self.down = False

        async def do_get(path, params=None):
            assert path == "/api/v4/contacts", path
            ids = [int(v) for k, v in params if k == "filter[id][]"]
            self.requests.append(ids)
            if self.down:
                return None
            found = [self.cards[i] for i in ids if i in self.cards]
            return {"_embedded": {"contacts": found}} if found else {}

        mp.setattr(amo_service, "_do_get", do_get)

    def put(self, cid=CONTACT, phones=(), telegram=None):
        fields = []
        if phones:
            fields.append({"field_code": "PHONE", "values": [{"value": p} for p in phones]})
        if telegram:
            fields.append({"field_id": SC.FIELD_TELEGRAM_ID, "values": [{"value": telegram}]})
        self.cards[cid] = {"id": cid, "custom_fields_values": fields}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(E, "_last_mode_id", None)
    monkeypatch.setattr(prompts, "_good", None)
    monkeypatch.setattr(prompts, "_last_ok", None)
    monkeypatch.setattr(cfg, "MODE_RETURN_DISABLED", False)
    F._warned.clear()


@pytest.fixture
def cards(env, monkeypatch):
    monkeypatch.setattr(cfg, "SAME_CLIENT_ENABLED", True)
    monkeypatch.setattr(cfg, "CHANNELS", {CHAN, WA})
    monkeypatch.setattr(store, "_now", lambda: env.t)
    return Cards(monkeypatch)


def incoming(env, *, channel, chat_id, chat_type, phone):
    m = env.msg("Здравствуйте")
    m.update(channelId=channel, chatId=chat_id, chatType=chat_type, contact={"name": "Тиана", "phone": phone})
    return m


def out(env, chat_id=PHONE, *, chat_type="whatsapp", channel=WA, phone=None, author=MANAGER):
    """Сообщение клиенту из другого чата. `phone`: что Wazzup кладёт в `contact.phone` (по умолчанию у WhatsApp сам номер, у остальных пусто)."""
    m = env.msg("Отвечу сам", echo=True, author=author)
    contact = {"name": "x", "phone": phone if phone is not None else (chat_id if chat_type == "whatsapp" else "")}
    m.update(channelId=channel, chatId=chat_id, chatType=chat_type, contact=contact)
    return m


async def led(env, *, channel=CHAN, chat_id=CHAT, chat_type="telegram", phone=PHONE):
    """ИИ взял сделку по сообщению клиента и закончил ход; дальше прошло несколько секунд."""
    if not phone:
        env.set_settings(contacts={"scope": "test", "restricted": True, "items": [{"contact_id": CONTACT, "phone_norm": "", "name": "Тест"}]})
    env.lead()
    await env.say(incoming(env, channel=channel, chat_id=chat_id, chat_type=chat_type, phone=phone))
    await env.advance(61)
    turn = env.turn_requests[-1]
    await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
    await drain()
    assert store.get_owner(DEAL)["owner"] == "ai"
    env.t += 5
    return turn


async def led_wa(env):
    return await led(env, channel=WA, chat_id=PHONE, chat_type="whatsapp")


async def say(env, message):
    await env.say(message)
    await drain()


def owner(deal=DEAL):
    row = store.get_owner(deal)
    return row["owner"], row["reason"]


RELEASED = ("human", "manager_message")


def many_ai_deals(env, n, cards, phones):
    for i in range(n):
        deal, chat, cid = 2000 + i, f"71000{i}", 3000 + i
        key = f"{CHAN}:{chat}"
        store.set_owner(deal, "ai", reason="client_message", chat={"chat_id": chat, "chat_type": "telegram", "channel_id": CHAN, "chat_key": key},
                        pipeline_id=PIPE_TEST, contact_ids=[cid], mode="test")
        cards.put(cid, phones=phones)
    env.t += 5
    return [2000 + i for i in range(n)]


# =============================================================================================
# ключи
# =============================================================================================

@pytest.mark.parametrize("chat_type,chat_id,phone,expected", [
    ("whatsapp", "79260000001", "", {("phone", "9260000001")}),
    ("wapi", "89260000001", "+7 926 000-00-01", {("phone", "9260000001")}),
    ("telegram", "9260000001", "", {("telegram", "9260000001")}),                                   # ID, похожий на телефон, телефоном не становится
    ("telegram", "9260000001", "9260000001", {("telegram", "9260000001")}),                          # и когда Wazzup кладёт его же в contact.phone
    ("telegram", "1920391385", "79260000001", {("telegram", "1920391385"), ("phone", "9260000001")}),
    ("telegroup", "79260000001", "79260000001", set()),
    ("instagram", "79260000001", "", set()),
    ("", "79260000001", "79260000001", set()),
    ("whatsapp", "123", "", set()),
])
def test_chat_keys_of_a_client(chat_type, chat_id, phone, expected):
    assert SC.chat_idents(chat_type, chat_id, phone) == expected


def test_contact_card_keys():
    card = {"custom_fields_values": [
        {"field_code": "PHONE", "values": [{"value": "+7 (926) 000-00-01"}, {"value": "123"}]},
        {"field_id": SC.FIELD_TELEGRAM_ID, "values": [{"value": " 555000111 "}]},
        {"field_code": "EMAIL", "values": [{"value": "a@b.ru"}]},
    ]}
    assert SC.contact_idents(card) == {("phone", "9260000001"), ("telegram", "555000111")}


# =============================================================================================
# тот же клиент
# =============================================================================================

@pytest.mark.parametrize("message", [
    dict(chat_id=PHONE), dict(chat_id="89260000001"), dict(chat_id=TG_ID, chat_type="telegram", phone="+7 926 000-00-01"),
], ids=["whatsapp", "whatsapp-8", "telegram-with-phone"])
def test_a_message_to_the_same_phone_in_another_chat_releases_the_deal(env, cards, message):
    async def go():
        await led(env)                                              # Telegram, телефон известен из сессии
        await say(env, out(env, **message))
        await say(env, out(env, **message))                          # повтор: одна заметка
        assert owner() == RELEASED and env.notes == [(DEAL, TEXT_MESSAGE)]
        assert cards.requests == []                                  # хватило базы движка
        assert env.alerts == [] and env.tasks_open == []
        await env.say(env.msg("А доставка?"))
        await env.advance(120)
        assert len(env.turn_requests) == 1

    run(go())


def test_a_telegram_deal_without_a_phone_is_found_by_the_contact_card(env, cards):
    async def go():
        await led(env, phone="")
        cards.put(phones=["+7 (926) 000-00-01"])
        await say(env, out(env, PHONE))
        assert owner() == RELEASED and env.notes == [(DEAL, TEXT_MESSAGE)] and cards.requests == [[CONTACT]]

    run(go())


def test_a_whatsapp_deal_is_found_by_the_telegram_id_of_the_card(env, cards):
    async def go():
        await led_wa(env)
        cards.put(phones=[PHONE], telegram=TG_ID)
        await say(env, out(env, TG_ID, chat_type="telegram", channel=CHAN))
        assert owner() == RELEASED and env.notes == [(DEAL, TEXT_MESSAGE)]

    run(go())


def test_the_same_telegram_id_on_another_channel(env, cards):
    async def go():
        await led(env, phone="")
        await say(env, out(env, CHAT, chat_type="telegram", channel=WA))
        assert owner() == RELEASED and cards.requests == []

    run(go())


@pytest.mark.parametrize("chat_type,chat_id", [
    ("whatsapp", "79260000002"),                                    # другой номер
    ("telegram", "9260000001"),                                     # анонимный ID, равный десяти цифрам телефона клиента
    ("telegram", "79260000001"),                                    # и полному номеру
    ("telegroup", PHONE), ("instagram", PHONE),
])
def test_a_foreign_client_with_similar_digits_is_not_released(env, cards, chat_type, chat_id):
    async def go():
        await led_wa(env)
        cards.put(phones=[PHONE], telegram=TG_ID)
        await say(env, out(env, chat_id, chat_type=chat_type, channel=CHAN, phone=""))
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_an_anonymous_telegram_id_of_the_deal_is_not_a_phone(env, cards):
    async def go():
        await led(env, chat_id="9260000001", phone="")
        cards.put(phones=["79990000000"])
        await say(env, out(env, "79260000001"))
        assert owner()[0] == "ai" and env.notes == []

    run(go())


@pytest.mark.parametrize("author", ["Admin", ""], ids=["admin", "empty"])
def test_robot_echo_in_another_chat_changes_nothing(env, cards, author):
    async def go():
        await led(env)
        await say(env, out(env, PHONE, author=author))
        assert owner()[0] == "ai" and env.notes == []

    run(go())


def test_a_channel_the_engine_does_not_listen_stays_invisible(env, cards):
    async def go():
        await led(env)
        await say(env, out(env, PHONE, channel=OTHER))
        assert owner()[0] == "ai" and env.notes == [] and cards.requests == []

    run(go())


def test_the_switch_leaves_only_the_chat_of_the_deal(env, cards, monkeypatch):
    async def go():
        await led(env)
        monkeypatch.setattr(cfg, "SAME_CLIENT_ENABLED", False)
        await say(env, out(env, PHONE))
        assert owner()[0] == "ai" and cards.requests == []

    run(go())


def test_a_message_in_the_chat_of_the_deal_does_not_look_for_other_chats(env, cards):
    async def go():
        await led(env, phone="")
        many_ai_deals(env, 1, cards, [PHONE])
        await say(env, env.msg("Беру на себя", echo=True, author=MANAGER))
        assert owner() == RELEASED and cards.requests == [] and owner(2000)[0] == "ai"

    run(go())


def test_the_deal_whose_chat_waits_in_a_skip_window_is_still_released(env, cards):
    async def go():
        await led(env)
        store.put_session(CHAT_KEY, state=store.S_SKIP, skip_until=env.t + 600)
        await say(env, env.msg("Беру на себя", echo=True, author=MANAGER))
        assert owner() == RELEASED and env.notes == [(DEAL, TEXT_MESSAGE)]

    run(go())


def test_a_deal_taken_after_the_message_is_not_released(env, cards):
    async def go():
        await led(env)
        async with E._get_lock():
            assert await SC._release(E, DEAL, env.t - 100, {("phone", "9260000001")}) is False
        assert owner()[0] == "ai"

    run(go())


# =============================================================================================
# amo, память, общий номер
# =============================================================================================

def test_a_silent_amo_keeps_the_local_match_and_uses_what_it_already_read(env, cards):
    async def go():
        await led(env, phone="")
        cards.down = True
        await say(env, out(env, PHONE))
        assert owner()[0] == "ai" and len(cards.requests) == 1        # карточку не прочитали ни разу: сверить не с чем
        cards.down = False
        cards.put(phones=[PHONE])
        await say(env, out(env, "79990000000"))                      # прочитали карточку
        env.t += SC.CACHE_TTL_S + 1
        cards.down = True
        await say(env, out(env, PHONE))
        assert owner() == RELEASED

    run(go())


def test_contact_cards_are_read_once_per_ttl(env, cards):
    async def go():
        await led(env, phone="")
        cards.put(phones=[PHONE])
        await say(env, out(env, "79990000000"))
        await say(env, out(env, "79990000001"))
        assert len(cards.requests) == 1 and owner()[0] == "ai"
        env.t += SC.CACHE_TTL_S + 1
        await say(env, out(env, "79990000002"))
        assert len(cards.requests) == 2

    run(go())


@pytest.mark.parametrize("n,expected", [(SC.MAGNET_LIMIT, "human"), (SC.MAGNET_LIMIT + 1, "ai")], ids=["up-to-the-limit", "magnet"])
def test_a_phone_shared_by_too_many_contacts_releases_nobody(env, cards, n, expected):
    async def go():
        deals = many_ai_deals(env, n, cards, [PHONE])
        await say(env, out(env, PHONE))
        assert {owner(d)[0] for d in deals} == {expected}

    run(go())


# =============================================================================================
# перехват, «Выключено», возврат
# =============================================================================================

def test_an_intercepted_deal_is_released_from_another_chat_and_start_returns_it(env, cards):
    async def go():
        await led(env)
        intercept._mark_taken(DEAL, CHAT_KEY, env.t, env.t, intercept.KIND_TAKE)
        env.t += 5
        await say(env, out(env, PHONE))
        assert owner() == RELEASED
        env.t += 5
        assert (await E.manager_start(DEAL))["state"] == "returned"
        env.turn_requests.clear()
        await env.advance(1)
        turn = env.turn_requests[-1]
        assert turn["intercepted"] is True
        assert E.decide_permit(DEAL, turn["epoch"], turn["turn_id"], turn["up_to_message_id"])[1]["allowed"] is True

    run(go())


def test_a_message_from_another_chat_while_off_hands_the_deal_over_at_once(env, cards):
    async def go():
        await led(env)
        env.set_settings(mode="off", pipelines=[])
        await E._handle_off_payload({"messages": [out(env, PHONE)]})
        await drain()
        assert owner() == RELEASED and env.notes == [(DEAL, TEXT_MESSAGE)]

    run(go())


@pytest.fixture
def w(env, cards):
    return World(env)


@pytest.mark.parametrize("session_phone,message", [
    (PHONE, dict(chat_id=PHONE)),                                                   # номер есть в сессии: ловит и прежняя проверка по чатам
    ("", dict(chat_id=PHONE)),                                                      # номер только в карточке контакта
    (PHONE, dict(chat_id=TG_ID, chat_type="telegram", phone=PHONE)),                 # другой Telegram-аккаунт с тем же номером: по `chat_id` не найти
], ids=["phone-in-the-chat", "phone-only-in-the-card", "phone-of-another-messenger"])
def test_a_deal_whose_client_got_a_message_elsewhere_while_off_is_not_returned(w, cards, session_phone, message):
    env = w.env

    async def go():
        await led(env, phone=session_phone)
        if not session_phone:
            cards.put(phones=[PHONE])
        await w.off()
        assert w.owner() == ("human", "mode_off")
        env.t += 100
        assert await webhook(env, out(env, **message)) is True
        if session_phone:
            w.on()
        else:
            w.on(contacts={"scope": "test", "restricted": True, "items": [{"contact_id": CONTACT, "phone_norm": "", "name": "Тест"}]})
        await w.sweep()
        assert w.owner() == ("human", "mode_off_kept") and "писал менеджер" in w.note()

    run(go())


@pytest.mark.parametrize("chat_type,chat_id", [("whatsapp", "79260000002"), ("telegram", "9260000001")], ids=["other-phone", "similar-telegram-id"])
def test_a_message_to_a_foreign_client_while_off_does_not_hold_the_deal(w, cards, chat_type, chat_id):
    env = w.env

    async def go():
        await led(env)
        cards.put(phones=[PHONE])
        await w.off()
        env.t += 100
        await webhook(env, out(env, chat_id, chat_type=chat_type, phone="" if chat_type == "telegram" else None))
        w.on()
        await w.sweep()
        assert w.owner() == ("ai", "dialog_returned")

    run(go())


def test_a_silent_amo_on_contacts_postpones_the_return(w, cards):
    env = w.env

    async def go():
        await led(env)
        cards.put(phones=[PHONE])
        await w.off()
        cards.down = True
        w.on()
        await w.sweep()
        assert w.owner() == ("human", "mode_off")
        cards.down = False
        await w.sweep(MR.RETRY_S)
        assert w.owner() == ("ai", "dialog_returned")

    run(go())
