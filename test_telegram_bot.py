"""Тесты устойчивости контура телеграм-уведомлений.

Написаны по инциденту 28-29.08.2026: 28.08 в 21:51 МСК на старте контейнера
`getMe` не прошёл через венский прокси, бот выключился и больше не пытался
подняться. Событийные алерты подавлялись молча почти сутки - 44 штуки, среди них
пропущенные звонки и «новый лид не взяли 2 часа». Снаружи всё выглядело
здоровым: контейнер Up, прокси отвечает, ошибок отправки в логе нет.

Каждый тест закрывает свой способ потерять уведомление молча.

Запуск: python3 -m pytest test_telegram_bot.py -q
"""

import asyncio
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
os.environ.setdefault("TG_BOT_TOKEN", "123:test-token")
os.environ.setdefault("TG_ALLOWED_CHAT_ID", "-1003931115357")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _install_stubs() -> None:
    """waybill_service тянет за собой пол-интеграции, а нам от него нужен один
    сеттер колбэка."""
    ws = types.ModuleType("waybill_service")
    ws.set_alert_callback = lambda cb: None
    sys.modules.setdefault("waybill_service", ws)


_install_stubs()

import telegram_bot  # noqa: E402
from aiogram.exceptions import (  # noqa: E402
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)

CHAT = -1003931115357


class _Method:
    """Заглушка метода Bot API - исключения aiogram читают у него chat_id."""

    def __init__(self, chat_id=CHAT):
        self.chat_id = chat_id


class _Session:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class _FakeBot:
    """Бот, который шлёт по сценарию: список исходов на последовательные вызовы.
    Исход - либо исключение (будет брошено), либо None (успех)."""

    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.calls: list[dict] = []
        self.session = _Session()

    async def send_message(self, chat_id, text, parse_mode=None, message_thread_id=None):
        self.calls.append(
            {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "thread_id": message_thread_id,
            }
        )
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if outcome is not None:
            raise outcome
        return True


def _reset(bot=None, configured=True):
    """Чистое состояние модуля перед каждым тестом.

    ⚠️ Локи пересоздаём обязательно: каждый вызов asyncio.run() заводит СВОЙ цикл
    событий, а asyncio.Lock привязывается к тому, в котором его впервые взяли.
    Лок из прошлого теста в новом цикле - это либо RuntimeError, либо вис."""
    telegram_bot._bot = bot
    telegram_bot._dp = None
    telegram_bot._polling_task = None
    telegram_bot._reconnect_task = None
    telegram_bot._init_lock = asyncio.Lock()
    telegram_bot._print_lock = asyncio.Lock()
    telegram_bot._last_lazy_attempt = 0.0
    telegram_bot._reconnect_failures = 0
    telegram_bot._state.update(
        {
            "configured": configured,
            "enabled": bot is not None,
            "polling": bot is not None,
            "sent": 0,
            "failed": 0,
            "suppressed": 0,
            "suppressed_streak": 0,
            "init_attempts": 0,
            "last_ok_ts": None,
            "last_fail_ts": None,
            "last_suppressed_ts": None,
            "last_error": None,
            "chat_remap": {},
        }
    )


@pytest.fixture(autouse=True)
def _fast_and_isolated(monkeypatch):
    """Повторы не растягивают тесты на реальные секунды, а фоновый переподъём не
    крутится внутри тестового цикла событий.

    ⚠️ Глобально подменять asyncio.sleep нельзя: с мгновенным сном фоновый цикл
    переподъёма вертится без остановки и asyncio.run не отдаёт управление.
    Поэтому обнуляем паузы константами, а сам цикл глушим."""
    monkeypatch.setattr(telegram_bot, "TG_INIT_BACKOFF_S", 0.0)
    monkeypatch.setattr(telegram_bot, "TG_SEND_BACKOFF_S", 0.0)
    monkeypatch.setattr(telegram_bot, "TG_RECONNECT_INTERVAL_S", 3600.0)
    monkeypatch.setattr(telegram_bot, "_start_reconnect_loop", lambda: None)


def _send(text="алерт", chat_id=None, thread_id=None):
    return asyncio.run(
        telegram_bot.send_alert(text, chat_id=chat_id, message_thread_id=thread_id)
    )


# --- то, из-за чего всё и случилось ----------------------------------------


def test_vyklyuchennyy_bot_probuet_podnyatsya_na_otpravke(monkeypatch):
    """Главный регресс. Раньше при _bot is None алерт подавлялся навсегда.
    Теперь отправка сама поднимает бота и сообщение уходит."""
    _reset(bot=None)
    fake = _FakeBot()

    async def fake_start(reason):
        telegram_bot._bot = fake
        telegram_bot._state["enabled"] = True
        return True

    monkeypatch.setattr(telegram_bot, "_start_bot_once", fake_start)

    assert _send("пропущенный звонок") is True
    assert len(fake.calls) == 1
    assert telegram_bot._state["suppressed"] == 0


def test_esli_podnyat_ne_udalos_alert_schitaetsya_a_ne_teryaetsya(monkeypatch):
    """Поднять не вышло - алерт всё равно не исчезает бесследно: растёт счётчик
    подавленных, и его видно снаружи в GET /."""
    _reset(bot=None)

    async def failing_start(reason):
        telegram_bot._state["last_error"] = "getMe: ConnectionResetError"
        return False

    monkeypatch.setattr(telegram_bot, "_start_bot_once", failing_start)

    assert _send("первый") is False
    assert _send("второй") is False
    health = telegram_bot.telegram_health()
    assert health["suppressed"] == 2
    assert health["suppressed_streak"] == 2
    assert health["enabled"] is False
    assert "ConnectionResetError" in (health["last_error"] or "")


def test_init_povtoryaet_getme_a_ne_sdayotsya_s_pervogo_raza(monkeypatch):
    """28.08 хватило ОДНОГО неудачного getMe. Теперь их несколько с паузами."""
    _reset(bot=None)
    attempts = {"n": 0}
    fake = _FakeBot()

    async def flaky_start(reason):
        attempts["n"] += 1
        if attempts["n"] < 3:
            telegram_bot._state["last_error"] = "getMe: ConnectionResetError"
            return False
        telegram_bot._bot = fake
        telegram_bot._state["enabled"] = True
        return True

    monkeypatch.setattr(telegram_bot, "_start_bot_once", flaky_start)
    monkeypatch.setattr(telegram_bot, "TG_INIT_RETRIES", 5)

    asyncio.run(telegram_bot.init_telegram_bot())

    assert attempts["n"] == 3
    assert telegram_bot._bot is fake
    assert telegram_bot._state["init_attempts"] == 3


def test_lenivyy_perepodyom_trottlitsya(monkeypatch):
    """Шквал алертов не должен превращаться в шквал getMe."""
    _reset(bot=None)
    calls = {"n": 0}

    async def failing_start(reason):
        calls["n"] += 1
        return False

    monkeypatch.setattr(telegram_bot, "_start_bot_once", failing_start)
    monkeypatch.setattr(telegram_bot, "TG_LAZY_REINIT_MIN_GAP_S", 300.0)

    _send("раз")
    _send("два")
    _send("три")

    assert calls["n"] == 1, "переподъём должен быть один на окно троттлинга"


# --- отказы отправки, на которых уведомления пропадали молча ----------------


def test_pereezd_gruppy_v_supergruppu_ne_teryaet_alert():
    """Обычную группу конвертировали - старый chat_id умер. Новый адрес приезжает
    в самой ошибке, значит сообщение можно доставить, а не потерять."""
    new_id = -1009999999999
    fake = _FakeBot([TelegramMigrateToChat(_Method(), "migrated", new_id)])
    _reset(bot=fake)

    assert _send("не перезвонили час", chat_id=CHAT, thread_id=10479) is True
    assert [c["chat_id"] for c in fake.calls] == [CHAT, new_id]
    assert fake.calls[1]["thread_id"] is None, "топиков старого чата в новом нет"
    assert telegram_bot._state["chat_remap"][str(CHAT)] == new_id


def test_posle_pereezda_sleduyushchiy_alert_idyot_srazu_po_novomu_adresu():
    new_id = -1009999999999
    fake = _FakeBot([TelegramMigrateToChat(_Method(), "migrated", new_id)])
    _reset(bot=fake)
    _send("первый", chat_id=CHAT)

    fake.calls.clear()
    assert _send("второй", chat_id=CHAT) is True
    assert fake.calls[0]["chat_id"] == new_id


def test_cepochka_pereezdov_ne_zaciklivaetsya():
    """Если чат отвечает переездом снова и снова, сервис не должен виснуть на
    одном алерте: перенаправлений не больше трёх."""
    fake = _FakeBot([TelegramMigrateToChat(_Method(), "migrated", -100 - i) for i in range(20)])
    _reset(bot=fake)

    assert _send("алерт") is False
    assert len(fake.calls) <= 3 + telegram_bot.TG_SEND_ATTEMPTS


def test_propavshiy_topik_ne_horonit_soobshenie():
    """Топик удалили - шлём в общую ленту чата, а не выбрасываем алерт."""
    fake = _FakeBot([TelegramBadRequest(_Method(), "message thread not found")])
    _reset(bot=fake)

    assert _send("клиент ждёт 15 минут", thread_id=10479) is True
    assert fake.calls[0]["thread_id"] == 10479
    assert fake.calls[1]["thread_id"] is None


def test_setevoy_sboy_povtoryaetsya():
    fake = _FakeBot(
        [
            TelegramNetworkError(_Method(), "connection reset"),
            TelegramNetworkError(_Method(), "connection reset"),
        ]
    )
    _reset(bot=fake)

    assert _send("счёт СБП не создался") is True
    assert len(fake.calls) == 3
    assert telegram_bot._state["sent"] == 1


def test_set_ne_podnyalas_za_vse_popytki_schitaem_proval():
    fake = _FakeBot([TelegramNetworkError(_Method(), "reset")] * 5)
    _reset(bot=fake)

    assert _send("алерт") is False
    assert len(fake.calls) == telegram_bot.TG_SEND_ATTEMPTS
    assert telegram_bot._state["failed"] == 1


def test_flud_limit_zhdyot_i_povtoryaet():
    # retry_after=1: единственная в файле НЕ обнулённая пауза, ждём её честно.
    fake = _FakeBot([TelegramRetryAfter(_Method(), "flood", 1)])
    _reset(bot=fake)

    assert _send("алерт") is True
    assert len(fake.calls) == 2


def test_bityy_token_gasit_bota_i_ne_dolbitsya_v_stenu():
    fake = _FakeBot([TelegramUnauthorizedError(_Method(), "Unauthorized")])
    _reset(bot=fake)

    assert _send("алерт") is False
    assert len(fake.calls) == 1, "повторять с мёртвым токеном бессмысленно"
    assert telegram_bot._bot is None
    assert telegram_bot._state["enabled"] is False


def test_bota_vygnali_iz_chata_povtorov_net():
    fake = _FakeBot([TelegramForbiddenError(_Method(), "bot was kicked")])
    _reset(bot=fake)

    assert _send("алерт") is False
    assert len(fake.calls) == 1
    assert telegram_bot._state["failed"] == 1


def test_pustoy_adresat_ne_uhodit_v_nikuda(monkeypatch):
    """Пустая переменная = контур выключен осознанно, но алерт всё равно считаем."""
    fake = _FakeBot()
    _reset(bot=fake)
    monkeypatch.setattr(telegram_bot, "TG_ALLOWED_CHAT_ID", None)

    assert _send("алерт", chat_id=None) is False
    assert fake.calls == []
    assert telegram_bot._state["suppressed"] == 1


# --- видимость наружу -------------------------------------------------------


def test_zdorovie_kontura_vidno_odnim_zaprosom():
    """Сторож должен смотреть сюда, а не в 44 строки WARNING в логе."""
    fake = _FakeBot()
    _reset(bot=fake)
    _send("алерт")

    health = telegram_bot.telegram_health()
    assert health["enabled"] is True
    assert health["sent"] == 1
    assert health["suppressed_streak"] == 0
    assert health["seconds_since_last_ok"] is not None
    assert health["seconds_since_last_ok"] < 60


def test_udachnaya_otpravka_obnulyaet_seriyu_podavlennyh(monkeypatch):
    _reset(bot=None)

    async def failing_start(reason):
        return False

    monkeypatch.setattr(telegram_bot, "_start_bot_once", failing_start)
    _send("потерян")
    assert telegram_bot._state["suppressed_streak"] == 1

    fake = _FakeBot()
    telegram_bot._bot = fake
    telegram_bot._state["enabled"] = True
    _send("дошёл")

    assert telegram_bot._state["suppressed_streak"] == 0
    assert telegram_bot._state["suppressed"] == 1, "накопительный счётчик не обнуляем"


# --- Ревью 29.08: GET / открыт наружу, секретам и адресам чатов там не место ---

def test_parol_shlyuza_ne_utekaet_v_sostoyanie_kontura():
    """`GET /` интеграции отдаёт 200 кому угодно (team.sunscrypt.ru/amo/ проверено
    живьём 29.08). Текст исключения пишем не мы: aiohttp и python_socks кладут в него
    адрес прокси вместе с логином и паролем. 28.08 токен и пароль уже утекли в
    systemd-журнал через argv - тем же путём второй раз не ходим."""
    _reset(bot=None)
    telegram_bot._state["last_error"] = (
        "network: Cannot connect to proxy http://sunscrypt:SuperSecret123@82.97.249.88:18080"
    )

    health = telegram_bot.telegram_health()
    assert "SuperSecret123" not in health["last_error"]
    assert "sunscrypt:" not in health["last_error"]
    assert "82.97.249.88" in health["last_error"], "адрес шлюза для диагностики нужен"


def test_token_bota_ne_utekaet_v_sostoyanie_kontura():
    _reset(bot=None)
    telegram_bot._state["last_error"] = (
        "getMe: ClientError: https://api.telegram.org/bot7123456789:AAF-realTokenLooksLikeThis/getMe"
    )

    health = telegram_bot.telegram_health()
    assert "AAF-realTokenLooksLikeThis" not in health["last_error"]
    assert "7123456789" not in health["last_error"]


def test_adresa_chatov_naruzhu_ne_uhodyat():
    """Переезд группы диагностировать надо, но chat_id наружу отдавать незачем -
    хватает признака «переезд был», сами адреса лежат в логе уровнем ERROR."""
    _reset(bot=None)
    telegram_bot._state["chat_remap"] = {"-5358037627": -1005358037627}

    health = telegram_bot.telegram_health()
    assert health["chat_remapped"] == 1
    assert "chat_remap" not in health
    assert "5358037627" not in str(health)
