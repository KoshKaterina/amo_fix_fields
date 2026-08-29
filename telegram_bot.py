"""Telegram-бот: aiogram 3, long polling. Команды /print и /retry для группы."""

import asyncio
import logging
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

import waybill_service
from waybill_config import TG_ALLOWED_CHAT_ID, TG_BOT_TOKEN, TG_PROXY_URL

logger = logging.getLogger("uvicorn")

# --- Устойчивость контура уведомлений (инцидент 28-29.08.2026) ---------------
# 28.08 в 21:51 МСК на старте контейнера getMe не прошёл через венский прокси
# (ConnectionResetError). Бот выключился и БОЛЬШЕ НЕ ПЫТАЛСЯ подняться: все
# событийные алерты подавлялись молча почти сутки, 44 штуки. Снаружи всё
# выглядело здоровым - контейнер Up, прокси отвечает, ошибок отправки в логе
# нет, потому что до sendMessage дело не доходило вовсе.
# Ручки вынесены в окружение, но у всех есть рабочие значения по умолчанию:
# .env править не обязательно.
TG_INIT_RETRIES = int(os.getenv("TG_INIT_RETRIES", "5"))
TG_INIT_BACKOFF_S = float(os.getenv("TG_INIT_BACKOFF_S", "3"))
TG_RECONNECT_INTERVAL_S = float(os.getenv("TG_RECONNECT_INTERVAL_S", "60"))
TG_LAZY_REINIT_MIN_GAP_S = float(os.getenv("TG_LAZY_REINIT_MIN_GAP_S", "30"))
TG_SEND_ATTEMPTS = int(os.getenv("TG_SEND_ATTEMPTS", "3"))
TG_SEND_BACKOFF_S = float(os.getenv("TG_SEND_BACKOFF_S", "2"))

_bot: Bot | None = None
_dp: Dispatcher | None = None
_polling_task: asyncio.Task | None = None
_reconnect_task: asyncio.Task | None = None
_drop_task: asyncio.Task | None = None
_print_lock = asyncio.Lock()
_init_lock = asyncio.Lock()
_last_lazy_attempt: float = 0.0
_reconnect_failures: int = 0

# Состояние контура. Его отдаёт GET / - чтобы «бот молчит» было ВИДНО снаружи
# одним запросом, а не лежало сорока строками WARNING в логе контейнера.
_state: dict = {
    "configured": False,      # токен и чат заданы
    "enabled": False,         # бот поднят, getMe прошёл
    "polling": False,         # long-poll жив
    "sent": 0,
    "failed": 0,
    "suppressed": 0,          # алерты, которые некуда было отправить
    "suppressed_streak": 0,   # подряд; обнуляется первой удачной отправкой
    "init_attempts": 0,
    "last_ok_ts": None,
    "last_fail_ts": None,
    "last_suppressed_ts": None,
    "last_error": None,
    "chat_remap": {},         # старый chat_id -> новый после переезда в супергруппу
}


def _ts() -> str:
    return datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%y_%H-%M")


def _build_dispatcher() -> Dispatcher:
    dp = Dispatcher()

    @dp.message(Command("print"), F.chat.id == TG_ALLOWED_CHAT_ID)
    async def on_print(message: Message) -> None:
        if _print_lock.locked():
            await message.answer("Уже выполняется печать — подожди завершения.")
            return
        async with _print_lock:
            await message.answer("Запрашиваю штрихкоды и собираю лист подбора. Это может занять до минуты.")
            try:
                result = await waybill_service.handle_print_command()
            except Exception as exc:
                logger.exception("/print handler crashed")
                await message.answer(f"Ошибка: {exc}")
                return

            ts = _ts()
            picking = result.get("picking_pdf")
            if picking:
                await message.answer_document(
                    BufferedInputFile(picking, filename=f"picking_list_{ts}.pdf"),
                    caption="Лист подбора",
                )
            barcodes = result.get("barcodes_pdf")
            if barcodes:
                await message.answer_document(
                    BufferedInputFile(barcodes, filename=f"barcodes_{ts}.pdf"),
                    caption="Штрихкоды СДЭК",
                )

            warning = result.get("warning")
            summary = result.get("summary") or ""
            if warning:
                summary = f"{summary}\n\n⚠ {warning}"
            if summary:
                await message.answer(summary)

            packed_ids = result.get("packed_lead_ids") or []
            if packed_ids and barcodes:
                ok_count, failed = await waybill_service.mark_leads_packed(packed_ids)
                if failed:
                    await message.answer(
                        f"Тег «посылка упакована» поставлен у {ok_count}/{len(packed_ids)}.\n"
                        f"Не удалось пометить: {failed}"
                    )

    @dp.message(Command("retry"), F.chat.id == TG_ALLOWED_CHAT_ID)
    async def on_retry(message: Message) -> None:
        await message.answer("Запускаю повторное создание накладных для сделок с ошибками…")
        try:
            result = await waybill_service.handle_retry_command()
        except Exception as exc:
            logger.exception("/retry handler crashed")
            await message.answer(f"Ошибка: {exc}")
            return
        await message.answer(result.get("summary") or "—")

    @dp.message(F.chat.id == TG_ALLOWED_CHAT_ID, Command("help"))
    async def on_help(message: Message) -> None:
        await message.answer(
            "Команды бота:\n"
            "/print — выгрузить штрихкоды СДЭК и лист подбора по всем сделкам "
            "в этапе «Готова накладная» без тега «посылка упакована».\n"
            "/retry — повторить создание накладных для сделок с тегом «ошибка накладной» "
            "в этапе «Сделать накладную»."
        )

    # Catch-all хендлер для диагностики: ловит ВСЕ сообщения, которые не подошли
    # под предыдущие хендлеры. По логам видно: (а) доходят ли вообще апдейты,
    # (б) с какого chat_id и какой текст. Регистрируется ПОСЛЕДНИМ.
    @dp.message()
    async def on_any_message(message: Message) -> None:
        logger.info(
            "TG message received (no handler matched): chat_id=%s thread_id=%s "
            "expected_chat_id=%s from=%s text=%r",
            message.chat.id,
            message.message_thread_id,
            TG_ALLOWED_CHAT_ID,
            message.from_user.id if message.from_user else None,
            message.text,
        )

    return dp


async def _start_bot_once(reason: str) -> bool:
    """Одна попытка поднять бота: сессия -> getMe -> polling.
    True - поднялся. Ошибка кладётся в состояние, наружу не бросается."""
    global _bot, _dp, _polling_task
    session = _build_session()
    bot = Bot(token=TG_BOT_TOKEN, session=session) if session else Bot(token=TG_BOT_TOKEN)
    try:
        me = await bot.get_me()
    except Exception as exc:
        _state["last_error"] = _redact(f"getMe: {type(exc).__name__}: {exc}")
        _state["last_fail_ts"] = time.time()
        try:
            await bot.session.close()
        except Exception:
            pass
        return False

    _bot = bot
    _dp = _build_dispatcher()
    waybill_service.set_alert_callback(send_alert)
    _polling_task = asyncio.create_task(_run_polling())
    _state["enabled"] = True
    _state["polling"] = True
    _state["last_error"] = None
    proxy_note = f" via proxy {_redact_proxy(TG_PROXY_URL)}" if TG_PROXY_URL else ""
    logger.info(
        "Telegram bot started (%s): @%s (id=%s) polling%s, allowed_chat_id=%s. "
        "ВАЖНО: Privacy Mode должен быть ВЫКЛЮЧЕН в @BotFather (Bot Settings -> "
        "Group Privacy -> Turn off).",
        reason, me.username, me.id, proxy_note, TG_ALLOWED_CHAT_ID,
    )
    return True


def _log_alert_targets() -> None:
    """Пустая переменная = контур молча выключен. Решение осознанное, но раз так -
    пусть на старте будет ВИДНО, какие адресаты пусты: иначе выключенный контур
    неотличим от сломанного."""
    targets: list[tuple[str, object]] = [
        ("технический чат (TG_ALLOWED_CHAT_ID)", TG_ALLOWED_CHAT_ID),
        ("руководство (ROP_ALERT_CHAT_ID)", os.getenv("ROP_ALERT_CHAT_ID") or None),
        ("отгрузки (SHIPMENT_ALERT_CHAT_ID)", os.getenv("SHIPMENT_ALERT_CHAT_ID") or None),
    ]
    try:
        import tg_recipients
        targets.append(("отдел продаж, топик УВЕДОМЛЕНИЯ", tg_recipients.NOTIFY_CHAT_ID))
        targets.append(("отдел продаж, топик ШОУРУМ", tg_recipients.SHOWROOM_ALERT_THREAD_ID))
    except Exception:
        logger.warning("tg_recipients не прочитался - адресаты отдела продаж в сводке не показаны")
    on = [name for name, val in targets if val]
    off = [name for name, val in targets if not val]
    logger.info("Адресаты уведомлений заданы: %s", ", ".join(on) or "НИ ОДНОГО")
    if off:
        logger.warning("Адресаты уведомлений ПУСТЫ (контур выключен): %s", ", ".join(off))


async def init_telegram_bot() -> None:
    global _reconnect_failures
    if not TG_BOT_TOKEN:
        logger.warning("TG_BOT_TOKEN not set in .env - Telegram bot DISABLED")
        return
    if TG_ALLOWED_CHAT_ID is None:
        logger.warning("TG_ALLOWED_CHAT_ID not set in .env - Telegram bot DISABLED")
        return
    _state["configured"] = True
    _log_alert_targets()

    async with _init_lock:
        for attempt in range(1, max(1, TG_INIT_RETRIES) + 1):
            _state["init_attempts"] += 1
            if await _start_bot_once(f"старт, попытка {attempt}"):
                return
            if attempt < TG_INIT_RETRIES:
                delay = TG_INIT_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning(
                    "Telegram getMe не прошёл (попытка %s из %s): %s. Повтор через %.0f с.",
                    attempt, TG_INIT_RETRIES, _state["last_error"], delay,
                )
                await asyncio.sleep(delay)

    _reconnect_failures = 0
    logger.error(
        "Telegram НЕ поднялся за %s попыток: %s. Уведомления пока подавляются, но "
        "контур НЕ сдался: фоновый переподъём каждые %.0f с плюс попытка на каждой "
        "отправке. Состояние - GET / поле telegram.enabled.",
        TG_INIT_RETRIES, _state["last_error"], TG_RECONNECT_INTERVAL_S,
    )
    _start_reconnect_loop()


def _start_reconnect_loop() -> None:
    global _reconnect_task
    if _bot is not None:
        return
    if _reconnect_task is not None and not _reconnect_task.done():
        return
    try:
        _reconnect_task = asyncio.create_task(_reconnect_loop())
    except RuntimeError:
        # Нет running loop (например, в синхронном тесте) - переподъём останется
        # на отправке, это допустимая деградация.
        _reconnect_task = None


async def _reconnect_loop() -> None:
    """Фоновый переподъём. Главное отличие от прежнего поведения: секундный сбой
    сети на старте больше НЕ выключает уведомления навсегда."""
    global _reconnect_failures
    try:
        while _bot is None:
            await asyncio.sleep(TG_RECONNECT_INTERVAL_S)
            if _bot is not None:
                return
            async with _init_lock:
                if _bot is not None:
                    return
                _state["init_attempts"] += 1
                ok = await _start_bot_once("фоновый переподъём")
            if ok:
                logger.error(
                    "Telegram ожил фоновым переподъёмом. За время молчания подавлено "
                    "%s алертов - они НЕ восстановятся, нигде не буферизуются.",
                    _state["suppressed_streak"],
                )
                _reconnect_failures = 0
                return
            _reconnect_failures += 1
            # Не сорим в лог каждую минуту: раз в 10 неудач - громкая строка.
            if _reconnect_failures % 10 == 0:
                logger.error(
                    "Telegram не поднимается %s попыток подряд: %s. Подавлено алертов: %s.",
                    _reconnect_failures, _state["last_error"], _state["suppressed_streak"],
                )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Telegram reconnect loop crashed")


async def _ensure_bot_lazy() -> None:
    """Переподъём прямо на отправке - вторая линия после фонового цикла.
    Троттлится, чтобы шквал алертов не устроил шквал getMe."""
    global _last_lazy_attempt
    if _bot is not None or not _state["configured"]:
        return
    now = time.monotonic()
    if now - _last_lazy_attempt < TG_LAZY_REINIT_MIN_GAP_S:
        return
    _last_lazy_attempt = now
    if _init_lock.locked():
        return
    async with _init_lock:
        if _bot is not None:
            return
        _state["init_attempts"] += 1
        await _start_bot_once("переподъём на отправке")
    _start_reconnect_loop()


def _build_session() -> AiohttpSession | None:
    if not TG_PROXY_URL:
        return None
    if TG_PROXY_URL.startswith(("http://", "https://")):
        # aiohttp нативно поддерживает HTTP/HTTPS прокси — просто передаём URL
        return AiohttpSession(proxy=TG_PROXY_URL)
    if TG_PROXY_URL.startswith("socks"):
        logger.error(
            "TG_PROXY_URL=%s — SOCKS proxy не поддержан out-of-the-box. "
            "Поставь aiohttp_socks и допиши custom connector в _build_session(), "
            "либо используй HTTP/HTTPS прокси.",
            _redact_proxy(TG_PROXY_URL),
        )
        return None
    logger.error("TG_PROXY_URL=%s — неизвестный схема прокси", _redact_proxy(TG_PROXY_URL))
    return None


def _redact_proxy(url: str) -> str:
    """Скрывает user:pass в URL для логов."""
    return re.sub(r"://[^@]+@", "://***:***@", url)


def _redact(text: str) -> str:
    """Чистит текст от секретов перед тем, как он уйдёт в лог или в GET /.
    Текст исключения - не наш: aiohttp и python_socks кладут в него адрес прокси
    целиком, вместе с логином и паролем шлюза. GET / у интеграции открыт наружу
    (team.sunscrypt.ru/amo/ отдаёт 200 кому угодно), поэтому сюда секрет попасть
    не должен ни разу. 28.08.2026 токен и пароль шлюза уже утекли в systemd-журнал
    через argv - второй раз тем же путём не ходим."""
    if not text:
        return text
    out = _redact_proxy(str(text))
    out = re.sub(r"(bot)\d+:[A-Za-z0-9_-]{20,}", r"\1***", out)
    if TG_BOT_TOKEN:
        out = out.replace(TG_BOT_TOKEN, "***")
    return out


async def _run_polling() -> None:
    assert _bot is not None and _dp is not None
    try:
        await _dp.start_polling(_bot, handle_signals=False)
    except asyncio.CancelledError:
        _state["polling"] = False
        raise
    except Exception:
        # Polling умер - контур глухой на приём. Отправка ещё может работать, но
        # оставлять так нельзя: это ровно та одноразовость, из-за которой 28.08
        # молчали сутки, только с другой стороны. Гасим бота и отдаём его фоновому
        # переподъёму - он поднимет и polling.
        _state["polling"] = False
        _state["last_error"] = "polling crashed"
        logger.exception("Telegram polling crashed")
        global _drop_task
        # ссылку держим: задачу без ссылки сборщик мусора вправе убрать на полпути
        _drop_task = asyncio.create_task(_drop_bot("polling упал"))
    else:
        _state["polling"] = False


async def shutdown_telegram_bot() -> None:
    global _bot, _dp, _polling_task, _reconnect_task
    if _reconnect_task is not None:
        _reconnect_task.cancel()
        try:
            await _reconnect_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("reconnect task shutdown failed")
        _reconnect_task = None
    if _dp is not None:
        try:
            await _dp.stop_polling()
        except RuntimeError:
            # Polling already stopped (crashed or never started)
            pass
        except Exception:
            logger.exception("dp.stop_polling failed")
    if _polling_task is not None:
        _polling_task.cancel()
        try:
            await _polling_task
        except asyncio.CancelledError:
            pass
        _polling_task = None
    if _bot is not None:
        try:
            await _bot.session.close()
        except Exception:
            logger.exception("bot.session.close failed")
        _bot = None
    _dp = None
    _state["enabled"] = False
    _state["polling"] = False
    logger.info("Telegram bot stopped")


def _suppress(text: str, why: str) -> bool:
    _state["suppressed"] += 1
    _state["suppressed_streak"] += 1
    _state["last_suppressed_ts"] = time.time()
    logger.warning("send_alert suppressed (%s; подряд %s): %s", why, _state["suppressed_streak"], text)
    return False


async def _drop_bot(why: str) -> None:
    """Гасим заведомо нерабочего бота и просим фоновый цикл поднять нового."""
    global _bot, _dp, _polling_task
    logger.error("Гашу телеграм-бота: %s. Включаю фоновый переподъём.", why)
    bot, _bot = _bot, None
    _state["enabled"] = False
    _state["polling"] = False
    if _polling_task is not None:
        _polling_task.cancel()
        _polling_task = None
    _dp = None
    if bot is not None:
        try:
            await bot.session.close()
        except Exception:
            pass
    _start_reconnect_loop()


async def _send_with_retry(
    target: int,
    text: str,
    parse_mode: str | None,
    thread_id: int | None,
) -> bool:
    """Отправка с повторами. Разбирает ИМЕННО те отказы, на которых контур молчал:
    переезд группы в супергруппу, пропавший топик, сеть, флуд-лимит, битый токен."""
    attempts = max(1, TG_SEND_ATTEMPTS)
    attempt = 0
    # Переезд чата и пропавший топик попытку не тратят - но крутиться на них
    # бесконечно нельзя: цепочка переездов замкнётся, и сервис повиснет на одном
    # алерте. Три перенаправления - потолок.
    redirects_left = 3
    while attempt < attempts:
        attempt += 1
        bot = _bot
        if bot is None:
            return _suppress(text, "бот выключен на повторе")
        try:
            await bot.send_message(
                chat_id=target,
                text=text,
                parse_mode=parse_mode,
                message_thread_id=thread_id,
            )
            _state["sent"] += 1
            _state["last_ok_ts"] = time.time()
            _state["last_error"] = None
            _state["suppressed_streak"] = 0
            return True
        except TelegramMigrateToChat as exc:
            # Обычную группу конвертировали в супергруппу - старый chat_id умер.
            # Молча терять алерты здесь нельзя: новый адрес приезжает в ошибке.
            new_id = exc.migrate_to_chat_id
            _state["chat_remap"][str(target)] = new_id
            logger.error(
                "Чат %s переехал в супергруппу %s. Шлю по новому адресу и запомнил его "
                "до перезапуска. ВПИШИ новый chat_id в .env, иначе после рестарта "
                "уведомления снова замолчат молча.",
                target, new_id,
            )
            target = new_id
            thread_id = None  # топиков старого чата в новом не существует
            if redirects_left > 0:
                redirects_left -= 1
                attempt -= 1  # переезд не тратит попытку
            continue
        except TelegramRetryAfter as exc:
            wait = min(float(getattr(exc, "retry_after", 5) or 5), 30.0)
            logger.warning("Telegram просит подождать %.0f с (флуд-лимит), повторяю.", wait)
            await asyncio.sleep(wait)
            continue
        except TelegramNetworkError as exc:
            _state["last_error"] = _redact(f"network: {exc}")
            if attempt < attempts:
                delay = TG_SEND_BACKOFF_S * attempt
                logger.warning(
                    "Сеть до Telegram (попытка %s из %s): %s. Повтор через %.0f с.",
                    attempt, attempts, exc, delay,
                )
                await asyncio.sleep(delay)
                continue
            break
        except TelegramBadRequest as exc:
            if thread_id is not None and "thread not found" in str(exc).lower():
                logger.error(
                    "Топик %s в чате %s не найден (удалён или переехал) - шлю в общую "
                    "ленту чата, чтобы алерт не пропал.",
                    thread_id, target,
                )
                thread_id = None
                if redirects_left > 0:
                    redirects_left -= 1
                    attempt -= 1
                continue
            _state["last_error"] = _redact(f"bad request: {exc}")
            logger.error("Telegram отклонил сообщение (chat=%s): %s | текст: %s", target, exc, text)
            break
        except TelegramUnauthorizedError as exc:
            _state["last_error"] = _redact(f"unauthorized: {exc}")
            await _drop_bot(f"токен не принят ({exc}) - ротировали ключ?")
            break
        except (TelegramForbiddenError, TelegramNotFound) as exc:
            _state["last_error"] = _redact(f"{type(exc).__name__}: {exc}")
            logger.error(
                "Telegram отказал по чату %s (бота выгнали или чат удалён): %s | текст: %s",
                target, exc, text,
            )
            break
        except Exception as exc:
            _state["last_error"] = _redact(f"{type(exc).__name__}: {exc}")
            logger.exception("send_alert failed (chat=%s): %s", target, text)
            break

    _state["failed"] += 1
    _state["last_fail_ts"] = time.time()
    return False


def telegram_health() -> dict:
    """Срез контура уведомлений для GET /. Сторож должен смотреть СЮДА, а не в
    логи: молчащий бот внутри живого контейнера иначе неотличим от тишины по
    отсутствию событий."""
    ok_ts = _state["last_ok_ts"]
    return {
        "configured": _state["configured"],
        "enabled": _state["enabled"],
        "polling": _state["polling"],
        "sent": _state["sent"],
        "failed": _state["failed"],
        "suppressed": _state["suppressed"],
        "suppressed_streak": _state["suppressed_streak"],
        "init_attempts": _state["init_attempts"],
        "seconds_since_last_ok": round(time.time() - ok_ts, 1) if ok_ts else None,
        "last_error": _redact(_state["last_error"]) if _state["last_error"] else None,
        # Сами chat_id наружу не отдаём - GET / открыт без авторизации. Сторожу
        # хватает признака «переезд был», адреса лежат в логе уровнем ERROR.
        "chat_remapped": len(_state["chat_remap"]),
    }


async def send_alert(
    text: str,
    parse_mode: str | None = None,
    chat_id: int | None = None,
    message_thread_id: int | None = None,
) -> bool:
    """Шлёт текст в Telegram. Возвращает True при успехе.
    chat_id — куда слать; None → дефолтный TG_ALLOWED_CHAT_ID (чат /print).
    message_thread_id — топик супергруппы-форума (None → General).
    parse_mode="HTML" — для кликабельных ссылок (uis_missed_call)."""
    target = chat_id if chat_id is not None else TG_ALLOWED_CHAT_ID
    if target is None:
        return _suppress(text, "адресат не задан")
    if _bot is None:
        # Раньше контур здесь сдавался навсегда. Теперь - пробуем поднять бота.
        await _ensure_bot_lazy()
    if _bot is None:
        return _suppress(text, "бот выключен")
    target = _state["chat_remap"].get(str(target), target)
    return await _send_with_retry(target, text, parse_mode, message_thread_id)
