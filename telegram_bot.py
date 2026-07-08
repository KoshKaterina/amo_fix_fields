"""Telegram-бот: aiogram 3, long polling. Команды /print и /retry для группы."""

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

import waybill_service
from waybill_config import TG_ALLOWED_CHAT_ID, TG_BOT_TOKEN, TG_PROXY_URL

logger = logging.getLogger("uvicorn")

_bot: Bot | None = None
_dp: Dispatcher | None = None
_polling_task: asyncio.Task | None = None
_print_lock = asyncio.Lock()


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


async def init_telegram_bot() -> None:
    global _bot, _dp, _polling_task
    if not TG_BOT_TOKEN:
        logger.warning("⚠ TG_BOT_TOKEN not set in .env — Telegram bot DISABLED")
        return
    if TG_ALLOWED_CHAT_ID is None:
        logger.warning("⚠ TG_ALLOWED_CHAT_ID not set in .env — Telegram bot DISABLED")
        return

    session = _build_session()
    _bot = Bot(token=TG_BOT_TOKEN, session=session) if session else Bot(token=TG_BOT_TOKEN)
    _dp = _build_dispatcher()
    waybill_service.set_alert_callback(send_alert)
    # Сначала валидируем токен/сеть синхронно — если getMe упадёт, polling
    # не стартуем и пишем понятную ошибку. Иначе фоновая задача упадёт молча.
    try:
        me = await _bot.get_me()
    except Exception:
        logger.exception(
            "Telegram getMe FAILED — токен невалиден или сеть до api.telegram.org недоступна "
            "(Telegram заблокирован в РФ — задай TG_PROXY_URL в .env). "
            "Polling не стартую, бот выключен."
        )
        try:
            await _bot.session.close()
        except Exception:
            pass
        _bot = None
        _dp = None
        return

    _polling_task = asyncio.create_task(_run_polling())
    proxy_note = f" via proxy {_redact_proxy(TG_PROXY_URL)}" if TG_PROXY_URL else ""
    logger.info(
        "Telegram bot started: @%s (id=%s) polling%s, allowed_chat_id=%s. "
        "ВАЖНО: Privacy Mode должен быть ВЫКЛЮЧЕН в @BotFather (Bot Settings → "
        "Group Privacy → Turn off).",
        me.username, me.id, proxy_note, TG_ALLOWED_CHAT_ID,
    )


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
    import re
    return re.sub(r"://[^@]+@", "://***:***@", url)


async def _run_polling() -> None:
    assert _bot is not None and _dp is not None
    try:
        await _dp.start_polling(_bot, handle_signals=False)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Telegram polling crashed")


async def shutdown_telegram_bot() -> None:
    global _bot, _dp, _polling_task
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
    logger.info("Telegram bot stopped")


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
    if _bot is None or target is None:
        logger.warning("send_alert suppressed (bot disabled): %s", text)
        return False
    try:
        await _bot.send_message(
            chat_id=target,
            text=text,
            parse_mode=parse_mode,
            message_thread_id=message_thread_id,
        )
        return True
    except Exception:
        logger.exception("send_alert failed (chat=%s): %s", target, text)
        return False
