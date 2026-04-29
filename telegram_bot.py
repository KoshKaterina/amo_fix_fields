"""Telegram-бот: aiogram 3, long polling. Команды /print и /retry для группы."""

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

import waybill_service
from waybill_config import TG_ALLOWED_CHAT_ID, TG_BOT_TOKEN

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

    return dp


async def init_telegram_bot() -> None:
    global _bot, _dp, _polling_task
    if not TG_BOT_TOKEN:
        logger.warning("TG_BOT_TOKEN not set — Telegram bot disabled")
        return
    if TG_ALLOWED_CHAT_ID is None:
        logger.warning("TG_ALLOWED_CHAT_ID not set — Telegram bot disabled")
        return

    _bot = Bot(token=TG_BOT_TOKEN)
    _dp = _build_dispatcher()
    waybill_service.set_alert_callback(send_alert)
    _polling_task = asyncio.create_task(_run_polling())
    logger.info("Telegram bot started in long-polling mode (chat=%s)", TG_ALLOWED_CHAT_ID)


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


async def send_alert(text: str) -> None:
    if _bot is None or TG_ALLOWED_CHAT_ID is None:
        logger.warning("send_alert suppressed (bot disabled): %s", text)
        return
    try:
        await _bot.send_message(chat_id=TG_ALLOWED_CHAT_ID, text=text)
    except Exception:
        logger.exception("send_alert failed: %s", text)
