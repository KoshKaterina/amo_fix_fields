"""Стикеры Telegram, которые Wazzup присылает вложением типа `document`.

Что видно на проде (23.11.2026, только чтение): клиент прислал анимированный стикер, в теле вебхука
`type: "document"`, подписи нет, есть `contentUri` вида `https://store.wazzup24.com/<uuid>` (ни имени файла, ни
расширения в ссылке). HTTP HEAD по ссылке отвечает 200 и называет вещь: `Content-Type: application/x-tgsticker`,
`Content-Disposition: attachment; filename*=UTF-8''AnimatedSticker.tgs`. Без этого ИИ получал «[Клиент прислал файл;
содержимого ИИ не видит]» и просил клиента описать файл, а панель подписывала сообщение «[документ]».

Здесь тип определяется ОДИН раз, на входе, до всех потребителей вебхука: `normalize_payload(payload)` меняет в теле
`type` с `document` на `sticker` (и оставляет `originalType: "document"` для разбора), а дальше везде живёт `sticker`:
очередь ИИ (`ai_engine.ATTACHMENT_WORDS["sticker"]`), пересылка в панель (`wazzup_forward`, `wazzup_message.msg_type`),
уведомления авто-режима. Вызывается из `webhooks.wazzup_webhook` перед первым потребителем; больше нигде не вызывать.

Правила (решение Кати 23.11.2026):
  • проверяем только входящее сообщение `document` БЕЗ подписи и с непустой ссылкой (с подписью это не голый стикер,
    исходящие `isEcho` не трогаем);
  • только HEAD, тело не скачиваем никогда; таймаут `HEAD_TIMEOUT_S` (3 с) жёсткий, на всё определение, а не на один этап;
  • стикер: `Content-Type` равен `application/x-tgsticker` ИЛИ имя файла в `Content-Disposition` содержит `sticker`
    без учёта регистра (`AnimatedSticker.tgs`, `VideoSticker.webm`); всё остальное остаётся `document`;
  • любой сбой (таймаут, сеть, не 200, 403/405 на HEAD, нет заголовков, чужой хост) = остаётся `document`, исключение
    наружу не выходит, сообщение не теряется;
  • результат запоминается по ссылке (до `CACHE_MAX` записей, самые старые вытесняются): определённый ответ (200 и любой
    4xx) запоминается, сбой (таймаут, сеть, 5xx) нет - повтор вебхука попробует ещё раз;
  • ссылку в лог не пишем: только тип, результат и первые 8 знаков идентификатора сообщения.

Аварийный рубильник: `WAZZUP_STICKER_DETECT_ENABLED=0` в `.env` движка (по умолчанию включено).
"""
import asyncio
import logging
import os
import re
from collections import OrderedDict
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

import wazzup_sla

logger = logging.getLogger("uvicorn")

ENABLED = os.getenv("WAZZUP_STICKER_DETECT_ENABLED", "1").strip() != "0"

HEAD_TIMEOUT_S = 3.0
CACHE_MAX = 500
STICKER_CONTENT_TYPE = "application/x-tgsticker"

# Ходим только на хранилище Wazzup: ссылка приходит в теле вебхука, и по чужому адресу сервер запрос слать не должен.
# Хост - сам домен или его поддомен (`store.wazzup24.com`). Переопределяется `WAZZUP_STICKER_HOSTS` (через запятую).
ALLOWED_HOSTS = tuple(
    h.strip().lower() for h in os.getenv("WAZZUP_STICKER_HOSTS", "wazzup24.com").split(",") if h.strip()
)

_transport: httpx.AsyncBaseTransport | None = None      # подмена сети в тестах (как в ai_engine_client)
_cache: "OrderedDict[str, tuple[bool, str]]" = OrderedDict()

_FILENAME_RE = re.compile(r"""filename\*?\s*=\s*(?:"([^"]*)"|([^;]*))""", re.IGNORECASE)


# =============================================================================================
# чистые правила
# =============================================================================================

def is_sticker_headers(content_type: str | None, content_disposition: str | None) -> tuple[bool, str]:
    """(стикер ли, причина одним словом) по заголовкам ответа на HEAD."""
    mime = str(content_type or "").split(";", 1)[0].strip().lower()
    if mime == STICKER_CONTENT_TYPE:
        return True, "content-type"
    for m in _FILENAME_RE.finditer(str(content_disposition or "")):
        name = m.group(1) if m.group(1) is not None else m.group(2)
        if "sticker" in unquote(str(name or "")).lower():
            return True, "имя файла"
    return False, "не стикер"


def _short_id(m: dict) -> str:
    return str(m.get("messageId") or "")[:8] or "без id"


def _host_allowed(uri: str) -> bool:
    try:
        parts = urlsplit(uri)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    if parts.scheme not in ("http", "https") or not host:
        return False
    return any(host == h or host.endswith("." + h) for h in ALLOWED_HOSTS)


def needs_check(m: Any) -> bool:
    """Нужна ли проверка: входящее `document` без подписи, с ссылкой на файл."""
    if not isinstance(m, dict):
        return False
    if str(m.get("type") or "").strip().lower() != "document":
        return False
    if wazzup_sla._is_outbound(m):          # направление считает SLA-таймер: правило одно на весь контур
        return False
    if str(m.get("text") or "").strip():
        return False
    uri = m.get("contentUri")
    return isinstance(uri, str) and bool(uri.strip())


# =============================================================================================
# сеть и кэш
# =============================================================================================

async def _head(uri: str) -> httpx.Response:
    async with httpx.AsyncClient(timeout=HEAD_TIMEOUT_S, transport=_transport, follow_redirects=False) as client:
        return await client.head(uri)


def _cache_get(uri: str) -> tuple[bool, str] | None:
    hit = _cache.get(uri)
    if hit is not None:
        _cache.move_to_end(uri)
    return hit


def _cache_put(uri: str, value: tuple[bool, str]) -> None:
    _cache[uri] = value
    _cache.move_to_end(uri)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)


async def detect(uri: str) -> tuple[bool, str]:
    """(стикер ли, причина для лога). Не бросает; дольше `HEAD_TIMEOUT_S` не ждёт; ссылку наружу не отдаёт."""
    uri = str(uri or "").strip()
    cached = _cache_get(uri)
    if cached is not None:
        return cached[0], cached[1] + " (из кэша)"
    if not _host_allowed(uri):
        return False, "чужой хост"
    try:
        resp = await asyncio.wait_for(_head(uri), timeout=HEAD_TIMEOUT_S)
    except asyncio.TimeoutError:
        return False, "таймаут"
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 - сеть, TLS, разбор ответа: любой сбой значит «остаётся document»
        return False, f"сбой {type(e).__name__}"
    code = resp.status_code
    if code == 200:
        result = is_sticker_headers(resp.headers.get("content-type"), resp.headers.get("content-disposition"))
    elif 400 <= code < 500 or 300 <= code < 400:
        result = (False, f"HEAD {code}")                  # ответ определённый: повторять нет смысла, запоминаем
    else:
        return False, f"HEAD {code}"                      # 5xx и прочее: сбой, не запоминаем
    _cache_put(uri, result)
    return result


# =============================================================================================
# врезка в вебхук
# =============================================================================================

async def _check_one(m: dict) -> bool:
    """Проверяет одно сообщение; при стикере правит его тип на месте. True - стал стикером."""
    is_sticker, why = await detect(str(m.get("contentUri") or ""))
    if is_sticker:
        m["originalType"] = str(m.get("type") or "document")
        m["type"] = "sticker"
        logger.info("Wazzup стикер: сообщение %s: document -> sticker (%s)", _short_id(m), why)
    else:
        logger.info("Wazzup стикер: сообщение %s: остался document (%s)", _short_id(m), why)
    return is_sticker


async def normalize_payload(payload: Any) -> int:
    """Меняет `type` у голых стикеров-«документов» в теле вебхука Wazzup. Возвращает, сколько сообщений стали стикером.

    Ждёт не дольше `HEAD_TIMEOUT_S` на всё тело (сообщения проверяются параллельно) и только когда в теле есть
    подходящее сообщение; во всех остальных случаях возвращается сразу. Не бросает."""
    if not ENABLED or not isinstance(payload, dict):
        return 0
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return 0
    try:
        todo = [m for m in messages if needs_check(m)]
        if not todo:
            return 0
        results = await asyncio.gather(*(_check_one(m) for m in todo), return_exceptions=True)
        return sum(1 for r in results if r is True)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("Wazzup стикер: определение не удалось, типы остались как пришли")
        return 0
