"""Перенос сделки Академии на этап «Вступил в чат» по факту вступления в чат.

Слушает у бота-админа чата апдейты `chat_member` длинным опросом (getUpdates) и,
когда человек стал участником, двигает его сделку Академии на «Вступил в чат».

Почему опрос, а не вебхук Telegram. Опрос не требует ни маршрута в nginx, ни
секрета в пути, а главное - переживает пересборку контейнера бесплатно: пока нас
нет, апдейты лежат на стороне Telegram (до суток) и вычитываются на старте. У
вебхука на том же месте было бы окно 502.

Сделка ищется двумя путями, в этом порядке:

1. По ИМЕНИ ссылки. Наши приглашения помечены `academy lead <id сделки>`
   (`academy_invite_link._create_link`), и Telegram отдаёт имя в событии - это
   попадание без догадок.
2. По TELEGRAM ID вступившего - поле контакта 571839 `TelegramId_WZ`. Проверено
   29.09.2026: ссылки клиентам выдаёт в основном сам сценарий BotHelp, а не мы,
   поэтому имени в событии чаще всего НЕТ, и этот путь основной, а не запасной.
   Ограничение честное: у кого поле пустое, того не найдём (на 29.09 заполнено
   у 233 из 798 контактов Академии).

Двигаем только ВПЕРЁД и только с перечисленных этапов: белый список закрыт
намеренно, чтобы новый этап воронки не попал под автоматику молча. «Лист
ожидания», «Не трогать этих клиентов!!!», «Не купили DEFI-3», Неразобранное и всё,
что стоит после «Вступил в чат», не трогаем никогда.

Клиенту модуль ничего не отправляет и в чат не пишет.
"""

import asyncio
import json
import logging
import os
import re
import time

import httpx

import amo_service
from waybill_config import (
    ACADEMY_INVITE_BOT_TOKEN,
    ACADEMY_PRACTICUM_CHAT_ID,
    PIPELINE_ACADEMY,
    TG_PROXY_URL,
)

# Свои настройки живут в общем waybill_config, как у соседних модулей, НО падать из-за
# них модуль не имеет права.
#
# ⚠️ Цена урока 29.09.2026. Этот модуль требовал из общего конфига четыре новых имени.
# В то же пятиминутное окно параллельная сессия залила свою копию того же конфига,
# собранную минутой раньше - без наших имён. Контейнер лёг в цикл перезапуска (девять
# попыток), 22 запроса получили 502. Затем то же повторилось в обратную сторону: наш
# откат конфига снёс их флаг, и упал уже их модуль.
#
# Отсюда правило: общий конфиг - ЖЕЛАЕМЫЙ источник, окружение - обязательный запасной.
# Так одновременная правка общего файла двумя сессиями больше не роняет сервис.
try:
    from waybill_config import (  # noqa: F401
        ACADEMY_CHAT_JOIN_DRY_RUN,
        ACADEMY_CHAT_JOIN_ENABLED,
        ACADEMY_CHAT_JOIN_POLL_TIMEOUT_S,
        ACADEMY_CHAT_JOIN_STATE_PATH,
        STATUS_ACADEMY_JOINED_CHAT,
    )
except ImportError:  # чужая заливка общего конфига могла унести наши строки
    ACADEMY_CHAT_JOIN_ENABLED = os.getenv("ACADEMY_CHAT_JOIN_ENABLED", "0").strip() == "1"
    ACADEMY_CHAT_JOIN_DRY_RUN = os.getenv("ACADEMY_CHAT_JOIN_DRY_RUN", "0").strip() == "1"
    ACADEMY_CHAT_JOIN_STATE_PATH = os.getenv(
        "ACADEMY_CHAT_JOIN_STATE_PATH", "/app/var/academy/academy_chat_join.json")
    ACADEMY_CHAT_JOIN_POLL_TIMEOUT_S = int(
        os.getenv("ACADEMY_CHAT_JOIN_POLL_TIMEOUT_S", "25") or "25")
    STATUS_ACADEMY_JOINED_CHAT = 88943006
    logging.getLogger("uvicorn").warning(
        "Академия-вступление: настроек нет в waybill_config - читаю окружение напрямую")

logger = logging.getLogger("uvicorn")

# Поле Wazzup с Telegram id человека. Объявлено так же в academy_bothelp_upsert и
# academy_invite_delivery - держим локально, чтобы не править чужие модули ради строки.
FIELD_WAZZUP_TELEGRAM_ID = 571839

# Этапы, с которых пускаем на «Вступил в чат», В ПОРЯДКЕ воронки. Закрытый список,
# а не «sort меньше целевого»: новый этап должен попадать сюда осознанно, руками.
# Порядок держим свой, а не спрашиваем у amo: get_status_sort отвечает из кэша
# воронок, а кэш греется в lifespan - в разовом прогоне и в тесте его нет, и выбор
# сделки молча деградировал бы до «любая».
STAGE_ORDER = (
    87654850,  # Входящий лид
    88464034,  # Взят в работу
    88466522,  # Квалификация проведена
    88527246,  # Первичные материалы отправлены
    88528190,  # Второе касание совершено (цены озвучены)
    88838378,  # Бот запущен
    88838382,  # Проходит анкету
    88838386,  # Анкета пройдена
    88835666,  # Записан на практикум - шаг прямо перед вступлением
)
MOVABLE_FROM_STATUSES = frozenset(STAGE_ORDER)

# Статусы участника, которые считаем «человек в чате».
_MEMBER_STATUSES = frozenset({"member", "administrator", "creator"})

_LINK_NAME_RE = re.compile(r"academy\s+lead\s+(\d+)", re.IGNORECASE)

_STATE_CAP = 20000
_state: dict = {"offset": 0, "seen": {}}
_state_loaded = False
_worker: asyncio.Task | None = None
_last_poll_at: float = 0.0
_last_error: str = ""
_moved_count = 0


def configured() -> bool:
    """Фича включена и есть, чем и куда слушать."""
    return bool(
        ACADEMY_CHAT_JOIN_ENABLED
        and ACADEMY_INVITE_BOT_TOKEN
        and ACADEMY_PRACTICUM_CHAT_ID
    )


# --------------------------------------------------------------------------- состояние


def _load_state() -> None:
    """Поднять offset и журнал переводов с диска. Файла нет или битый - с нуля."""
    global _state_loaded
    if _state_loaded:
        return
    _state_loaded = True
    try:
        with open(ACADEMY_CHAT_JOIN_STATE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _state["offset"] = int(data.get("offset") or 0)
            seen = data.get("seen")
            if isinstance(seen, dict):
                _state["seen"] = {str(k): v for k, v in seen.items()}
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception(
            "Академия-вступление: не прочитался %s - начинаем с нуля",
            ACADEMY_CHAT_JOIN_STATE_PATH,
        )


def _save_state() -> None:
    """Сбросить состояние на диск. Не удалось - работаем дальше из памяти."""
    try:
        os.makedirs(os.path.dirname(ACADEMY_CHAT_JOIN_STATE_PATH), exist_ok=True)
        tmp = f"{ACADEMY_CHAT_JOIN_STATE_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_state, f, ensure_ascii=False)
        os.replace(tmp, ACADEMY_CHAT_JOIN_STATE_PATH)
    except Exception:
        logger.exception(
            "Академия-вступление: не записался %s", ACADEMY_CHAT_JOIN_STATE_PATH)


def _trim_seen() -> None:
    seen = _state.get("seen") or {}
    if len(seen) <= _STATE_CAP:
        return
    extra = len(seen) - _STATE_CAP
    for key in sorted(seen, key=lambda k: (seen[k] or {}).get("at") or 0)[:extra]:
        seen.pop(key, None)


def _seen_key(chat_id, user_id) -> str:
    return f"{chat_id}:{user_id}"


def _already_handled(chat_id, user_id) -> bool:
    _load_state()
    return _seen_key(chat_id, user_id) in (_state.get("seen") or {})


def _remember(chat_id, user_id, lead_id, outcome: str) -> None:
    _load_state()
    (_state.setdefault("seen", {}))[_seen_key(chat_id, user_id)] = {
        "lead_id": lead_id, "outcome": outcome, "at": int(time.time()),
    }
    _trim_seen()
    _save_state()


# --------------------------------------------------------------------------- Telegram


async def _telegram(method: str, payload: dict, *, timeout: float = 20.0) -> dict | None:
    """Вызов Bot API через венский прокси.

    ⚠️ Прокси передаём ЯВНО. `api.telegram.org` стоит в NO_PROXY контейнера, и
    клиент, читающий окружение сам, ушёл бы напрямую - а напрямую с сервера
    Telegram недоступен (`Network is unreachable`).
    """
    url = f"https://api.telegram.org/bot{ACADEMY_INVITE_BOT_TOKEN}/{method}"
    try:
        async with httpx.AsyncClient(timeout=timeout, proxy=TG_PROXY_URL or None) as client:
            response = await client.post(url, json=payload)
        data = response.json()
    except Exception as exc:
        # Текст ошибки без URL: в нём токен.
        raise TelegramUnavailable(type(exc).__name__) from exc
    if response.status_code != 200 or not data.get("ok"):
        raise TelegramRefused(
            response.status_code, str(data.get("description") or "без описания"))
    return data.get("result")


class TelegramUnavailable(Exception):
    """Сеть или прокси не дали дойти до Telegram."""


class TelegramRefused(Exception):
    """Telegram ответил, но отказом."""

    def __init__(self, status_code: int, description: str):
        super().__init__(f"{status_code}: {description}")
        self.status_code = status_code
        self.description = description


def is_member_status(member: dict | None) -> bool:
    """Человек сейчас в чате?

    `restricted` двусмыслен: это и «участник с ограничениями», и «ограничен,
    но уже не участник» - различает только флаг is_member.
    """
    status = str((member or {}).get("status") or "")
    if status in _MEMBER_STATUSES:
        return True
    if status == "restricted":
        return bool((member or {}).get("is_member"))
    return False


def lead_id_from_link_name(name) -> int | None:
    """Номер сделки из имени НАШЕЙ ссылки («academy lead 36566471»)."""
    match = _LINK_NAME_RE.search(str(name or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- поиск сделки


def _telegram_id_matches(contact: dict, user_id) -> bool:
    """Точное совпадение по полю, а не по факту находки.

    Поиск в amo полнотекстовый: по строке цифр он может вытащить контакт, у
    которого это чужой телефон или кусок другого поля.
    """
    value = amo_service.get_custom_field_value(contact, FIELD_WAZZUP_TELEGRAM_ID)
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return bool(digits) and digits == str(user_id)


async def _leads_of_contact(contact: dict) -> list[dict]:
    ids: list[int] = []
    for lead in ((contact.get("_embedded") or {}).get("leads")) or []:
        try:
            ids.append(int(lead["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    if not ids:
        return []
    return await amo_service.get_leads_by_ids(ids)


def pick_lead(leads: list[dict]) -> dict | None:
    """Кандидат на перевод: дальше всех прошедший по воронке, при равенстве - свежее.

    Берём только сделки Академии на разрешённых этапах. У человека их бывает
    несколько (склейка карточек, повторные заявки), и двигать надо ту, что ближе
    к цели, а не первую попавшуюся.
    """
    candidates = [
        lead for lead in leads
        if str(lead.get("pipeline_id")) == str(PIPELINE_ACADEMY)
        and int(lead.get("status_id") or 0) in MOVABLE_FROM_STATUSES
    ]
    if not candidates:
        return None

    def rank(lead: dict) -> tuple[int, int]:
        return (
            STAGE_ORDER.index(int(lead.get("status_id") or 0)),
            int(lead.get("created_at") or 0),
        )

    return max(candidates, key=rank)


async def find_lead_for_user(user_id, link_name=None) -> tuple[dict | None, str]:
    """Найти сделку вступившего. Возвращает (сделка, как нашли или почему нет)."""
    lead_id = lead_id_from_link_name(link_name)
    if lead_id:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if lead and str(lead.get("pipeline_id")) == str(PIPELINE_ACADEMY):
            return lead, "по имени нашей ссылки"
        # Имя было, но сделка не та - не выдумываем, идём обычным путём.

    contacts = await amo_service.find_contacts_by_query(str(user_id), limit=10)
    if contacts is None:
        return None, "поиск контакта в amo не ответил"
    matched = [c for c in contacts if _telegram_id_matches(c, user_id)]
    if not matched:
        return None, "контакт с таким Telegram id не найден"

    leads: list[dict] = []
    for contact in matched:
        full = await amo_service.get_contact_by_id(contact.get("id"), with_=("leads",))
        if full:
            leads.extend(await _leads_of_contact(full))
    if not leads:
        return None, "у контакта нет сделок"
    lead = pick_lead(leads)
    if not lead:
        return None, "нет сделки Академии на подходящем этапе"
    return lead, "по Telegram id контакта"


# --------------------------------------------------------------------------- перевод


async def process_join(user_id, *, chat_id=None, link_name=None,
                       display: str = "") -> str:
    """Обработать одно вступление. Возвраты стабильны для тестов и наблюдаемости.

    disabled, already_handled, not_found, already_on_stage, moved, dry_run,
    patch_error.
    """
    global _moved_count
    if not configured():
        return "disabled"
    chat_id = chat_id or ACADEMY_PRACTICUM_CHAT_ID
    if _already_handled(chat_id, user_id):
        return "already_handled"

    lead, how = await find_lead_for_user(user_id, link_name)
    if not lead:
        logger.info(
            "Академия-вступление: %s вошёл в чат, сделку не двигаем - %s",
            display or user_id, how)
        return "not_found"

    lead_id = lead.get("id")
    status_id = int(lead.get("status_id") or 0)
    if status_id == int(STATUS_ACADEMY_JOINED_CHAT):
        _remember(chat_id, user_id, lead_id, "already_on_stage")
        return "already_on_stage"

    if ACADEMY_CHAT_JOIN_DRY_RUN:
        logger.info(
            "Академия-вступление (холостой ход): перевёл бы сделку %s на «Вступил в чат», "
            "нашёл %s, сейчас этап %s", lead_id, how, status_id)
        return "dry_run"

    result = await amo_service.patch_lead(
        lead_id, status_id=int(STATUS_ACADEMY_JOINED_CHAT),
        pipeline_id=int(PIPELINE_ACADEMY),
    )
    if not result.get("ok"):
        logger.warning(
            "Академия-вступление: amo не приняла перевод сделки %s на «Вступил в чат»",
            lead_id)
        return "patch_error"

    _moved_count += 1
    _remember(chat_id, user_id, lead_id, "moved")
    logger.info(
        "Академия-вступление: сделка %s переведена на «Вступил в чат» (%s, %s)",
        lead_id, how, display or user_id)
    return "moved"


async def handle_update(update: dict) -> str:
    """Разобрать один апдейт Telegram. Возвраты: ignored_* или результат перевода."""
    event = (update or {}).get("chat_member")
    if not isinstance(event, dict):
        return "ignored_not_chat_member"
    chat_id = str(((event.get("chat") or {}).get("id")) or "")
    if chat_id != str(ACADEMY_PRACTICUM_CHAT_ID):
        return "ignored_other_chat"
    old = event.get("old_chat_member") or {}
    new = event.get("new_chat_member") or {}
    if is_member_status(old) or not is_member_status(new):
        # Не вступление: выход, бан, смена прав внутри чата.
        return "ignored_not_a_join"

    user = new.get("user") or {}
    user_id = user.get("id")
    if not user_id:
        return "ignored_no_user"
    username = user.get("username")
    display = f"@{username}" if username else (user.get("first_name") or str(user_id))
    link_name = ((event.get("invite_link") or {}).get("name"))
    return await process_join(
        user_id, chat_id=chat_id, link_name=link_name, display=display)


# --------------------------------------------------------------------------- опрос


async def poll_once() -> int:
    """Один длинный опрос. Возвращает число разобранных апдейтов."""
    global _last_poll_at, _last_error
    _load_state()
    timeout = int(ACADEMY_CHAT_JOIN_POLL_TIMEOUT_S)
    updates = await _telegram(
        "getUpdates",
        {
            "offset": int(_state.get("offset") or 0),
            "timeout": timeout,
            # ⚠️ Без явного allowed_updates Telegram не отдаёт chat_member вовсе -
            # это самая частая причина «бот админ, а событий нет».
            "allowed_updates": ["chat_member"],
        },
        timeout=timeout + 15,
    ) or []
    _last_poll_at = time.time()
    _last_error = ""

    handled = 0
    for update in updates:
        try:
            update_id = int(update.get("update_id") or 0)
        except (TypeError, ValueError):
            continue
        try:
            await handle_update(update)
        except Exception:
            # Апдейт всё равно подтверждаем: иначе битое событие заклинит опрос навсегда.
            logger.exception("Академия-вступление: апдейт %s не обработан", update_id)
        handled += 1
        if update_id >= int(_state.get("offset") or 0):
            _state["offset"] = update_id + 1
    if handled:
        _save_state()
    return handled


async def _worker_loop() -> None:
    global _last_error
    while True:
        try:
            await poll_once()
        except asyncio.CancelledError:
            raise
        except TelegramRefused as exc:
            _last_error = str(exc)
            if exc.status_code == 409:
                # Кто-то поставил боту вебхук: опрос и вебхук взаимно исключают друг друга.
                logger.error(
                    "Академия-вступление: Telegram отдал 409 - у бота стоит вебхук, "
                    "опрос работать не будет (%s)", exc.description)
                await asyncio.sleep(300)
            else:
                logger.warning("Академия-вступление: Telegram отказал (%s)", exc)
                await asyncio.sleep(30)
        except TelegramUnavailable as exc:
            _last_error = str(exc)
            logger.warning("Академия-вступление: Telegram недоступен (%s)", exc)
            await asyncio.sleep(15)
        except Exception:
            logger.exception("Академия-вступление: ошибка опроса")
            await asyncio.sleep(15)


def start() -> None:
    global _worker
    if configured() and (_worker is None or _worker.done()):
        _load_state()
        _worker = asyncio.create_task(_worker_loop())
        logger.info(
            "Академия-вступление: слушаю вступления в чат практикума%s",
            " (холостой ход)" if ACADEMY_CHAT_JOIN_DRY_RUN else "")


async def stop() -> None:
    global _worker
    if _worker is not None:
        _worker.cancel()
        await asyncio.gather(_worker, return_exceptions=True)
        _worker = None


def stats() -> dict:
    """Срез для ручки здоровья: молчащий опрос внутри живого контейнера иначе
    неотличим от тишины по отсутствию вступлений."""
    if not configured():
        return {"enabled": False}
    _load_state()
    return {
        "enabled": True,
        "dry_run": bool(ACADEMY_CHAT_JOIN_DRY_RUN),
        "offset": int(_state.get("offset") or 0),
        "moved_since_start": _moved_count,
        "known_joins": len(_state.get("seen") or {}),
        "last_poll_age_s": int(time.time() - _last_poll_at) if _last_poll_at else None,
        "last_error": _last_error or None,
    }
