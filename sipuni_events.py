"""События АТС Sipuni: разбор, учёт звонков по call_id и решение «пропущенный».

Телефония переезжает с UIS на Sipuni, но UIS остаётся (номер 495), поэтому этот модуль
живёт РЯДОМ с uis_missed_call.py и ничего в UIS-путях не меняет. Модуль чистый: без сети, без
aiogram, без amoCRM. Всё, что требует Telegram и amo, лежит в uis_missed_call.py, а сюда
приходит только поток событий.

⚠️ ФОРМАТ СОБЫТИЙ ВЗЯТ ИЗ ОПИСАНИЯ, А НЕ ИЗ РЕАЛЬНЫХ ОБРАЗЦОВ. Интеграция Sipuni с нами ещё не
подключена, звонков не было. Всё, что ниже помечено «ПРЕДПОЛОЖЕНИЕ», проверяется на первом
тестовом звонке по строкам журнала режима log (см. log_line). Разбор поэтому терпимый: любой
мусор даёт событие с kind=None, а не исключение.

Что известно из описания «События на АТС» (HTTP, метод GET или POST):
  event      1 начало вызова или перевода ноги, 2 завершение, 3 ответ,
             4 промежуточное завершение при переводе с консультацией
  call_id    уникальный id вызова, сохраняется при переводе (ПРЕДПОЛОЖЕНИЕ: общий у всех ног)
  src_num, dst_num, src_type, dst_type   (1 внешний, 2 внутренний; dst_num бывает пустым)
  status     у событий 2 и 4: ANSWER, BUSY, NOANSWER, CANCEL, CONGESTION, CHANUNAVAIL
  call_start_timestamp, call_answer_timestamp   (0, если не отвечен)
  is_inner_call=1 у события 1 внутреннего звонка; treeName, treeNumber (схема)

Отдельного события «пропущенный звонок» у Sipuni НЕТ. Решение (принято 28.10.2026):
  * входящий звонок: есть событие 1 с внешним звонящим (src_type=1) и внутренним адресатом
    (dst_type=2); внутренний звонок и исходящий в счёт пропущенных не идут;
  * ответ: событие 3, ИЛИ событие 2/4 со status=ANSWER, ИЛИ call_answer_timestamp > 0;
  * пропущенным звонок считается, когда по call_id ни одна нога не отвечена, число событий 2
    и 4 покрывает число событий 1 (нет открытых ног) и после последнего события прошло тихое
    окно SIPUNI_MISSED_QUIET_S. Нога без ответа при последующем ответе другой ноги пропущенным
    НЕ считается. Один алерт на call_id.

Тихое окно перевзводится на КАЖДОМ событии звонка (asyncio.call_later), поэтому очередь с
несколькими ногами и перевод между очередями не дают раннего ложного алерта. Всё состояние
живёт в памяти процесса с TTL и ограничением размера, как у соседних счётчиков: рестарт
контейнера его теряет, и это осознанный размен (так же устроен uis_missed_call).

Кроме алерта модуль отдаёт состояние другим модулям:
  * phone_status(телефон) -> connected / active / missed / unknown - для гейта
    распределения лидов (lead_distribution);
  * missed_record(телефон) - «перезвонили» по ПОТОКУ ЗВОНКОВ, а не по тегам (у UIS теги
    уже не ставятся, у Sipuni теги не описаны): после пропущенного от номера P появился
    отвеченный звонок с участием P, исходящий на P или входящий от P.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import inspect
import json
import logging
import time
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl

from shared.phones import digits_only, ru_8_to_7
from waybill_config import SIPUNI_MISSED_QUIET_S

logger = logging.getLogger("uvicorn")

EVENT_START = 1      # начало вызова или перевода ноги
EVENT_END = 2        # завершение
EVENT_ANSWER = 3     # ответ
EVENT_TRANSFER = 4   # промежуточное завершение при переводе с консультацией

DIR_IN = "in"
DIR_OUT = "out"
DIR_INNER = "inner"

PHONE_CONNECTED = "connected"   # по номеру уже был ОТВЕЧЕННЫЙ звонок
PHONE_ACTIVE = "active"         # идёт звонок, ответа ещё нет и звонок не закончился
PHONE_MISSED = "missed"         # был звонок, и он закончился без ответа
PHONE_UNKNOWN = "unknown"       # событий по номеру нет

# Ограничения памяти. Сутки - потолок жизни записи «пропущенный, ждём перезвона»: так же
# живёт ожидание у UIS (uis_missed_call._TTL_SECONDS).
CALL_TTL_S = 3 * 3600
CALL_MAX = 3000
MISSED_TTL_S = 24 * 3600
MISSED_MAX = 3000
ALERTED_MAX = 5000
# Нога без события 2 дольше этого времени считается закончившейся (потеряли событие).
# Звонок без ответа звонит минуту, не пять, поэтому ждать дольше нет смысла.
STUCK_LEG_S = 300.0
# Сколько статусов ног держим в состоянии (для журнала).
_STATUS_KEEP = 8
# Сколько ключей событий держим на звонок для отсева повторов доставки.
_SEEN_PER_CALL = 64


# ---------------------------------------------------------------------------
# Телефон и разбор события
# ---------------------------------------------------------------------------


def normalize_phone(raw: Any) -> str:
    """Внешний номер к виду +7XXXXXXXXXX. Пусто, если на номер не похоже.

    Понимает 8XXXXXXXXXX, 7XXXXXXXXXX, +7XXXXXXXXXX, 10 цифр без кода страны и любые
    знаки-разделители. Зарубежные номера (11-15 цифр) отдаёт с плюсом без изменений.
    Внутренние логины Sipuni (вид 012345101, 9 цифр) и короткие номера дают пустую строку.
    Только примитивы из shared.phones: отдельной «общей нормализации» в репозитории нет.
    """
    digits = ru_8_to_7(digits_only("" if raw is None else str(raw)))
    if len(digits) == 10:
        digits = "7" + digits
    if 11 <= len(digits) <= 15:
        return "+" + digits
    return ""


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return _clean(value[0]) if value else ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def _to_int(value: Any) -> int | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return int(float(text))
    except ValueError:
        return None


@dataclasses.dataclass(frozen=True)
class SipuniEvent:
    """Одно событие АТС после разбора. kind=None - не удалось понять, что это за событие."""

    kind: int | None
    call_id: str
    src_num: str
    dst_num: str
    src_type: int | None
    dst_type: int | None
    src_phone: str          # нормализованный внешний номер, пусто у внутренних и мусора
    dst_phone: str
    timestamp: int | None   # Unix UTC из события
    status: str             # ANSWER, NOANSWER и так далее, в верхнем регистре
    call_start_ts: int | None
    call_answer_ts: int | None
    is_inner: bool
    tree_name: str
    tree_number: str
    transfer_from: str
    has_record: bool

    @property
    def answered(self) -> bool:
        """Событие несёт признак ответа: событие 3, либо 2/4 с ANSWER или временем ответа."""
        if self.kind == EVENT_ANSWER:
            return True
        if self.kind in (EVENT_END, EVENT_TRANSFER):
            return self.status == "ANSWER" or (self.call_answer_ts or 0) > 0
        return False


def parse_event(raw: Mapping[str, Any] | None) -> SipuniEvent:
    """Сырые параметры события -> SipuniEvent. Никогда не бросает исключение.

    Ключи читаются без учёта регистра, значения могут быть строками (query, форма) и
    числами (JSON). Незнакомый или пропавший event даёт kind=None.
    """
    fields: dict[str, Any] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            fields[str(key).strip().lower()] = value

    def get(*names: str) -> Any:
        for name in names:
            if name in fields:
                return fields[name]
        return None

    kind = _to_int(get("event"))
    if kind not in (EVENT_START, EVENT_END, EVENT_ANSWER, EVENT_TRANSFER):
        kind = None
    src_num = _clean(get("src_num"))
    dst_num = _clean(get("dst_num"))
    src_type = _to_int(get("src_type"))
    dst_type = _to_int(get("dst_type"))
    is_inner_raw = _clean(get("is_inner_call")).lower()
    return SipuniEvent(
        kind=kind,
        call_id=_clean(get("call_id")),
        src_num=src_num,
        dst_num=dst_num,
        src_type=src_type,
        dst_type=dst_type,
        # Внутренний логин вида 0123451010 при десяти цифрах выглядел бы как телефон:
        # номер у внутренней стороны не нормализуем вовсе.
        src_phone="" if src_type == 2 else normalize_phone(src_num),
        dst_phone="" if dst_type == 2 else normalize_phone(dst_num),
        timestamp=_to_int(get("timestamp")),
        status=_clean(get("status")).upper(),
        call_start_ts=_to_int(get("call_start_timestamp")),
        call_answer_ts=_to_int(get("call_answer_timestamp")),
        is_inner=is_inner_raw in ("1", "true", "yes"),
        tree_name=_clean(get("treename", "tree_name")),
        tree_number=_clean(get("treenumber", "tree_number")),
        transfer_from=_clean(get("transfer_from")),
        has_record=bool(_clean(get("call_record_link"))),
    )


_WRAP_KEYS = ("data", "payload", "params", "body", "message", "events", "event")


def _has_event_keys(obj: Mapping[Any, Any]) -> bool:
    keys = {str(k).strip().lower() for k in obj}
    if "call_id" in keys:
        return True
    return "event" in keys and not isinstance(obj.get("event"), (dict, list))


def _as_dicts(obj: Any, depth: int = 0) -> list[dict]:
    """Всё, что похоже на события, из JSON-тела: словарь, список словарей, обёртка."""
    if depth > 3:
        return []
    if isinstance(obj, (list, tuple)):
        out: list[dict] = []
        for item in obj:
            out.extend(_as_dicts(item, depth + 1))
        return out
    if isinstance(obj, Mapping):
        if _has_event_keys(obj):
            return [dict(obj)]
        for key in _WRAP_KEYS:
            for name in (key, key.upper(), key.capitalize()):
                if name in obj and isinstance(obj[name], (dict, list, tuple)):
                    found = _as_dicts(obj[name], depth + 1)
                    if found:
                        return found
        return [dict(obj)] if obj else []
    return []


def collect_payload(
    query: Mapping[str, Any] | None,
    body: bytes | str | None = None,
) -> list[dict]:
    """Параметры запроса и тело (JSON или форма) -> список сырых событий.

    ПРЕДПОЛОЖЕНИЕ: формат тела POST в описании Sipuni не указан, поэтому принимаем все три
    вида: параметры в адресе (GET), форму application/x-www-form-urlencoded и JSON, одно
    событие или список. Тело выигрывает у адреса. Не разобралось ничего - пустой список.
    """
    q = {str(k): v for k, v in (query or {}).items()}
    text = ""
    if isinstance(body, (bytes, bytearray)):
        text = bytes(body).decode("utf-8", "replace")
    elif isinstance(body, str):
        text = body
    text = text.strip().lstrip("﻿")

    parsed: Any = None
    if text:
        if text[0] in "{[":
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
        if parsed is None:
            pairs = parse_qsl(text, keep_blank_values=True)
            if pairs:
                parsed = {key: value for key, value in pairs}

    events = _as_dicts(parsed)
    if not events:
        return [q] if q else []
    return [{**q, **ev} for ev in events]


def classify_start(ev: SipuniEvent) -> str:
    """Направление звонка по событию 1: DIR_IN, DIR_OUT, DIR_INNER или пустая строка.

    Пустая строка - нога, по которой направление не определить: внешний на внешний
    (переадресация на мобильный: это нога входящего звонка, но сама по себе она не
    говорит, что звонок входящий) или события без типов и без разборчивых номеров.
    """
    if ev.is_inner or (ev.src_type == 2 and ev.dst_type == 2):
        return DIR_INNER
    if ev.src_type == 1 and ev.dst_type in (2, None):
        return DIR_IN
    if ev.src_type == 2 and ev.dst_type == 1:
        return DIR_OUT
    if ev.src_type is None and ev.dst_type is None:
        # ПРЕДПОЛОЖЕНИЕ: типов в событии может не быть - тогда судим по номерам.
        if ev.src_phone and not ev.dst_phone:
            return DIR_IN
        if ev.dst_phone and not ev.src_phone:
            return DIR_OUT
    return ""


# ---------------------------------------------------------------------------
# Состояние звонка и решение «пропущенный»
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class CallState:
    call_id: str
    first_wall: float
    last_wall: float
    direction: str = ""
    client_phone: str = ""
    tree_name: str = ""
    tree_number: str = ""
    first_ts: int | None = None
    last_ts: int | None = None
    starts: int = 0          # событий 1: сколько ног открыли
    ends: int = 0            # событий 2 и 4: сколько ног закрыли
    answered: bool = False
    answered_wall: float | None = None
    finalized: bool = False  # звонок считаем законченным, решение принято
    missed: bool = False
    statuses: collections.deque = dataclasses.field(
        default_factory=lambda: collections.deque(maxlen=_STATUS_KEEP))
    seen: collections.OrderedDict = dataclasses.field(default_factory=collections.OrderedDict)
    timer: Any = None
    token: int = 0

    @property
    def open_legs(self) -> int:
        return max(0, self.starts - self.ends)


@dataclasses.dataclass(frozen=True)
class MissedCall:
    """Итог по звонку, который тихое окно признало пропущенным."""

    call_id: str
    phone: str            # нормализованный +7..., пусто у скрытого номера
    tree_name: str
    tree_number: str
    first_ts: int | None
    last_ts: int | None
    legs: int
    statuses: tuple


class SipuniCallTracker:
    """Учёт звонков по call_id. Всё в памяти, с TTL и потолком размера.

    `on_missed` - функция (обычная или async), её зовут один раз на пропущенный звонок.
    `clock` - часы стены, подменяются в тестах. Тихое окно считает цикл asyncio, не clock.
    Без запущенного цикла событий таймеры не взводятся, и решение принимает sweep().
    """

    def __init__(
        self, *, quiet_s: float | None = None, ttl_s: float = CALL_TTL_S,
        max_calls: int = CALL_MAX, stuck_s: float = STUCK_LEG_S,
        clock: Callable[[], float] = time.time,
        on_missed: Callable[[MissedCall], Any] | None = None,
    ) -> None:
        self._quiet_override = quiet_s
        self.ttl_s = ttl_s
        self.max_calls = max_calls
        self.stuck_s = stuck_s
        self.clock = clock
        self.on_missed = on_missed
        self._calls: collections.OrderedDict[str, CallState] = collections.OrderedDict()
        self._alerted: collections.OrderedDict[str, None] = collections.OrderedDict()
        self._missed: collections.OrderedDict[str, dict] = collections.OrderedDict()
        self._bg: set = set()

    # -- настройки -------------------------------------------------------

    def quiet_s(self) -> float:
        value = self._quiet_override if self._quiet_override is not None else SIPUNI_MISSED_QUIET_S
        return max(0.0, float(value))

    def reset(self) -> None:
        """Забыть всё и снять таймеры. Для тестов и остановки."""
        for st in self._calls.values():
            self._cancel_timer(st)
        self._calls.clear()
        self._alerted.clear()
        self._missed.clear()

    # -- приём события ---------------------------------------------------

    def ingest(self, ev: SipuniEvent) -> tuple[CallState | None, bool]:
        """Учесть событие. Возвращает (состояние звонка, повтор ли это).

        (None, False) - событие не по делу: нет call_id или не распознан тип.
        (состояние, True) - повтор уже учтённого события, счётчики не тронуты.
        """
        if ev.kind is None or not ev.call_id:
            return None, False
        now = self.clock()
        self._gc(now)
        st = self._calls.get(ev.call_id)
        if st is None:
            st = CallState(call_id=ev.call_id, first_wall=now, last_wall=now)
            self._calls[ev.call_id] = st

        key = (ev.kind, ev.src_num, ev.dst_num, ev.timestamp, ev.status,
               ev.call_start_ts, ev.call_answer_ts)
        if key in st.seen:
            return st, True  # повтор доставки: второй раз считать нельзя, ноги разъедутся
        st.seen[key] = None
        if len(st.seen) > _SEEN_PER_CALL:
            st.seen.popitem(last=False)

        st.last_wall = now
        self._calls.move_to_end(ev.call_id)
        if ev.timestamp:
            st.first_ts = ev.timestamp if st.first_ts is None else min(st.first_ts, ev.timestamp)
            st.last_ts = ev.timestamp if st.last_ts is None else max(st.last_ts, ev.timestamp)
        if ev.tree_name and not st.tree_name:
            st.tree_name = ev.tree_name
        if ev.tree_number and not st.tree_number:
            st.tree_number = ev.tree_number
        if ev.status:
            st.statuses.append(ev.status)

        if ev.kind == EVENT_START:
            st.starts += 1
            direction = classify_start(ev)
            if direction and not st.direction:
                st.direction = direction
        elif ev.kind in (EVENT_END, EVENT_TRANSFER):
            st.ends += 1

        # Клиент: у входящего это звонящий, у исходящего - тот, кому звонят.
        if not st.client_phone:
            if st.direction == DIR_IN and ev.src_type != 2 and ev.src_phone:
                st.client_phone = ev.src_phone
            elif st.direction == DIR_OUT and ev.dst_type != 2 and ev.dst_phone:
                st.client_phone = ev.dst_phone

        if ev.answered and not st.answered:
            st.answered = True
            st.answered_wall = now
            if st.finalized and st.missed:
                logger.warning(
                    "Sipuni: ответ ПОСЛЕ решения «пропущенный» (call=%s) - тихое окно %.0f с "
                    "может быть коротким", st.call_id, self.quiet_s())
        self._after_update(st, now)
        if not st.finalized:
            self._arm(st)
        self._gc(now)  # потолок размера держим и ПОСЛЕ добавления: новый звонок тоже занимает место
        return st, False

    # -- решение ---------------------------------------------------------

    def _arm(self, st: CallState) -> None:
        self._cancel_timer(st)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        need = self.quiet_s() if st.open_legs <= 0 else max(self.stuck_s, self.quiet_s())
        st.token += 1
        st.timer = loop.call_later(need, self._timer_fired, st.call_id, st.token)

    @staticmethod
    def _cancel_timer(st: CallState) -> None:
        if st.timer is not None:
            st.timer.cancel()
            st.timer = None

    def _timer_fired(self, call_id: str, token: int) -> None:
        st = self._calls.get(call_id)
        # Токен устарел - после этого таймера пришло новое событие и взвело свой. Время
        # по часам стены здесь НЕ перепроверяем: asyncio вправе сработать на долю
        # разрешения часов раньше срока, и повторная проверка потеряла бы алерт молча.
        if st is None or st.token != token:
            return
        st.timer = None
        self._finalize(st, self.clock())

    def sweep(self, now: float | None = None) -> list[MissedCall]:
        """Принять решение по всем звонкам, у которых тихое окно истекло. Нужна без цикла
        событий (тесты) и как страховка. Возвращает пропущенные, по которым решено."""
        now = self.clock() if now is None else now
        out: list[MissedCall] = []
        for st in list(self._calls.values()):
            if st.finalized:
                continue
            need = self.quiet_s() if st.open_legs <= 0 else max(self.stuck_s, self.quiet_s())
            if now - st.last_wall < need:
                continue
            missed = self._finalize(st, now)
            if missed is not None:
                out.append(missed)
        return out

    def _finalize(self, st: CallState, now: float) -> MissedCall | None:
        if st.finalized:
            return None
        self._cancel_timer(st)
        st.finalized = True
        if st.direction != DIR_IN or st.answered:
            return None
        if st.call_id in self._alerted:
            return None  # этот call_id уже давал алерт (повтор после вытеснения состояния)
        st.missed = True
        self._alerted[st.call_id] = None
        while len(self._alerted) > ALERTED_MAX:
            self._alerted.popitem(last=False)
        missed = MissedCall(
            call_id=st.call_id, phone=st.client_phone, tree_name=st.tree_name,
            tree_number=st.tree_number, first_ts=st.first_ts, last_ts=st.last_ts,
            legs=st.starts, statuses=tuple(st.statuses),
        )
        if st.client_phone:
            self._register_missed(st, now)
        self._emit(missed)
        return missed

    def _emit(self, missed: MissedCall) -> None:
        handler = self.on_missed
        if handler is None:
            return
        try:
            result = handler(missed)
            if inspect.isawaitable(result):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    if inspect.iscoroutine(result):
                        result.close()
                    logger.warning("Sipuni: обработчик пропущенного не запущен - нет цикла событий")
                    return
                task = loop.create_task(result) if inspect.iscoroutine(result) \
                    else asyncio.ensure_future(result, loop=loop)
                self._bg.add(task)
                task.add_done_callback(self._bg_done)
        except Exception:  # noqa: BLE001 - сбой обработчика не должен ронять учёт звонков
            logger.exception("Sipuni: ошибка обработчика пропущенного (call=%s)", missed.call_id)

    def _bg_done(self, task: Any) -> None:
        self._bg.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Sipuni: обработчик пропущенного упал (%r)", task.exception())

    # -- «перезвонили» по потоку звонков --------------------------------------

    def _register_missed(self, st: CallState, now: float) -> None:
        phone = st.client_phone
        existing = self._missed.get(phone)
        if existing is not None and existing["called_back_at"] is None:
            return  # по номеру уже ждём перезвона: второй пропущенный счётчик не сдвигает
        record = {"call_id": st.call_id, "missed_at": now, "first_wall": st.first_wall,
                  "called_back_at": None, "called_back_call_id": ""}
        self._missed[phone] = record
        self._missed.move_to_end(phone)
        while len(self._missed) > MISSED_MAX:
            self._missed.popitem(last=False)
        # Отвеченный звонок с этим номером мог случиться ещё ДО решения (клиент тут же
        # перезвонил, пока первая попытка дозванивалась): это уже перезвон.
        for other in self._calls.values():
            if (other.call_id != st.call_id and other.client_phone == phone and other.answered
                    and (other.answered_wall or 0) >= st.first_wall):
                record["called_back_at"] = other.answered_wall
                record["called_back_call_id"] = other.call_id
                break

    def _after_update(self, st: CallState, now: float) -> None:
        """Отвеченный звонок с участием номера P после пропущенного от P - это перезвон."""
        if not (st.answered and st.client_phone):
            return
        record = self._missed.get(st.client_phone)
        if record is None or record["called_back_at"] is not None:
            return
        if record["call_id"] == st.call_id:
            return
        if (st.answered_wall or now) < record["first_wall"]:
            return
        record["called_back_at"] = st.answered_wall or now
        record["called_back_call_id"] = st.call_id

    def missed_record(self, phone: str, now: float | None = None) -> dict | None:
        """Запись о последнем пропущенном от номера: {call_id, missed_at, called_back_at,
        ...}. None - записи нет (не было пропущенного, истёк TTL или рестарт)."""
        key = normalize_phone(phone)
        record = self._missed.get(key) if key else None
        if record is None:
            return None
        now = self.clock() if now is None else now
        if now - record["missed_at"] > MISSED_TTL_S:
            self._missed.pop(key, None)
            return None
        return dict(record)

    # -- состояние для других модулей ----------------------------------------

    def phone_status(self, phone: str, *, since: float | None = None) -> str:
        """connected, active, missed или unknown по звонкам номера.

        since - время стены (Unix): звонки, закончившиеся раньше, не учитываются. Для гейта
        распределения это «не раньше, чем за несколько минут до создания сделки», чтобы
        ответ вчерашнего звонка не считался дозвоном до сегодняшнего.
        """
        key = normalize_phone(phone)
        if not key:
            return PHONE_UNKNOWN
        self._gc(self.clock())
        status = PHONE_UNKNOWN
        for st in reversed(self._calls.values()):
            if st.client_phone != key:
                continue
            if since is not None and st.last_wall < since:
                continue
            if st.answered:
                return PHONE_CONNECTED
            if not st.finalized:
                status = PHONE_ACTIVE
            elif st.missed and status == PHONE_UNKNOWN:
                status = PHONE_MISSED
        return status

    def get_call(self, call_id: str) -> CallState | None:
        return self._calls.get(call_id)

    def __len__(self) -> int:
        return len(self._calls)

    # -- память --------------------------------------------------------------

    def _gc(self, now: float) -> None:
        """Выбросить просроченные звонки и лишние сверх потолка (старейшие первыми)."""
        while self._calls:
            call_id, st = next(iter(self._calls.items()))
            if now - st.last_wall > self.ttl_s or len(self._calls) > self.max_calls:
                self._cancel_timer(st)
                self._calls.pop(call_id, None)
            else:
                break


def log_line(ev: SipuniEvent, st: CallState | None) -> str:
    """Структурная строка журнала по событию: ровно то, что нужно сверить на тестовом звонке.

    Без секрета (его в событии и нет), без ссылки на запись (только факт, что она есть).
    Телефоны пишем как есть: так делает и uis_missed_call.
    """
    if ev.kind is None or not ev.call_id:
        return (f"Sipuni событие: не распознано (event={ev.kind} call={ev.call_id or '-'} "
                f"src={ev.src_num or '-'} dst={ev.dst_num or '-'})")
    legs = f"{st.starts}/{st.ends}" if st is not None else "-"
    return (
        f"Sipuni событие: ev={ev.kind} call={ev.call_id} dir={(st.direction if st else '') or '-'} "
        f"src={ev.src_num or '-'}({ev.src_type if ev.src_type is not None else '-'}) "
        f"dst={ev.dst_num or '-'}({ev.dst_type if ev.dst_type is not None else '-'}) "
        f"status={ev.status or '-'} ans={'1' if ev.answered else '0'} "
        f"inner={'1' if ev.is_inner else '0'} legs={legs} "
        f"tree={ev.tree_number or ev.tree_name or '-'} rec={'1' if ev.has_record else '0'} "
        f"ts={ev.timestamp if ev.timestamp is not None else '-'}"
    )


# Общий экземпляр процесса. Тесты создают свои SipuniCallTracker или зовут tracker.reset().
tracker = SipuniCallTracker()
