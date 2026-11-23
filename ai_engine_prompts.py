"""Словарь «воронка -> промпт» ИИ-сервиса: ИИ ведёт сделку, только если для её воронки в сервисе есть промпт (решение Кати 23.11.2026).

Правило «нет промпта: не пишем» не зависит от настроек режима. Режим (`pipelines_any`, воронки режима) говорит, где ИИ МОЖНО работать, а
этот модуль говорит, где ему ЕСТЬ ЧЕМ работать: итог любого решения о воронке это «разрешено режимом И воронка есть в словаре с промптом»
(`ai_engine_settings.pipeline_allowed`, единственное место). Даже при `pipelines_any: true` сделка воронки без промпта не берётся, а уже
взятая и перенесённая в такую воронку снимается как `left_pipeline`.

Источник истины - ИИ-сервис: `GET <AI_SERVICE_URL>/v1/funnels` (те же заголовки `X-Service-Token` и `X-Contract: 2`, что у `POST /v1/turn`).
Форма ответа (контракт с сервисом, не менять):

    {"contract": "2", "generated_at": "<ISO UTC>",
     "funnels": [{"slug": "sunscrypt", "title": "...", "pipelines": [<id воронок amo>], "has_prompt": true, "prompt_source": "var:..."|"image:..."|"fallback"}, ...],
     "test_pipelines": [<id воронки «Тест»>]}

Разрешённые воронки = объединение `pipelines` у funnels с `has_prompt: true`. Воронки `test_pipelines` (и «Тест» движка) считаются только когда
активный режим `test`: в Полном, Помощи и Перехвате воронка «Тест» из словаря вычитается.

⚠️ Отказоустойчивость, строго «не шире, чем сейчас»: ни один сбой не расширяет множество, и ни один сбой сервиса не снимает сделки.
  • Опрос раз в минуту (`REFRESH_S`) отдельным циклом движка, ответ хранится в памяти; на диск ничего не пишется.
  • Сервис не ответил (сеть, таймаут, не 200, не настроен) или ответ непонятный (не JSON-объект, чужой `contract`, `funnels` не список, у воронки нет
    списка номеров или `has_prompt` не настоящий bool, номера не положительные целые, `test_pipelines` не список номеров): действует то, что действовало
    (последний применённый ответ, без срока годности). Ответ принимается целиком или не принимается вовсе: разобранной «половины» не бывает.
  • ПУСТОЙ ИТОГ тоже непонятный ответ (решение 23.11.2026): если из ответа получается пустое множество воронок (`funnels` пуст или ни у одной воронки
    нет `has_prompt: true`, либо остались одни тестовые), он отбрасывается как сбой сервиса, как при его недоступности: действует прежнее множество (или
    статический запас), в лог предупреждение, в срезе `cached` или `static`. Остановить ИИ можно режимом «Выключено», а не сбоем сервиса.
  • Удачного ответа ещё не было (старт движка, сервис недоступен): статический запас - две воронки `cfg.MODE_PIPELINES["prod"]` («ОП розница», TangemShop).
    Старый ответ из прошлого процесса не восстанавливается (память, не диск), поэтому после рестарта при лежащем сервисе множество уже, чем было.
  • СУЖЕНИЕ С ЗАДЕРЖКОЙ (решение 23.11.2026): удачный ответ, в котором какая-то воронка ПРОПАЛА по сравнению с действующим множеством (или
    запасом, если удачного ответа не было), применяется только после того, как ДВА УДАЧНЫХ ОПРОСА ПОДРЯД вернули то же суженное множество (`_pending`).
    Первый такой ответ только запоминается: сделки по-прежнему ведутся, в срезе `pending_narrowing` стоят пропавшие воронки. Что обнуляет ожидание: ответ,
    который не повторил суженное множество (в том числе вернувший прежнее), и любой неудачный или отброшенный опрос между двумя ответами. РАСШИРЕНИЕ
    (воронка с промптом появилась) применяется сразу; в смешанном ответе (одна пропала, другая появилась) появившаяся применяется сразу, а пропавшая
    ждёт подтверждения. Цель: промпт на проде кладёт скрипт `funnel_prompt.py push`, и файл на миг может отсутствовать; секундный сбой не должен снимать сделки
    с ИИ. Опрос раз в 60 с, поэтому от пропажи промпта до снятия проходит не больше двух минут.
  • «Тест» (`cfg.MODE_PIPELINES["test"]`) в режиме `test` работает всегда, даже без ответа сервиса.

Срез `GET /` (`ai_engine.status`) показывает `prompt_pipelines`, `prompt_pipelines_source`, `prompt_pipelines_age_s` и `pending_narrowing` (`status_fields`).
"""
import asyncio
import dataclasses
import logging
import time
from dataclasses import dataclass
from typing import Any

import ai_engine_client as client
import ai_engine_config as cfg

logger = logging.getLogger("uvicorn")

REFRESH_S = 60                                  # не чаще раза в минуту на любой исход (удачный или нет)
SOURCE_SERVICE, SOURCE_CACHED, SOURCE_STATIC = "service", "cached", "static"


@dataclass(frozen=True)
class Snapshot:
    """Применённый ответ сервиса, уже разобранный. Неизменяемый: подменяется целиком одним присваиванием, поэтому читать его можно
    из любого потока без замка."""
    prompt: frozenset[int]          # объединение `pipelines` у funnels с `has_prompt: true`
    test: frozenset[int]            # `test_pipelines` ответа
    at: float                       # когда получен (стенные часы)


_good: Snapshot | None = None       # действующий ответ сервиса (None - ещё не было, действует статический запас)
_pending: Snapshot | None = None    # суженный ответ, который ждёт второго такого же опроса подряд
_last_ok: bool | None = None        # чем кончилась последняя попытка (None - попыток не было)
_last_why = ""                      # почему отброшена последняя неудачная попытка (чтобы не повторять одно и то же предупреждение каждую минуту)


def _now() -> float:
    return time.time()


def _base(prompt: frozenset[int], test: frozenset[int]) -> frozenset[int]:
    """Воронки с промптом без тестовых: то, что действует во всех режимах кроме `test`."""
    return prompt - test - cfg.MODE_PIPELINES["test"]


def _sets(snap: "Snapshot") -> tuple[frozenset[int], frozenset[int]]:
    """Что в ответе значит для решений: воронки с промптом (без тестовых) и тестовые воронки СВЕРХ статической «Теста» (она в режиме `test` всегда есть)."""
    return _base(snap.prompt, snap.test), snap.test - cfg.MODE_PIPELINES["test"]


# ---------------------------------------------------------------------------------------------
# разбор ответа
# ---------------------------------------------------------------------------------------------

def _ids(raw: Any) -> frozenset[int] | None:
    """Список номеров воронок. Любой элемент, кроме положительного целого (bool не считается числом), делает весь список непонятным: None."""
    if not isinstance(raw, list):
        return None
    out: set[int] = set()
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            return None
        out.add(item)
    return frozenset(out)


def _parse(data: Any, now: float | None = None) -> tuple[Snapshot | None, str]:
    """Ответ `GET /v1/funnels` -> (Snapshot, "") либо (None, причина), если ответ непонятный. Чужой `contract` непонятный; нет ключа `contract`
    допустимо (сверка версии идёт заголовком `X-Contract`, при несовпадении сервис отвечает 426). Пустой итог (ни одной воронки с промптом) непонятный."""
    if not isinstance(data, dict):
        return None, "ответ не JSON-объект"
    contract = data.get("contract")
    if contract is not None and str(contract) != cfg.CONTRACT_VERSION:
        return None, f"чужой contract {contract!r}"
    funnels = data.get("funnels")
    if not isinstance(funnels, list):
        return None, "funnels не список"
    prompt: set[int] = set()
    for funnel in funnels:
        if not isinstance(funnel, dict):
            return None, "воронка словаря не объект"
        pipelines = _ids(funnel.get("pipelines"))
        has_prompt = funnel.get("has_prompt")
        if pipelines is None or not isinstance(has_prompt, bool):
            return None, "у воронки словаря нет списка номеров или has_prompt не bool"
        if has_prompt:
            prompt |= pipelines
    test = _ids(data.get("test_pipelines", []))
    if test is None:
        return None, "test_pipelines не список номеров"
    snap = Snapshot(prompt=frozenset(prompt), test=test, at=_now() if now is None else now)
    if not _base(snap.prompt, snap.test):
        return None, "ни у одной воронки нет промпта (пустой итог)"
    return snap, ""


def parse(data: Any, now: float | None = None) -> Snapshot | None:
    return _parse(data, now)[0]


# ---------------------------------------------------------------------------------------------
# опрос сервиса
# ---------------------------------------------------------------------------------------------

def _effective() -> Snapshot:
    """Что действует сейчас: применённый ответ или статический запас (запас как ответ без тестовых воронок)."""
    snap = _good
    return snap if snap is not None else Snapshot(prompt=static_pipelines(), test=frozenset(), at=_now())


async def fetch_once() -> bool:
    """Один опрос. True - ответ принят (применён сразу или запомнен до подтверждения). Любой сбой действующее множество НЕ трогает: см. шапку."""
    global _good, _pending, _last_ok, _last_why
    now = _now()
    data = await client.get_funnels()
    snap, why = _parse(data, now) if data is not None else (None, "нет ответа")
    if snap is None:
        _pending = None                       # подтверждению нужны ДВА удачных опроса подряд: неудачный между ними обнуляет ожидание
        if _last_ok is not False or why != _last_why:
            logger.warning(
                "ai_engine_prompts: словарь воронок от сервиса не принят (%s), действует %s",
                why, "прежний ответ" if _good is not None else "статический запас: две воронки розницы",
            )
        _last_ok, _last_why = False, why
        return False
    _last_ok, _last_why = True, ""
    cur = _effective()
    (cur_base, cur_test), (new_base, new_test) = _sets(cur), _sets(snap)
    removed = (cur_base - new_base) | (cur_test - new_test)
    if not removed:
        # то же самое или расширение: применяется сразу
        if _pending is not None:
            logger.info("ai_engine_prompts: сужение не подтвердилось, воронки на месте")
        _pending = None
        was_reserve = _good is None
        _good = snap
        if cur_base != new_base:
            logger.info("ai_engine_prompts: воронки с промптом %s (было %s)", sorted(new_base),
                        "статический запас" if was_reserve else sorted(cur_base))
        return True
    if _pending is not None and _sets(_pending) == (new_base, new_test):
        _pending = None
        _good = snap
        logger.warning("ai_engine_prompts: сужение подтверждено вторым опросом, пропали воронки %s, теперь %s", sorted(removed), sorted(new_base))
        return True
    _pending = snap
    if (new_base - cur_base) or (new_test - cur_test):
        _good = Snapshot(prompt=cur.prompt | snap.prompt, test=cur.test | snap.test, at=now)       # появившееся применяется сразу, пропавшее ждёт
    elif _good is not None:
        _good = dataclasses.replace(_good, at=now)                                                   # ответ свежий, воронки прежние
    logger.warning("ai_engine_prompts: в ответе сервиса пропали воронки %s; применю, если следующий опрос вернёт то же (сейчас действуют %s)",
                   sorted(removed), sorted(_sets(_effective())[0]))
    return True


async def poll_loop() -> None:
    """Опрос раз в `REFRESH_S`. Первый сразу при старте: пока его нет, действует статический запас."""
    global _last_ok, _pending
    while True:
        try:
            await fetch_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            _last_ok, _pending = False, None      # упавший разбор не должен выглядеть свежим ответом и не может подтверждать сужение
            logger.exception("ai_engine_prompts: ошибка цикла опроса")
        await asyncio.sleep(REFRESH_S)


# ---------------------------------------------------------------------------------------------
# что можно спрашивать
# ---------------------------------------------------------------------------------------------

def static_pipelines() -> frozenset[int]:
    """Запас, пока удачного ответа не было: воронки, которые движок знает как `prod` (на них сегодня два промпта, sunscrypt и tangem)."""
    return cfg.MODE_PIPELINES["prod"]


def pipelines_for(mode: str | None = None) -> frozenset[int]:
    """Воронки, на которые есть промпт, для режима `mode` (`off` | `test` | `prod`; None - как не `test`).
    Не `test`: объединение `pipelines` воронок с промптом БЕЗ тестовых (`test_pipelines` и «Тест» движка). `test`: то же плюс «Тест» движка и
    `test_pipelines` ответа. Нет удачного ответа: статический запас (в `test` плюс «Тест»)."""
    snap = _good
    if snap is None:
        base = static_pipelines()
        test = cfg.MODE_PIPELINES["test"]
    else:
        test = snap.test | cfg.MODE_PIPELINES["test"]
        base = snap.prompt - test
    return base | test if mode == "test" else base


def has_prompt(pipeline_id: int, mode: str | None = None) -> bool:
    return pipeline_id in pipelines_for(mode)


def pending_narrowing() -> list[int]:
    """Воронки, которые пропали в последнем ответе сервиса и ждут второго такого же опроса, чтобы перестать действовать (по возрастанию; пусто, если ждать нечего)."""
    pend = _pending
    if pend is None:
        return []
    (cur_base, cur_test), (pend_base, pend_test) = _sets(_effective()), _sets(pend)
    return sorted((cur_base - pend_base) | (cur_test - pend_test))


def source() -> str:
    """`service` - последняя попытка удалась; `cached` - не удалась (или ответ отброшен), держим последний удачный ответ; `static` - удачного ответа не было."""
    if _good is None:
        return SOURCE_STATIC
    return SOURCE_SERVICE if _last_ok else SOURCE_CACHED


def age_s() -> float | None:
    """Возраст действующего ответа сервиса в секундах. Статический запас не имеет возраста: None."""
    snap = _good
    return None if snap is None else max(0.0, _now() - snap.at)


def status_fields(mode: str | None = None) -> dict[str, Any]:
    """Поля среза `GET /`: действующие воронки с промптом (по возрастанию), откуда они (`service` | `cached` | `static`), возраст данных и
    воронки, пропавшие из словаря и ждущие подтверждения вторым опросом."""
    age = age_s()
    return {
        "prompt_pipelines": sorted(pipelines_for(mode)),
        "prompt_pipelines_source": source(),
        "prompt_pipelines_age_s": None if age is None else round(age, 1),
        "pending_narrowing": pending_narrowing(),
    }
