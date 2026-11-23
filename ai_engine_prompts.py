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

⚠️ Отказоустойчивость, строго «не шире, чем сейчас»: ни один сбой не расширяет множество.
  • Опрос раз в минуту (`REFRESH_S`) отдельным циклом движка, ответ хранится в памяти; на диск ничего не пишется.
  • Сервис не ответил (сеть, таймаут, не 200, не настроен) или ответ непонятный (не JSON-объект, чужой `contract`, `funnels` не список, у воронки нет
    списка номеров или `has_prompt` не настоящий bool, номера не положительные целые, `test_pipelines` не список номеров): берётся ПОСЛЕДНИЙ УДАЧНЫЙ
    ответ, без срока годности. Ответ принимается целиком или не принимается вовсе: разобранной «половины» не бывает.
  • Удачного ответа ещё не было (старт движка, сервис недоступен): статический запас - две воронки `cfg.MODE_PIPELINES["prod"]` («ОП розница», TangemShop).
    Старый ответ из прошлого процесса не восстанавливается (память, не диск), поэтому после рестарта при лежащем сервисе множество уже, чем было.
  • Удачный ответ с пустым итогом (ни у одной воронки нет промпта) принимается как есть: ИИ нигде не пишет. Это сужение, не расширение.
  • «Тест» (`cfg.MODE_PIPELINES["test"]`) в режиме `test` работает всегда, даже без ответа сервиса.

Срез `GET /` (`ai_engine.status`) показывает `prompt_pipelines`, `prompt_pipelines_source` и `prompt_pipelines_age_s` (`status_fields`).
"""
import asyncio
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
    """Последний удачный ответ сервиса, уже разобранный. Неизменяемый: подменяется целиком одним присваиванием, поэтому читать его можно
    из любого потока без замка."""
    prompt: frozenset[int]          # объединение `pipelines` у funnels с `has_prompt: true`
    test: frozenset[int]            # `test_pipelines` ответа
    at: float                       # когда получен (стенные часы)


_good: Snapshot | None = None       # последний удачный ответ
_last_ok: bool | None = None        # чем кончилась последняя попытка (None - попыток не было)


def _now() -> float:
    return time.time()


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


def parse(data: Any, now: float | None = None) -> Snapshot | None:
    """Ответ `GET /v1/funnels` -> Snapshot либо None, если ответ непонятный (тогда остаётся прежний). Чужой `contract` непонятный;
    нет ключа `contract` допустимо (сверка версии идёт заголовком `X-Contract`, при несовпадении сервис отвечает 426)."""
    if not isinstance(data, dict):
        return None
    contract = data.get("contract")
    if contract is not None and str(contract) != cfg.CONTRACT_VERSION:
        return None
    funnels = data.get("funnels")
    if not isinstance(funnels, list):
        return None
    prompt: set[int] = set()
    for funnel in funnels:
        if not isinstance(funnel, dict):
            return None
        pipelines = _ids(funnel.get("pipelines"))
        has_prompt = funnel.get("has_prompt")
        if pipelines is None or not isinstance(has_prompt, bool):
            return None
        if has_prompt:
            prompt |= pipelines
    test = _ids(data.get("test_pipelines", []))
    if test is None:
        return None
    return Snapshot(prompt=frozenset(prompt), test=test, at=_now() if now is None else now)


# ---------------------------------------------------------------------------------------------
# опрос сервиса
# ---------------------------------------------------------------------------------------------

async def fetch_once() -> bool:
    """Один опрос. True - ответ принят. Любой сбой прежний ответ НЕ трогает (и не расширяет множество): см. шапку."""
    global _good, _last_ok
    now = _now()
    data = await client.get_funnels()
    snap = parse(data, now) if data is not None else None
    if snap is None:
        if _last_ok is not False:
            logger.warning(
                "ai_engine_prompts: сервис не дал словарь воронок (%s), остаётся %s",
                "нет ответа" if data is None else "ответ непонятный",
                "последний удачный" if _good is not None else "статический запас: две воронки розницы",
            )
        _last_ok = False
        return False
    before = _good.prompt if _good is not None else None
    _good, _last_ok = snap, True
    if before != snap.prompt:
        logger.info("ai_engine_prompts: воронки с промптом %s (было %s)", sorted(snap.prompt),
                    "статический запас" if before is None else sorted(before))
    return True


async def poll_loop() -> None:
    """Опрос раз в `REFRESH_S`. Первый сразу при старте: пока его нет, действует статический запас."""
    global _last_ok
    while True:
        try:
            await fetch_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            _last_ok = False                      # упавший разбор не должен выглядеть свежим ответом
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


def source() -> str:
    """`service` - последняя попытка удалась; `cached` - не удалась, держим последний удачный ответ; `static` - удачного ответа не было."""
    if _good is None:
        return SOURCE_STATIC
    return SOURCE_SERVICE if _last_ok else SOURCE_CACHED


def age_s() -> float | None:
    """Возраст действующего ответа сервиса в секундах. Статический запас не имеет возраста: None."""
    snap = _good
    return None if snap is None else max(0.0, _now() - snap.at)


def status_fields(mode: str | None = None) -> dict[str, Any]:
    """Поля среза `GET /`: действующие воронки с промптом (по возрастанию), откуда они (`service` | `cached` | `static`) и возраст данных."""
    age = age_s()
    return {
        "prompt_pipelines": sorted(pipelines_for(mode)),
        "prompt_pipelines_source": source(),
        "prompt_pipelines_age_s": None if age is None else round(age, 1),
    }
