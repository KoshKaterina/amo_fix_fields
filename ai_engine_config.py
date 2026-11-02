"""Конфиг движка для ИИ-менеджера: переменные окружения, константы воронок и тайминги.

Живёт отдельным файлом, а не в `waybill_config.py`, чтобы выкатка нового контура не трогала общий
конфиг: AGENTS.md уже перечисляет флаги, которые читаются вне `waybill_config.py`, это ещё один.
Что включает контур и какие ключи ему нужны - `knowledge`-заметка в `features/ii-agent-avtorezhima/`.

⚠️ Рубильников два, и они независимы:
  • `AI_ENGINE_ENABLED=1` в `.env` на сервере - аварийный. Без него модуль не поднимает ни цикл,
    ни опрос панели, ни базу, а `/internal/ai/v1/*` отвечает 503. По умолчанию ВЫКЛЮЧЕН.
  • Режим `off`/`test`/`prod` из панели (`GET /api/ingest/ai/settings`). Нет ответа панели,
    нет кэша или он протух - считаем `off`.
"""
import os

from waybill_config import (
    PIPELINE_CLEVER_MAIN,
    PIPELINE_TANGEMSHOP,
    RETAIL_GUARD_CHANNELS,
    STATUS_CLOSED_LOST,
    STATUS_PAYMENT_RECEIVED,
    STATUS_SUCCESS,
    STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED,
    TEAM_PANEL_BASE_URL,
)

CONTRACT_VERSION = "2"

ENABLED = os.getenv("AI_ENGINE_ENABLED", "").strip() == "1"

# --- связь с ИИ-сервисом (docker-сеть, публичного адреса нет) -------------------------------
# Движок -> сервис: `X-Service-Token` = AI_SERVICE_TOKEN (на стороне сервиса AIMGR_SERVICE_TOKEN).
SERVICE_URL = os.getenv("AI_SERVICE_URL", "").strip().rstrip("/")
SERVICE_TOKEN = os.getenv("AI_SERVICE_TOKEN", "").strip()
# Сервис -> движок: `X-Service-Token` = AI_ENGINE_TOKEN (на стороне сервиса AIMGR_ENGINE_TOKEN).
# Пусто - ручки `/internal/ai/v1/permit|result` отвечают 503: свой токен на каждое направление.
ENGINE_TOKEN = os.getenv("AI_ENGINE_TOKEN", "").strip()
# Третий токен - для действий ЧЕЛОВЕКА (вернуть диалог ИИ, срез состояния). ИИ-сервису он не
# выдаётся: иначе сервис мог бы сам взять назад сделку, которую у него забрали.
ADMIN_TOKEN = os.getenv("AI_ADMIN_TOKEN", "").strip()

# --- панель ---------------------------------------------------------------------------------
PANEL_URL = os.getenv("AI_PANEL_URL", TEAM_PANEL_BASE_URL).strip().rstrip("/")
PANEL_TOKEN = os.getenv("AI_INGEST_TOKEN", "").strip()
SETTINGS_POLL_INTERVAL_S = int(os.getenv("AI_SETTINGS_POLL_INTERVAL_S", "30"))
# Настройки старше этого - как будто панели нет: режим `off`. Панель может лечь на выкатке, и
# пару минут движок честно работает по последнему слову, но «вчерашний prod» не должен жить вечно.
SETTINGS_STALE_S = int(os.getenv("AI_SETTINGS_STALE_S", "900"))

# --- состояние на диске ---------------------------------------------------------------------
DB_PATH = os.getenv("AI_ENGINE_DB_PATH", "/app/var/ai_engine.sqlite3")
SETTINGS_CACHE_PATH = os.getenv("AI_ENGINE_SETTINGS_PATH", "/app/var/ai_engine_settings.json")

# --- каналы ---------------------------------------------------------------------------------
# Допустимые каналы Wazzup. По умолчанию - клиентские каналы розницы (те же, что у сторожа
# розничных лидов): WhatsApp, два телеграма. Каналы TangemShop добавляются перечислением в
# `AI_ENGINE_CHANNELS`, пока не добавлены, TangemShop ИИ не видит.
CHANNELS = {
    c.strip() for c in os.getenv("AI_ENGINE_CHANNELS", ",".join(sorted(RETAIL_GUARD_CHANNELS))).split(",")
    if c.strip()
}

# --- воронки и режимы -----------------------------------------------------------------------
PIPELINE_TEST = 8642414
# Какие воронки допустимы при каком режиме. Числа продублированы в панели и в ИИ-сервисе, и
# каждая сторона сверяет их с тем, что пришло: расхождение = режим `off`, а не «верим соседу».
MODE_PIPELINES: dict[str, frozenset[int]] = {
    "test": frozenset({PIPELINE_TEST}),
    "prod": frozenset({int(PIPELINE_CLEVER_MAIN), int(PIPELINE_TANGEMSHOP)}),
}
CLOSED_STATUS_IDS = frozenset({int(STATUS_SUCCESS), int(STATUS_CLOSED_LOST)})

# --- служебные события ----------------------------------------------------------------------
# «Оплата получена» в «ОП розница» и в TangemShop (id этапов в amo уникальны на весь аккаунт, воронку сверять не нужно)
PAYMENT_RECEIVED_STATUSES = frozenset({int(STATUS_PAYMENT_RECEIVED), int(STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED)})
FIELD_PAYMENT_STATE = 576669                              # «Статус оплаты»
ENUM_PAYMENT_STATE_PAID = 1040151                         # значение «Оплачен»

# --- тайминги -------------------------------------------------------------------------------
TICK_S = float(os.getenv("AI_ENGINE_TICK_S", "1"))
HEALTH_POLL_S = float(os.getenv("AI_ENGINE_HEALTH_POLL_S", "15"))
RESOLVE_RETRY_S = int(os.getenv("AI_ENGINE_RESOLVE_RETRY_S", "60"))
# Паузы повторов `POST /v1/turn`, как у `wazzup_forward`; дальше ход лежит в очереди `backlog`
# (sqlite на томе `var`) и уходит, когда `/health` снова отвечает.
SEND_RETRY_DELAYS_S = (1, 5, 25)
# Повторы хода после `failed` (контракт): 30 и 120 секунд, потом человек.
FAIL_RETRY_DELAYS_S = (30, 120)
# Сколько ждать `result` по принятому ходу. Таймаут хода у сервиса свой; это страховка движка.
TURN_TIMEOUT_S = int(os.getenv("AI_ENGINE_TURN_TIMEOUT_S", "900"))
# Диалог, переданный человеку без привязки к сделке (правила A и D), через сутки разбирается заново.
HUMAN_CHAT_TTL_S = 24 * 3600
SKIP_TTL_S = 600
RETENTION_DAYS = int(os.getenv("AI_ENGINE_RETENTION_DAYS", "30"))

# --- журнал приёма (таблица `intake`): тело вебхука пишется на диск ДО ответа 200 ---------------
# Потолок тела: Wazzup повторов не шлёт и историю не отдаёт, поэтому всё, что принято, должно лечь на диск, но
# сообщения клиентов короткие. Больше потолка - тело не пишется, откат на очередь в памяти и строка в лог.
INTAKE_MAX_BODY_BYTES = int(os.getenv("AI_ENGINE_INTAKE_MAX_BYTES", str(256 * 1024)))
# Устаревшая строка после рестарта в ход не идёт. Сообщение клиента: первое ожидание 60 с + предел пачки 60 с + поиск
# сделки до 600 с = около 12 минут, столько движок сам готов работать над сообщением; три часа - с большим запасом на
# пересборку и сбои, но ответ ИИ на вопрос трёхчасовой давности клиента скорее сбивает, чем помогает (его давно
# ведёт менеджер или он ушёл). Событие сделки (закрытие, оплата) описывает состояние, а не повод заговорить,
# и остаётся верным дольше: сутки.
INTAKE_STALE_WAZZUP_S = int(os.getenv("AI_ENGINE_INTAKE_STALE_WAZZUP_S", str(3 * 3600)))
INTAKE_STALE_LEAD_S = int(os.getenv("AI_ENGINE_INTAKE_STALE_LEAD_S", str(24 * 3600)))
# Разобранные строки живут сутки (для разбора полётов) и убираются в часовой уборке рядом с `store.prune`.
INTAKE_KEEP_DONE_S = 24 * 3600
# Как часто цикл приёма заглядывает в таблицу сам, даже если его не будили (очередь в памяти была полна).
INTAKE_POLL_S = float(os.getenv("AI_ENGINE_INTAKE_POLL_S", "5"))
# Сразу после старта режим `off` может значить «настройки панели ещё не пришли»: пока не прошло столько секунд, строки
# не сбрасываются как `mode_off`, а ждут. Столько же, самое большее, таймеры ждут окончания первого разбора.
INTAKE_SETTINGS_WAIT_S = float(os.getenv("AI_ENGINE_INTAKE_SETTINGS_WAIT_S", "120"))
# Возврат диалога ИИ реакцией менеджера на алерт: бот ставит RETURN_EMOJI под алертом передачи, тегнутый менеджер
# ставит такую же - ИИ получает диалог обратно (`dialog_returned`). Нужен бот-администратор чата алертов.
# Задача «Связаться с клиентом, срочно» на ответственного при передаче человеку, которую делает движок, и её закрытие при возврате диалога ИИ.
HANDOFF_TASKS = os.getenv("AI_HANDOFF_TASKS", "1").strip() == "1"
# При ПЕРВОМ входе ИИ в сделку закрываются системные задачи приёма лида, которые ИИ делает сам (см. ai_engine_tasks.INTAKE_TASKS).
INTAKE_TASKS_CLOSE = os.getenv("AI_INTAKE_TASKS_CLOSE", "1").strip() == "1"
RETURN_BY_REACTION = os.getenv("AI_RETURN_BY_REACTION", "1").strip() == "1"
RETURN_EMOJI = os.getenv("AI_RETURN_EMOJI", "👍").strip()            # 👍 метка «ждёт возврата» и нужная реакция
def _parse_also(raw: str) -> frozenset:
    return frozenset(x.lstrip("@").lower() for x in raw.split() if x.strip("@"))


# Ники Telegram (через пробел, без @), чья реакция возвращает диалог ИИ ВСЕГДА, даже если человека нет среди тегнутых в алерте
# (решение Кати 29.10.2026: её аккаунт может возвращать ИИ). Тегнутые в алерте менеджеры могут и без этого списка.
RETURN_ALSO = _parse_also(os.getenv("AI_RETURN_ALSO", ""))
RETURNED_EMOJI = os.getenv("AI_RETURNED_EMOJI", "👌").strip()        # 👌 бот меняет метку, когда ИИ вернулся
