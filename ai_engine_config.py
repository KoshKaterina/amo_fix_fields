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
import json
import os

from waybill_config import (
    PIPELINE_CLEVER_MAIN,
    PIPELINE_DB_WORK,
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
# Четвёртый токен - узкий, для кнопки «Дать сделку ИИ» в панели Team (16.11.2026). Он открывает ТОЛЬКО `/internal/ai/v1/give_deal`:
# админский токен в панель не кладём, а панель должна уметь отдавать сделку. Пусто: ручка по этому токену отвечает 503.
GIVE_TOKEN = os.getenv("AI_GIVE_TOKEN", "").strip()

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
# Воронки, где ИИ вправе проявлять ИНИЦИАТИВУ (лестница тишины, окна УР, отложенные касания, напоминание), когда панель не прислала
# `initiative_pipelines`: «ОП розница», TangemShop и «Работа с базой» 11166334 (решение 23.11.2026: у неё инициатива как у розницы; множество задано явно, а не
# через `MODE_PIPELINES`, которое остаётся двумя розничными воронками). В остальных воронках при `pipelines_any` ИИ только отвечает. ⚠️ Инициатива в «Работе с базой»
# действует, только если ИИ вообще вправе вести сделки этой воронки: `pipeline_allowed` (`AI_PIPELINES_ANY_ALLOWED=1`, `pipelines_any` панели и промпт воронки).
INITIATIVE_PIPELINES_DEFAULT: frozenset[int] = frozenset({int(PIPELINE_CLEVER_MAIN), int(PIPELINE_TANGEMSHOP), int(PIPELINE_DB_WORK)})
# Пул 1 («розница») подбора существующих сделок (`ai_engine_sweep`): ровно эти две воронки, НЕ `initiative_pipelines`: иначе «Работа с базой» с её старыми сделками тихо попала бы
# в пул, где берут и молчащих клиентов. «Работа с базой» в подборе это пул 2 (только ответ на неотвеченное). Переопределяется `existing.sweep.retail_pipelines` панели.
SWEEP_RETAIL_PIPELINES: frozenset[int] = frozenset({int(PIPELINE_CLEVER_MAIN), int(PIPELINE_TANGEMSHOP)})
# «Любая воронка» (`pipelines_any` документа панели) включается ТОЛЬКО здесь: `AI_PIPELINES_ANY_ALLOWED=1` в .env движка (по умолчанию не задан) и
# перезапуск контейнера. Пока переменная не равна «1», `pipelines_any` из документа игнорируется и движок ведёт себя как при документе без него (две
# воронки режима, поведение этапа 1): ИИ-сервис до своего выката отвечает `mode_mismatch` на воронки вне двух розничных, и включённый `pipelines_any`
# породил бы передачу человеку и алерт на каждый ход. Включать вместе с выкатом сервиса, который принимает чужие воронки.
PIPELINES_ANY_ALLOWED = os.getenv("AI_PIPELINES_ANY_ALLOWED", "").strip() == "1"
CLOSED_STATUS_IDS = frozenset({int(STATUS_SUCCESS), int(STATUS_CLOSED_LOST)})
# Подбор существующих сделок (`ai_engine_sweep`, режим «Полный», решение Кати 23.11.2026) включает панель (`existing.sweep.enabled`). Аварийный рубильник движка:
# `AI_SWEEP_DISABLED=1` в .env (по умолчанию не задан) и перезапуск контейнера: подбор не запускается, что бы ни прислала панель.
SWEEP_DISABLED = os.getenv("AI_SWEEP_DISABLED", "").strip() == "1"
# Стоп и старт ИИ примечанием «стоп»/«старт», закреплённая заметка «ИИ ведёт сделку» (`ai_engine_pause`, решение Кати 24.11.2026). Включено по умолчанию;
# рубильник `AI_PAUSE_NOTES=0` в .env и перезапуск контейнера: ни опроса примечаний, ни заметок. `AI_PAUSE_POLL_S`: как часто читать новые примечания.
PAUSE_NOTES_ENABLED = os.getenv("AI_PAUSE_NOTES", "1").strip() != "0"
PAUSE_POLL_S = int(os.getenv("AI_PAUSE_POLL_S", "15"))

# Режимы работы ИИ-менеджера (план 20.11.2026, этап 1): один активный `mode_id`. Старый ключ `mode` остаётся с прежним смыслом
# (`off`/`test`/`prod`): это то, чему воронки и движок верили до режимов. Словарь ниже сводит новый id к старому ключу: документ, в
# котором они не согласуются, считается ошибкой панели, и ИИ выключается (как при несовпадении воронок).
MODE_IDS = ("test", "full", "help", "intercept")
MODE_ID_TO_LEGACY = {"test": "test", "full": "prod", "help": "prod", "intercept": "prod"}
LEGACY_TO_MODE_ID = {"off": "off", "test": "test", "prod": "full"}

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
# Возврат диалога ИИ реакцией менеджера на алерт: бот ставит RETURN_EMOJI под алертом передачи, тегнутый менеджер
# ставит такую же - ИИ получает диалог обратно (`dialog_returned`). Нужен бот-администратор чата алертов.
# Задача «Связаться с клиентом, срочно» на ответственного при передаче человеку, которую делает движок, и её закрытие при возврате диалога ИИ.
HANDOFF_TASKS = os.getenv("AI_HANDOFF_TASKS", "1").strip() == "1"
# При ПЕРВОМ входе ИИ в сделку закрываются системные задачи приёма лида, которые ИИ делает сам (см. ai_engine_tasks.INTAKE_TASKS).
INTAKE_TASKS_CLOSE = os.getenv("AI_INTAKE_TASKS_CLOSE", "1").strip() == "1"
# Всегда пингуется в каждом алерте ИИ-менеджера, помимо смены (решение Кати 12.11.2026); пусто - без добавочного пинга.
ALERT_EXTRA_TAG = os.getenv("AI_ALERT_EXTRA_TAG", "@koshkatarina").strip()
# Лестница тишины (решение Кати 12.11.2026): через SILENCE_ALT_S после первого сообщения ИИ без ответа ИИ пробует другой канал,
# ещё через SILENCE_ALERT_S алерт менеджерам.
SILENCE_ALT_S = int(os.getenv("AI_SILENCE_ALT_S", "3600"))
SILENCE_ALERT_S = int(os.getenv("AI_SILENCE_ALERT_S", "3600"))
# Окна перевода в УР (решение Кати 12.11.2026), время МСК: первое окно, второе окно и граница «последнее сообщение ИИ было поздно»:
# кому ИИ писал не раньше границы, те идут во второе окно.
UR_WAVE_1 = os.getenv("AI_UR_WAVE_1", "17:50").strip()
UR_WAVE_2 = os.getenv("AI_UR_WAVE_2", "18:45").strip()
# Первое окно на отдельные даты (JSON {"ГГГГ-ММ-ДД": "ЧЧ:ММ"}): 12.11.2026 Катя велела провести его в 16:50. Дата прошла, и правило перестаёт действовать само.
try:
    UR_WAVE_1_BY_DAY = {str(k): str(v) for k, v in json.loads(os.getenv("AI_UR_WAVE_1_BY_DAY", '{"2026-11-12": "16:50"}')).items()}
except ValueError:
    UR_WAVE_1_BY_DAY = {}
UR_LATE_FROM = os.getenv("AI_UR_LATE_FROM", "17:00").strip()
RETURN_BY_REACTION = os.getenv("AI_RETURN_BY_REACTION", "1").strip() == "1"
RETURN_EMOJI = os.getenv("AI_RETURN_EMOJI", "👍").strip()            # 👍 метка «ждёт возврата» и нужная реакция
def _parse_also(raw: str) -> frozenset:
    return frozenset(x.lstrip("@").lower() for x in raw.split() if x.strip("@"))


# Ники Telegram (через пробел, без @), чья реакция возвращает диалог ИИ ВСЕГДА, даже если человека нет среди тегнутых в алерте
# (решение Кати 29.10.2026: её аккаунт может возвращать ИИ). Тегнутые в алерте менеджеры могут и без этого списка.
RETURN_ALSO = _parse_also(os.getenv("AI_RETURN_ALSO", ""))
RETURNED_EMOJI = os.getenv("AI_RETURNED_EMOJI", "👌").strip()        # 👌 бот меняет метку, когда ИИ вернулся
