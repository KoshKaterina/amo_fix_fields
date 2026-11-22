"""Режим у сделки: ИИ ведёт сделку, только если взял её в активном режиме (этап 1 плана «режимы ИИ-менеджера», 20.11.2026).

Решения Кати 20.11.2026: сделка из Теста не наследуется Полным, а при включении Полного ИИ перестаёт вести сделки, данные в Тесте.
Для этого сделка помнит режим, в котором ИИ её взял: `deal_owner.mode` (и `deal_given.mode` у сделок, которые отдали вручную).
Значения: `test`, `full`, `help`, `intercept`; активный режим отдаёт `ai_engine_settings.active_mode_id()`.

Что происходит, когда активный режим сменился, а сделка осталась взятой в другом (`ai_engine._release_for_mode`):

  • сделка тихо снимается: `owner = engine`, `epoch + 1` (идущий ход отбит по эпохе), ход отменён, отложенные касания и лестница тишины
    сняты, чат забыт (запрет для ИИ держит владелец `engine`, а не чат: у клиента в новом режиме может быть другая подходящая сделка);
  • в amo НЕ пишется ничего (ни примечания, ни задачи, ни смены этапа), клиенту ничего не уходит, алерта нет;
  • в журнал движка (лог и таблица `mode_release` ниже) идёт «снята при смене режима»; то же событие уходит в панель, в журнал ИИ
    на её экране (`release_event` собирает тело, `ai_engine_events` отправляет фоном, потеря допустима: правда в `mode_release`);
  • дальше обычный порядок без ИИ: сценарии движка, сторож розничных лидов, SLA-таймер. Обратно ИИ сделку сам не берёт (владелец
    `engine` значит «вернули сценарию»): вернуть её может только человек («Дать сделку ИИ» или возврат диалога), и тогда режим сделки
    станет активным.

Где режим проверяется (все точки, где ИИ что-то делает со сделкой): вход по сообщению клиента (`_on_inbound`, `_process_collecting`,
`_apply_resolution`, через него идёт и постановка хода), отправка хода (`_send_turn`), любое служебное событие (`_queue_event`: оплата, возврат
диалога, касание, лестница тишины, окна УР, перехват), разрешение на отправку (`decide_permit`, а через него и `schedule_followup`),
таймеры (`_fire_followup`, `_silence_step`, `_ur_tick`), событие сделки из amo (`_lead_event_under_lock`), правило C, ручные
`give_deal` и `return_dialog`, а также проход `_sweep_mode_mismatch` при смене режима и раз в 10 с.

Режим `off` сюда не относится: у него своё правило (`ai_engine._sweep_if_off`, сделки уходят людям), а протухшие настройки (панель
недоступна) не должны ничего снимать.

Таблица `mode_release` создаётся при первом обращении (`IF NOT EXISTS`), в той же sqlite, что у `ai_engine_store`.
"""
import datetime
from typing import Any

import ai_engine_config as cfg
import ai_engine_store as store

RELEASED_NOTE = "снята при смене режима"
KEEP_S = 90 * 86400
EVENT_NOTE_MAX = 255                  # длина `note` в событии для панели

_SCHEMA = ("CREATE TABLE IF NOT EXISTS mode_release (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, "
           "from_mode TEXT, to_mode TEXT, epoch INTEGER, note TEXT NOT NULL)")


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.execute(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def mode_of(row: dict[str, Any] | None) -> str | None:
    """Режим, в котором ИИ взял сделку. Нет записи о сделке: None. Записи нет в колонке (строка старше режимов, или её создали без
    режима): так же, как при миграции, Тест по воронке «Тест», всё остальное Полный. До режимов в колонку писали `prod`: это Полный."""
    if not row:
        return None
    raw = row.get("mode")
    if raw in cfg.MODE_IDS:
        return raw
    if raw == "prod":
        return "full"
    return "test" if row.get("pipeline_id") == cfg.PIPELINE_TEST else "full"


def is_stale(row: dict[str, Any] | None, active: str) -> bool:
    """ИИ ведёт сделку (`owner = ai`, не закрыта), но взял её не в активном режиме. Режим `off` не считается: см. шапку."""
    if not row or active == "off" or row.get("owner") != store.OWNER_AI or row.get("closed"):
        return False
    return mode_of(row) != active


def log_release(deal_id: int, from_mode: str | None, to_mode: str, epoch: int | None, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO mode_release (deal_id, at, from_mode, to_mode, epoch, note) VALUES (?,?,?,?,?,?)",
                     (int(deal_id), float(now), from_mode, to_mode, epoch, RELEASED_NOTE))
        conn.execute("DELETE FROM mode_release WHERE at<?", (float(now) - KEEP_S,))


def release_event(deal_id: int, from_mode: str | None, to_mode: str, epoch: int | None, now: float) -> dict[str, Any]:
    """Тело события для панели (`POST /api/ingest/ai/engine-event`), форма договорена с панелью. `at` строится из того же `now`, что
    пишется в `mode_release.at`: по нему вместе с `deal_id` панель отсекает повторы. Личных данных клиента в теле нет, только сделка."""
    at = datetime.datetime.fromtimestamp(float(now), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "kind": "mode_release",
        "deal_id": int(deal_id),
        "from_mode": from_mode if isinstance(from_mode, str) else None,
        "to_mode": str(to_mode),
        "epoch": None if epoch is None else int(epoch),
        "at": at,
        "note": RELEASED_NOTE[:EVENT_NOTE_MAX],
    }


def releases(limit: int = 50) -> list[dict[str, Any]]:
    """Последние снятия, свежее первым: для проверки руками (`sqlite3`) и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM mode_release ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]
