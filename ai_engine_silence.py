"""Лестница тишины: клиент не отвечает на сообщение ИИ (решение Кати 12.11.2026).

С первого сообщения ИИ клиенту (его разрешение на отправку `permit`) идёт отсчёт. Нет ответа 1 час: ИИ получает событие `silent_alt`
и пробует написать в другом канале (Telegram или WhatsApp). Нет ответа ещё час: алерт менеджерам в чат ОП. Ответ клиента в любом
канале, закрытие сделки или передача человеку снимают отсчёт. Отсчёт идёт только в рабочие часы ИИ (окно отправки открыто).

Таблица создаётся при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`.
"""
import ai_engine_store as store

_SCHEMA = ("CREATE TABLE IF NOT EXISTS silence (deal_id INTEGER PRIMARY KEY, chat_key TEXT NOT NULL, armed_at REAL NOT NULL, "
           "stage INTEGER NOT NULL DEFAULT 0, stage_at REAL)")


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.execute(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def arm(deal_id: int, chat_key: str, now: float) -> None:
    """Начать отсчёт, если он ещё не идёт (первое сообщение ИИ; повторные сообщения отсчёт не сдвигают)."""
    with _Db() as conn:
        conn.execute("INSERT OR IGNORE INTO silence (deal_id, chat_key, armed_at, stage, stage_at) VALUES (?,?,?,0,NULL)",
                     (int(deal_id), chat_key, float(now)))


def clear(deal_id: int) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM silence WHERE deal_id=?", (int(deal_id),))


def due(now: float, alt_after_s: float, alert_after_s: float) -> list[dict]:
    """Сделки, у которых пора на следующую ступень: stage 0 и прошёл час, либо stage 1 и прошёл ещё час."""
    with _Db() as conn:
        rows = conn.execute("SELECT * FROM silence WHERE (stage=0 AND armed_at<=?) OR (stage=1 AND stage_at<=?)",
                            (float(now) - alt_after_s, float(now) - alert_after_s)).fetchall()
        return [dict(r) for r in rows]


def set_stage(deal_id: int, stage: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("UPDATE silence SET stage=?, stage_at=? WHERE deal_id=?", (int(stage), float(now), int(deal_id)))


def get(deal_id: int) -> dict | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM silence WHERE deal_id=?", (int(deal_id),)).fetchone()
        return dict(row) if row else None
