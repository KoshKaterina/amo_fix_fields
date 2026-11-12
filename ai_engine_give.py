"""Переданные ИИ сделки (решение Кати 12.11.2026, «дать сделку»).

Обычно ИИ входит в сделку по новому сообщению клиента (`ai_engine._apply_resolution`). Для сделок, где клиент уже написал, а
менеджер ещё не ответил (заказы с сайта tangemshop.ru на этапе «Новый лид»), менеджер передаёт сделку явно: `ai_engine.give_deal`
ставит `owner = ai` и служебное событие `deal_given`, ИИ перечитывает переписку и продолжает с того места, где остановились.

Здесь одна маленькая таблица: когда сделку отдали. От неё считается `manager_cutoff` в ходе: сообщения менеджера ДО передачи ИИ
не останавливают (иначе человек, написавший клиенту и потом отдавший сделку ИИ, получил бы молчащего ИИ по правилу «менеджер в
переписке»). Сообщения менеджера после передачи останавливают, как и везде.

Таблица создаётся при первом обращении (`IF NOT EXISTS`), в той же sqlite, что у `ai_engine_store`.
"""
import datetime

import ai_engine_store as store

_SCHEMA = "CREATE TABLE IF NOT EXISTS deal_given (deal_id INTEGER PRIMARY KEY, given_at REAL NOT NULL, given_iso TEXT NOT NULL)"


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.execute(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def mark(deal_id: int, now: float) -> str:
    """Отметить передачу (повторная передача обновляет время). Возвращает ISO-время."""
    iso = _iso(now)
    with _Db() as conn:
        conn.execute("INSERT INTO deal_given (deal_id, given_at, given_iso) VALUES (?,?,?) "
                     "ON CONFLICT(deal_id) DO UPDATE SET given_at=excluded.given_at, given_iso=excluded.given_iso",
                     (int(deal_id), float(now), iso))
    return iso


def cutoff_iso(deal_id: int) -> str | None:
    """Когда сделку отдали ИИ; None, если её не передавали (ИИ вошёл сам по сообщению клиента)."""
    with _Db() as conn:
        row = conn.execute("SELECT given_iso FROM deal_given WHERE deal_id=?", (int(deal_id),)).fetchone()
        return row["given_iso"] if row else None
