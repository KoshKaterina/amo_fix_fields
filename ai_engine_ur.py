"""Окна перевода в «Успешно реализовано» (решение Кати 12.11.2026).

В 17:50 МСК каждая сделка, которую ведёт ИИ в «ОП розница» или TangemShop (в режиме «Тест» ещё и в «Тесте»; решение Кати 26.11.2026,
`ai_engine_settings.ur_window_pipelines`), получает событие `ur_wave`: ИИ решает по сделке (оплачен заказ: в УР без подтверждения;
наложка и клиент подтвердил данные заказа: в УР независимо от ответов на другие сообщения; подтверждения нет и не оплачен: передача
менеджеру). Исключение: если последнее сообщение ИИ клиенту ушло в 17:00 или позже, сделка пропускает первое окно и получает событие
во втором окне в 18:45 по тому же правилу.

Здесь состояние: когда ИИ последний раз писал клиенту по сделке и что уже сделано в окнах сегодняшнего дня (одно окно на сделку в
день). Таблицы создаются при первом обращении (`IF NOT EXISTS`), в той же sqlite, что у `ai_engine_store`.
"""
import ai_engine_store as store

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_last_sent (deal_id INTEGER PRIMARY KEY, last_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS ur_wave (day TEXT NOT NULL, deal_id INTEGER NOT NULL, wave INTEGER NOT NULL, state TEXT NOT NULL, at REAL NOT NULL,
                                    PRIMARY KEY (day, deal_id, wave));
"""


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def note_sent(deal_id: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO ai_last_sent (deal_id, last_at) VALUES (?,?) ON CONFLICT(deal_id) DO UPDATE SET last_at=excluded.last_at",
                     (int(deal_id), float(now)))


def last_sent(deal_id: int) -> float | None:
    with _Db() as conn:
        row = conn.execute("SELECT last_at FROM ai_last_sent WHERE deal_id=?", (int(deal_id),)).fetchone()
        return float(row["last_at"]) if row else None


def state(day: str, deal_id: int, wave: int) -> str | None:
    with _Db() as conn:
        row = conn.execute("SELECT state FROM ur_wave WHERE day=? AND deal_id=? AND wave=?", (day, int(deal_id), int(wave))).fetchone()
        return row["state"] if row else None


def cancel_deferred(day: str, now: float) -> int:
    """Окна УР выключили в панели: сделки, ждавшие второго окна сегодня (`deferred`), его не получат, даже если окна включат снова до
    вечера: стоящий таймер снят, а не отложен. Возвращает, сколько снято. Сегодняшние `fired` и `cancelled` не трогает."""
    with _Db() as conn:
        return conn.execute("UPDATE ur_wave SET state='cancelled', at=? WHERE day=? AND state='deferred'", (float(now), day)).rowcount


def mark(day: str, deal_id: int, wave: int, st: str, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT OR REPLACE INTO ur_wave (day, deal_id, wave, state, at) VALUES (?,?,?,?,?)", (day, int(deal_id), int(wave), st, float(now)))
