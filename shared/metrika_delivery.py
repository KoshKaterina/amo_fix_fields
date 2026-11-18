"""Durable CDP delivery metadata; never stores contacts or uploaded payloads.

HTTP acceptance is pending. Only PASSED for the exact one-order upload confirms
validation, which is deliberately distinct from attribution/order readback.
"""
from contextlib import closing
import sqlite3
from pathlib import Path


class DeliveryStore:
    def __init__(self, path):
        self.path = Path(path)

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(str(self.path), timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('''CREATE TABLE IF NOT EXISTS delivery (
            counter_id INTEGER, order_id INTEGER, lead_id INTEGER,
            signature TEXT NOT NULL, state TEXT NOT NULL,
            uploading_id TEXT, attempted_at REAL NOT NULL,
            PRIMARY KEY(counter_id, order_id))''')
        return db

    def get(self, counter_id, order_id):
        if not self.path.exists():
            return None
        with closing(self._connect()) as db:
            row = db.execute('SELECT * FROM delivery WHERE counter_id=? AND order_id=?',
                             (counter_id, order_id)).fetchone()
            return dict(row) if row else None

    def put(self, counter_id, order_id, lead_id, signature, state, uploading_id, now):
        with closing(self._connect()) as db, db:
            db.execute('''INSERT INTO delivery VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(counter_id,order_id) DO UPDATE SET
                lead_id=excluded.lead_id, signature=excluded.signature,
                state=excluded.state, uploading_id=excluded.uploading_id,
                attempted_at=excluded.attempted_at''',
                (counter_id, order_id, lead_id, signature, state, uploading_id, now))

    def pending(self, counter_id):
        if not self.path.exists():
            return []
        with closing(self._connect()) as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM delivery WHERE counter_id=? AND state != 'validated'",
                (counter_id,))]

    def confirm(self, counter_id, order_id, uploading_id):
        # A late receipt must not acknowledge a newer version of the same order.
        with closing(self._connect()) as db, db:
            db.execute("UPDATE delivery SET state='validated' WHERE counter_id=? "
                       "AND order_id=? AND uploading_id=?",
                       (counter_id, order_id, uploading_id))

    def counts(self, counter_id):
        if not self.path.exists():
            return {}
        with closing(self._connect()) as db:
            return dict(db.execute('SELECT state,count(*) FROM delivery '
                                   'WHERE counter_id=? GROUP BY state', (counter_id,)))
