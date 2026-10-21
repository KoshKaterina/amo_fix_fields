#!/usr/bin/env python3
"""Receive amoCRM lead webhooks and debounce Academy deduplication cycles."""
from __future__ import annotations

import json
import os
import pathlib
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = pathlib.Path(__file__).resolve().parent
STATE = pathlib.Path(os.environ.get("ACADEMY_DEDUP_WEBHOOK_STATE", "/var/lib/academy-dedup"))
DB_PATH = STATE / "webhook-queue.sqlite3"
AMO_ENV = "/opt/integrations/amo_fix_fields/.env"
HOST = "127.0.0.1"
PORT = 8031
ACADEMY_PIPELINE = 8642410
CLOSED = {142, 143}
DEBOUNCE_S = 60
MAX_WAIT_S = 60
FAILED_RETRY_S = 6 * 60 * 60
MIN_CYCLE_INTERVAL_S = 60
ACCOUNT = "new5a2e8ea7b16b4"
MAX_BODY = 256 * 1024
LEAD_KEY = re.compile(r"^leads\[(add|update)\]\[\d+\]\[id\]$")

os.environ.setdefault("AMO_ENV", AMO_ENV)
sys.path.insert(0, str(HERE))
import amo  # noqa: E402


def log(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def init_db() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    with connect() as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS pending (
                   lead_id INTEGER PRIMARY KEY,
                   first_seen INTEGER NOT NULL,
                   last_seen INTEGER NOT NULL,
                   due_at INTEGER NOT NULL
               )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS runtime (
                   key TEXT PRIMARY KEY,
                   value INTEGER NOT NULL
               )"""
        )


def enqueue(lead_ids: set[int]) -> None:
    now = int(time.time())
    with connect() as db:
        for lead_id in lead_ids:
            db.execute(
                """INSERT INTO pending(lead_id, first_seen, last_seen, due_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(lead_id) DO UPDATE SET
                       last_seen=excluded.last_seen,
                       due_at=MIN(pending.first_seen + ?, excluded.due_at)""",
                (lead_id, now, now, now + DEBOUNCE_S, MAX_WAIT_S),
            )


def due_rows() -> list[int]:
    now = int(time.time())
    with connect() as db:
        return [int(row[0]) for row in db.execute(
            "SELECT lead_id FROM pending WHERE due_at <= ? ORDER BY due_at LIMIT 100", (now,)
        )]


def delete_rows(lead_ids: list[int]) -> None:
    if not lead_ids:
        return
    with connect() as db:
        db.executemany("DELETE FROM pending WHERE lead_id=?", [(value,) for value in lead_ids])


def delay_rows(lead_ids: list[int], seconds: int) -> None:
    if not lead_ids:
        return
    due = int(time.time()) + seconds
    with connect() as db:
        db.executemany("UPDATE pending SET due_at=? WHERE lead_id=?", [(due, value) for value in lead_ids])


def queue_size() -> int:
    with connect() as db:
        return int(db.execute("SELECT COUNT(*) FROM pending").fetchone()[0])


def runtime_value(key: str) -> int:
    with connect() as db:
        row = db.execute("SELECT value FROM runtime WHERE key=?", (key,)).fetchone()
    return int(row[0]) if row else 0


def set_runtime_value(key: str, value: int) -> None:
    with connect() as db:
        db.execute(
            "INSERT INTO runtime(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, int(value)),
        )


def lead_is_academy(lead_id: int) -> tuple[bool, bool]:
    data = amo.get(f"leads/{lead_id}") or {}
    if data.get("_error"):
        return False, True
    if not data.get("id"):
        return False, False
    return (
        int(data.get("pipeline_id") or 0) == ACADEMY_PIPELINE
        and int(data.get("status_id") or 0) not in CLOSED,
        False,
    )


def run_worker(stop: threading.Event) -> None:
    while not stop.wait(5):
        rows = due_rows()
        if not rows:
            continue
        relevant = False
        api_failed = False
        for lead_id in rows:
            is_academy, failed = lead_is_academy(lead_id)
            relevant = relevant or is_academy
            api_failed = api_failed or failed
        if not relevant and not api_failed:
            delete_rows(rows)
            log(f"discarded {len(rows)} non-Academy/missing webhook leads")
            continue
        last_cycle = runtime_value("last_cycle_at")
        wait_for = MIN_CYCLE_INTERVAL_S - (int(time.time()) - last_cycle)
        if wait_for > 0:
            delay_rows(rows, wait_for)
            log(f"webhook batch coalesced for {wait_for}s after recent cycle")
            continue
        log(f"starting dedup cycle from webhook batch: events={len(rows)}, academy={relevant}")
        try:
            result = subprocess.run(
                ["systemctl", "start", "academy-dedup.service"],
                timeout=21 * 60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            log("dedup service start timed out; keeping webhook batch")
            delay_rows(rows, FAILED_RETRY_S)
            continue
        if result.returncode == 0:
            set_runtime_value("last_cycle_at", int(time.time()))
            delete_rows(rows)
            log(f"webhook batch complete: removed={len(rows)}")
        else:
            delay_rows(rows, FAILED_RETRY_S)
            log(f"dedup service failed rc={result.returncode}; retry delayed")


class Handler(BaseHTTPRequestHandler):
    server_version = "academy-dedup-webhook/1"

    def log_message(self, *_args) -> None:
        return

    def reply(self, code: int, body: bytes = b"ok") -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path != "/health":
            self.reply(404, b"not found")
            return
        body = json.dumps({"status": "ok", "pending": queue_size()}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/hook":
            self.reply(404, b"not found")
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self.reply(400, b"bad length")
            return
        if length <= 0 or length > MAX_BODY:
            self.reply(413, b"bad body")
            return
        raw = self.rfile.read(length).decode("utf-8", "replace")
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        account = (form.get("account[subdomain]") or [""])[0]
        if account != ACCOUNT:
            self.reply(403, b"wrong account")
            return
        lead_ids = {
            int(values[0])
            for key, values in form.items()
            if LEAD_KEY.match(key) and values and str(values[0]).isdigit()
        }
        if lead_ids:
            enqueue(lead_ids)
            log(f"queued webhook lead ids: count={len(lead_ids)}")
        self.reply(200)


def main() -> int:
    init_db()
    stop = threading.Event()
    worker = threading.Thread(target=run_worker, args=(stop,), daemon=True, name="dedup-worker")
    worker.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    log(f"listening on {HOST}:{PORT}")
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        stop.set()
        server.server_close()
        worker.join(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
