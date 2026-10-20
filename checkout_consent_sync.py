"""One MS order field -> existing contact consents. Standalone, dry-run by default.

No public endpoint, contact creation, lead mutation or client sends. Run through the
existing amo-fix-fields container to reuse its designated API clients/credentials.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import uuid

from shared.timez import MSK, to_msk

MS_FIELD = "327bfdfa-cbd4-11f1-0a80-1c9d00f0e1bf"
VERSION = "checkout-marketing-v1-2026-10-19"
PD = (578239, 578241, 578279)
AD = (578245, 578247, 578277)
FIELDS = PD + AD
PATTERN = re.compile(
    r"ПДн: да, реклама \(email/SMS/мессенджеры/звонки\): (да|нет), "
    r"дата: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}), "
    r"версия ПДн: (policy-[0-9a-f]{12}), версия рекламы: " + re.escape(VERSION)
)


@dataclass(frozen=True)
class Evidence:
    marketing: bool
    at: datetime
    pd_version: str
    digest: str


class UncertainRead(RuntimeError):
    pass


def field_value(contact: dict, field_id: int) -> str:
    rows = [x for x in contact.get("custom_fields_values") or [] if x.get("field_id") == field_id]
    if len(rows) > 1:
        raise UncertainRead("duplicate contact field")
    values = rows[0].get("values", []) if rows else []
    if len(values) > 1:
        raise UncertainRead("multiple consent values")
    return str(values[0].get("value") or "").strip() if values else ""


def parse_order(order: dict, now: datetime) -> Evidence | None:
    attrs = [x for x in order.get("attributes") or [] if isinstance(x, dict) and x.get("id") == MS_FIELD]
    if len(attrs) != 1 or not isinstance(attrs[0].get("value"), str):
        return None
    text = attrs[0]["value"]
    match = PATTERN.fullmatch(text)
    if not match:
        return None
    try:
        at = datetime.fromisoformat(match[2])
    except ValueError:
        return None
    if at.date().isoformat() < "2026-10-19" or at > now + timedelta(minutes=1):
        return None
    return Evidence(match[1] == "да", at, match[3], hashlib.sha256(text.encode()).hexdigest())


def stored_time(value: str) -> tuple[datetime, bool] | None:
    try:
        at = datetime.fromisoformat(value)
        if at.tzinfo is not None:
            return (at, False) if at.year >= 2000 else None
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            at = datetime.strptime(value, fmt).replace(tzinfo=MSK)
            return (at, True) if at.year >= 2000 else None
        except ValueError:
            pass
    return None


def plan(contact: dict, evidence: Evidence, changed_after: set[int] | None = None) -> dict[int, str]:
    """Conservative ordering. Undated/ambiguous records and same-day legacy dates win."""
    patch = {}
    changed_after = changed_after or set()
    groups = [(PD, evidence.pd_version)]
    # An unchecked checkout box is not an unsubscribe event, even for an empty contact.
    if evidence.marketing:
        groups.append((AD, VERSION))
    for group, version in groups:
        current = [field_value(contact, fid) for fid in group]
        if changed_after.intersection(group):
            continue
        if any(current):
            recorded = stored_time(current[2])
            if recorded is None:
                continue
            at, date_only = recorded
            if date_only:
                if to_msk(evidence.at).date() <= at.date():
                    continue
            elif evidence.at <= at:
                continue
        patch.update(zip(group, ("Да", version, evidence.at.isoformat(timespec="seconds"))))
    return patch


class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS cursor (id INTEGER PRIMARY KEY CHECK(id=1), at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
              order_id TEXT NOT NULL, digest TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
              contact_id INTEGER, patch TEXT, outcome TEXT, last_attempt INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(order_id,digest));
        """)

    def enqueue(self, oid: str, evidence: Evidence):
        self.db.execute("INSERT OR IGNORE INTO jobs(order_id,digest) VALUES(?,?)", (oid, evidence.digest))
        self.db.commit()

    def finish(self, job, state: str, outcome: str, contact_id=None, patch=None):
        self.db.execute("UPDATE jobs SET state=?,outcome=?,contact_id=?,patch=? WHERE order_id=? AND digest=?",
                        (state, outcome, contact_id, json.dumps(patch) if patch else None, job['order_id'], job['digest']))
        self.db.commit()

    def active(self):
        # Parked writes are reconciled by reading, never blindly retried.
        rows = self.db.execute("SELECT * FROM jobs WHERE state IN ('pending','sending','parked') ORDER BY last_attempt,rowid LIMIT 20").fetchall()
        for row in rows:
            self.db.execute("UPDATE jobs SET last_attempt=? WHERE order_id=? AND digest=?", (time.time_ns(), row['order_id'], row['digest']))
        self.db.commit()
        return rows

    def matches(self, contact: dict, patch: dict[int, str]):
        return all(field_value(contact, int(fid)) == value for fid, value in patch.items())


async def process_job(job, store: Store, io, *, dry_run: bool, now: datetime) -> str:
    if job['state'] in ('sending', 'parked'):
        patch = json.loads(job['patch'] or '{}')
        contact = await io.contact(job['contact_id'])
        if patch and store.matches(contact, patch):
            store.finish(job, 'done', 'reconciled', job['contact_id'], patch)
            return 'reconciled'
        return 'parked'
    order = await io.order(job['order_id'])
    evidence = parse_order(order, now)
    if not evidence or evidence.digest != job['digest']:
        store.finish(job, 'done', 'superseded')
        return 'superseded'
    contact_id = await io.linked_contact(job['order_id'])
    if contact_id is None:
        return 'awaiting_crm_link'
    before = await io.contact(contact_id)
    patch = plan(before, evidence)
    if not patch:
        store.finish(job, 'done', 'no_new_choice')
        return 'no_new_choice'
    # Protect blank/cleared fields and choices whose legacy date is incomplete.
    changed = await io.changes_after(contact_id, evidence.at)
    patch = plan(before, evidence, changed)
    if not patch:
        store.finish(job, 'done', 'newer_contact_event')
        return 'newer_contact_event'
    fresh = await io.contact(contact_id)
    # Re-read immediately before the write; do not race a detected other writer.
    if any(field_value(before, fid) != field_value(fresh, fid) for fid in FIELDS):
        return 'contact_changed_retry'
    if dry_run:
        return 'would_patch'
    # Persist intent BEFORE sending. Even an exception may follow a committed PATCH.
    store.finish(job, 'sending', 'intent', contact_id, patch)
    try:
        result = await io.patch_contact(contact_id, patch)
        after = await io.contact(contact_id)
    except Exception:
        store.finish(job, 'parked', 'unknown_write', contact_id, patch)
        return 'parked'
    if result and store.matches(after, patch):
        store.finish(job, 'done', 'applied', contact_id, patch)
        return 'applied'
    store.finish(job, 'parked', 'unconfirmed_write', contact_id, patch)
    return 'parked'


async def scan(store: Store, io, start: datetime, end: datetime, *, max_pages: int = 10):
    scanned = 0
    for page in range(max_pages):
        rows = await io.orders(start, end, page * 100)
        scanned += len(rows)
        for order in rows:
            oid = str(order.get('id', ''))
            try:
                uuid.UUID(oid)
            except ValueError:
                continue
            evidence = parse_order(order, end)
            if evidence:
                store.enqueue(oid, evidence)
        if len(rows) < 100:
            store.db.execute("INSERT OR REPLACE INTO cursor VALUES(1,?)", (end.isoformat(),))
            store.db.commit()
            return scanned
    raise UncertainRead('scan limit reached; cursor not advanced')


class Runtime:
    """Lazy imports: pure tests never load credentials or live HTTP clients."""
    def __init__(self):
        import api
        import amo_service
        import ms_client
        from waybill_config import FIELD_MOYSKLAD_ORDER_UUID
        self.api, self.amo, self.ms = api, amo_service, ms_client
        self.order_field = FIELD_MOYSKLAD_ORDER_UUID
        self.last_request = 0.0

    async def pace(self):
        await asyncio.sleep(max(0.0, 1.05 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()

    async def validate_fields(self):
        for fid in FIELDS:
            await self.pace()
            data = await self.amo._do_get(f'/api/v4/contacts/custom_fields/{fid}')
            if not data or data.get('id') != fid or data.get('type') != 'text':
                raise UncertainRead('contact field schema mismatch')
        await self.pace()
        data = await self.ms.get('entity/customerorder/metadata/attributes/' + MS_FIELD)
        if not data or data.get('id') != MS_FIELD or data.get('type') != 'text':
            raise UncertainRead('MS consent field schema mismatch')

    async def orders(self, start, end, offset):
        await self.pace()
        data = await self.ms.get('entity/customerorder', params={
            'filter': f'updated>={to_msk(start):%Y-%m-%d %H:%M:%S};updated<={to_msk(end):%Y-%m-%d %H:%M:%S}',
            'order': 'updated,asc', 'limit': 100, 'offset': offset})
        if (not isinstance(data, dict) or not isinstance(data.get('rows'), list)
                or not all(isinstance(row, dict) for row in data['rows'])):
            raise UncertainRead('MS scan unavailable')
        return data['rows']

    async def order(self, oid):
        uuid.UUID(oid)
        await self.pace()
        data = await self.ms.get('entity/customerorder/' + oid)
        if not data or data.get('id') != oid:
            raise UncertainRead('MS order unavailable')
        return data

    async def linked_contact(self, oid):
        await self.pace()
        rows = await self.amo.find_leads_by_query(oid, limit=250)
        if rows is None or len(rows) >= 250:
            raise UncertainRead('CRM search incomplete')
        leads = [x for x in rows if self.amo.get_custom_field_value(x, self.order_field) == oid]
        contacts = set()
        for row in leads:
            await self.pace()
            lead = await self.amo.get_lead_full(row['id'], with_=('contacts',))
            if not lead or self.amo.get_custom_field_value(lead, self.order_field) != oid:
                raise UncertainRead('CRM link changed')
            linked = (lead.get('_embedded') or {}).get('contacts') or []
            main = [x for x in linked if x.get('is_main')]
            selected = main if len(main) == 1 else linked
            if not selected:
                return None
            if len(selected) != 1 or not selected[0].get('id'):
                raise UncertainRead('ambiguous contact link')
            contacts.add(int(selected[0]['id']))
        if len(contacts) > 1:
            raise UncertainRead('different contacts for one order')
        return next(iter(contacts), None)

    async def contact(self, cid):
        await self.pace()
        data = await self.amo.get_contact_by_id(cid)
        if not data or data.get('id') != cid:
            raise UncertainRead('CRM contact unavailable')
        return data

    async def changes_after(self, cid, at):
        await self.pace()
        data = await self.amo._do_get('/api/v4/events', [
            ('filter[entity]', 'contact'), ('filter[entity_id][]', str(cid)),
            *[("filter[type][]", f'custom_field_{fid}_value_changed') for fid in FIELDS],
            ('filter[created_at][from]', str(int(at.timestamp()))), ('limit', '100')])
        if data is None:
            raise UncertainRead('CRM consent history unavailable')
        events = (data.get('_embedded') or {}).get('events') or []
        if len(events) >= 100 or data.get('_links', {}).get('next'):
            raise UncertainRead('CRM consent history incomplete')
        changed = set()
        for event in events:
            match = re.fullmatch(r'custom_field_(\d+)_value_changed', event.get('type', ''))
            if (event.get('entity_type') == 'contact' and event.get('entity_id') == cid
                    and int(event.get('created_at', 0)) >= int(at.timestamp()) and match):
                changed.add(int(match[1]))
        return changed

    async def patch_contact(self, cid, patch):
        await self.pace()
        if not patch or not set(patch).issubset(FIELDS):
            raise ValueError('only existing consent fields may be patched')
        response = await self.api.submit_request(
            'PATCH', self.api.BASE_URL + f'/api/v4/contacts/{cid}', self.api.headers,
            json_body={'custom_fields_values': [
                {'field_id': fid, 'values': [{'value': value}]} for fid, value in patch.items()]})
        return 200 <= response.status_code < 300


async def run(args, store):
    io = Runtime()
    io.api.init_api_pipeline()
    io.ms.init()
    try:
        await io.validate_fields()
        now = datetime.now(timezone.utc)
        cursor = store.db.execute('SELECT at FROM cursor WHERE id=1').fetchone()
        since = datetime.fromisoformat(cursor['at']) if cursor else (now if args.since == 'now' else datetime.fromisoformat(args.since))
        if since.tzinfo is None or since > now:
            raise ValueError('since must have a timezone and must not be in the future')
        scanned = 0
        # Bounded catch-up windows plus overlap reduce offset-pagination races.
        # Persist each completed window; a failure never skips an unfinished one.
        for _ in range(5):
            until = min(since + timedelta(minutes=2), now)
            scanned += await scan(store, io, since - timedelta(minutes=5), until)
            since = until
            if until >= now:
                break
        counts = Counter()
        for job in store.active():
            try:
                outcome = await process_job(job, store, io, dry_run=not args.apply, now=now)
            except (UncertainRead, ValueError):
                outcome = 'read_or_identity_error'
            counts[outcome] += 1
        print(json.dumps({'mode': 'apply' if args.apply else 'dry-run', 'scanned_rows': scanned, 'caught_up': since >= now, 'outcomes': dict(counts)}, sort_keys=True))
        return 2 if counts['parked'] or counts['read_or_identity_error'] else 0
    finally:
        await io.ms.aclose()
        await io.api.shutdown_api_pipeline()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Allow writes to existing contact consent fields only')
    parser.add_argument('--since', required=True, help='First activation ISO timestamp with timezone, or now')
    parser.add_argument('--state', default='/app/var/checkout-consents.sqlite3')
    args = parser.parse_args()
    # Dry runs cannot advance the apply cursor or consume pending jobs.
    if not args.apply:
        return asyncio.run(run(args, Store(':memory:')))
    path = Path(args.state)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path) + '.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('{"outcome":"already_running"}')
            return 0
        return asyncio.run(run(args, Store(str(path))))


if __name__ == '__main__':
    raise SystemExit(main())
