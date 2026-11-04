"""Read-only payment evidence for an existing Sunscrypt website order.

Never changes Woo/CRM/MS, never creates another Metrika order. A browser return
and a completed status alone are not payment evidence. The original Woo total
is used as the threshold; later manager changes to the basket are irrelevant.
"""
import datetime
from contextlib import closing
import logging
import re
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import amo_service
import ms_client
import woo_client
from waybill_config import FIELD_SITE_ORDER_NUMBER, FIELD_MOYSKLAD_ORDER_UUID

logger = logging.getLogger("uvicorn")

_STORE = Path(__file__).parent / "var" / "metrika_payment_evidence.sqlite3"


def _connection():
    _STORE.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(_STORE), timeout=10)
    connection.execute("CREATE TABLE IF NOT EXISTS receipt (lead_id INTEGER, order_id TEXT, source TEXT, timestamp INTEGER, original_total TEXT, PRIMARY KEY(lead_id, order_id))")
    return connection


def remember(lead_id, order_id, evidence):
    with closing(_connection()) as connection, connection:
        connection.execute("INSERT INTO receipt VALUES (?,?,?,?,?) ON CONFLICT(lead_id,order_id) DO UPDATE SET source=excluded.source,timestamp=excluded.timestamp,original_total=excluded.original_total",
            (int(lead_id),str(order_id),evidence.source,evidence.timestamp,evidence.original_total))


def cached(lead):
    order_id = str(amo_service.get_custom_field_value(lead, FIELD_SITE_ORDER_NUMBER) or "").strip()
    if not order_id.isdigit() or not _STORE.exists():
        return None
    with closing(_connection()) as connection, connection:
        row = connection.execute("SELECT source,timestamp,original_total FROM receipt WHERE lead_id=? AND order_id=?",(int(lead["id"]),order_id)).fetchone()
    return Evidence(*row) if row else None


@dataclass(frozen=True)
class Evidence:
    source: str
    timestamp: int
    original_total: str


def money(value):
    try:
        result = Decimal(str(value).replace(" ", "").replace("\xa0", "").replace(",", "."))
        return result if result.is_finite() and result >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def paid_timestamp(value):
    try:
        return int(datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
            tzinfo=datetime.timezone.utc).timestamp())
    except (ValueError, TypeError, OverflowError):
        return None


def website_receipt(order):
    total = money(order.get("total"))
    timestamp = paid_timestamp(order.get("date_paid_gmt"))
    if (total is not None and total > 0 and timestamp
            and str(order.get("transaction_id") or "").strip()
            and order.get("status") not in {"cancelled", "refunded", "failed"}):
        return Evidence("woo_transaction", timestamp, str(total))
    return None


def manager_receipt(order, notes, lead_id):
    """Only automatic Ozon confirmations, deduplicated by payment marker.

    Several manager payments can cover the original order; repeated notification
    notes for the same payment cannot turn a deposit into a fully paid order.
    MS sums are not added here: they may already include the same receipt.
    """
    total = money(order.get("total"))
    if not total or order.get("status") == "refunded":
        return None
    receipts = {}
    pattern = r"^Оплата подтверждена Ozon \([^)]*\):\s*([\d\s.,]+)\s*₽"
    for note in notes:
        text = str((note.get("params") or {}).get("text") or "")
        match = re.search(pattern, text)
        marker = re.search(r"(?:extId|paymentId)\s+([^\s]+)", text)
        if not match or not marker:
            continue
        key = marker.group(1)
        # A lead's other/unrelated invoice is not evidence for this order.
        if key.startswith("amo-") and not key.startswith(f"amo-{lead_id}-"):
            continue
        amount = money(match.group(1))
        timestamp = int(note.get("created_at") or 0)
        if amount and timestamp:
            receipts.setdefault(key, (amount, timestamp))
    amount = sum((x[0] for x in receipts.values()), Decimal(0))
    if receipts and amount >= total:
        return Evidence("manager_ozon_receipt", max(x[1] for x in receipts.values()), str(total))
    return None


async def for_lead(lead):
    # A missing identifier/API outage must never undo an already confirmed fact.
    existing = cached(lead)
    if existing:
        return existing
    evidence = await _read_for_lead(lead)
    if evidence:
        remember(lead["id"], amo_service.get_custom_field_value(lead, FIELD_SITE_ORDER_NUMBER), evidence)
    return evidence


async def _read_for_lead(lead):
    raw_id = amo_service.get_custom_field_value(lead, FIELD_SITE_ORDER_NUMBER)
    order_id = str(raw_id or "").strip()
    if not order_id.isdigit() or not woo_client.is_configured():
        return None
    try:
        # Only reads. No complete_order(), active payment resolve or write hooks.
        response = await woo_client._ensure()._request(
            "GET", f"/orders/{order_id}?_fields=id,status,total,transaction_id,date_paid_gmt")
        if response.status_code != 200:
            return None
        order = response.json()
        if str(order.get("id")) != order_id:
            return None
        evidence = website_receipt(order)
        if evidence:
            return evidence
        if order.get("status") == "refunded":
            return None
        notes = []
        for page in range(1, 9):
            data = await amo_service._do_get(f"/api/v4/leads/{lead['id']}/notes",
                [("limit", "250"), ("page", str(page))])
            if data is None:
                return None
            notes.extend((data.get("_embedded") or {}).get("notes") or [])
            if not (data.get("_links") or {}).get("next"):
                break
        else:
            # Do not sum an incomplete receipt history.
            return None
        evidence = manager_receipt(order, notes, lead["id"])
        if evidence:
            return evidence
        # A fully recorded receipt is positive evidence. Missing/partial MS data
        # is never a reason to downgrade an order: manager payments are absent.
        uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "")
        if re.fullmatch(r"[0-9a-fA-F-]{36}", uuid):
            ms_order = await ms_client.get(f"entity/customerorder/{uuid}")
            paid = money((ms_order or {}).get("payedSum"))
            total = money(order.get("total"))
            if paid is not None and total and paid / 100 >= total:
                timestamp = paid_timestamp((ms_order or {}).get("updated"))
                if timestamp:
                    return Evidence("ms_recorded_receipt", timestamp, str(total))
    except Exception as exc:
        # A failed positive check must not disrupt the existing order sync.
        logger.warning("Metrika payment evidence: order %s read failed (%s)", order_id, type(exc).__name__)
    return None
