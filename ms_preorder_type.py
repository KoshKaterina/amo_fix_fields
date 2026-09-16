"""Pure, fail-closed reading of the task-6 preorder summary on an MS order.

This module does not decide whether to create, hold, or update an amoCRM lead.
It has no configuration or integration imports. Reasons are stable diagnostic
codes and deliberately contain no order/customer/attribute values.
"""

from dataclasses import dataclass
import re
from typing import Literal, Mapping
from urllib.parse import urlsplit
from uuid import UUID


OrderKind = Literal["order", "preorder", "unknown"]
Reason = Literal[
    "ok",
    "invalid_order",
    "invalid_expected_uuid",
    "invalid_attributes",
    "attribute_not_found",
    "attribute_identity_conflict",
    "duplicate_attribute",
    "invalid_attribute_value",
    "invalid_summary",
]


@dataclass(frozen=True)
class ParsedOrderType:
    kind: OrderKind
    reason: Reason


_WAITING_LINE = re.compile(r"- (?P<title>.+) \[SKU: (?P<sku>.*)\] × [1-9][0-9]*\Z")
_ATTRIBUTE_PATH = "/entity/customerorder/metadata/attributes/"


def _canonical_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = str(UUID(value))
    except (ValueError, AttributeError):
        return None
    # Reject URNs, braces and other alternate representations in MS data.
    return parsed if value.lower() == parsed else None


def _uuid_from_href(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    if parts.scheme != "https" or not parts.netloc or parts.query or parts.fragment:
        return None
    prefix, separator, suffix = parts.path.rpartition(_ATTRIBUTE_PATH)
    if not separator or prefix != "/api/remap/1.2" or "/" in suffix:
        return None
    return _canonical_uuid(suffix)


def _summary_kind(value: str) -> OrderKind:
    if value == "Тип: Заказ\nОжидаемые позиции: нет":
        return "order"
    lines = value.split("\n")
    if (len(lines) >= 3 and lines[:2] == ["Тип: Предзаказ", "Ожидаемые позиции:"]
            and all(_valid_waiting_line(line) for line in lines[2:])):
        return "preorder"
    return "unknown"


def _valid_waiting_line(line: str) -> bool:
    match = _WAITING_LINE.fullmatch(line)
    if match is None:
        return False
    # Task 6 collapses all source whitespace within each saved name and SKU.
    title, sku = match.group("title", "sku")
    return bool(title.strip()) and all(part == " ".join(part.split()) for part in (title, sku))


def parse_ms_preorder_type(
    customerorder: Mapping[str, object], expected_attribute_uuid: str,
) -> ParsedOrderType:
    """Return order/preorder/unknown from one exact MS customerorder attribute.

    MS read fixtures identify attributes by ``id``; the task-6 writer identifies
    the same field with ``meta.href``. Either representation is accepted, but
    conflicting identities or duplicate matches are not. Names and live stock
    are never used. A missing or malformed value cannot become an order.
    """
    if not isinstance(customerorder, Mapping):
        return ParsedOrderType("unknown", "invalid_order")
    expected = _canonical_uuid(expected_attribute_uuid)
    if expected is None:
        return ParsedOrderType("unknown", "invalid_expected_uuid")
    attributes = customerorder.get("attributes")
    if not isinstance(attributes, list):
        return ParsedOrderType("unknown", "invalid_attributes")

    matches: list[Mapping[str, object]] = []
    for attribute in attributes:
        if not isinstance(attribute, Mapping):
            return ParsedOrderType("unknown", "invalid_attributes")
        raw_id = attribute.get("id")
        identity_id = _canonical_uuid(raw_id)
        meta = attribute.get("meta")
        raw_href = meta.get("href") if isinstance(meta, Mapping) else None
        identity_href = _uuid_from_href(raw_href)

        if identity_id == expected or identity_href == expected:
            if "meta" in attribute and meta is not None and not isinstance(meta, Mapping):
                return ParsedOrderType("unknown", "attribute_identity_conflict")
            if (raw_id is not None and identity_id != expected) or (
                raw_href is not None and identity_href != expected
            ):
                return ParsedOrderType("unknown", "attribute_identity_conflict")
            if isinstance(meta, Mapping) and meta.get("type") not in (None, "attributemetadata"):
                return ParsedOrderType("unknown", "attribute_identity_conflict")
            matches.append(attribute)

    if not matches:
        return ParsedOrderType("unknown", "attribute_not_found")
    if len(matches) != 1:
        return ParsedOrderType("unknown", "duplicate_attribute")
    value = matches[0].get("value")
    if not isinstance(value, str):
        return ParsedOrderType("unknown", "invalid_attribute_value")
    kind = _summary_kind(value)
    return ParsedOrderType(kind, "ok" if kind != "unknown" else "invalid_summary")
