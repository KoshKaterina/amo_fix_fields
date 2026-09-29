"""Offline assessment of an already-created amGroup deal; never writes to amo.

``candidates_complete`` must be backed by an exhaustive external read. The
caller must separately verify the amGroup source and that the order is after
the agreed classification cutoff. This module neither obtains that evidence
nor models CRM event history: a mismatch always needs a person, never an
automatic correction. A no-op only means that no correction is needed for the
supplied, scoped, unambiguous current snapshot; it says nothing about history.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from ms_preorder_type import ParsedOrderType, parse_ms_preorder_type


Action = Literal["no-op", "manual-review"]
Reason = Literal[
    "invalid_config", "invalid_order_uuid", "unknown_order_type",
    "unverified_amgroup_source", "unverified_cutoff",
    "incomplete_candidates", "invalid_candidates", "ambiguous_identity",
    "missing_lead", "duplicate_lead", "invalid_lead_type",
    "different_lead_type", "already_correct",
]


@dataclass(frozen=True)
class DryRunDecision:
    action: Action
    reason: Reason
    parsed: ParsedOrderType


def _canonical_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = str(UUID(value))
    except (ValueError, AttributeError):
        return None
    return parsed if value.lower() == parsed else None


def _one_field_value(fields: list, field_id: int) -> tuple[str, Mapping | None]:
    matches = [field for field in fields if isinstance(field, Mapping)
               and type(field.get("field_id")) is int and field["field_id"] == field_id]
    if not matches:
        return "missing", None
    if len(matches) != 1:
        return "invalid", None
    values = matches[0].get("values")
    if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], Mapping):
        return "invalid", None
    return "ok", values[0]


def decide_existing_amgroup_lead(
    customerorder: Mapping[str, object],
    expected_attribute_uuid: str,
    lead_candidates: list[Mapping[str, object]],
    *,
    candidates_complete: bool = False,
    verified_amgroup_source: bool = False,
    post_cutoff: bool = False,
    order_uuid_field_id: int,
    order_type_field_id: int,
    order_enum_id: int,
    preorder_enum_id: int,
) -> DryRunDecision:
    """Classify the *existing* lead as no-op or requiring manual review.

    Field/enum IDs are explicit inputs, not unverified live account constants.
    The candidates are raw amo leads with ``custom_fields_values``; a caller
    must independently establish that the supplied set is complete, the deal
    came from amGroup, and the order is past an externally agreed cutoff.
    Boolean assertions are not evidence by themselves. No candidate names,
    order text, or customer details enter the result.
    """
    parsed = parse_ms_preorder_type(customerorder, expected_attribute_uuid)

    def review(reason: Reason) -> DryRunDecision:
        return DryRunDecision("manual-review", reason, parsed)

    ids = (order_uuid_field_id, order_type_field_id, order_enum_id, preorder_enum_id)
    if (any(type(item) is not int or item <= 0 for item in ids)
            or order_uuid_field_id == order_type_field_id
            or order_enum_id == preorder_enum_id):
        return review("invalid_config")
    order_uuid = _canonical_uuid(customerorder.get("id")) if isinstance(customerorder, Mapping) else None
    if order_uuid is None:
        return review("invalid_order_uuid")
    if parsed.kind == "unknown":
        return review("unknown_order_type")
    if verified_amgroup_source is not True:
        return review("unverified_amgroup_source")
    if post_cutoff is not True:
        return review("unverified_cutoff")
    if candidates_complete is not True:
        return review("incomplete_candidates")
    if not isinstance(lead_candidates, list):
        return review("invalid_candidates")

    matching: list[Mapping[str, object]] = []
    seen_lead_ids: set[int] = set()
    for lead in lead_candidates:
        if (not isinstance(lead, Mapping)
                or type(lead.get("id")) is not int or lead["id"] <= 0):
            return review("invalid_candidates")
        if lead["id"] in seen_lead_ids:
            return review("invalid_candidates")
        seen_lead_ids.add(lead["id"])
        fields = lead.get("custom_fields_values")
        if (not isinstance(fields, list)
                or not all(isinstance(field, Mapping)
                           and type(field.get("field_id")) is int
                           and field["field_id"] > 0 for field in fields)):
            return review("invalid_candidates")
        status, value = _one_field_value(fields, order_uuid_field_id)
        if status == "invalid":
            return review("ambiguous_identity")
        if status == "missing":
            continue
        if value is None or _canonical_uuid(value.get("value")) is None:
            return review("ambiguous_identity")
        if value["value"].lower() == order_uuid:
            matching.append(lead)

    if not matching:
        return review("missing_lead")
    if len(matching) != 1:
        return review("duplicate_lead")
    status, value = _one_field_value(matching[0]["custom_fields_values"], order_type_field_id)
    if status != "ok" or value is None or type(value.get("enum_id")) is not int:
        return review("invalid_lead_type")
    # amo may return both enum_id and a display value. A contradictory display
    # value makes the snapshot ambiguous even when enum_id alone looks right.
    if "value" in value:
        label_by_enum = {order_enum_id: "Заказ", preorder_enum_id: "Предзаказ"}
        if value.get("value") != label_by_enum.get(value["enum_id"]):
            return review("invalid_lead_type")
    expected_enum = order_enum_id if parsed.kind == "order" else preorder_enum_id
    if value["enum_id"] != expected_enum:
        return review("different_lead_type")
    return DryRunDecision("no-op", "already_correct", parsed)
