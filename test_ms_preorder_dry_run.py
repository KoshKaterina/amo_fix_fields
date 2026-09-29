"""Pure dry-run decisions only: no service imports, credentials or API calls."""

from ms_preorder_dry_run import decide_existing_amgroup_lead
from ms_preorder_type import ParsedOrderType


ORDER_UUID = "123e4567-e89b-42d3-a456-426614174010"
OTHER_UUID = "123e4567-e89b-42d3-a456-426614174011"
ATTR_UUID = "123e4567-e89b-42d3-a456-426614174000"
ORDER_FIELD = 101
TYPE_FIELD = 102
ORDER_ENUM = 201
PREORDER_ENUM = 202
ORDINARY = "Тип: Заказ\nОжидаемые позиции: нет"
PREORDER = "Тип: Предзаказ\nОжидаемые позиции:\n- Product [SKU: TEST] × 1"


def order(summary=ORDINARY, *, attribute=True):
    attributes = [{"id": ATTR_UUID, "value": summary}] if attribute else []
    return {"id": ORDER_UUID, "attributes": attributes}


def lead(uuid=ORDER_UUID, enum=ORDER_ENUM, *, lead_id=301):
    return {"id": lead_id, "custom_fields_values": [
        {"field_id": ORDER_FIELD, "values": [{"value": uuid}]},
        {"field_id": TYPE_FIELD, "values": [{"enum_id": enum}]},
    ]}


def decide(customerorder, candidates, *, complete=True, **overrides):
    args = dict(candidates_complete=complete, order_uuid_field_id=ORDER_FIELD,
                order_type_field_id=TYPE_FIELD, order_enum_id=ORDER_ENUM,
                preorder_enum_id=PREORDER_ENUM, verified_amgroup_source=True,
                post_cutoff=True)
    args.update(overrides)
    return decide_existing_amgroup_lead(customerorder, ATTR_UUID, candidates, **args)


def test_ordinary_order_already_correct_is_no_op():
    result = decide(order(), [lead(), lead(OTHER_UUID, lead_id=302)])
    assert (result.action, result.reason, result.parsed) == (
        "no-op", "already_correct", ParsedOrderType("order", "ok"))


def test_preorder_already_correct_is_no_op():
    result = decide(order(PREORDER), [lead(enum=PREORDER_ENUM)])
    assert (result.action, result.reason, result.parsed) == (
        "no-op", "already_correct", ParsedOrderType("preorder", "ok"))


def test_wrong_type_never_proposes_patch_even_for_unique_lead():
    result = decide(order(PREORDER), [lead(enum=ORDER_ENUM)])
    assert (result.action, result.reason) == ("manual-review", "different_lead_type")


def test_missing_or_duplicate_order_uuid_needs_review():
    missing = decide(order(), [lead(OTHER_UUID)])
    duplicate = decide(order(), [lead(), lead(lead_id=302)])
    duplicate_field = lead()
    duplicate_field["custom_fields_values"].append(
        {"field_id": ORDER_FIELD, "values": [{"value": ORDER_UUID}]})
    assert (missing.action, missing.reason) == ("manual-review", "missing_lead")
    assert (duplicate.action, duplicate.reason) == ("manual-review", "duplicate_lead")
    duplicate_in_one_lead = decide(order(), [duplicate_field])
    assert (duplicate_in_one_lead.action, duplicate_in_one_lead.reason) == (
        "manual-review", "ambiguous_identity")


def test_unknown_ms_attribute_requires_review_even_if_crm_enum_is_order():
    result = decide(order(attribute=False), [lead()])
    assert (result.action, result.reason, result.parsed) == (
        "manual-review", "unknown_order_type", ParsedOrderType("unknown", "attribute_not_found"))


def test_other_enum_requires_review():
    result = decide(order(), [lead(enum=203)])
    assert (result.action, result.reason) == ("manual-review", "different_lead_type")


def test_conflicting_amo_enum_and_text_cannot_be_no_op():
    conflicting = lead(enum=ORDER_ENUM)
    conflicting["custom_fields_values"][1]["values"][0]["value"] = "Предзаказ"
    result = decide(order(), [conflicting])
    assert (result.action, result.reason) == ("manual-review", "invalid_lead_type")


def test_consistent_amo_enum_and_text_can_be_no_op():
    consistent = lead(enum=PREORDER_ENUM)
    consistent["custom_fields_values"][1]["values"][0]["value"] = "Предзаказ"
    result = decide(order(PREORDER), [consistent])
    assert (result.action, result.reason) == ("no-op", "already_correct")


def test_invalid_config_is_manual_review():
    assert decide(order(), [lead()], order_uuid_field_id=TYPE_FIELD).reason == "invalid_config"
    assert decide(order(), [lead()], order_enum_id=PREORDER_ENUM).reason == "invalid_config"
    assert decide(order(), [lead()], order_type_field_id=True).reason == "invalid_config"


def test_damaged_candidates_are_manual_review():
    assert decide(order(), None).reason == "invalid_candidates"
    assert decide(order(), [{"id": 301, "custom_fields_values": None}]).reason == "invalid_candidates"
    malformed = lead()
    malformed["custom_fields_values"][0]["field_id"] = "101"
    assert decide(order(), [malformed]).reason == "invalid_candidates"


def test_incomplete_candidates_cannot_establish_uniqueness():
    result = decide(order(PREORDER), [lead(enum=PREORDER_ENUM)], complete=False)
    assert (result.action, result.reason) == ("manual-review", "incomplete_candidates")


def test_source_and_cutoff_must_be_independently_verified():
    source_unknown = decide(order(PREORDER), [lead(enum=PREORDER_ENUM)],
                            verified_amgroup_source=False)
    cutoff_unknown = decide(order(PREORDER), [lead(enum=PREORDER_ENUM)], post_cutoff=False)
    assert (source_unknown.action, source_unknown.reason) == (
        "manual-review", "unverified_amgroup_source")
    assert (cutoff_unknown.action, cutoff_unknown.reason) == (
        "manual-review", "unverified_cutoff")
    direct = decide_existing_amgroup_lead(
        order(PREORDER), ATTR_UUID, [lead(enum=PREORDER_ENUM)],
        candidates_complete=True, order_uuid_field_id=ORDER_FIELD,
        order_type_field_id=TYPE_FIELD, order_enum_id=ORDER_ENUM,
        preorder_enum_id=PREORDER_ENUM)
    assert (direct.action, direct.reason) == ("manual-review", "unverified_amgroup_source")


def test_uncertain_identity_or_type_never_becomes_no_op():
    uncertain = lead()
    uncertain["custom_fields_values"][0]["values"] = [{"value": ORDER_UUID}, {"value": OTHER_UUID}]
    assert decide(order(), [uncertain]).reason == "ambiguous_identity"
    unknown_enum = lead()
    unknown_enum["custom_fields_values"][1]["values"] = [{"value": "Заказ"}]
    assert decide(order(), [unknown_enum]).reason == "invalid_lead_type"
    assert decide({**order(), "id": "not-a-uuid"}, [lead()]).reason == "invalid_order_uuid"


def test_same_lead_id_with_inconsistent_snapshots_is_invalid_input():
    malformed_duplicate_id = [lead(lead_id=301), lead(OTHER_UUID, lead_id=301)]
    assert decide(order(), malformed_duplicate_id).reason == "invalid_candidates"


def test_no_result_includes_customer_or_lead_details():
    candidate = lead()
    candidate["name"] = "private customer name"
    result = decide(order(PREORDER), [candidate])
    assert "private customer name" not in repr(result)
    assert ORDER_UUID not in repr(result)
