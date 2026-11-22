import dataclasses

import pytest

from adops.domain.accepted_change_record import (
    AcceptedChangeRecord,
    cover,
    dump_accepted_record,
    load_accepted_record,
    parse_change,
)
from adops.domain.errors import PipelineValidationError, RuleViolation
from adops.domain.schema_snapshot import (
    BreakingChange,
    BreakingChangeKind,
    TableShape,
    find_breaking_changes,
    normalize_column_type,
)

DROPPED = BreakingChangeKind.DROPPED
RETYPED = BreakingChangeKind.RETYPED
SENTINEL = "SECRET_x"

FOUND_RETYPE, FOUND_DROP = find_breaking_changes(
    TableShape({"customer_id": "int4", "amount": "int4"}),
    TableShape({"amount": "text"}),
)


def record(record_id: str, *changes: str) -> AcceptedChangeRecord:
    return AcceptedChangeRecord(
        record_id, "shop_sync", "orders", tuple(parse_change(text) for text in changes)
    )


def test_found_changes_are_one_drop_and_one_retype():
    assert FOUND_DROP.kind is DROPPED
    assert FOUND_RETYPE.kind is RETYPED


def test_parse_reads_a_real_retype_message_back_to_the_same_change():
    assert parse_change(FOUND_RETYPE.message) == FOUND_RETYPE


def test_parse_reads_a_real_drop_message_without_the_old_type():
    parsed = parse_change(FOUND_DROP.message)

    assert parsed == BreakingChange("customer_id", DROPPED, "")
    assert parsed.message == FOUND_DROP.message


def test_parse_normalizes_both_retype_types():
    parsed = parse_change("amount retyped (int4 -> TEXT)")

    assert parsed.previous_type == normalize_column_type("int4")
    assert parsed.current_type == normalize_column_type("TEXT")


def test_parse_keeps_type_parameters():
    parsed = parse_change("amount retyped (numeric(10,2) -> numeric(12, 2))")

    assert (parsed.previous_type, parsed.current_type) == ("numeric(10, 2)", "numeric(12, 2)")


def test_parse_splits_a_column_name_that_contains_the_keywords():
    assert parse_change("old retyped (x) dropped") == BreakingChange("old retyped (x)", DROPPED, "")
    assert parse_change("a retyped b retyped (int -> text)") == BreakingChange(
        "a retyped b", RETYPED, "integer", "text"
    )
    assert parse_change("first name dropped").column == "first name"


@pytest.mark.parametrize(
    "text",
    [
        "amount removed",
        "amount retyped int -> text",
        "",
        " dropped",
        "dropped",
        "amount retyped ( -> text)",
        "amount retyped (int ->  )",
    ],
)
def test_parse_rejects_any_other_wording(text):
    with pytest.raises(RuleViolation):
        parse_change(text)


def test_dump_then_load_round_trips_a_drop_and_a_retype():
    original = record("20261122-orders", FOUND_DROP.message, FOUND_RETYPE.message)

    assert load_accepted_record("20261122-orders", dump_accepted_record(original)) == original


def test_dump_writes_message_wording_with_normalized_types():
    text = dump_accepted_record(record("r", "amount retyped (int4 -> TEXT)"))

    assert "amount retyped (integer -> text)" in text
    assert text == dump_accepted_record(record("r", "amount retyped (integer -> text)"))


def test_load_takes_the_id_from_the_caller():
    loaded = load_accepted_record(
        "my-id", "pipeline: shop_sync\ntable: orders\nchanges: [customer_id dropped]\n"
    )

    assert loaded == AcceptedChangeRecord(
        "my-id", "shop_sync", "orders", (BreakingChange("customer_id", DROPPED, ""),)
    )


def assert_sanitized(text: str) -> list[str]:
    with pytest.raises(PipelineValidationError) as caught:
        load_accepted_record("r", text)
    assert SENTINEL not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__
    return list(caught.value.problems)


def test_load_rejects_invalid_yaml():
    problems = assert_sanitized(f"pipeline: [{SENTINEL}\n")

    assert problems[0].startswith("(root): not valid YAML")


def test_load_rejects_a_top_level_list():
    assert assert_sanitized(f"- {SENTINEL}\n") == ["(root): must be a mapping of settings"]


def test_load_rejects_a_missing_pipeline():
    problems = assert_sanitized(f"table: {SENTINEL}\nchanges: [a dropped]\n")

    assert problems == ["pipeline: Field required"]


def test_load_rejects_a_missing_table():
    problems = assert_sanitized(f"pipeline: {SENTINEL}\nchanges: [a dropped]\n")

    assert problems == ["table: Field required"]


def test_load_rejects_empty_changes():
    problems = assert_sanitized(f"pipeline: {SENTINEL}\ntable: t\nchanges: []\n")

    assert [line.split(":")[0] for line in problems] == ["changes"]


def test_load_rejects_changes_that_are_not_a_list():
    problems = assert_sanitized(f"pipeline: p\ntable: t\nchanges: {SENTINEL} dropped\n")

    assert [line.split(":")[0] for line in problems] == ["changes"]


def test_load_rejects_a_non_string_change():
    problems = assert_sanitized(f"pipeline: {SENTINEL}\ntable: t\nchanges: [42]\n")

    assert [line.split(":")[0] for line in problems] == ["changes[0]"]


def test_load_rejects_an_unknown_key():
    problems = assert_sanitized(f"pipeline: p\ntable: t\nchanges: [a dropped]\nnote: {SENTINEL}\n")

    assert [line.split(":")[0] for line in problems] == ["note"]


def test_load_names_every_badly_worded_change_without_its_text():
    problems = assert_sanitized(
        f"pipeline: p\ntable: t\nchanges: [a dropped, {SENTINEL} removed, {SENTINEL} gone]\n"
    )

    assert [line.split(":")[0] for line in problems] == ["changes[1]", "changes[2]"]


def test_record_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        record("r", "a dropped").table = "other"  # type: ignore[misc]


def test_cover_full():
    coverage = cover(
        (FOUND_DROP, FOUND_RETYPE), [record("r1", FOUND_DROP.message, FOUND_RETYPE.message)], ()
    )

    assert coverage.uncovered == ()
    assert coverage.used_ids == ("r1",)


def test_cover_partial_leaves_only_the_uncovered_change():
    coverage = cover((FOUND_DROP, FOUND_RETYPE), [record("r1", FOUND_DROP.message)], ())

    assert coverage.uncovered == (FOUND_RETYPE,)
    assert coverage.used_ids == ("r1",)


def test_cover_by_two_records_together():
    records = [record("r1", FOUND_DROP.message), record("r2", FOUND_RETYPE.message)]

    coverage = cover((FOUND_DROP, FOUND_RETYPE), records, ())

    assert coverage.uncovered == ()
    assert coverage.used_ids == ("r1", "r2")


def test_cover_ignores_an_already_applied_record():
    coverage = cover((FOUND_DROP,), [record("r1", FOUND_DROP.message)], {"r1"})

    assert coverage.uncovered == (FOUND_DROP,)
    assert coverage.used_ids == ()


def test_cover_leaves_out_a_record_that_matches_nothing():
    records = [record("unused", "other dropped"), record("r1", FOUND_DROP.message)]

    coverage = cover((FOUND_DROP,), records, ())

    assert coverage.used_ids == ("r1",)


def test_cover_does_not_accept_a_different_retype_of_the_same_column():
    found = BreakingChange("amount", RETYPED, "integer", "bigint")

    coverage = cover((found,), [record("r1", "amount retyped (int -> text)")], ())

    assert coverage.uncovered == (found,)
    assert coverage.used_ids == ()


def test_cover_matches_retypes_on_normalized_types():
    found = BreakingChange("amount", RETYPED, "int4", "TEXT")

    coverage = cover((found,), [record("r1", "amount retyped (integer -> text)")], ())

    assert coverage.uncovered == ()


def test_cover_does_not_accept_a_retype_with_a_drop_record():
    coverage = cover((FOUND_RETYPE,), [record("r1", "amount dropped")], ())

    assert coverage.uncovered == (FOUND_RETYPE,)


def test_cover_by_a_record_listing_more_than_was_found():
    records = [record("r1", FOUND_DROP.message, FOUND_RETYPE.message, "extra dropped")]

    coverage = cover((FOUND_DROP,), records, ())

    assert coverage.uncovered == ()
    assert coverage.used_ids == ("r1",)


def test_cover_lists_each_used_record_once_in_record_order():
    records = [
        record("b", FOUND_RETYPE.message, FOUND_DROP.message),
        record("a", FOUND_DROP.message),
    ]

    coverage = cover((FOUND_RETYPE, FOUND_DROP), records, ())

    assert coverage.used_ids == ("b", "a")
