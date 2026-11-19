from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from adops.domain.reverse_sync_diff import (
    PREVIEW_SAMPLE_SIZE,
    Diff,
    PreviewSample,
    RowUpdate,
    compute_diff,
    normalize_value,
    preview_sample,
    writes_allowed,
)

MAPPING = {"customer_id": "customer_id", "orders": "order_count"}
SENTINEL = "SENTINEL_VALUE"


def diff_of(result, target, delete_missing=False, mapping=MAPPING):
    return compute_diff(result, target, "customer_id", mapping, delete_missing)


@pytest.mark.parametrize("name", ["stage", "prod"])
def test_only_stage_and_prod_write(name):
    assert writes_allowed(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "dev",
        "Stage",
        "PROD",
        " stage",
        "stage ",
        "staging",
        "production",
        "prod_copy",
        "",
        "feature_x",
    ],
)
def test_every_other_environment_gets_a_preview(name):
    assert writes_allowed(name) is False


def test_new_changed_unchanged_and_missing_rows_are_told_apart():
    result = [
        {"customer_id": 1, "orders": 3},
        {"customer_id": 2, "orders": 4},
        {"customer_id": 3, "orders": 1},
    ]
    target = [
        {"customer_id": 1, "order_count": 3},
        {"customer_id": 2, "order_count": 3},
        {"customer_id": 9, "order_count": 7},
    ]

    diff = diff_of(result, target)

    assert diff == Diff(
        key="customer_id",
        inserts=({"customer_id": 3, "order_count": 1},),
        updates=(RowUpdate(2, ("order_count",), {"order_count": 4}),),
        deletes=(),
        kept=(9,),
        delete_missing=False,
    )


def test_rows_missing_from_the_result_are_deleted_only_when_opted_in():
    result = [{"customer_id": 1, "orders": 3}]
    target = [{"customer_id": 1, "order_count": 3}, {"customer_id": 9, "order_count": 7}]

    kept = diff_of(result, target, delete_missing=False)
    deleted = diff_of(result, target, delete_missing=True)

    assert (kept.deletes, kept.kept, kept.delete_missing) == ((), (9,), False)
    assert (deleted.deletes, deleted.kept, deleted.delete_missing) == ((9,), (), True)


def test_the_mapping_renames_result_columns_before_comparing():
    diff = diff_of([{"customer_id": 1, "orders": 3}], [{"customer_id": 1, "order_count": 3}])

    assert (diff.inserts, diff.updates, diff.kept) == ((), (), ())


def test_a_key_left_out_of_the_mapping_keeps_its_name():
    diff = diff_of(
        [{"customer_id": 1, "orders": 4}],
        [{"customer_id": 1, "order_count": 3}],
        mapping={"orders": "order_count"},
    )

    assert diff.updates == (RowUpdate(1, ("order_count",), {"order_count": 4}),)


def test_a_mapped_key_is_matched_by_its_target_name():
    diff = compute_diff(
        [{"id": 1, "orders": 3}],
        [{"customer_id": 1, "order_count": 3}],
        "id",
        {"id": "customer_id", "orders": "order_count"},
        delete_missing=True,
    )

    assert diff.key == "customer_id"
    assert (diff.inserts, diff.updates, diff.deletes) == ((), (), ())


def test_an_update_names_only_the_changed_columns():
    mapping = MAPPING | {"total": "total_amount"}
    diff = diff_of(
        [{"customer_id": 1, "orders": 4, "total": Decimal("10.50")}],
        [{"customer_id": 1, "order_count": 3, "total_amount": 10.5}],
        mapping=mapping,
    )

    assert diff.updates == (RowUpdate(1, ("order_count",), {"order_count": 4}),)


@pytest.mark.parametrize(
    ("result_value", "target_value"),
    [
        (Decimal("10.50"), 10.5),
        (Decimal("0.1"), 0.1),
        (3, Decimal("3.00")),
        (3, 3.0),
        (Decimal("12345678901234567890.123456789"), Decimal("12345678901234567890.123456789")),
        (datetime(2031, 1, 1, 12), datetime(2031, 1, 1, 12, tzinfo=UTC)),
        (
            datetime(2031, 1, 1, 12, tzinfo=UTC),
            datetime(2031, 1, 1, 14, tzinfo=timezone(timedelta(hours=2))),
        ),
        (date(2031, 1, 1), date(2031, 1, 1)),
        ("a", "a"),
        (None, None),
        (True, True),
    ],
)
def test_equal_values_read_from_different_engines_are_unchanged(result_value, target_value):
    diff = diff_of(
        [{"customer_id": 1, "orders": result_value}],
        [{"customer_id": 1, "order_count": target_value}],
    )

    assert diff.updates == ()


@pytest.mark.parametrize(
    ("result_value", "target_value"),
    [
        (True, 1),
        (1, True),
        (False, 0),
        (True, False),
        (Decimal("12345678901234567890.1"), Decimal("12345678901234567890.2")),
        (datetime(2031, 1, 1, 12), datetime(2031, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))),
        (None, 0),
        ("a", "b"),
    ],
)
def test_different_values_are_updates(result_value, target_value):
    diff = diff_of(
        [{"customer_id": 1, "orders": result_value}],
        [{"customer_id": 1, "order_count": target_value}],
    )

    assert diff.updates == (RowUpdate(1, ("order_count",), {"order_count": result_value}),)


def test_keys_from_different_engines_match():
    diff = diff_of(
        [{"customer_id": Decimal("7"), "orders": 1}], [{"customer_id": 7, "order_count": 1}]
    )

    assert (diff.inserts, diff.updates, diff.kept) == ((), (), ())


def test_normalize_value_keeps_booleans_and_puts_timestamps_in_utc():
    assert normalize_value(True) is True
    assert normalize_value(datetime(2031, 1, 1, 12)) == datetime(2031, 1, 1, 12, tzinfo=UTC)
    assert normalize_value(datetime(2031, 1, 1, 12)).tzinfo is UTC
    assert normalize_value(0.1) == Decimal("0.1")


def test_a_diff_cannot_be_changed():
    diff = diff_of([], [])

    with pytest.raises(FrozenInstanceError):
        diff.delete_missing = True  # type: ignore[misc]


def many_kinds(count: int, delete_missing: bool) -> Diff:
    result = [{"customer_id": i, "orders": SENTINEL} for i in range(count)]
    result += [{"customer_id": 100 + i, "orders": SENTINEL} for i in range(count)]
    target = [{"customer_id": 100 + i, "order_count": f"old {SENTINEL}"} for i in range(count)]
    target += [{"customer_id": 200 + i, "order_count": SENTINEL} for i in range(count)]
    return diff_of(result, target, delete_missing=delete_missing)


def test_the_sample_holds_at_most_limit_keys_per_kind_in_order():
    sample = preview_sample(many_kinds(12, delete_missing=True), limit=5)

    assert sample == PreviewSample(
        inserts=(0, 1, 2, 3, 4),
        updates=tuple((100 + i, ("order_count",)) for i in range(5)),
        deletes=(200, 201, 202, 203, 204),
        kept=(),
    )


def test_the_sample_lists_kept_rows_when_deletes_are_off():
    sample = preview_sample(many_kinds(12, delete_missing=False))

    assert sample.deletes == ()
    assert sample.kept == tuple(200 + i for i in range(PREVIEW_SAMPLE_SIZE))
    assert PREVIEW_SAMPLE_SIZE == 5


def test_the_sample_never_carries_values():
    sample = preview_sample(many_kinds(12, delete_missing=True))

    assert SENTINEL not in repr(sample)
