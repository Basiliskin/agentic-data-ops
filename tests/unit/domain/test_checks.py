import copy
from datetime import UTC, datetime, timedelta, timezone

import pytest

from adops.domain.checks import (
    CheckName,
    CheckResult,
    CheckStatus,
    LoadFacts,
    ReverseSyncFacts,
    SourceFacts,
    evaluate_declared_checks,
    evaluate_load_checks,
    evaluate_preflight_checks,
    evaluate_reverse_sync,
)
from adops.domain.pipeline import (
    AcceptedValuesCheck,
    FreshnessCheck,
    MinRowsCheck,
    NotNullCheck,
    SkipChecks,
    SyncTable,
)
from adops.domain.reverse_sync import ReverseSyncStep

FULL = SyncTable(name="customers", mode="full_refresh", primary_key="id")
INCREMENTAL = SyncTable(name="orders", mode="incremental", primary_key="id", cursor="updated_at")
JAN = datetime(2031, 1, 1)
FEB = datetime(2031, 2, 1)


def load_facts(**overrides) -> LoadFacts:
    facts = {
        "warehouse_rows_before": 10,
        "warehouse_rows_after": 10,
        "duplicate_keys": 0,
        "null_keys": 0,
        "source_rows": 10,
        "cursor_max_before": JAN,
        "cursor_max_after": JAN,
    }
    return LoadFacts(**(facts | overrides))


def statuses(results: list[CheckResult]) -> dict[str, CheckStatus]:
    return {result.check: result.status for result in results}


def status_of(table: SyncTable, check: CheckName, facts: LoadFacts) -> CheckStatus:
    return statuses(evaluate_load_checks(table, facts))[check]


@pytest.mark.parametrize(
    ("table", "facts", "status", "detail"),
    [
        (INCREMENTAL, SourceFacts(True, frozenset({"id", "updated_at", "x"})), "pass", "passed"),
        (INCREMENTAL, SourceFacts(False, frozenset()), "fail", "table not found in the Source"),
        (
            INCREMENTAL,
            SourceFacts(True, frozenset({"x"})),
            "fail",
            "missing columns in the Source: id, updated_at",
        ),
        (FULL, SourceFacts(True, frozenset({"id"})), "pass", "passed"),
        (FULL, SourceFacts(True, frozenset()), "fail", "missing columns in the Source: id"),
    ],
)
def test_source_tables_exist(table, facts, status, detail):
    result, _ = evaluate_preflight_checks(table, facts)

    assert (result.check, result.table, result.status) == (
        CheckName.SOURCE_TABLES_EXIST,
        table.name,
        status,
    )
    assert result.message.startswith(f"{table.name}: source_tables_exist")
    assert result.message.endswith(detail)


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
@pytest.mark.parametrize(
    ("check", "field"),
    [
        (CheckName.PRIMARY_KEY_UNIQUE, "duplicate_keys"),
        (CheckName.PRIMARY_KEY_NOT_NULL, "null_keys"),
    ],
)
@pytest.mark.parametrize(("count", "status"), [(0, "pass"), (1, "fail"), (5, "fail")])
def test_primary_key_checks(table, check, field, count, status):
    assert status_of(table, check, load_facts(**{field: count})) == status


@pytest.mark.parametrize(
    ("after", "source", "status"),
    [(10, 10, "pass"), (9, 10, "fail"), (11, 10, "fail"), (0, 0, "pass"), (10, None, "fail")],
)
def test_row_count_matches(after, source, status):
    facts = load_facts(warehouse_rows_after=after, source_rows=source)

    assert status_of(FULL, CheckName.ROW_COUNT_MATCHES, facts) == status


@pytest.mark.parametrize(
    ("before", "after", "status"),
    [(10, 10, "pass"), (10, 11, "pass"), (10, 9, "fail"), (0, 0, "pass")],
)
def test_no_row_loss(before, after, status):
    facts = load_facts(warehouse_rows_before=before, warehouse_rows_after=after)

    assert status_of(INCREMENTAL, CheckName.NO_ROW_LOSS, facts) == status


@pytest.mark.parametrize(
    ("before", "after", "status"),
    [
        (JAN, JAN, "pass"),
        (JAN, FEB, "pass"),
        (FEB, JAN, "fail"),
        (None, None, "pass"),
        (None, JAN, "pass"),
        (JAN, None, "fail"),
        (7, 7, "pass"),
        (7, 6, "fail"),
    ],
)
def test_cursor_not_backwards(before, after, status):
    facts = load_facts(cursor_max_before=before, cursor_max_after=after)

    assert status_of(INCREMENTAL, CheckName.CURSOR_NOT_BACKWARDS, facts) == status


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
def test_an_empty_table_passes_every_applicable_check(table):
    facts = LoadFacts(
        warehouse_rows_before=0,
        warehouse_rows_after=0,
        duplicate_keys=0,
        null_keys=0,
        source_rows=0,
    )

    assert set(statuses(evaluate_load_checks(table, facts)).values()) == {CheckStatus.PASS}


def test_each_mode_yields_only_its_own_load_checks_in_a_fixed_order():
    assert [r.check for r in evaluate_load_checks(FULL, load_facts())] == [
        "primary_key_unique",
        "primary_key_not_null",
        "row_count_matches",
    ]
    assert [r.check for r in evaluate_load_checks(INCREMENTAL, load_facts())] == [
        "primary_key_unique",
        "primary_key_not_null",
        "no_row_loss",
        "cursor_not_backwards",
    ]


def test_same_facts_always_give_the_same_results():
    facts = load_facts(duplicate_keys=2, cursor_max_after=datetime(2030, 1, 1))
    source = SourceFacts(True, frozenset({"id"}))

    first = evaluate_load_checks(INCREMENTAL, facts)

    assert first == evaluate_load_checks(INCREMENTAL, facts)
    assert first == evaluate_load_checks(INCREMENTAL, copy.deepcopy(facts))
    assert evaluate_preflight_checks(FULL, source) == evaluate_preflight_checks(
        FULL, copy.deepcopy(source)
    )


@pytest.mark.parametrize(
    "value",
    [load_facts(), SourceFacts(True, frozenset()), CheckResult("c", "t", CheckStatus.PASS, "m")],
)
def test_facts_and_results_are_immutable(value):
    with pytest.raises(AttributeError):
        value.table = "other"


def test_source_facts_carry_column_types_when_measured():
    assert SourceFacts(True, frozenset({"id"})).column_types == {}
    assert SourceFacts(True, frozenset({"id"}), {"id": "integer"}).column_types == {"id": "integer"}


def test_messages_name_table_and_check_but_never_cursor_values():
    leaking = "LEAK-2031-01-01"
    facts = load_facts(
        cursor_max_before=leaking,
        cursor_max_after="LEAK-2030-01-01",
        duplicate_keys=1,
        null_keys=1,
        warehouse_rows_after=3,
    )

    results = evaluate_load_checks(INCREMENTAL, facts)

    assert {r.status for r in results} == {CheckStatus.FAIL}
    for result in results:
        assert "LEAK" not in result.message
        assert result.message.startswith(f"orders: {result.check} failed — ")


def test_a_skipped_check_is_reported_with_its_reason_and_not_evaluated():
    table = INCREMENTAL.model_copy(
        update={"skip_checks": SkipChecks(checks=["primary_key_unique"], reason="dupes by design")}
    )

    results = {
        r.check: r for r in evaluate_load_checks(table, load_facts(duplicate_keys=3, null_keys=1))
    }

    skipped = results[CheckName.PRIMARY_KEY_UNIQUE]
    assert skipped.status is CheckStatus.SKIPPED
    assert skipped.message == "orders: primary_key_unique skipped — dupes by design"
    assert results[CheckName.PRIMARY_KEY_NOT_NULL].status is CheckStatus.FAIL
    assert results[CheckName.NO_ROW_LOSS].status is CheckStatus.PASS


def test_source_tables_exist_can_be_skipped():
    table = FULL.model_copy(
        update={"skip_checks": SkipChecks(checks=["source_tables_exist"], reason="view")}
    )

    result, _ = evaluate_preflight_checks(table, SourceFacts(False, frozenset()))

    assert (result.status, result.message) == (
        CheckStatus.SKIPPED,
        "customers: source_tables_exist skipped — view",
    )


def test_preflight_yields_source_tables_exist_then_source_not_empty():
    results = evaluate_preflight_checks(FULL, SourceFacts(True, frozenset({"id"}), row_count=3))

    assert [r.check for r in results] == ["source_tables_exist", "source_not_empty"]


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
@pytest.mark.parametrize(
    ("row_count", "status"),
    [(0, CheckStatus.FAIL), (1, CheckStatus.PASS), (None, CheckStatus.PASS)],
)
def test_source_not_empty_fails_only_on_a_measured_empty_table(table, row_count, status):
    facts = SourceFacts(True, frozenset({"id", "updated_at"}), row_count=row_count)

    assert statuses(evaluate_preflight_checks(table, facts))["source_not_empty"] is status


def test_an_empty_source_table_fails_with_a_fixed_message():
    facts = SourceFacts(True, frozenset({"id"}), row_count=0)

    _, result = evaluate_preflight_checks(FULL, facts)

    assert result.message == (
        "customers: source_not_empty failed — table customers is empty in the Source"
    )


def test_a_table_missing_from_the_source_is_not_also_reported_empty():
    results = evaluate_preflight_checks(FULL, SourceFacts(False, frozenset()))

    assert statuses(results) == {
        "source_tables_exist": CheckStatus.FAIL,
        "source_not_empty": CheckStatus.PASS,
    }


def test_source_not_empty_can_be_skipped_with_its_reason():
    table = INCREMENTAL.model_copy(
        update={"skip_checks": SkipChecks(checks=["source_not_empty"], reason="filled weekly")}
    )

    _, result = evaluate_preflight_checks(
        table, SourceFacts(True, frozenset({"id", "updated_at"}), row_count=0)
    )

    assert (result.status, result.message) == (
        CheckStatus.SKIPPED,
        "orders: source_not_empty skipped — filled weekly",
    )


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
def test_source_not_empty_applies_to_every_mode_as_a_preflight_check(table):
    assert CheckName.SOURCE_NOT_EMPTY in table.applicable_checks()
    assert CheckName.SOURCE_NOT_EMPTY not in statuses(evaluate_load_checks(table, load_facts()))


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
def test_without_skip_checks_nothing_is_skipped(table):
    results = evaluate_load_checks(table, load_facts()) + evaluate_preflight_checks(
        table, SourceFacts(True, frozenset({"id", "updated_at"}))
    )

    assert CheckStatus.SKIPPED not in {r.status for r in results}


NOW = datetime(2031, 3, 1, 12, 0, tzinfo=UTC)
DECLARED = FULL.model_copy(
    update={
        "checks": [
            NotNullCheck(type="not_null", column="email"),
            AcceptedValuesCheck(type="accepted_values", column="status", values=["a", "b"]),
            MinRowsCheck(type="min_rows", count=10),
            FreshnessCheck(type="freshness", column="updated_at", max_age_hours=24),
        ]
    }
)


def declared_facts(**overrides) -> LoadFacts:
    measured = {
        "null_counts": {"email": 0},
        "unexpected_value_counts": {"status": 0},
        "newest_values": {"updated_at": NOW},
    }
    return load_facts(**(measured | overrides))


def declared(name: str, facts: LoadFacts, now: datetime = NOW) -> CheckResult:
    return {r.check: r for r in evaluate_declared_checks(DECLARED, facts, now)}[name]


def test_declared_checks_yield_one_result_each_in_declaration_order():
    results = evaluate_declared_checks(DECLARED, declared_facts(), NOW)

    assert [(r.check, r.table, r.status) for r in results] == [
        ("not_null(email)", "customers", CheckStatus.PASS),
        ("accepted_values(status)", "customers", CheckStatus.PASS),
        ("min_rows", "customers", CheckStatus.PASS),
        ("freshness(updated_at)", "customers", CheckStatus.PASS),
    ]
    assert results[0].message == "customers: not_null(email) passed"


def test_a_table_without_declared_checks_yields_no_declared_results():
    assert evaluate_declared_checks(FULL, declared_facts(), NOW) == []


@pytest.mark.parametrize(
    ("nulls", "status", "message"),
    [
        ({"email": 0}, "pass", "customers: not_null(email) passed"),
        ({"email": 1}, "fail", "customers: not_null(email) failed — 1 rows with no value"),
        ({}, "fail", "customers: not_null(email) failed — the null count was not measured"),
    ],
)
def test_not_null(nulls, status, message):
    result = declared("not_null(email)", declared_facts(null_counts=nulls))

    assert (result.status, result.message) == (status, message)


@pytest.mark.parametrize(
    ("unexpected", "status", "detail"),
    [
        ({"status": 0}, "pass", None),
        ({"status": 1}, "fail", "1 rows with a value outside the accepted list"),
        ({}, "fail", "the count of values outside the accepted list was not measured"),
    ],
)
def test_accepted_values(unexpected, status, detail):
    result = declared("accepted_values(status)", declared_facts(unexpected_value_counts=unexpected))

    assert result.status == status
    if detail is not None:
        assert result.message == f"customers: accepted_values(status) failed — {detail}"


@pytest.mark.parametrize(("rows", "status"), [(10, "pass"), (11, "pass"), (9, "fail")])
def test_min_rows(rows, status):
    assert declared("min_rows", declared_facts(warehouse_rows_after=rows)).status == status


def test_min_rows_counts_the_warehouse_table_not_the_rows_loaded_this_run():
    facts = declared_facts(warehouse_rows_before=10, warehouse_rows_after=10, source_rows=0)
    short = declared_facts(warehouse_rows_before=0, warehouse_rows_after=9, source_rows=50)

    assert declared("min_rows", facts).status is CheckStatus.PASS
    assert declared("min_rows", short).message == (
        "customers: min_rows failed — Warehouse has 9 rows, at least 10 required"
    )


@pytest.mark.parametrize(
    ("newest", "status"),
    [
        (NOW, "pass"),
        (NOW + timedelta(hours=1), "pass"),
        (NOW - timedelta(hours=24), "pass"),
        (NOW - timedelta(hours=24, seconds=1), "fail"),
        (datetime(2031, 2, 28, 12, 0), "pass"),
        (datetime(2031, 2, 28, 11, 59, 59), "fail"),
        (datetime(2031, 2, 28, 14, 0, tzinfo=timezone(timedelta(hours=2))), "pass"),
        (datetime(2031, 2, 28, 13, 59, 59, tzinfo=timezone(timedelta(hours=2))), "fail"),
        (datetime(2031, 2, 28, 7, 0, tzinfo=timezone(timedelta(hours=-5))), "pass"),
        (datetime(2031, 2, 28, 6, 59, 59, tzinfo=timezone(timedelta(hours=-5))), "fail"),
    ],
)
def test_freshness_boundaries_across_naive_and_offset_timestamps(newest, status):
    facts = declared_facts(newest_values={"updated_at": newest})

    assert declared("freshness(updated_at)", facts).status == status


def test_freshness_treats_a_naive_now_as_utc():
    facts = declared_facts(newest_values={"updated_at": NOW - timedelta(hours=25)})

    result = declared("freshness(updated_at)", facts, now=NOW.replace(tzinfo=None))

    assert result.status is CheckStatus.FAIL


@pytest.mark.parametrize(
    ("newest_values", "detail"),
    [
        ({"updated_at": None}, "the column has no values"),
        ({}, "the newest value was not measured"),
    ],
)
def test_freshness_fails_without_a_newest_value(newest_values, detail):
    result = declared("freshness(updated_at)", declared_facts(newest_values=newest_values))

    assert result.message == f"customers: freshness(updated_at) failed — {detail}"


def test_declared_check_messages_hold_counts_and_ages_but_no_row_values():
    stale = datetime(2031, 2, 27, 9, 30, 15)
    facts = declared_facts(
        null_counts={"email": 4},
        unexpected_value_counts={"status": 7},
        newest_values={"updated_at": stale},
        warehouse_rows_after=3,
    )

    results = {r.check: r for r in evaluate_declared_checks(DECLARED, facts, NOW)}

    assert {r.status for r in results.values()} == {CheckStatus.FAIL}
    assert results["accepted_values(status)"].message == (
        "customers: accepted_values(status) failed — 7 rows with a value outside the accepted list"
    )
    freshness = results["freshness(updated_at)"].message
    assert freshness.endswith("the newest value is 50.5 hours old, more than 24 hours")
    for result in results.values():
        assert stale.isoformat() not in result.message
        assert "2031" not in result.message
        assert result.message.startswith(f"customers: {result.check} failed — ")


def test_a_skipped_declared_check_is_reported_with_its_reason_and_not_evaluated():
    table = DECLARED.model_copy(
        update={"skip_checks": SkipChecks(checks=["not_null(email)"], reason="backfill")}
    )

    results = {
        r.check: r
        for r in evaluate_declared_checks(table, declared_facts(null_counts={"email": 3}), NOW)
    }

    assert results["not_null(email)"].status is CheckStatus.SKIPPED
    assert results["not_null(email)"].message == "customers: not_null(email) skipped — backfill"
    assert results["min_rows"].status is CheckStatus.PASS


def test_same_facts_and_now_always_give_the_same_declared_results():
    facts = declared_facts(null_counts={"email": 2})

    first = evaluate_declared_checks(DECLARED, facts, NOW)

    assert first == evaluate_declared_checks(DECLARED, copy.deepcopy(facts), NOW)


def test_built_in_and_declared_results_compare_by_check_name():
    result = CheckResult(CheckName.PRIMARY_KEY_UNIQUE, "t", CheckStatus.PASS, "m")

    assert result.check == "primary_key_unique"
    assert load_facts().null_counts == {}
    assert load_facts().unexpected_value_counts == {}
    assert load_facts().newest_values == {}


PUSH = ReverseSyncStep(
    type="reverse_sync",
    name="push_stats",
    model="customer_stats",
    target="shop",
    target_table="customer_stats",
    key="customer_id",
    mapping={"customer_id": "customer_id", "orders": "order_count"},
)


def reverse_facts(**overrides) -> ReverseSyncFacts:
    facts = {
        "target_table_exists": True,
        "missing_target_columns": (),
        "result_rows": 10,
        "duplicate_keys": 0,
        "null_keys": 0,
        "target_rows": 10,
        "diff_updates": 0,
        "diff_deletes": 0,
    }
    return ReverseSyncFacts(**(facts | overrides))


def reverse_result(check: CheckName, facts: ReverseSyncFacts, step=PUSH) -> CheckResult:
    return {result.check: result for result in evaluate_reverse_sync(step, facts)}[check]


def test_a_clean_reverse_sync_passes_every_check_in_order():
    results = evaluate_reverse_sync(PUSH, reverse_facts())

    assert isinstance(results, tuple)
    assert [result.check for result in results] == [
        "target_columns_exist",
        "key_unique",
        "key_not_null",
        "max_change",
        "result_not_empty",
    ]
    assert {result.status for result in results} == {CheckStatus.PASS}
    assert results[0] == CheckResult(
        "target_columns_exist",
        "customer_stats",
        CheckStatus.PASS,
        "customer_stats: target_columns_exist passed",
    )


@pytest.mark.parametrize(
    ("step", "facts", "status"),
    [
        (PUSH, reverse_facts(diff_updates=5), "pass"),
        (PUSH, reverse_facts(diff_updates=6), "fail"),
        (PUSH, reverse_facts(diff_updates=3, diff_deletes=3), "pass"),
        (
            PUSH.model_copy(update={"delete_missing": True}),
            reverse_facts(diff_updates=3, diff_deletes=2),
            "pass",
        ),
        (
            PUSH.model_copy(update={"delete_missing": True}),
            reverse_facts(diff_updates=3, diff_deletes=3),
            "fail",
        ),
        (
            PUSH.model_copy(update={"max_change_share": 0.3}),
            reverse_facts(diff_updates=3),
            "pass",
        ),
        (
            PUSH.model_copy(update={"max_change_share": 0.3}),
            reverse_facts(diff_updates=4),
            "fail",
        ),
        (PUSH, reverse_facts(target_rows=0, result_rows=40), "pass"),
    ],
)
def test_max_change_fails_only_above_the_allowed_share(step, facts, status):
    assert reverse_result(CheckName.MAX_CHANGE, facts, step).status == status


def test_max_change_failure_reports_counts():
    result = reverse_result(CheckName.MAX_CHANGE, reverse_facts(diff_updates=6))

    assert result.message == (
        "customer_stats: max_change failed — 6 of 10 target rows would change, "
        "more than the allowed share 0.5"
    )


@pytest.mark.parametrize(
    ("check", "facts", "detail"),
    [
        (
            CheckName.RESULT_NOT_EMPTY,
            reverse_facts(result_rows=0),
            "the dbt model customer_stats has no rows",
        ),
        (
            CheckName.KEY_UNIQUE,
            reverse_facts(duplicate_keys=1),
            "1 duplicate customer_id values in the dbt result",
        ),
        (
            CheckName.KEY_NOT_NULL,
            reverse_facts(null_keys=1),
            "1 rows with an empty customer_id in the dbt result",
        ),
        (
            CheckName.TARGET_COLUMNS_EXIST,
            reverse_facts(target_table_exists=False),
            "table customer_stats not found in the target shop",
        ),
        (
            CheckName.TARGET_COLUMNS_EXIST,
            reverse_facts(missing_target_columns=("order_count",)),
            "missing columns in the target: order_count",
        ),
    ],
)
def test_reverse_sync_checks_fail_on_bad_facts(check, facts, detail):
    result = reverse_result(check, facts)

    assert result.status == CheckStatus.FAIL
    assert result.message == f"customer_stats: {check} failed — {detail}"


def test_a_skipped_reverse_sync_check_carries_its_reason():
    step = PUSH.model_copy(
        update={"skip_checks": SkipChecks(checks=["max_change"], reason="first full load")}
    )

    results = evaluate_reverse_sync(step, reverse_facts(diff_updates=10))

    assert reverse_result(
        CheckName.MAX_CHANGE, reverse_facts(diff_updates=10), step
    ) == CheckResult(
        "max_change",
        "customer_stats",
        CheckStatus.SKIPPED,
        "customer_stats: max_change skipped — first full load",
    )
    assert [r.status for r in results].count(CheckStatus.SKIPPED) == 1
