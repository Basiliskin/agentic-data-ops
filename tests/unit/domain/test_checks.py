import copy
from datetime import datetime

import pytest

from adops.domain.checks import (
    CheckName,
    CheckResult,
    CheckStatus,
    LoadFacts,
    SourceFacts,
    evaluate_load_checks,
    evaluate_preflight_checks,
)
from adops.domain.pipeline import SkipChecks, SyncTable

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
    (result,) = evaluate_preflight_checks(table, facts)

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

    (result,) = evaluate_preflight_checks(table, SourceFacts(False, frozenset()))

    assert (result.status, result.message) == (
        CheckStatus.SKIPPED,
        "customers: source_tables_exist skipped — view",
    )


@pytest.mark.parametrize("table", [FULL, INCREMENTAL])
def test_without_skip_checks_nothing_is_skipped(table):
    results = evaluate_load_checks(table, load_facts()) + evaluate_preflight_checks(
        table, SourceFacts(True, frozenset({"id", "updated_at"}))
    )

    assert CheckStatus.SKIPPED not in {r.status for r in results}
