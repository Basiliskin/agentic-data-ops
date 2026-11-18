import pytest

from adops.domain.checks import CheckName, SyncMode
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline
from adops.domain.pipeline import FreshnessCheck, Pipeline, SyncTable

VALID = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
"""


def problems_of(text: str) -> tuple[str, ...]:
    with pytest.raises(PipelineValidationError) as caught:
        load_pipeline(text)
    return caught.value.problems


def pipeline_with_tables(tables: str) -> str:
    return f"""
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
{tables}
"""


def test_valid_pipeline_loads_into_steps_and_tables():
    pipeline = load_pipeline(VALID)

    assert isinstance(pipeline, Pipeline)
    step = pipeline.steps[0]
    assert (step.name, step.source) == ("sync_shop", "shop")
    assert [(t.name, t.mode, t.primary_key, t.cursor) for t in step.tables] == [
        ("customers", SyncMode.FULL_REFRESH, "id", None),
        ("orders", SyncMode.INCREMENTAL, "id", "updated_at"),
    ]


def test_unknown_key_is_rejected_at_its_path():
    text = VALID.replace(
        "        cursor: updated_at", "        cursor: updated_at\n        where: x"
    )

    assert problems_of(text) == ("steps[0].tables[1].where: Extra inputs are not permitted",)


def test_empty_table_list_is_rejected():
    text = pipeline_with_tables("      []")

    assert problems_of(text)[0].startswith("steps[0].tables: List should have at least 1 item")


def test_pipeline_without_steps_is_rejected():
    assert problems_of("name: shop_sync\nsteps: []")[0].startswith("steps: List should have")


@pytest.mark.parametrize("mode", ["full_refresh", "incremental"])
def test_table_without_primary_key_is_rejected(mode):
    text = pipeline_with_tables(f"      - {{name: t, mode: {mode}, cursor: updated_at}}")

    assert problems_of(text) == ("steps[0].tables[0].primary_key: Field required",)


def test_incremental_table_without_cursor_is_rejected():
    text = pipeline_with_tables("      - {name: t, mode: incremental, primary_key: id}")

    assert problems_of(text) == ("steps[0].tables[0].cursor: required for an incremental table",)


def test_mode_other_than_full_refresh_or_incremental_is_rejected():
    text = pipeline_with_tables("      - {name: t, mode: cdc, primary_key: id}")

    assert problems_of(text) == (
        "steps[0].tables[0].mode: Input should be 'full_refresh' or 'incremental'",
    )


def test_duplicate_step_names_are_rejected_at_each_duplicate():
    text = (
        VALID
        + """  - name: sync_shop
    source: shop
    tables:
      - {name: t, mode: full_refresh, primary_key: id}
"""
    )

    assert problems_of(text) == (
        "steps[0].name: another Step already has this name",
        "steps[1].name: another Step already has this name",
    )


@pytest.mark.parametrize("name", ["Sync-Shop", "1st", ""])
def test_step_name_must_be_a_lowercase_identifier(name):
    text = VALID.replace("name: sync_shop", f"name: '{name}'")

    assert problems_of(text)[0].startswith("steps[0].name: String should match pattern")


def test_skip_checks_with_a_reason_is_accepted():
    text = pipeline_with_tables(
        "      - name: t\n"
        "        mode: full_refresh\n"
        "        primary_key: id\n"
        "        skip_checks: {checks: [primary_key_unique], reason: keys repeat by design}"
    )

    table = load_pipeline(text).steps[0].tables[0]

    assert table.skip_reason(CheckName.PRIMARY_KEY_UNIQUE) == "keys repeat by design"
    assert table.skip_reason(CheckName.PRIMARY_KEY_NOT_NULL) is None


def test_skip_checks_without_reason_is_rejected():
    text = pipeline_with_tables(
        "      - {name: t, mode: full_refresh, primary_key: id,"
        " skip_checks: {checks: [primary_key_unique]}}"
    )

    assert problems_of(text) == ("steps[0].tables[0].skip_checks.reason: Field required",)


@pytest.mark.parametrize("reason", ["''", "'   '"])
def test_skip_checks_with_blank_reason_is_rejected(reason):
    text = pipeline_with_tables(
        "      - {name: t, mode: full_refresh, primary_key: id,"
        f" skip_checks: {{checks: [primary_key_unique], reason: {reason}}}}}"
    )

    assert problems_of(text) == (
        "steps[0].tables[0].skip_checks.reason: must explain why these Checks are skipped",
    )


def test_skip_checks_with_an_empty_check_list_is_rejected():
    text = pipeline_with_tables(
        "      - {name: t, mode: full_refresh, primary_key: id,"
        " skip_checks: {checks: [], reason: r}}"
    )

    assert problems_of(text)[0].startswith("steps[0].tables[0].skip_checks.checks: List should")


def test_unknown_check_name_is_rejected_listing_the_allowed_names_without_echoing_it():
    text = pipeline_with_tables(
        "      - {name: t, mode: full_refresh, primary_key: id,"
        " skip_checks: {checks: [primary_key_unique, LEAK-7c1e], reason: r}}"
    )

    (problem,) = problems_of(text)

    assert problem == (
        "steps[0].tables[0].skip_checks.checks[1]: must be a Check declared on this table or one"
        " of: source_tables_exist, primary_key_unique, primary_key_not_null, row_count_matches"
    )
    assert "LEAK-7c1e" not in problem


@pytest.mark.parametrize(
    ("mode", "check"),
    [
        ("incremental", "row_count_matches"),
        ("full_refresh", "no_row_loss"),
        ("full_refresh", "cursor_not_backwards"),
    ],
)
def test_skipping_a_check_that_does_not_apply_to_the_mode_is_rejected(mode, check):
    text = pipeline_with_tables(
        f"      - {{name: t, mode: {mode}, primary_key: id, cursor: updated_at,"
        f" skip_checks: {{checks: [primary_key_unique, {check}], reason: r}}}}"
    )

    assert problems_of(text) == (
        f"steps[0].tables[0].skip_checks.checks[1]: does not apply to a {mode} table",
    )


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (
            SyncMode.FULL_REFRESH,
            (
                "source_tables_exist",
                "primary_key_unique",
                "primary_key_not_null",
                "row_count_matches",
            ),
        ),
        (
            SyncMode.INCREMENTAL,
            (
                "source_tables_exist",
                "primary_key_unique",
                "primary_key_not_null",
                "no_row_loss",
                "cursor_not_backwards",
            ),
        ),
    ],
)
def test_every_applicable_check_is_on_by_default_in_a_fixed_order(mode, expected):
    table = SyncTable(name="t", mode=mode, primary_key="id", cursor="updated_at")

    assert table.applicable_checks() == expected
    assert all(table.skip_reason(check) is None for check in CheckName)


def test_required_columns_are_the_primary_key_and_the_cursor_when_declared():
    assert SyncTable(name="t", mode="full_refresh", primary_key="id").required_columns() == ("id",)
    assert SyncTable(
        name="t", mode="incremental", primary_key="id", cursor="updated_at"
    ).required_columns() == ("id", "updated_at")


ALL_DECLARED = """\
        checks:
          - {type: not_null, column: email}
          - {type: accepted_values, column: status, values: [active, closed, 3]}
          - {type: min_rows, count: 5}
          - {type: freshness, column: updated_at, max_age_hours: 24}"""


def table_with_checks(checks: str, extra: str = "") -> str:
    return pipeline_with_tables(
        f"      - name: t\n        mode: full_refresh\n        primary_key: id\n{extra}{checks}"
    )


def test_a_table_without_declared_checks_has_none():
    table = load_pipeline(VALID).steps[0].tables[0]

    assert table.checks == []
    assert table.declared_check_names() == ()


def test_all_four_declared_check_kinds_load_with_distinct_display_names():
    table = load_pipeline(table_with_checks(ALL_DECLARED)).steps[0].tables[0]

    assert [check.type for check in table.checks] == [
        "not_null",
        "accepted_values",
        "min_rows",
        "freshness",
    ]
    assert table.declared_check_names() == (
        "not_null(email)",
        "accepted_values(status)",
        "min_rows",
        "freshness(updated_at)",
    )
    assert table.checks[1].values == ["active", "closed", 3]
    assert (table.checks[2].count, table.checks[3].max_age_hours) == (5, 24)


def test_declared_checks_on_different_columns_are_both_accepted():
    text = table_with_checks(
        "        checks:\n"
        "          - {type: not_null, column: email}\n"
        "          - {type: not_null, column: status}"
    )

    names = load_pipeline(text).steps[0].tables[0].declared_check_names()

    assert names == ("not_null(email)", "not_null(status)")


def test_the_same_declared_check_on_two_tables_is_accepted():
    text = pipeline_with_tables(
        "      - {name: a, mode: full_refresh, primary_key: id,"
        " checks: [{type: not_null, column: email}]}\n"
        "      - {name: b, mode: full_refresh, primary_key: id,"
        " checks: [{type: not_null, column: email}]}"
    )

    tables = load_pipeline(text).steps[0].tables

    assert [t.declared_check_names() for t in tables] == [("not_null(email)",)] * 2


@pytest.mark.parametrize(
    ("checks", "duplicate_index", "first_index"),
    [
        (
            "[{type: not_null, column: email}, {type: not_null, column: email}]",
            1,
            0,
        ),
        (
            "[{type: min_rows, count: 1}, {type: not_null, column: x}, {type: min_rows, count: 9}]",
            2,
            0,
        ),
        (
            "[{type: accepted_values, column: s, values: [a]},"
            " {type: accepted_values, column: s, values: [b]}]",
            1,
            0,
        ),
        (
            "[{type: freshness, column: u, max_age_hours: 1},"
            " {type: freshness, column: u, max_age_hours: 2}]",
            1,
            0,
        ),
    ],
)
def test_a_repeated_declared_check_is_rejected_at_its_own_index(
    checks, duplicate_index, first_index
):
    text = pipeline_with_tables(
        f"      - {{name: t, mode: full_refresh, primary_key: id, checks: {checks}}}"
    )

    assert problems_of(text) == (
        f"steps[0].tables[0].checks[{duplicate_index}]: the same Check as checks[{first_index}]",
    )


@pytest.mark.parametrize(
    ("check", "problem"),
    [
        (
            "{type: not_null, column: email, where: x}",
            "steps[0].tables[0].checks[0].where: Extra inputs are not permitted",
        ),
        (
            "{type: accepted_values, column: s, values: []}",
            "steps[0].tables[0].checks[0].values: List should have at least 1 item"
            " after validation, not 0",
        ),
        (
            "{type: accepted_values, column: s, values: [a, b, a]}",
            "steps[0].tables[0].checks[0].values[2]: repeats an earlier value",
        ),
        (
            "{type: min_rows, count: 0}",
            "steps[0].tables[0].checks[0].count: Input should be greater than or equal to 1",
        ),
        (
            "{type: freshness, column: u, max_age_hours: 0}",
            "steps[0].tables[0].checks[0].max_age_hours:"
            " Input should be greater than or equal to 1",
        ),
        (
            "{type: not_null}",
            "steps[0].tables[0].checks[0].column: Field required",
        ),
        (
            "{type: freshness, max_age_hours: 1}",
            "steps[0].tables[0].checks[0].column: Field required",
        ),
        (
            "{type: not_null, column: ''}",
            "steps[0].tables[0].checks[0].column: String should have at least 1 character",
        ),
        (
            "{type: unique, column: id}",
            "steps[0].tables[0].checks[0]: type must be one of:"
            " not_null, accepted_values, min_rows, freshness",
        ),
        (
            "{column: id}",
            "steps[0].tables[0].checks[0]: type must be one of:"
            " not_null, accepted_values, min_rows, freshness",
        ),
        (
            "not_null",
            "steps[0].tables[0].checks[0]: type must be one of:"
            " not_null, accepted_values, min_rows, freshness",
        ),
    ],
)
def test_a_malformed_declared_check_is_rejected_at_its_path(check, problem):
    text = pipeline_with_tables(
        f"      - {{name: t, mode: full_refresh, primary_key: id, checks: [{check}]}}"
    )

    assert problems_of(text) == (problem,)


def test_accepted_values_compares_values_by_type_and_reports_each_repeat():
    text = table_with_checks(
        "        checks:\n          - {type: accepted_values, column: s, values: [1, '1', 2, 1]}"
    )

    (problem,) = problems_of(text)

    assert problem == "steps[0].tables[0].checks[0].values[3]: repeats an earlier value"


def test_declared_check_models_are_accepted_as_objects():
    table = SyncTable(
        name="t",
        mode="full_refresh",
        primary_key="id",
        checks=[FreshnessCheck(type="freshness", column="u", max_age_hours=2)],
    )

    assert table.declared_check_names() == ("freshness(u)",)


def test_a_declared_check_can_be_skipped_by_its_display_name():
    text = table_with_checks(
        ALL_DECLARED,
        extra="        skip_checks: {checks: [not_null(email), primary_key_unique], reason: r}\n",
    )

    table = load_pipeline(text).steps[0].tables[0]

    assert table.skip_reason("not_null(email)") == "r"
    assert table.skip_reason(CheckName.PRIMARY_KEY_UNIQUE) == "r"
    assert table.skip_reason("min_rows") is None


@pytest.mark.parametrize("name", ["not_null(emial)", "min_rows", "freshness"])
def test_skipping_a_check_not_declared_on_the_table_is_rejected(name):
    text = pipeline_with_tables(
        "      - {name: t, mode: full_refresh, primary_key: id,"
        " checks: [{type: not_null, column: email}],"
        f" skip_checks: {{checks: ['{name}'], reason: r}}}}"
    )

    (problem,) = problems_of(text)

    assert problem.startswith(
        "steps[0].tables[0].skip_checks.checks[0]: must be a Check declared on this table"
    )
    assert name not in problem


def test_skipping_a_check_declared_only_on_another_table_is_rejected():
    text = pipeline_with_tables(
        "      - {name: a, mode: full_refresh, primary_key: id,"
        " checks: [{type: min_rows, count: 1}]}\n"
        "      - {name: b, mode: full_refresh, primary_key: id,"
        " skip_checks: {checks: [min_rows], reason: r}}"
    )

    (problem,) = problems_of(text)

    assert problem.startswith("steps[0].tables[1].skip_checks.checks[0]: must be a Check")
