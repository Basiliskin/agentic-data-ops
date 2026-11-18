import pytest

from adops.domain.checks import CheckName, SyncMode
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline
from adops.domain.pipeline import Pipeline, SyncTable

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

    assert problem.startswith("steps[0].tables[0].skip_checks.checks[1]: Input should be")
    assert all(f"'{check}'" in problem for check in CheckName)
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
