import pytest
import yaml

from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline
from adops.domain.reverse_sync import DEFAULT_MAX_CHANGE_SHARE, ReverseSyncStep

SENTINEL = "leak9f3a"

PIPELINE = """
name: shop
steps:
  - name: sync_shop
    source: shop
    tables: [{name: customers, mode: full_refresh, primary_key: id}]
  - type: transform
    name: model_shop
    project: dbt
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: shop
    target_table: customer_stats
    key: customer_id
    mapping: {customer_id: customer_id, order_count: orders}
"""


def problems_of(text: str) -> tuple[str, ...]:
    with pytest.raises(PipelineValidationError) as caught:
        load_pipeline(text)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    return caught.value.problems


def with_line(old: str, new: str) -> str:
    assert old in PIPELINE
    return PIPELINE.replace(old, new)


def with_extra(line: str) -> str:
    return PIPELINE + f"    {line}\n"


def test_a_reverse_sync_step_loads_with_safe_defaults():
    step = load_pipeline(PIPELINE).steps[2]

    assert step == ReverseSyncStep(
        type="reverse_sync",
        name="push_stats",
        model="customer_stats",
        target="shop",
        target_table="customer_stats",
        key="customer_id",
        mapping={"customer_id": "customer_id", "order_count": "orders"},
    )
    assert step.delete_missing is False
    assert step.max_change_share == DEFAULT_MAX_CHANGE_SHARE == 0.5
    assert step.skip_checks is None


def test_every_option_can_be_set():
    text = PIPELINE + (
        "    delete_missing: true\n"
        "    max_change_share: 1\n"
        "    skip_checks: {checks: [max_change, result_not_empty], reason: first load}\n"
    )

    step = load_pipeline(text).steps[2]

    assert step.delete_missing is True
    assert step.max_change_share == 1
    assert step.skip_checks.checks == ["max_change", "result_not_empty"]


@pytest.mark.parametrize(
    ("old", "new", "problem"),
    [
        ("model: customer_stats", "model: Customer_Stats", "steps[2].model: String should match"),
        ("target: shop", "target: shop-db", "steps[2].target: String should match"),
        (
            "target_table: customer_stats",
            "target_table: public.customer_stats",
            "steps[2].target_table: String should match",
        ),
        (
            "target_table: customer_stats",
            "target_table: customer stats",
            "steps[2].target_table: String should match",
        ),
        ("key: customer_id", "key: Customer_ID", "steps[2].key: String should match"),
        ("key: customer_id", 'key: "customer_id\'"', "steps[2].key: String should match"),
        ("name: push_stats", "name: push-stats", "steps[2].name: String should match"),
        (
            "order_count: orders}",
            'order_count: "x; drop table t"}',
            "steps[2].mapping.order_count: String should match",
        ),
        (
            "order_count: orders}",
            "order_count: Orders}",
            "steps[2].mapping.order_count: String should match",
        ),
        (
            "order_count: orders}",
            "order_count: 7}",
            "steps[2].mapping.order_count: Input should be a valid string",
        ),
    ],
)
def test_every_sql_bound_name_must_be_an_identifier(old, new, problem):
    (found,) = problems_of(with_line(old, new))

    assert found.startswith(problem)


@pytest.mark.parametrize(
    "mapping",
    [
        '{customer_id: customer_id, "x; drop table t": orders}',
        "{customer_id: customer_id, Order_Count: orders}",
        "{customer_id: customer_id, 7: orders}",
    ],
)
def test_a_bad_dbt_column_name_is_reported_by_position_without_echoing_it(mapping):
    text = with_line("{customer_id: customer_id, order_count: orders}", mapping)

    assert problems_of(text) == (
        "steps[2].mapping: entry 2: the dbt column name must match ^[a-z][a-z0-9_]*$",
    )


@pytest.mark.parametrize(
    ("mapping", "problem"),
    [
        ("{}", "steps[2].mapping: Dictionary should have at least 1 item"),
        ("[customer_id]", "steps[2].mapping: Input should be a valid dictionary"),
    ],
)
def test_the_mapping_must_name_at_least_one_column(mapping, problem):
    text = with_line("{customer_id: customer_id, order_count: orders}", mapping)

    (found,) = problems_of(text)

    assert found.startswith(problem)


@pytest.mark.parametrize("missing", ["model", "target", "target_table", "key", "mapping"])
def test_required_fields_are_named_when_missing(missing):
    lines = [line for line in PIPELINE.splitlines() if not line.startswith(f"    {missing}:")]

    assert problems_of("\n".join(lines)) == (f"steps[2].{missing}: Field required",)


@pytest.mark.parametrize("field", ["write", "dry_run", "preview", "allow_writes"])
def test_unknown_fields_such_as_a_write_switch_are_rejected(field):
    assert problems_of(with_extra(f"{field}: true")) == (
        f"steps[2].{field}: Extra inputs are not permitted",
    )


@pytest.mark.parametrize("share", ["0", "-0.1"])
def test_max_change_share_must_be_above_zero(share):
    assert problems_of(with_extra(f"max_change_share: {share}")) == (
        "steps[2].max_change_share: Input should be greater than 0",
    )


def test_max_change_share_may_not_exceed_one():
    assert problems_of(with_extra("max_change_share: 1.5")) == (
        "steps[2].max_change_share: Input should be less than or equal to 1",
    )


@pytest.mark.parametrize("reason", ["''", "'   '"])
def test_skipping_a_check_needs_a_written_reason(reason):
    text = with_extra(f"skip_checks: {{checks: [max_change], reason: {reason}}}")

    assert problems_of(text) == (
        "steps[2].skip_checks.reason: must explain why these Checks are skipped",
    )


@pytest.mark.parametrize(
    "check",
    ["target_columns_exist", "key_unique", "key_not_null", "max_change", "result_not_empty"],
)
def test_every_reverse_sync_check_may_be_skipped_with_a_reason(check):
    text = with_extra(f"skip_checks: {{checks: [{check}], reason: backfill in progress}}")

    step = load_pipeline(text).steps[2]

    assert step.skip_checks.checks == [check]
    assert step.skip_checks.reason == "backfill in progress"


@pytest.mark.parametrize("check", ["primary_key_unique", "no_row_loss", "source_tables_exist"])
def test_a_sync_check_may_not_be_skipped_on_a_reverse_sync_step(check):
    text = with_extra(f"skip_checks: {{checks: [max_change, {check}], reason: r}}")

    assert problems_of(text) == (
        "steps[2].skip_checks.checks[1]: does not apply to a Reverse Sync Step",
    )


def test_an_unknown_check_name_lists_the_reverse_sync_checks():
    text = with_extra("skip_checks: {checks: [max_changes], reason: r}")

    assert problems_of(text) == (
        "steps[2].skip_checks.checks[0]: must be one of: target_columns_exist, key_unique, "
        "key_not_null, max_change, result_not_empty",
    )


def test_a_reverse_sync_error_path_carries_no_step_type():
    text = with_extra(f"reverse_sync: {SENTINEL}")

    (problem,) = problems_of(text)

    assert problem == "steps[2].reverse_sync: Extra inputs are not permitted"
    assert SENTINEL not in problem


def test_the_step_name_must_be_unique_across_step_kinds():
    text = with_line("name: push_stats", "name: sync_shop")

    assert problems_of(text) == (
        "steps[0].name: another Step already has this name",
        "steps[2].name: another Step already has this name",
    )


def test_the_type_is_required_to_be_reverse_sync():
    data = yaml.safe_load(PIPELINE)["steps"][2]
    data.pop("type")

    with pytest.raises(ValueError):
        ReverseSyncStep.model_validate(data)
