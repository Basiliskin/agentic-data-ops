from pathlib import Path

import pytest
import yaml

import adops
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    ensure_targets_defined,
    format_path,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.transform import TransformStep

SENTINEL = "LEAK-9f3a"

SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: h, port: 5432, database: d, user: u, password_env: WH_PW}
    sources:
      shop: {type: postgres, host: h, port: 5432, database: d, user: u, password_env: SHOP_PW}
"""

PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables: [{name: customers, mode: full_refresh, primary_key: id}]
  - name: sync_crm
    source: crm
    tables: [{name: leads, mode: full_refresh, primary_key: id}]
"""


def raised_by(load, text: str) -> PipelineValidationError:
    with pytest.raises(PipelineValidationError) as caught:
        load(text)
    return caught.value


def assert_secret_free(error: PipelineValidationError) -> None:
    assert SENTINEL not in str(error)
    assert SENTINEL not in repr(error)
    assert all(SENTINEL not in str(arg) for arg in error.args)
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("load", [load_pipeline, load_project_settings])
@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("name: [unclosed", "(root): not valid YAML (line 1, column 16)"),
        ("- a\n- b", "(root): must be a mapping of settings"),
        ("just text", "(root): must be a mapping of settings"),
        ("", "(root): must be a mapping of settings"),
        ("\x07", "(root): not valid YAML"),
    ],
)
def test_text_that_is_not_a_yaml_mapping_is_a_validation_error(load, text, problem):
    error = raised_by(load, text)

    assert error.problems == (problem,)
    assert error.__context__ is None


def test_yaml_syntax_error_does_not_quote_the_offending_line():
    error = raised_by(load_pipeline, f"name: shop_sync\nsteps: [{{name: {SENTINEL}: x}}]")

    assert error.problems[0].startswith("(root): not valid YAML (line 2")
    assert_secret_free(error)


def test_python_object_tags_are_refused_by_the_safe_loader():
    error = raised_by(load_pipeline, "name: !!python/object/apply:os.system ['echo hi']")

    assert error.problems[0].startswith("(root): not valid YAML")


def test_field_values_are_never_echoed_and_all_problems_are_reported_together():
    pipeline = f"""
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - {{name: t, mode: {SENTINEL}, primary_key: id}}
      - {{name: u, mode: full_refresh, primary_key: id, colour: {SENTINEL}}}
"""
    error = raised_by(load_pipeline, pipeline)

    assert error.problems == (
        "steps[0].tables[0].mode: Input should be 'full_refresh' or 'incremental'",
        "steps[0].tables[1].colour: Extra inputs are not permitted",
    )
    assert_secret_free(error)


def test_bad_secret_reference_and_literal_password_are_not_echoed():
    settings = SETTINGS.replace("password_env: WH_PW", f"password: {SENTINEL}").replace(
        "password_env: SHOP_PW", f"password_env: {SENTINEL.lower()}"
    )
    error = raised_by(load_project_settings, settings)

    assert len(error.problems) == 2
    assert SENTINEL.lower() not in str(error)
    assert_secret_free(error)


def test_sources_used_by_steps_must_exist_in_the_environment():
    environment = load_project_settings(SETTINGS).environments["dev"]
    pipeline = load_pipeline(PIPELINE)

    error = raised_by(lambda p: ensure_sources_defined(p, environment), pipeline)

    assert error.problems == ("steps[1].source: no Source with this name in the Environment",)
    assert "crm" not in str(error)


def test_pipeline_whose_sources_all_exist_passes_the_cross_check():
    environment = load_project_settings(SETTINGS).environments["dev"]
    pipeline = load_pipeline(PIPELINE.split("  - name: sync_crm")[0])

    assert ensure_sources_defined(pipeline, environment) is None


@pytest.mark.parametrize(
    ("location", "path"),
    [
        ((), "(root)"),
        (("steps", 0, "tables", 1, "skip_checks"), "steps[0].tables[1].skip_checks"),
        (("environments", "dev", "warehouse"), "environments.dev.warehouse"),
    ],
)
def test_error_paths_read_like_yaml_paths(location, path):
    assert format_path(location) == path


@pytest.mark.parametrize(
    ("check", "problem"),
    [
        (
            f"{{type: {SENTINEL}, column: id}}",
            "steps[0].tables[0].checks[0]: type must be one of:"
            " not_null, accepted_values, min_rows, freshness",
        ),
        (
            f"{{type: accepted_values, column: s, values: [{SENTINEL}, {SENTINEL}]}}",
            "steps[0].tables[0].checks[0].values[1]: repeats an earlier value",
        ),
        (
            "{type: accepted_values, column: s, values: []}",
            "steps[0].tables[0].checks[0].values: List should have at least 1 item"
            " after validation, not 0",
        ),
        (
            f"{{type: min_rows, count: 0, note: {SENTINEL}}}",
            "steps[0].tables[0].checks[0].count: Input should be greater than or equal to 1",
        ),
        (
            "{type: freshness, max_age_hours: 3}",
            "steps[0].tables[0].checks[0].column: Field required",
        ),
    ],
)
def test_malformed_declared_checks_are_reported_at_yaml_paths_without_values(check, problem):
    pipeline = (
        "name: shop_sync\nsteps:\n  - name: sync_shop\n    source: shop\n"
        f"    tables: [{{name: t, mode: full_refresh, primary_key: id, checks: [{check}]}}]"
    )

    error = raised_by(load_pipeline, pipeline)

    assert error.problems[0] == problem
    assert_secret_free(error)


DEMO_PIPELINE = Path(adops.__file__).parent / "templates/demo/pipelines/shop_sync.yaml"

MIXED = """
name: shop
steps:
  - name: sync_shop
    source: shop
    tables: [{name: customers, mode: full_refresh, primary_key: id}]
  - type: transform
    name: model_shop
    project: dbt
"""


def test_the_demo_pipeline_without_step_types_loads_as_sync_steps():
    text = DEMO_PIPELINE.read_text()
    sync, transform, reverse_sync = load_pipeline(text).steps

    assert "type" not in yaml.safe_load(text)["steps"][0]
    assert isinstance(sync, SyncStep)
    assert isinstance(transform, TransformStep)
    assert isinstance(reverse_sync, ReverseSyncStep)


def test_sync_and_transform_steps_load_side_by_side():
    text = MIXED.replace("  - name: sync_shop", "  - type: sync\n    name: sync_shop")

    sync, transform = load_pipeline(text).steps

    assert isinstance(sync, SyncStep)
    assert sync.type == "sync"
    assert transform == TransformStep(type="transform", name="model_shop", project="dbt")


def test_steps_given_as_models_are_accepted():
    sync = SyncStep(
        name="sync_shop",
        source="shop",
        tables=[{"name": "t", "mode": "full_refresh", "primary_key": "id"}],
    )
    transform = TransformStep(type="transform", name="model_shop", project="dbt")

    assert Pipeline(name="shop", steps=[sync, transform]).steps == [sync, transform]


@pytest.mark.parametrize("step_type", [SENTINEL, "5", "[transform]"])
def test_an_unknown_step_type_is_rejected_without_echoing_the_step(step_type):
    text = MIXED.replace("type: transform", f"type: {step_type}").replace(
        "project: dbt", f"project: {SENTINEL}-project"
    )

    error = raised_by(load_pipeline, text)

    assert error.problems == ("steps[1]: type must be one of: sync, transform, reverse_sync",)
    assert_secret_free(error)


def test_a_sync_and_a_transform_step_may_not_share_a_name():
    text = MIXED.replace("name: model_shop", "name: sync_shop")

    assert raised_by(load_pipeline, text).problems == (
        "steps[0].name: another Step already has this name",
        "steps[1].name: another Step already has this name",
    )


@pytest.mark.parametrize(
    ("old", "new", "problem"),
    [
        ("    project: dbt\n", "", "steps[1].project: Field required"),
        ("name: model_shop", "name: Model-Shop", "steps[1].name: String should match pattern"),
        ("project: dbt", "project: ''", "steps[1].project: String should have at least 1"),
        (
            "mode: full_refresh, primary_key: id",
            f"mode: {SENTINEL}, primary_key: id",
            "steps[0].tables[0].mode: Input should be 'full_refresh' or 'incremental'",
        ),
    ],
)
def test_step_errors_are_reported_at_yaml_paths_without_the_step_type(old, new, problem):
    error = raised_by(load_pipeline, MIXED.replace(old, new))

    assert error.problems[0].startswith(problem)
    assert_secret_free(error)


@pytest.mark.parametrize(
    ("old", "new", "problem"),
    [
        (
            "    source: shop\n",
            "    source: shop\n    sync: x\n",
            "steps[0].sync: Extra inputs are not permitted",
        ),
        (
            "primary_key: id}",
            "primary_key: id, transform: x}",
            "steps[0].tables[0].transform: Extra inputs are not permitted",
        ),
        (
            "    project: dbt\n",
            "    project: dbt\n    transform: x\n",
            "steps[1].transform: Extra inputs are not permitted",
        ),
    ],
)
def test_user_keys_named_like_a_step_type_stay_in_the_path(old, new, problem):
    assert raised_by(load_pipeline, MIXED.replace(old, new)).problems == (problem,)


def test_transform_steps_need_no_source_in_the_environment():
    environment = load_project_settings(SETTINGS).environments["dev"]

    assert ensure_sources_defined(load_pipeline(MIXED), environment) is None


def test_a_missing_source_is_still_found_next_to_a_transform_step():
    environment = load_project_settings(SETTINGS).environments["dev"]
    pipeline = load_pipeline(MIXED.replace("source: shop", "source: crm"))

    error = raised_by(lambda p: ensure_sources_defined(p, environment), pipeline)

    assert error.problems == ("steps[0].source: no Source with this name in the Environment",)


@pytest.mark.parametrize(
    ("warehouse", "problem"),
    [
        ("{type: duckdb}", "environments.dev.warehouse.path: Field required"),
        (
            f"{{type: duckdb, path: x, host: {SENTINEL}}}",
            "environments.dev.warehouse.host: Extra inputs are not permitted",
        ),
        (
            "{type: postgres, host: h, database: d, user: u, password_env: WH_PW}",
            "environments.dev.warehouse.port: Field required",
        ),
        (
            "{type: postgres, host: h, port: 5432, database: d, user: u, password_env: WH_PW,"
            f" duckdb: {SENTINEL}}}",
            "environments.dev.warehouse.duckdb: Extra inputs are not permitted",
        ),
    ],
)
def test_warehouse_errors_are_reported_without_the_warehouse_type(warehouse, problem):
    text = SETTINGS.replace(SETTINGS.splitlines()[3].split("warehouse: ")[1], warehouse)

    error = raised_by(load_project_settings, text)

    assert error.problems == (problem,)
    assert_secret_free(error)


def test_a_source_named_warehouse_keeps_its_keys_in_the_path():
    text = SETTINGS.replace("      shop:", "      warehouse:").replace(
        "password_env: SHOP_PW}", "password_env: SHOP_PW, postgres: x}"
    )

    assert raised_by(load_project_settings, text).problems == (
        "environments.dev.sources.warehouse.postgres: Extra inputs are not permitted",
    )


REVERSE = """
name: shop
steps:
  - type: transform
    name: model_shop
    project: dbt
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: shop
    target_table: customer_stats
    key: customer_id
    mapping: {customer_id: customer_id}
"""

TARGETS = """
    targets:
      shop: {type: postgres, host: h, port: 5432, database: d, user: u, password_env: TARGET_PW}
"""


def test_a_reverse_sync_target_must_be_defined_in_the_environment():
    environment = load_project_settings(SETTINGS).environments["dev"]

    error = raised_by(lambda p: ensure_targets_defined(p, environment), load_pipeline(REVERSE))

    assert error.problems == (
        "steps[1].target: Step push_stats writes to Target shop, which the Environment does "
        "not define",
    )


def test_a_source_of_the_same_name_does_not_count_as_a_target():
    environment = load_project_settings(SETTINGS).environments["dev"]

    assert "shop" in environment.sources
    with pytest.raises(PipelineValidationError):
        ensure_targets_defined(load_pipeline(REVERSE), environment)


def test_a_defined_target_passes_without_resolving_its_password(monkeypatch):
    monkeypatch.delenv("TARGET_PW", raising=False)
    environment = load_project_settings(SETTINGS + TARGETS).environments["dev"]

    assert ensure_targets_defined(load_pipeline(REVERSE), environment) is None


def test_pipelines_without_reverse_sync_need_no_targets():
    environment = load_project_settings(SETTINGS).environments["dev"]

    assert environment.targets == {}
    assert ensure_targets_defined(load_pipeline(MIXED), environment) is None


def test_target_errors_never_echo_the_password_value(monkeypatch):
    monkeypatch.setenv("TARGET_PW", SENTINEL)
    text = (SETTINGS + TARGETS).replace("password_env: TARGET_PW", f"password: {SENTINEL}")

    error = raised_by(load_project_settings, text)

    assert error.problems[0].startswith(
        "environments.dev.targets.shop.password: literal passwords are not allowed"
    )
    assert_secret_free(error)
