import pytest

from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    format_path,
    load_pipeline,
    load_project_settings,
)

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
