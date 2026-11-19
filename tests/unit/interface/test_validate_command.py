import socket

import pytest
from typer.testing import CliRunner

from adops.infrastructure import dbt_transform
from adops.interface import cli

runner = CliRunner()

SENTINEL = "SENTINEL_VALUE_7f3a"

SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5432, database: w, user: w,
                password_env: WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: SHOP_PASSWORD}
"""


def pipeline_yaml(name, tables, source="shop"):
    return f"name: {name}\nsteps:\n  - name: sync\n    source: {source}\n    tables:\n{tables}"


def table_yaml(name, skip=""):
    return f"      - name: {name}\n        mode: full_refresh\n        primary_key: id\n{skip}"


def skip_yaml(checks, reason="kept for audit"):
    listed = ", ".join(checks)
    return f"        skip_checks:\n          checks: [{listed}]\n          reason: {reason}\n"


@pytest.fixture
def project(tmp_path):
    (tmp_path / "adops.yaml").write_text(SETTINGS)
    (tmp_path / "pipelines").mkdir()
    return tmp_path


def write_pipeline(project, file_name, text):
    (project / "pipelines" / f"{file_name}.yaml").write_text(text)


def validate(*args):
    return runner.invoke(cli.app, ["validate", *args])


@pytest.fixture
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("validate must not open a network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


def test_demo_project_validates_offline_without_env_file(tmp_path, monkeypatch, no_network):
    for name in ("SHOP_PASSWORD", "WAREHOUSE_PASSWORD", "DAGSTER_PG_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    runner.invoke(cli.app, ["init", str(tmp_path), "--demo"])

    result = validate("--project", str(tmp_path))

    assert result.exit_code == 0, result.output
    assert result.output == "ok\n"


TRANSFORM_STEP = "  - name: transform_shop\n    type: transform\n    project: dbt\n"


@pytest.fixture
def dbt_never_runs(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("validate must not run dbt")

    monkeypatch.setattr(dbt_transform, "dbtRunner", refuse)
    monkeypatch.setattr(dbt_transform.DbtTransform, "run", refuse)


@pytest.mark.usefixtures("no_network", "dbt_never_runs")
def test_demo_with_its_dbt_project_validates_without_running_dbt(tmp_path):
    runner.invoke(cli.app, ["init", str(tmp_path), "--demo"])

    result = validate("--project", str(tmp_path))

    assert result.exit_code == 0, result.output
    assert result.output == "ok\n"


@pytest.mark.usefixtures("dbt_never_runs")
def test_missing_dbt_project_file_names_the_step_and_the_relative_path(tmp_path):
    runner.invoke(cli.app, ["init", str(tmp_path), "--demo"])
    (tmp_path / "dbt" / "dbt_project.yml").unlink()

    result = validate("--project", str(tmp_path))

    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "pipelines/shop_sync.yaml: Step transform_shop: dbt/dbt_project.yml not found"
    ]
    assert str(tmp_path) not in result.output


def test_dbt_folder_without_dbt_project_file_is_reported(project):
    (project / "dbt").mkdir()
    write_pipeline(
        project, "shop_sync", pipeline_yaml("shop_sync", table_yaml("orders")) + TRANSFORM_STEP
    )

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert "Step transform_shop: dbt/dbt_project.yml not found" in result.output


def test_transform_step_is_left_out_of_the_skipped_checks(project):
    (project / "dbt").mkdir()
    (project / "dbt" / "dbt_project.yml").write_text("name: shop\n")
    tables = table_yaml("orders", skip_yaml(["row_count_matches"]))
    write_pipeline(project, "shop_sync", pipeline_yaml("shop_sync", tables) + TRANSFORM_STEP)

    result = validate("--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "orders: row_count_matches skipped — kept for audit",
        "ok",
    ]


def test_validate_defaults_to_the_current_folder(project, monkeypatch):
    write_pipeline(project, "a", pipeline_yaml("a", table_yaml("orders")))
    monkeypatch.chdir(project)

    result = validate()

    assert result.exit_code == 0, result.output
    assert result.output == "ok\n"


def test_skipped_checks_are_listed_in_pipeline_table_check_order(project):
    write_pipeline(
        project,
        "b_second",
        pipeline_yaml("b_second", table_yaml("zeta", skip_yaml(["primary_key_unique"]))),
    )
    write_pipeline(
        project,
        "a_first",
        pipeline_yaml(
            "a_first",
            table_yaml("orders", skip_yaml(["row_count_matches", "primary_key_unique"]))
            + table_yaml("customers", skip_yaml(["primary_key_not_null"], "legacy rows")),
        ),
    )

    first = validate("--project", str(project))
    second = validate("--project", str(project))

    assert first.exit_code == 0, first.output
    assert first.output.splitlines() == [
        "customers: primary_key_not_null skipped — legacy rows",
        "orders: primary_key_unique skipped — kept for audit",
        "orders: row_count_matches skipped — kept for audit",
        "zeta: primary_key_unique skipped — kept for audit",
        "ok",
    ]
    assert second.output == first.output


def test_a_skipped_source_not_empty_is_listed_with_its_reason(project):
    tables = table_yaml("returns", skip_yaml(["source_not_empty"], "filled only at month end"))
    write_pipeline(project, "shop", pipeline_yaml("shop", tables))

    result = validate("--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "returns: source_not_empty skipped — filled only at month end",
        "ok",
    ]


def test_one_named_pipeline_is_validated_alone(project):
    write_pipeline(project, "good", pipeline_yaml("good", table_yaml("orders")))
    write_pipeline(project, "bad", "name: bad\n")

    result = validate("good", "--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output == "ok\n"


def test_every_pipeline_is_validated_and_the_invalid_one_named(project):
    write_pipeline(project, "a_good", pipeline_yaml("a_good", table_yaml("orders")))
    write_pipeline(project, "z_bad", "name: z_bad\n")

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert "pipelines/z_bad.yaml: steps: Field required" in result.output
    assert "a_good" not in result.output
    assert "ok" not in result.output.splitlines()


@pytest.mark.parametrize(
    "skip",
    [
        "        skip_checks:\n          checks: [primary_key_unique]\n",
        f"        skip_checks:\n          checks: [{SENTINEL}]\n          reason: x\n",
        f"        skip_checks:\n          checks: [no_row_loss]\n          reason: {SENTINEL}\n",
    ],
    ids=["no-reason", "unknown-check", "check-not-for-mode"],
)
def test_invalid_skip_checks_fail_with_one_line_per_problem(project, skip):
    write_pipeline(project, "p", pipeline_yaml("p", table_yaml("orders", skip)))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    problems = result.output.splitlines()
    assert len(problems) == 1
    assert problems[0].startswith("pipelines/p.yaml: steps[0].tables[0].skip_checks")
    assert "Traceback" not in result.output
    assert SENTINEL not in result.output


def test_malformed_declared_check_is_reported_with_its_yaml_path(project):
    checks = "        checks:\n          - {type: min_rows, count: 0}\n"
    write_pipeline(project, "p", pipeline_yaml("p", table_yaml("orders", checks)))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    problems = result.output.splitlines()
    assert len(problems) == 1
    assert problems[0].startswith("pipelines/p.yaml: steps[0].tables[0].checks[0]")
    assert "Traceback" not in result.output


def test_step_source_missing_from_an_environment_is_reported(project):
    write_pipeline(project, "p", pipeline_yaml("p", table_yaml("orders"), source="crm"))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output == (
        "pipelines/p.yaml: steps[0].source: no Source with this name in the Environment "
        "(Environment dev)\n"
    )


def test_invalid_settings_are_reported_and_pipelines_still_checked(project):
    (project / "adops.yaml").write_text(f"environments: {{}}\nextra: {SENTINEL}\n")
    write_pipeline(project, "p", "name: p\n")

    result = validate("--project", str(project))

    assert result.exit_code == 1
    lines = result.output.splitlines()
    assert "adops.yaml: extra: Extra inputs are not permitted" in lines
    assert "pipelines/p.yaml: steps: Field required" in lines
    assert SENTINEL not in result.output


def test_valid_pipeline_still_lists_skips_when_settings_are_invalid(project):
    (project / "adops.yaml").write_text("environments: []\n")
    write_pipeline(
        project, "p", pipeline_yaml("p", table_yaml("orders", skip_yaml(["primary_key_unique"])))
    )

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "orders: primary_key_unique skipped — kept for audit",
        "adops.yaml: environments: Input should be a valid dictionary",
    ]


def test_missing_settings_file_is_reported(tmp_path):
    result = validate("--project", str(tmp_path))

    assert result.exit_code == 1
    assert result.output == "adops.yaml: file not found\n"


def test_missing_named_pipeline_is_reported(project):
    result = validate("absent", "--project", str(project))

    assert result.exit_code == 1
    assert result.output == "pipelines/absent.yaml: file not found\n"


def test_non_text_pipeline_is_reported(project):
    (project / "pipelines" / "p.yaml").write_bytes(b"\xff\xfe\x00name")

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output == "pipelines/p.yaml: not UTF-8 text\n"


def write_warehouse(project, warehouse):
    settings = SETTINGS.replace(
        "{type: postgres, host: localhost, port: 5432, database: w, user: w,\n"
        "                password_env: WAREHOUSE_PASSWORD}",
        warehouse,
    )
    (project / "adops.yaml").write_text(settings)
    write_pipeline(project, "p", pipeline_yaml("p", table_yaml("orders")))


def test_a_duckdb_warehouse_validates(project):
    write_warehouse(project, "{type: duckdb, path: warehouse.duckdb}")

    result = validate("--project", str(project))

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    ("warehouse", "problem"),
    [
        (
            f"{{type: {SENTINEL}, path: x}}",
            "adops.yaml: environments.dev.warehouse: type must be one of: postgres, duckdb",
        ),
        (
            f"{{type: duckdb, path: x, host: {SENTINEL}}}",
            "adops.yaml: environments.dev.warehouse.host: Extra inputs are not permitted",
        ),
        ("{type: duckdb}", "adops.yaml: environments.dev.warehouse.path: Field required"),
    ],
)
def test_a_malformed_warehouse_is_reported_at_its_yaml_path(project, warehouse, problem):
    write_warehouse(project, warehouse)

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert problem in result.output.splitlines()
    assert SENTINEL not in result.output
    assert ".duckdb." not in result.output
    assert "union_tag" not in result.output
