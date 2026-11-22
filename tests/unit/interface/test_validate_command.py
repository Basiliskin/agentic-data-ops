import socket

import pytest
from typer.testing import CliRunner

from adops.infrastructure import dbt_transform
from adops.interface import cli, run_command, validate_command

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
    assert result.output == "shop_sync: no schedule\nok\n"


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
    assert result.output == "shop_sync: no schedule\nok\n"


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
        "shop_sync: no schedule",
        "ok",
    ]


def test_validate_defaults_to_the_current_folder(project, monkeypatch):
    write_pipeline(project, "a", pipeline_yaml("a", table_yaml("orders")))
    monkeypatch.chdir(project)

    result = validate()

    assert result.exit_code == 0, result.output
    assert result.output == "a: no schedule\nok\n"


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
        "a_first: no schedule",
        "b_second: no schedule",
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
        "shop: no schedule",
        "ok",
    ]


def test_one_named_pipeline_is_validated_alone(project):
    write_pipeline(project, "good", pipeline_yaml("good", table_yaml("orders")))
    write_pipeline(project, "bad", "name: bad\n")

    result = validate("good", "--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output == "good: no schedule\nok\n"


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


CRM_TARGET = """    targets:
      crm: {type: postgres, host: crm.internal, port: 5440, database: crm, user: crm_writer,
            password_env: CRM_PASSWORD}
"""

REVERSE_SETTINGS = f"""
environments:
  dev:
    warehouse: {{type: duckdb, path: dev.duckdb}}
    sources:
      shop: {{type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: SHOP_PASSWORD}}
{CRM_TARGET}  stage:
    warehouse: {{type: duckdb, path: stage.duckdb}}
    sources:
      shop: {{type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: SHOP_PASSWORD}}
"""


def reverse_step_yaml(name, skip=""):
    return (
        f"  - type: reverse_sync\n    name: {name}\n    model: customer_stats\n    target: crm\n"
        f"    target_table: customer_stats\n    key: customer_id\n"
        f"    mapping: {{order_count: orders}}\n{skip}"
    )


def reverse_skip_yaml(checks, reason):
    return f"    skip_checks:\n      checks: [{', '.join(checks)}]\n      reason: {reason}\n"


@pytest.fixture
def reverse_sync_never_built(monkeypatch, no_network):
    def refuse(*args, **kwargs):
        raise AssertionError("validate must not build the Reverse Sync adapter")

    monkeypatch.setattr(run_command, "build_reverse_sync", refuse)


@pytest.mark.usefixtures("reverse_sync_never_built")
def test_target_missing_from_an_environment_is_reported_by_environment_and_key(project):
    (project / "adops.yaml").write_text(REVERSE_SETTINGS)
    write_pipeline(project, "p", "name: p\nsteps:\n" + reverse_step_yaml("push_stats"))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output == (
        "pipelines/p.yaml: steps[0].target: Step push_stats writes to Target crm, which the "
        "Environment does not define (Environment stage)\n"
    )


@pytest.mark.usefixtures("reverse_sync_never_built")
def test_reverse_sync_skips_are_listed_after_table_skips_with_their_reasons(project):
    (project / "adops.yaml").write_text(REVERSE_SETTINGS + CRM_TARGET)
    steps = (
        "  - name: sync\n    source: shop\n    tables:\n"
        + table_yaml("orders", skip_yaml(["primary_key_unique"]))
        + reverse_step_yaml("push_z", reverse_skip_yaml(["max_change"], "monthly rebuild"))
        + reverse_step_yaml("push_plain")
        + reverse_step_yaml(
            "push_a", reverse_skip_yaml(["result_not_empty", "max_change"], "seasonal model")
        )
    )
    write_pipeline(project, "p", "name: p\nsteps:\n" + steps)

    result = validate("--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "orders: primary_key_unique skipped — kept for audit",
        "push_a: max_change skipped — seasonal model",
        "push_a: result_not_empty skipped — seasonal model",
        "push_z: max_change skipped — monthly rebuild",
        "p: no schedule",
        "ok",
    ]


def scheduled_pipeline_yaml(name, schedule):
    return pipeline_yaml(name, table_yaml("orders")) + f"schedule: {schedule}\n"


def test_each_pipeline_schedule_is_listed_with_its_cron_and_the_project_time_zone(project):
    (project / "adops.yaml").write_text(SETTINGS + "timezone: Europe/Berlin\n")
    write_pipeline(project, "z_nightly", scheduled_pipeline_yaml("z_nightly", "daily at 02:00"))
    write_pipeline(project, "m_manual", pipeline_yaml("m_manual", table_yaml("orders")))
    write_pipeline(project, "a_cron", scheduled_pipeline_yaml("a_cron", '"*/15 9-17 * * 1-5"'))

    first = validate("--project", str(project))
    second = validate("--project", str(project))

    assert first.exit_code == 0, first.output
    assert first.output.splitlines() == [
        "a_cron: */15 9-17 * * 1-5 (cron */15 9-17 * * 1-5, Europe/Berlin)",
        "m_manual: no schedule",
        "z_nightly: daily at 02:00 (cron 0 2 * * *, Europe/Berlin)",
        "ok",
    ]
    assert second.output == first.output


def test_without_a_time_zone_the_schedule_is_listed_in_utc(project):
    write_pipeline(project, "p", scheduled_pipeline_yaml("p", "weekly on sunday at 06:30"))

    result = validate("--project", str(project))

    assert result.exit_code == 0, result.output
    assert result.output == "p: weekly on sunday at 06:30 (cron 30 6 * * 0, UTC)\nok\n"


def test_a_bad_schedule_fails_without_echoing_it_or_listing_any_schedule(project):
    write_pipeline(project, "a_good", scheduled_pipeline_yaml("a_good", "hourly"))
    write_pipeline(project, "p", scheduled_pipeline_yaml("p", '"SENTINEL_9f3 daily"'))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "pipelines/p.yaml: schedule: not a known schedule; use hourly, daily at HH:MM, "
        "weekly on <day> at HH:MM or a 5-field cron"
    ]
    assert "SENTINEL_9f3" not in result.output


def test_a_bad_time_zone_is_reported_against_adops_yaml_without_its_value(project):
    (project / "adops.yaml").write_text(SETTINGS + "timezone: Mars/SENTINEL_tz\n")
    write_pipeline(project, "p", scheduled_pipeline_yaml("p", "hourly"))

    result = validate("--project", str(project))

    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "adops.yaml: timezone: not a known time zone; use an IANA name such as Europe/Berlin or UTC"
    ]
    assert "SENTINEL_tz" not in result.output


RECORDED_PIPELINE = (
    pipeline_yaml("a", table_yaml("orders"))
    + "  - type: reverse_sync\n    name: push\n    model: customers\n    target: crm\n"
    "    target_table: customers\n    key: id\n    mapping: {n: n}\n"
)


RECORDED_SETTINGS = SETTINGS + CRM_TARGET
UNKNOWN_TABLE = "table: no Sync Step of this Pipeline loads a table with this name"


def record_yaml(pipeline="a", table="orders", changes="[customer_id dropped]"):
    return f"pipeline: {pipeline}\ntable: {table}\nchanges: {changes}\n"


def write_record(project, record_id, text):
    folder = project / "pipelines" / "accepted"
    folder.mkdir(exist_ok=True)
    (folder / f"{record_id}.yaml").write_text(text)


@pytest.fixture
def recorded(project):
    (project / "adops.yaml").write_text(RECORDED_SETTINGS)
    write_pipeline(project, "a", RECORDED_PIPELINE)
    return project


def record_problems(project, pipeline=None):
    report = validate_command.validate_project(project, pipeline)
    return [line for line in report.problems if line.startswith("pipelines/accepted/")]


def test_a_valid_record_gives_no_problem(recorded, no_network):
    write_record(
        recorded, "r1", record_yaml(changes="[customer_id dropped, amount retyped (int -> text)]")
    )

    assert validate_command.validate_project(recorded, None).problems == []


def test_a_record_with_bad_yaml_is_named(recorded):
    write_record(recorded, "r1", "pipeline: [a\n")

    problems = record_problems(recorded)

    assert len(problems) == 1
    assert problems[0].startswith("pipelines/accepted/r1.yaml: ")


def test_a_record_for_an_unknown_pipeline_is_named(recorded):
    write_record(recorded, "r1", record_yaml(pipeline="nope"))

    assert record_problems(recorded) == [
        "pipelines/accepted/r1.yaml: pipeline: no Pipeline with this name"
    ]


def test_a_record_table_no_sync_step_loads_is_unknown(recorded):
    write_record(recorded, "r1", record_yaml(table="ordrs"))

    assert record_problems(recorded) == [f"pipelines/accepted/r1.yaml: {UNKNOWN_TABLE}"]


def test_a_record_table_only_a_reverse_sync_step_names_is_unknown(recorded):
    write_record(recorded, "r1", record_yaml(table="customers"))

    assert record_problems(recorded) == [f"pipelines/accepted/r1.yaml: {UNKNOWN_TABLE}"]


def test_a_record_with_no_changes_is_named(recorded):
    write_record(recorded, "r1", record_yaml(changes="[]"))

    problems = record_problems(recorded)

    assert len(problems) == 1
    assert problems[0].startswith("pipelines/accepted/r1.yaml: changes: ")


def test_a_malformed_change_is_named_without_echoing_it(recorded):
    write_record(recorded, "r1", record_yaml(changes=f"[customer_id dropped, {SENTINEL} removed]"))

    problems = record_problems(recorded)

    assert problems == [
        "pipelines/accepted/r1.yaml: changes[1]: must read '<column> dropped' or "
        "'<column> retyped (<from> -> <to>)'"
    ]
    assert SENTINEL not in "\n".join(validate_command.validate_project(recorded, None).problems)


def test_a_record_with_an_unknown_field_does_not_echo_its_value(recorded):
    write_record(recorded, "r1", record_yaml() + f"note: {SENTINEL}\n")

    problems = record_problems(recorded)

    assert problems and all(p.startswith("pipelines/accepted/r1.yaml: ") for p in problems)
    assert SENTINEL not in "\n".join(problems)


def test_every_bad_record_is_reported_in_one_run(recorded):
    write_record(recorded, "r1", record_yaml(pipeline="nope"))
    write_record(recorded, "r2", record_yaml(changes="[]"))

    problems = record_problems(recorded)

    assert [line.split(":")[0] for line in problems] == [
        "pipelines/accepted/r1.yaml",
        "pipelines/accepted/r2.yaml",
    ]


def test_a_record_file_ending_in_yml_is_named(recorded):
    write_record(recorded, "r1", record_yaml())
    (recorded / "pipelines" / "accepted" / "r2.yml").write_text(record_yaml())

    assert record_problems(recorded) == [
        "pipelines/accepted/r2.yml: a record file must end in .yaml"
    ]


def test_a_non_text_record_is_named(recorded):
    write_record(recorded, "r1", "")
    (recorded / "pipelines" / "accepted" / "r1.yaml").write_bytes(b"\xff\xfe")

    assert record_problems(recorded) == ["pipelines/accepted/r1.yaml: not UTF-8 text"]


def test_records_are_checked_without_credentials_or_network(recorded, no_network, monkeypatch):
    (recorded / "adops.yaml").write_text(
        RECORDED_SETTINGS.replace("localhost", "unreachable.invalid")
    )
    for name in ("WAREHOUSE_PASSWORD", "SHOP_PASSWORD", "CRM_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    write_record(recorded, "r1", record_yaml(table="ordrs"))

    assert len(record_problems(recorded)) == 1


def test_a_named_pipeline_skips_cross_checks_of_other_pipelines_records(recorded):
    write_record(recorded, "r1", record_yaml(pipeline="b"))
    write_record(recorded, "r2", record_yaml(table="ordrs"))

    assert record_problems(recorded, "a") == [f"pipelines/accepted/r2.yaml: {UNKNOWN_TABLE}"]


def test_a_named_pipeline_still_reports_every_unparseable_record(recorded):
    write_record(recorded, "r1", record_yaml(pipeline="b", changes="[]"))

    problems = record_problems(recorded, "a")

    assert len(problems) == 1
    assert problems[0].startswith("pipelines/accepted/r1.yaml: changes: ")


def test_a_record_of_a_pipeline_that_failed_to_load_keeps_only_that_error(recorded):
    write_pipeline(recorded, "b", "name: b\nsteps: oops\n")
    write_record(recorded, "r1", record_yaml(pipeline="b"))

    problems = validate_command.validate_project(recorded, None).problems

    assert problems and all(line.startswith("pipelines/b.yaml: ") for line in problems)


def test_the_cli_fails_on_a_bad_record(recorded, monkeypatch):
    monkeypatch.chdir(recorded)
    write_record(recorded, "r1", record_yaml(pipeline="nope"))

    result = validate()

    assert result.exit_code == 1
    assert "pipelines/accepted/r1.yaml: pipeline: no Pipeline with this name" in result.output
