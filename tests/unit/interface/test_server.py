import json
import re
from pathlib import Path

import pytest
import yaml
from dagster import Definitions
from typer.testing import CliRunner

from adops.application.ports import RunResult, StepReport, StepStatus, WarehouseLocked
from adops.application.run_pipeline import InOrderOrchestrator
from adops.interface import cli, run_command, server
from adops.interface.server import ServerConfigError, load_definitions

runner = CliRunner()

SENTINEL = "s3cr3t-server-sentinel"
SCHEDULE = "schedule: daily at 06:00\n"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    path = tmp_path / "project"
    result = runner.invoke(cli.app, ["init", "--demo", str(path)])
    assert result.exit_code == 0, result.output
    pipeline = path / "pipelines" / "shop_sync.yaml"
    pipeline.write_text(pipeline.read_text() + SCHEDULE)
    return path


def environ(project: Path, environment: str = "stage") -> dict[str, str]:
    return {"ADOPS_PROJECT": str(project), "ADOPS_ENVIRONMENT": environment}


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    calls: dict[str, object] = {}

    def capture(pipelines, time_zone, run, on_failure):
        calls.update(pipelines=pipelines, time_zone=time_zone, run=run, on_failure=on_failure)
        return Definitions()

    monkeypatch.setattr(server, "build_server_definitions", capture)
    return calls


@pytest.mark.parametrize("environment", ["dev", "feature-x", "Stage", None])
def test_only_stage_or_prod_may_be_served(project, environment):
    values = environ(project)
    if environment is None:
        del values["ADOPS_ENVIRONMENT"]
    else:
        values["ADOPS_ENVIRONMENT"] = environment

    with pytest.raises(ServerConfigError) as raised:
        load_definitions(values)

    message = str(raised.value)
    assert "ADOPS_ENVIRONMENT" in message
    assert "stage or prod" in message
    if environment is not None:
        assert environment not in message


def test_an_environment_missing_from_adops_yaml_is_refused(project):
    settings = yaml.safe_load((project / "adops.yaml").read_text())
    del settings["environments"]["prod"]
    (project / "adops.yaml").write_text(yaml.safe_dump(settings))

    with pytest.raises(ServerConfigError, match=r"^ADOPS_ENVIRONMENT: .* not in adops\.yaml$"):
        load_definitions(environ(project, "prod"))


def test_an_invalid_project_lists_only_sanitized_problems(project):
    (project / "pipelines" / "broken.yaml").write_text(
        f"name: broken\nsteps:\n  - name: s\n    source: {SENTINEL}\n    tables: 7\n"
    )

    with pytest.raises(ServerConfigError) as raised:
        load_definitions(environ(project))

    lines = str(raised.value).splitlines()
    assert lines
    assert all(re.match(r"^pipelines/broken\.yaml: \S", line) for line in lines)
    assert SENTINEL not in str(raised.value)


def test_each_pipeline_becomes_a_job_and_its_schedule_runs_in_the_project_time_zone(project):
    (project / "adops.yaml").write_text(
        (project / "adops.yaml").read_text() + "timezone: Europe/Berlin\n"
    )

    definitions = load_definitions(environ(project))

    assert definitions.get_job_def("shop_sync").name == "shop_sync"
    [schedule] = definitions.schedules or []
    assert schedule.name == "shop_sync_schedule"
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "Europe/Berlin"


def test_a_pipeline_without_a_schedule_gets_a_job_but_no_schedule(project, captured):
    pipeline = project / "pipelines" / "shop_sync.yaml"
    pipeline.write_text(pipeline.read_text().replace(SCHEDULE, ""))

    load_definitions(environ(project))

    assert captured["pipelines"] == [("shop_sync", None)]
    assert captured["time_zone"] == "UTC"


def test_the_project_defaults_to_the_working_directory(project, captured, monkeypatch):
    monkeypatch.chdir(project)

    load_definitions({"ADOPS_ENVIRONMENT": "stage"})

    [(name, schedule)] = captured["pipelines"]
    assert (name, schedule.cron) == ("shop_sync", "0 6 * * *")


def test_runs_go_through_run_in_environment_with_the_in_order_orchestrator(
    project, captured, monkeypatch
):
    calls = []
    passed = RunResult("shop_sync", "stage", (StepReport("sync_shop", StepStatus.PASSED),))

    def run_in_environment(project_dir, pipeline, environment, orchestrator):
        calls.append((project_dir, pipeline, environment, orchestrator))
        return passed

    monkeypatch.setattr(server, "run_in_environment", run_in_environment)
    load_definitions(environ(project))

    assert captured["run"]("shop_sync") is passed
    [(project_dir, pipeline, environment, orchestrator)] = calls
    assert (project_dir, pipeline, environment) == (project.resolve(), "shop_sync", "stage")
    assert isinstance(orchestrator, InOrderOrchestrator)


@pytest.fixture
def secrets_set(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_PASSWORD", SENTINEL)
    monkeypatch.setenv("SHOP_PASSWORD", SENTINEL)


@pytest.mark.usefixtures("secrets_set")
def test_a_scheduled_run_holds_the_environments_warehouse_lock(
    project, captured, locks, monkeypatch
):
    passed = RunResult("shop_sync", "stage", (StepReport("sync_shop", StepStatus.PASSED),))

    def run_pipeline(*_, **__):
        locks.mark("work")
        return passed

    monkeypatch.setattr(run_command, "run_pipeline", run_pipeline)
    load_definitions(environ(project))

    assert captured["run"]("shop_sync") is passed
    assert captured["run"]("shop_sync") is passed
    assert locks.events == [("enter", "stage"), ("work",), ("exit", "stage")] * 2


@pytest.mark.usefixtures("secrets_set")
def test_a_lock_timeout_fails_once_without_retry_and_alerts(project, locks, monkeypatch):
    locks.error = WarehouseLocked("stage", 30 * 60)
    failures: list[RunResult] = []
    monkeypatch.setattr(server, "print_failure", failures.append)
    job = load_definitions(environ(project)).get_job_def("shop_sync")

    outcome = job.execute_in_process(raise_on_error=False)

    assert not outcome.success
    [failure] = failures
    assert failure.steps[0].status is StepStatus.FAILED
    assert failure.steps[0].error == (
        "Another run is using the stage Warehouse; gave up after 30 minutes."
    )
    assert not failure.retryable
    assert SENTINEL not in failure.steps[0].error


def test_a_missing_secret_becomes_the_failed_result_adops_run_prints(
    project, captured, monkeypatch
):
    monkeypatch.delenv("WAREHOUSE_PASSWORD", raising=False)
    monkeypatch.delenv("SHOP_PASSWORD", raising=False)
    cli_result = runner.invoke(
        cli.app, ["run", "shop_sync", "--env", "stage", "--project", str(project)]
    )
    assert cli_result.exit_code == 1
    load_definitions(environ(project))

    result = captured["run"]("shop_sync")

    assert result.steps[0].error == cli_result.output.strip()
    assert "WAREHOUSE_PASSWORD" in result.steps[0].error


def test_an_unexpected_error_keeps_only_its_type_name(project, captured, monkeypatch):
    def explode(*_):
        raise RuntimeError(f"postgres://u:{SENTINEL}@h")

    monkeypatch.setattr(server, "run_in_environment", explode)
    load_definitions(environ(project))

    result = captured["run"]("shop_sync")

    assert result.steps[0].error == "RuntimeError"
    assert not result.succeeded
    assert not result.retryable
    assert [(step.name, step.status) for step in result.steps] == [("sync_shop", StepStatus.FAILED)]


def test_a_failed_run_reaches_on_failure_from_the_dagster_job(project, monkeypatch):
    def explode(*_):
        raise RuntimeError(SENTINEL)

    failures: list[RunResult] = []
    monkeypatch.setattr(server, "run_in_environment", explode)
    monkeypatch.setattr(server, "print_failure", failures.append)
    job = load_definitions(environ(project)).get_job_def("shop_sync")

    outcome = job.execute_in_process(raise_on_error=False)

    assert not outcome.success
    [failure] = failures
    assert (failure.pipeline, failure.environment) == ("shop_sync", "stage")
    assert failure.steps[0].error == "RuntimeError"


@pytest.mark.parametrize(
    ("report", "expected"),
    [
        (
            StepReport("sync_shop", StepStatus.FAILED, error="MissingSecret: X"),
            "Step sync_shop: MissingSecret: X",
        ),
        (StepReport("sync_shop", StepStatus.CHECK_FAILED), "Step sync_shop: check_failed"),
    ],
)
def test_print_failure_writes_one_summary_line(capsys, report, expected):
    passed = StepReport("first", StepStatus.PASSED)

    server.print_failure(RunResult("shop_sync", "stage", (passed, report)))

    assert capsys.readouterr().err == (
        f"Pipeline shop_sync failed in Environment stage: {expected}\n"
    )


def test_defs_is_built_on_access_from_the_process_environment(project, monkeypatch):
    monkeypatch.setenv("ADOPS_PROJECT", str(project))
    monkeypatch.setenv("ADOPS_ENVIRONMENT", "stage")

    assert isinstance(server.defs, Definitions)


def test_other_missing_attributes_still_raise():
    with pytest.raises(AttributeError, match="nope"):
        server.nope  # noqa: B018


WEBHOOK_VARIABLE = "ALERT_WEBHOOK_URL"
TOKEN_PATH = "/services/SENTINEL_TOKEN"


@pytest.fixture
def alerting(project: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv(WEBHOOK_VARIABLE, raising=False)
    assert f"alert_webhook_env: {WEBHOOK_VARIABLE}\n" in (project / "adops.yaml").read_text()

    def explode(*_):
        raise RuntimeError(SENTINEL)

    monkeypatch.setattr(server, "run_in_environment", explode)
    return project


def run_shop_sync(project: Path):
    job = load_definitions(environ(project)).get_job_def("shop_sync")
    return job.execute_in_process(raise_on_error=False)


def failure_outcome(outcome) -> tuple[bool, list[str | None]]:
    events = outcome.get_step_failure_events()
    return outcome.success, [event.event_specific_data.error.message for event in events]


def test_without_alert_webhook_env_alerts_are_off(project, capsys):
    settings = project / "adops.yaml"
    webhook_line = f"alert_webhook_env: {WEBHOOK_VARIABLE}\n"
    settings.write_text(settings.read_text().replace(webhook_line, ""))
    assert "alert_webhook_env" not in settings.read_text()
    load_definitions(environ(project))

    assert capsys.readouterr().out.count("alerts are off") == 1


@pytest.mark.parametrize("value", [None, ""])
def test_an_unset_or_empty_webhook_variable_means_alerts_are_off(alerting, capsys, value):
    if value is not None:
        (alerting / ".env").write_text(f"{WEBHOOK_VARIABLE}={value}\n")

    load_definitions(environ(alerting))

    assert capsys.readouterr().out.count("alerts are off") == 1


def test_a_set_webhook_variable_turns_alerts_on(alerting, capsys, monkeypatch, webhook):
    monkeypatch.setenv(WEBHOOK_VARIABLE, webhook.url())

    load_definitions(environ(alerting))

    assert "alerts are off" not in capsys.readouterr().out


def test_a_final_failure_posts_one_alert_read_at_failure_time(alerting, monkeypatch, webhook):
    job = load_definitions(environ(alerting)).get_job_def("shop_sync")
    monkeypatch.setenv(WEBHOOK_VARIABLE, webhook.url(TOKEN_PATH))

    outcome = job.execute_in_process(raise_on_error=False)

    assert not outcome.success
    [(path, _, body)] = webhook.requests
    assert path == TOKEN_PATH
    assert json.loads(body) == {
        "text": "Pipeline shop_sync failed in Environment stage at Step sync_shop: RuntimeError",
        "pipeline": "shop_sync",
        "environment": "stage",
        "step": "sync_shop",
        "message": "RuntimeError",
    }
    assert SENTINEL not in body.decode()
    run = outcome.dagster_run
    assert "SENTINEL_TOKEN" not in f"{run.run_config} {run.tags} {job.tags} {job.metadata}"


def test_a_failing_webhook_leaves_the_run_outcome_as_with_alerts_off(
    alerting, capfd, monkeypatch, webhook
):
    off = failure_outcome(run_shop_sync(alerting))
    webhook.status = 500
    monkeypatch.setenv(WEBHOOK_VARIABLE, webhook.url(TOKEN_PATH))

    failing = failure_outcome(run_shop_sync(alerting))

    assert failing == off
    assert len(webhook.requests) == 1
    err = capfd.readouterr().err
    assert "alert could not be sent (HTTPError)" in err
    assert "SENTINEL_TOKEN" not in err
    assert "127.0.0.1" not in err


def test_a_webhook_variable_emptied_after_load_sends_nothing(alerting, monkeypatch, webhook):
    monkeypatch.setenv(WEBHOOK_VARIABLE, webhook.url())
    job = load_definitions(environ(alerting)).get_job_def("shop_sync")
    monkeypatch.setenv(WEBHOOK_VARIABLE, "")

    assert not job.execute_in_process(raise_on_error=False).success
    assert webhook.requests == []
