"""The Server's Dagster code location: `defs` for the one Environment in ADOPS_ENVIRONMENT.

`defs` is built on first access, not at import, so importing this module reads no file and no
environment variable.
"""

import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path

from adops.application.alerts import failure_alert, notify_failure
from adops.application.ports import RunResult, StepReport, StepStatus
from adops.application.run_pipeline import InOrderOrchestrator
from adops.domain.loader import load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline
from adops.domain.schedule import runs_on_schedule
from adops.domain.secret_reference import SecretReference
from adops.infrastructure.dagster_server import Definitions, build_server_definitions
from adops.infrastructure.webhook_alerts import WebhookAlertSender
from adops.interface.run_command import (
    RUN_ERRORS,
    error_lines,
    pipeline_files,
    project_env,
    read_settings,
    run_in_environment,
)
from adops.interface.validate_command import validate_project

PROJECT_VARIABLE = "ADOPS_PROJECT"
ENVIRONMENT_VARIABLE = "ADOPS_ENVIRONMENT"
NOT_SCHEDULED = (
    "ADOPS_ENVIRONMENT must be stage or prod; got an Environment that cannot run on a Schedule"
)
ALERTS_OFF = "alerts are off: no webhook URL is set"


class ServerConfigError(Exception):
    pass


def load_definitions(environ: Mapping[str, str]) -> Definitions:
    project = Path(environ.get(PROJECT_VARIABLE) or Path.cwd()).resolve()
    environment = environ.get(ENVIRONMENT_VARIABLE, "")
    if not runs_on_schedule(environment):
        raise ServerConfigError(NOT_SCHEDULED)
    problems = validate_project(project, None).problems
    if problems:
        raise ServerConfigError("\n".join(problems))
    settings = load_project_settings(read_settings(project))
    if environment not in settings.environments:
        raise ServerConfigError(
            f"{ENVIRONMENT_VARIABLE}: Environment {environment} is not in adops.yaml"
        )
    pipelines = _pipelines(project)
    first_steps = {name: pipeline.steps[0].name for name, pipeline in pipelines.items()}
    return build_server_definitions(
        [(name, pipeline.schedule) for name, pipeline in pipelines.items()],
        settings.timezone,
        _runner(project, environment, first_steps),
        _on_failure(project, settings.alert_webhook_env),
        release=str(project),
    )


def _pipelines(project: Path) -> dict[str, Pipeline]:
    return {
        path.stem: load_pipeline(path.read_text(encoding="utf-8"))
        for path in pipeline_files(project)
    }


def _runner(
    project: Path, environment: str, first_steps: Mapping[str, str]
) -> Callable[[str], RunResult]:
    def run(pipeline: str) -> RunResult:
        # A run waiting for the Warehouse lock holds the single run slot
        # (max_concurrent_runs: 1), so later scheduled runs queue behind it. This is accepted.
        try:
            return run_in_environment(project, pipeline, environment, InOrderOrchestrator())
        except RUN_ERRORS as error:
            message = "; ".join(error_lines(error))
        except Exception as error:
            # Only the type: an unexpected error was never redacted and may hold a secret.
            message = type(error).__name__
        failed = StepReport(first_steps[pipeline], StepStatus.FAILED, error=message)
        return RunResult(pipeline, environment, (failed,))

    return run


def _on_failure(project: Path, webhook: SecretReference | None) -> Callable[[RunResult], None]:
    if webhook is None or not project_env(project).get(webhook.name):
        print(ALERTS_OFF)
    if webhook is None:
        return print_failure

    def report_and_alert(result: RunResult) -> None:
        print_failure(result)
        # Read on each failure, inside the run worker, so the URL never enters Dagster config.
        url = project_env(project).get(webhook.name)
        if not url:
            return
        problem = notify_failure(result, WebhookAlertSender(url))
        if problem:
            print(problem, file=sys.stderr)

    return report_and_alert


def print_failure(result: RunResult) -> None:
    alert = failure_alert(result)
    print(
        f"Pipeline {alert.pipeline} failed in Environment {alert.environment}: "
        f"Step {alert.step}: {alert.message}",
        file=sys.stderr,
    )


def __getattr__(name: str) -> Definitions:
    if name == "defs":
        return load_definitions(os.environ)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
