import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, NoReturn

import typer
from dotenv import dotenv_values

from adops.application.ports import Orchestrator, RunResult, Sync
from adops.application.run_pipeline import run_pipeline
from adops.application.secrets import MissingSecretError
from adops.domain.errors import PipelineValidationError
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dlt_sync import DltSync

SETTINGS_FILE = Path("adops.yaml")
PIPELINES_DIR = Path("pipelines")
DOTENV_FILE = ".env"
DAGSTER_FILE = "dagster.yaml"


class ProjectFileError(Exception):
    pass


def build_sync(project: Path, pipeline: str) -> Sync:
    return DltSync(project, pipeline)


def build_orchestrator() -> Orchestrator:
    return DagsterOrchestrator()


def run(
    pipeline: Annotated[str, typer.Argument(help="Pipeline name (pipelines/<name>.yaml).")],
    env: Annotated[str, typer.Option("--env", help="Environment from adops.yaml.")] = "dev",
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Run a Pipeline: sync its tables into the Warehouse and run its Checks.

    Variables in the Project's .env are used unless the same name is set in your shell.
    """
    try:
        result = run_project(project.resolve(), pipeline, env)
    except PipelineValidationError as error:
        _fail(*error.problems)
    except (ProjectFileError, MissingSecretError, JobCompileError) as error:
        _fail(str(error))
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        _fail(f"unexpected {type(error).__name__} while running {pipeline}")
    _print_result(result)
    if not result.succeeded:
        _fail(f"Pipeline {result.pipeline} failed in Environment {result.environment}.")
    typer.echo(f"Pipeline {result.pipeline} passed in Environment {result.environment}.")


def run_project(project: Path, pipeline: str, environment: str) -> RunResult:
    settings_text = _read(project, SETTINGS_FILE)
    pipeline_text = _read(project, PIPELINES_DIR / f"{pipeline}.yaml")
    dotenv = {k: v for k, v in dotenv_values(project / DOTENV_FILE).items() if v is not None}
    env = {**dotenv, **os.environ}
    dagster_env = {}
    if (project / DAGSTER_FILE).is_file():
        # Dagster resolves dagster.yaml's `env:` values from the process environment.
        dagster_env = {**env, "DAGSTER_HOME": str(project)}
    with _scoped_environ(dagster_env):
        return run_pipeline(
            pipeline_text,
            settings_text,
            environment,
            env,
            build_sync(project, pipeline),
            build_orchestrator(),
        )


def _read(project: Path, relative: Path) -> str:
    try:
        return (project / relative).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ProjectFileError(f"{relative}: file not found") from None


@contextmanager
def _scoped_environ(updates: Mapping[str, str]) -> Iterator[None]:
    saved = dict(os.environ)
    os.environ.update(updates)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _print_result(result: RunResult) -> None:
    for step in result.steps:
        rows = ", ".join(f"{table} {count} rows" for table, count in step.rows_loaded.items())
        typer.echo(f"step {step.name}: {step.status}" + (f" — {rows}" if rows else ""))
        for check in step.checks:
            typer.echo(f"  check {check.table} {check.check}: {check.status}")
        if step.error:
            typer.echo(f"  {step.error}", err=True)


def _fail(*lines: str) -> NoReturn:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(1)
