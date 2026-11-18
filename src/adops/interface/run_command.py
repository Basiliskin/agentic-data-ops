import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, NoReturn

import typer
from dotenv import dotenv_values

from adops.application.ports import Clock, Orchestrator, RunResult, SchemaSnapshotStore, Sync
from adops.application.run_pipeline import run_pipeline
from adops.application.secrets import MissingSecretError
from adops.domain.errors import PipelineValidationError
from adops.infrastructure.clock import SystemClock
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dlt_sync import DltSync
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore

SETTINGS_FILE = Path("adops.yaml")
PIPELINES_DIR = Path("pipelines")
DOTENV_FILE = ".env"
DAGSTER_FILE = "dagster.yaml"


class ProjectFileError(Exception):
    pass


def build_sync(project: Path, pipeline: str) -> Sync:
    return DltSync(project, pipeline)


def build_snapshot_store(project: Path) -> SchemaSnapshotStore:
    return WarehouseSnapshotStore(project, "postgres")


def build_clock() -> Clock:
    return SystemClock()


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
        fail(*error.problems)
    except (ProjectFileError, MissingSecretError, JobCompileError) as error:
        fail(str(error))
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while running {pipeline}")
    _print_result(result)
    if not result.succeeded:
        fail(f"Pipeline {result.pipeline} failed in Environment {result.environment}.")
    typer.echo(f"Pipeline {result.pipeline} passed in Environment {result.environment}.")


def run_project(project: Path, pipeline: str, environment: str) -> RunResult:
    settings_text, pipeline_text = read_project_files(project, pipeline)
    env = project_env(project)
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
            build_snapshot_store(project),
            build_clock(),
            build_orchestrator(),
        )


def read_project_files(project: Path, pipeline: str) -> tuple[str, str]:
    """adops.yaml and the Pipeline file, as text."""
    return _read(project, SETTINGS_FILE), _read(project, PIPELINES_DIR / f"{pipeline}.yaml")


def project_env(project: Path) -> dict[str, str]:
    """The Project's .env merged under the process environment, which wins."""
    dotenv = {k: v for k, v in dotenv_values(project / DOTENV_FILE).items() if v is not None}
    return {**dotenv, **os.environ}


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


def fail(*lines: str) -> NoReturn:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(1)
