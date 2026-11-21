from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from adops.domain.environment import ProjectSettings
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    ensure_targets_defined,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.transform import TransformStep

SETTINGS_FILE = "adops.yaml"
PIPELINES_DIR = "pipelines"
DBT_PROJECT_FILE = "dbt_project.yml"


@dataclass(frozen=True)
class Report:
    problems: list[str]
    skipped: list[str]
    schedules: list[str]


def validate(
    pipeline: Annotated[
        str | None,
        typer.Argument(help="Pipeline name (pipelines/<name>.yaml). Default: every Pipeline."),
    ] = None,
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Check adops.yaml and Pipelines without connecting to anything.

    Skipped Checks are listed as `table: check skipped — reason`, or `step: check skipped — reason`
    for a Reverse Sync Step. When everything is valid, each Pipeline's Schedule is listed as
    `pipeline: schedule (cron <cron>, <time zone>)` or `pipeline: no schedule`.
    """
    report = validate_project(project, pipeline)
    for line in report.skipped:
        typer.echo(line)
    if report.problems:
        for line in report.problems:
            typer.echo(line, err=True)
        raise typer.Exit(1)
    for line in report.schedules:
        typer.echo(line)
    typer.echo("ok")


def validate_project(project: Path, pipeline: str | None) -> Report:
    report = Report(problems=[], skipped=[], schedules=[])
    settings = _load(project, Path(SETTINGS_FILE), load_project_settings, report)
    for relative in _pipeline_files(project, pipeline):
        loaded = _load(project, relative, load_pipeline, report)
        if loaded is None:
            continue
        if settings is not None:
            _check_connections(relative, loaded, settings, report)
            report.schedules.append(_schedule_line(loaded, settings.timezone))
        _check_dbt_projects(project, relative, loaded, report)
        report.skipped.extend(_skipped_checks(loaded))
    return report


def _pipeline_files(project: Path, pipeline: str | None) -> list[Path]:
    if pipeline is not None:
        return [Path(PIPELINES_DIR) / f"{pipeline}.yaml"]
    return sorted(path.relative_to(project) for path in (project / PIPELINES_DIR).glob("*.yaml"))


def _load[T](project: Path, relative: Path, loader: Callable[[str], T], report: Report) -> T | None:
    try:
        text = (project / relative).read_text(encoding="utf-8")
    except FileNotFoundError:
        report.problems.append(f"{relative}: file not found")
        return None
    except UnicodeDecodeError:
        report.problems.append(f"{relative}: not UTF-8 text")
        return None
    try:
        return loader(text)
    except PipelineValidationError as error:
        report.problems.extend(f"{relative}: {problem}" for problem in error.problems)
        return None


def _check_connections(
    relative: Path, pipeline: Pipeline, settings: ProjectSettings, report: Report
) -> None:
    for name, environment in sorted(settings.environments.items()):
        for ensure_defined in (ensure_sources_defined, ensure_targets_defined):
            try:
                ensure_defined(pipeline, environment)
            except PipelineValidationError as error:
                report.problems.extend(
                    f"{relative}: {problem} (Environment {name})" for problem in error.problems
                )


def _check_dbt_projects(project: Path, relative: Path, pipeline: Pipeline, report: Report) -> None:
    for step in pipeline.steps:
        if not isinstance(step, TransformStep):
            continue
        dbt_project_file = Path(step.project) / DBT_PROJECT_FILE
        if not (project / dbt_project_file).is_file():
            report.problems.append(
                f"{relative}: Step {step.name}: {dbt_project_file.as_posix()} not found"
            )


def _schedule_line(pipeline: Pipeline, timezone: str) -> str:
    if pipeline.schedule is None:
        return f"{pipeline.name}: no schedule"
    return f"{pipeline.name}: {pipeline.schedule.text} (cron {pipeline.schedule.cron}, {timezone})"


def _skipped_checks(pipeline: Pipeline) -> list[str]:
    tables = sorted(
        (table for step in pipeline.steps if isinstance(step, SyncStep) for table in step.tables),
        key=lambda t: t.name,
    )
    reverse_syncs = sorted(
        (step for step in pipeline.steps if isinstance(step, ReverseSyncStep)),
        key=lambda s: s.name,
    )
    return [
        f"{table.name}: {check} skipped — {table.skip_checks.reason}"
        for table in tables
        if table.skip_checks is not None
        for check in sorted(table.skip_checks.checks)
    ] + [
        f"{step.name}: {check} skipped — {step.skip_checks.reason}"
        for step in reverse_syncs
        if step.skip_checks is not None
        for check in sorted(step.skip_checks.checks)
    ]
