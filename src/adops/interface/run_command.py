import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, NamedTuple, NoReturn

import typer
from dotenv import dotenv_values

from adops.application.ports import (
    Clock,
    Orchestrator,
    ProjectFiles,
    ResolvedConnection,
    ResolvedWarehouse,
    ReverseSync,
    ReverseSyncPreview,
    ReverseSyncRead,
    RunResult,
    SchemaSnapshotStore,
    StepFingerprintStore,
    StepReport,
    StepStatus,
    Sync,
    Transform,
    TransformResult,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import (
    StageNotRunError,
    StepFingerprinting,
    WarehouseUnreachableError,
    pick_environment,
    plan_branch_run,
    run_pipeline,
    stage_branch_name,
    stage_environment,
)
from adops.application.secrets import (
    MissingSecretError,
    redact_secret_values,
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
from adops.domain.accepted_change_record import AcceptedChangeRecord, load_accepted_record
from adops.domain.branch_environment import (
    STAGE,
    SchemaLayout,
    read_layout,
)
from adops.domain.environment import Environment
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline, load_project_settings
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff
from adops.domain.run_log import is_plain_name
from adops.domain.transform import TransformStep
from adops.infrastructure.clock import SystemClock
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dbt_transform import DbtTransform
from adops.infrastructure.dlt_sync import DltSync
from adops.infrastructure.postgres_reverse_sync import PostgresReverseSync
from adops.infrastructure.run_log_store import write_run_log
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore
from adops.infrastructure.step_fingerprint_store import ProjectTree, WarehouseFingerprintStore
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT, warehouse_lock
from adops.interface import project_secrets

SETTINGS_FILE = Path("adops.yaml")
PIPELINES_DIR = Path("pipelines")
ACCEPTED_RECORDS_DIR = "accepted"
DOTENV_FILE = ".env"
DAGSTER_FILE = "dagster.yaml"


DEFAULT_LAYOUT = SchemaLayout.default()
BRANCH_WITH_ENV = "cannot be combined with --env: a Branch Environment always derives from stage"


class ProjectFileError(Exception):
    pass


RUN_ERRORS = (
    PipelineValidationError,
    ProjectFileError,
    MissingSecretError,
    JobCompileError,
    StageNotRunError,
    WarehouseLocked,
)


def error_lines(error: Exception) -> tuple[str, ...]:
    """What `adops run` prints for one of RUN_ERRORS: already redacted, never a traceback."""
    if isinstance(error, PipelineValidationError):
        return tuple(error.problems)
    return (str(error),)


def build_sync(
    project: Path,
    pipeline: str,
    environment: str,
    kind: str,
    *,
    layout: SchemaLayout = DEFAULT_LAYOUT,
) -> Sync:
    return DltSync(project, pipeline, environment=environment, warehouse_kind=kind, layout=layout)


def build_snapshot_store(
    project: Path,
    environment: str,
    kind: str,
    *,
    layout: SchemaLayout = DEFAULT_LAYOUT,
    read_only: bool = False,
) -> SchemaSnapshotStore:
    return WarehouseSnapshotStore(
        project, kind, environment=environment, layout=layout, read_only=read_only
    )


def build_clock() -> Clock:
    return SystemClock()


RunLogWriter = Callable[[Path, str, str, datetime, str], Path | None]


def build_run_log_writer() -> RunLogWriter:
    return write_run_log


def build_orchestrator() -> Orchestrator:
    return DagsterOrchestrator()


def build_transform(
    project: Path,
    kind: str,
    *,
    target_schema: str = DEFAULT_LAYOUT.analytics,
    raw_schemas: Mapping[str, str] | None = None,
) -> Transform:
    return DbtTransform(
        project, warehouse_kind=kind, target_schema=target_schema, raw_schemas=raw_schemas or {}
    )


def build_reverse_sync(
    project: Path,
    environment: str,
    kind: str,
    *,
    analytics_schema: str = DEFAULT_LAYOUT.analytics,
) -> ReverseSync:
    return PostgresReverseSync(
        project, environment=environment, warehouse_kind=kind, analytics_schema=analytics_schema
    )


def build_fingerprint_store(
    project: Path, environment: str, kind: str, warehouse: ResolvedWarehouse
) -> StepFingerprintStore:
    return WarehouseFingerprintStore(project, kind, warehouse, environment=environment)


def build_project_files(project: Path) -> ProjectFiles:
    return ProjectTree(project)


def build_warehouse_lock(
    warehouse: ResolvedWarehouse,
    project: Path,
    environment: str,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> AbstractContextManager[None]:
    return warehouse_lock(warehouse, project, environment, timeout)


@dataclass(frozen=True)
class StepTransforms:
    """One Transform per Step, because each Step of a Branch run reads its own raw schemas."""

    by_step: Mapping[str, Transform]

    def run(self, step: TransformStep, warehouse: ResolvedWarehouse) -> TransformResult:
        return self.by_step[step.name].run(step, warehouse)


@dataclass(frozen=True)
class StepReverseSyncs:
    """One Reverse Sync per Step, because each Step of a Branch run reads its own analytics."""

    by_step: Mapping[str, ReverseSync]

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead:
        return self.by_step[step.name].read(step, warehouse, target)

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        self.by_step[step.name].apply(step, target, diff)


def warehouse_kind(settings_text: str, environment: str) -> str:
    """The Warehouse type of the chosen Environment, so adapters are built for that kind."""
    settings = load_project_settings(settings_text)
    return pick_environment(settings.environments, environment).warehouse.type


def run(
    pipeline: Annotated[
        str | None, typer.Argument(help="Pipeline name (pipelines/<name>.yaml).")
    ] = None,
    run_all: Annotated[
        bool,
        typer.Option("--all", help="Run every Pipeline in name order; stop at the first failure."),
    ] = False,
    env: Annotated[
        str | None, typer.Option("--env", help="Environment from adops.yaml [default: dev].")
    ] = None,
    branch: Annotated[
        str | None,
        typer.Option(
            "--branch",
            help="Run in this Git branch's Branch Environment, built from stage's settings.",
        ),
    ] = None,
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Run a Pipeline: sync its tables into the Warehouse, run its dbt Transforms and its Checks.

    Reverse Sync Steps write their Target only in the stage and prod Environments; every other
    Environment prints a Preview of what would change.

    With --branch, only the Steps that differ from stage's last successful run are rebuilt, into
    the Branch's own schemas in stage's Warehouse; earlier Steps are deferred to stage.

    Variables in the Project's .env are used unless the same name is set in your shell.
    """
    if env is not None and branch is not None:
        raise typer.BadParameter(BRANCH_WITH_ENV, param_hint="'--branch'")
    root = project.resolve()
    if run_all:
        if pipeline is not None:
            raise typer.BadParameter(
                "cannot be combined with a Pipeline name", param_hint="'--all'"
            )
        names = [path.stem for path in pipeline_files(root)]
        if not names:
            fail(f"No Pipelines in {PIPELINES_DIR}/")
    elif pipeline is None:
        raise typer.BadParameter(
            "give a Pipeline name, or --all to run every Pipeline", param_hint="'PIPELINE'"
        )
    else:
        names = [pipeline]
    for name in names:
        _run_one(root, name, env or "dev", branch)


def pipeline_files(project: Path) -> list[Path]:
    """pipelines/*.yaml in name order: the order `adops run --all` and the Server both use."""
    return sorted((project / PIPELINES_DIR).glob("*.yaml"))


def accepted_record_files(project: Path) -> list[Path]:
    """pipelines/accepted/*.yaml in name order; each file's stem is its record id."""
    return sorted((project / PIPELINES_DIR / ACCEPTED_RECORDS_DIR).glob("*.yaml"))


def load_project_records(project: Path) -> tuple[AcceptedChangeRecord, ...]:
    """The Project's accepted-change records; none when pipelines/accepted/ does not exist."""
    folder = project / PIPELINES_DIR / ACCEPTED_RECORDS_DIR
    misnamed = sorted(folder.glob("*.yml"))
    if misnamed:
        raise PipelineValidationError(
            [f"{_relative(project, path)}: a record file must end in .yaml" for path in misnamed]
        )
    return tuple(_load_record(project, path) for path in accepted_record_files(project))


def _load_record(project: Path, path: Path) -> AcceptedChangeRecord:
    relative = _relative(project, path)
    try:
        return load_accepted_record(path.stem, _read(project, Path(relative)))
    except PipelineValidationError as error:
        raise PipelineValidationError(
            [f"{relative}: {problem}" for problem in error.problems]
        ) from None


def _relative(project: Path, path: Path) -> str:
    return path.relative_to(project).as_posix()


def _run_one(project: Path, pipeline: str, env: str, branch: str | None) -> None:
    try:
        result = run_project(project, pipeline, env, branch)
    except RUN_ERRORS as error:
        fail(*error_lines(error))
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while running {pipeline}")
    for line in result_output(result):
        typer.echo(line.text, err=line.err)
    if not result.succeeded:
        raise typer.Exit(1)


def run_project(
    project: Path,
    pipeline: str,
    environment: str,
    branch: str | None = None,
    *,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> RunResult:
    """Run a Pipeline, waiting up to `lock_timeout` seconds for its Warehouse lock.

    Writes one Run Log holding what `adops run` prints for it, unless the Environment or
    Pipeline name cannot safely name a Run Log file.
    """
    try:
        result = _run_project(project, pipeline, environment, branch, lock_timeout)
    except RUN_ERRORS as error:
        # A Run Log problem never changes the run's outcome.
        with suppress(Exception):
            _log_failure(project, pipeline, environment, branch, error)
        raise
    with suppress(Exception):
        _write_run_log(project, result.environment, result.pipeline, result_lines(result))
    return result


def _run_project(
    project: Path, pipeline: str, environment: str, branch: str | None, lock_timeout: float
) -> RunResult:
    with _scoped_environ(_dagster_environ(project)):
        if branch is None:
            return run_in_environment(
                project, pipeline, environment, build_orchestrator(), lock_timeout=lock_timeout
            )
        settings_text, pipeline_text = read_project_files(project, pipeline)
        return _run_branch(
            project,
            pipeline,
            branch,
            settings_text,
            pipeline_text,
            project_env(project),
            lock_timeout,
        )


def _log_failure(
    project: Path, pipeline: str, environment: str, branch: str | None, error: Exception
) -> None:
    name = _failed_run_environment(project, environment, branch)
    if name is not None and is_plain_name(pipeline):
        _write_run_log(project, name, pipeline, error_lines(error))


def _failed_run_environment(project: Path, environment: str, branch: str | None) -> str | None:
    """The Environment a failed run used, or None when it cannot be named safely."""
    if branch is None:
        return environment if is_plain_name(environment) else None
    try:
        stage = stage_environment(load_project_settings(read_settings(project)).environments)
        return stage_branch_name(stage, branch)
    except RUN_ERRORS:
        return None


def _write_run_log(project: Path, environment: str, pipeline: str, lines: Sequence[str]) -> None:
    text = redact_secret_values("".join(f"{line}\n" for line in lines), _secret_values(project))
    build_run_log_writer()(project, environment, pipeline, build_clock().now(), text)


def _secret_values(project: Path) -> list[str]:
    """Best effort: when adops.yaml or .env cannot be read, the text is already redacted."""
    try:
        return project_secrets.project_secret_values(project)
    except (*RUN_ERRORS, OSError, ValueError):
        return []


def _dagster_environ(project: Path) -> dict[str, str]:
    if not (project / DAGSTER_FILE).is_file():
        return {}
    # Dagster resolves dagster.yaml's `env:` values from the process environment.
    return {**project_env(project), "DAGSTER_HOME": str(project)}


def run_in_environment(
    project: Path,
    pipeline: str,
    environment: str,
    orchestrator: Orchestrator,
    *,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> RunResult:
    """Run a Pipeline in a named Environment, holding its Warehouse lock for the whole run.

    The Project files and .env are read at call time.
    """
    settings_text, pipeline_text = read_project_files(project, pipeline)
    env = project_env(project)
    chosen = pick_environment(load_project_settings(settings_text).environments, environment)
    kind = chosen.warehouse.type
    warehouse = resolve_environment_warehouse(chosen, env)

    def run() -> RunResult:
        return run_pipeline(
            pipeline_text,
            settings_text,
            environment,
            env,
            build_sync(project, pipeline, environment, kind),
            build_snapshot_store(project, environment, kind),
            build_clock(),
            orchestrator,
            build_transform(project, kind),
            build_reverse_sync(project, environment, kind),
            _fingerprinting(project, environment, kind, warehouse),
            records=load_project_records(project),
        )

    return _holding_lock(
        project, environment, warehouse, pipeline_text, environment, run, lock_timeout
    )


def _run_branch(
    project: Path,
    pipeline: str,
    branch: str,
    settings_text: str,
    pipeline_text: str,
    env: Mapping[str, str],
    lock_timeout: float,
) -> RunResult:
    stage = stage_environment(load_project_settings(settings_text).environments)
    name = stage_branch_name(stage, branch)
    warehouse = resolve_environment_warehouse(stage, env)

    def run() -> RunResult:
        return _run_branch_locked(
            project, pipeline, name, stage, warehouse, settings_text, pipeline_text, env
        )

    # A Branch Environment lives in stage's Warehouse, so it shares stage's lock.
    return _holding_lock(project, STAGE, warehouse, pipeline_text, name, run, lock_timeout)


def _run_branch_locked(
    project: Path,
    pipeline: str,
    name: str,
    stage: Environment,
    warehouse: ResolvedWarehouse,
    settings_text: str,
    pipeline_text: str,
    env: Mapping[str, str],
) -> RunResult:
    kind = stage.warehouse.type
    fingerprinting = _fingerprinting(project, STAGE, kind, warehouse)
    plan = plan_branch_run(pipeline_text, name, fingerprinting)
    layout = SchemaLayout.for_branch(name)
    steps = load_pipeline(pipeline_text).steps
    reads = {} if plan.first_changed is None else read_layout(layout, plan.first_changed, steps)
    transforms: dict[str, Transform] = {}
    reverse_syncs: dict[str, ReverseSync] = {}
    for step in steps:
        if step.name not in reads:
            continue
        if isinstance(step, TransformStep):
            transforms[step.name] = build_transform(
                project, kind, target_schema=layout.analytics, raw_schemas=reads[step.name].raw
            )
        elif isinstance(step, ReverseSyncStep):
            reverse_syncs[step.name] = build_reverse_sync(
                project, name, kind, analytics_schema=reads[step.name].analytics
            )
    return run_pipeline(
        pipeline_text,
        settings_text,
        name,
        env,
        build_sync(project, pipeline, name, kind, layout=layout),
        build_snapshot_store(project, name, kind, layout=layout),
        build_clock(),
        build_orchestrator(),
        StepTransforms(transforms),
        StepReverseSyncs(reverse_syncs),
        fingerprinting,
        plan,
        records=load_project_records(project),
    )


def _holding_lock(
    project: Path,
    lock_environment: str,
    warehouse: ResolvedWarehouse,
    pipeline_text: str,
    environment: str,
    run: Callable[[], RunResult],
    lock_timeout: float,
) -> RunResult:
    with ExitStack() as stack:
        try:
            stack.enter_context(
                build_warehouse_lock(warehouse, project, lock_environment, lock_timeout)
            )
        except WarehouseUnreachable:
            return _warehouse_unreachable(pipeline_text, environment)
        return run()


def _warehouse_unreachable(pipeline_text: str, environment: str) -> RunResult:
    """Reported as the first Step's failure, as a Sync would, so the Server retries it."""
    pipeline = load_pipeline(pipeline_text)
    first, *rest = pipeline.steps
    error = WarehouseUnreachableError(pipeline.name, first.name)
    failed = StepReport(first.name, StepStatus.FAILED, error=str(error), retryable=True)
    skipped = (StepReport(step.name, StepStatus.SKIPPED) for step in rest)
    return RunResult(pipeline.name, environment, (failed, *skipped))


def resolve_environment_warehouse(
    settings: Environment, env: Mapping[str, str]
) -> ResolvedWarehouse:
    secrets = resolve_secret_references(env, secret_references(settings.warehouse, ()))
    return resolve_warehouse(settings.warehouse, secrets)


def _fingerprinting(
    project: Path, environment: str, kind: str, warehouse: ResolvedWarehouse
) -> StepFingerprinting:
    return StepFingerprinting(
        build_fingerprint_store(project, environment, kind, warehouse),
        build_project_files(project),
    )


def read_project_files(project: Path, pipeline: str) -> tuple[str, str]:
    """adops.yaml and the Pipeline file, as text."""
    return read_settings(project), _read(project, PIPELINES_DIR / f"{pipeline}.yaml")


def read_settings(project: Path) -> str:
    return _read(project, SETTINGS_FILE)


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


class OutputLine(NamedTuple):
    text: str
    err: bool = False


def result_lines(result: RunResult) -> list[str]:
    """What `adops run` prints for a finished run, stdout and stderr in print order."""
    return [line.text for line in result_output(result)]


def result_output(result: RunResult) -> list[OutputLine]:
    lines: list[OutputLine] = []
    for step in result.steps:
        rows = ", ".join(f"{table} {count} rows" for table, count in step.rows_loaded.items())
        if step.status is StepStatus.DEFERRED:
            lines.append(OutputLine(f"step {step.name}: deferred to stage"))
            continue
        status = f"step {step.name}: {step.status}" + (f" — {rows}" if rows else "")
        lines.append(OutputLine(status))
        lines += (OutputLine(f"  {note}") for note in step.notes)
        lines += (
            OutputLine(f"  check {check.table} {check.check}: {check.status}")
            for check in step.checks
        )
        if step.preview is not None:
            lines += (OutputLine(text) for text in _preview_lines(step.preview))
        if step.error:
            lines.append(OutputLine(f"  {step.error}", err=True))
    lines += _summary_lines(result)
    return lines


def _summary_lines(result: RunResult) -> list[OutputLine]:
    pipeline, environment = result.pipeline, result.environment
    if not result.succeeded:
        return [OutputLine(f"Pipeline {pipeline} failed in Environment {environment}.", err=True)]
    lines = []
    # Only a Branch run defers Steps to stage.
    if all(step.status is StepStatus.DEFERRED for step in result.steps):
        unchanged = f"Nothing changed since stage's last run; {environment} uses stage."
        lines.append(OutputLine(unchanged))
    lines.append(OutputLine(f"Pipeline {pipeline} passed in Environment {environment}."))
    return lines


def _preview_lines(preview: ReverseSyncPreview) -> list[str]:
    if preview.written:
        counts = [f"inserted {preview.inserts}", f"updated {preview.updates}"]
        deleted = f"deleted {preview.deletes}"
    else:
        counts = [f"would insert {preview.inserts}", f"would update {preview.updates}"]
        deleted = f"would delete {preview.deletes}"
    counts.append(f"kept {preview.kept} (absent from dbt result)" if preview.kept else deleted)
    lines = [f"  {'wrote' if preview.written else 'Preview'}: {', '.join(counts)}"]
    sample = preview.sample
    lines += _sample_key_lines("insert", sample.inserts)
    if sample.updates:
        changes = (f"{key} ({', '.join(columns)})" for key, columns in sample.updates)
        lines.append(f"  sample update keys: {'; '.join(changes)}")
    lines += _sample_key_lines("delete", sample.deletes)
    lines += _sample_key_lines("kept", sample.kept)
    return lines


def _sample_key_lines(kind: str, keys: tuple[object, ...]) -> list[str]:
    return [f"  sample {kind} keys: {', '.join(str(key) for key in keys)}"] if keys else []


def fail(*lines: str) -> NoReturn:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(1)
