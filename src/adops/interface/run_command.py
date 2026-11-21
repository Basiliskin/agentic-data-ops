import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, NoReturn

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
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
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
from adops.domain.transform import TransformStep
from adops.infrastructure.clock import SystemClock
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dbt_transform import DbtTransform
from adops.infrastructure.dlt_sync import DltSync
from adops.infrastructure.postgres_reverse_sync import PostgresReverseSync
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore
from adops.infrastructure.step_fingerprint_store import ProjectTree, WarehouseFingerprintStore
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT, warehouse_lock

SETTINGS_FILE = Path("adops.yaml")
PIPELINES_DIR = Path("pipelines")
DOTENV_FILE = ".env"
DAGSTER_FILE = "dagster.yaml"


DEFAULT_LAYOUT = SchemaLayout.default()


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
    project: Path, environment: str, kind: str, *, layout: SchemaLayout = DEFAULT_LAYOUT
) -> SchemaSnapshotStore:
    return WarehouseSnapshotStore(project, kind, environment=environment, layout=layout)


def build_clock() -> Clock:
    return SystemClock()


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
        raise typer.BadParameter(
            "cannot be combined with --env: a Branch Environment always derives from stage",
            param_hint="'--branch'",
        )
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


def _run_one(project: Path, pipeline: str, env: str, branch: str | None) -> None:
    try:
        result = run_project(project, pipeline, env, branch)
    except RUN_ERRORS as error:
        fail(*error_lines(error))
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while running {pipeline}")
    _print_result(result)
    if not result.succeeded:
        fail(f"Pipeline {result.pipeline} failed in Environment {result.environment}.")
    if branch is not None and all(s.status is StepStatus.DEFERRED for s in result.steps):
        typer.echo(f"Nothing changed since stage's last run; {result.environment} uses stage.")
    typer.echo(f"Pipeline {result.pipeline} passed in Environment {result.environment}.")


def run_project(
    project: Path,
    pipeline: str,
    environment: str,
    branch: str | None = None,
    *,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> RunResult:
    """Run a Pipeline, waiting up to `lock_timeout` seconds for its Warehouse lock."""
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


def _print_result(result: RunResult) -> None:
    for step in result.steps:
        rows = ", ".join(f"{table} {count} rows" for table, count in step.rows_loaded.items())
        if step.status is StepStatus.DEFERRED:
            typer.echo(f"step {step.name}: deferred to stage")
            continue
        typer.echo(f"step {step.name}: {step.status}" + (f" — {rows}" if rows else ""))
        for check in step.checks:
            typer.echo(f"  check {check.table} {check.check}: {check.status}")
        if step.preview is not None:
            _print_preview(step.preview)
        if step.error:
            typer.echo(f"  {step.error}", err=True)


def _print_preview(preview: ReverseSyncPreview) -> None:
    if preview.written:
        counts = [f"inserted {preview.inserts}", f"updated {preview.updates}"]
        deleted = f"deleted {preview.deletes}"
    else:
        counts = [f"would insert {preview.inserts}", f"would update {preview.updates}"]
        deleted = f"would delete {preview.deletes}"
    counts.append(f"kept {preview.kept} (absent from dbt result)" if preview.kept else deleted)
    typer.echo(f"  {'wrote' if preview.written else 'Preview'}: {', '.join(counts)}")
    sample = preview.sample
    _print_sample_keys("insert", sample.inserts)
    if sample.updates:
        changes = (f"{key} ({', '.join(columns)})" for key, columns in sample.updates)
        typer.echo(f"  sample update keys: {'; '.join(changes)}")
    _print_sample_keys("delete", sample.deletes)
    _print_sample_keys("kept", sample.kept)


def _print_sample_keys(kind: str, keys: tuple[object, ...]) -> None:
    if keys:
        typer.echo(f"  sample {kind} keys: {', '.join(str(key) for key in keys)}")


def fail(*lines: str) -> NoReturn:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(1)
