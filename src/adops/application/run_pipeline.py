import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import assert_never

from adops.application.ports import (
    MASK,
    Clock,
    DeployStateStore,
    Orchestrator,
    ProjectFiles,
    ResolvedConnection,
    ResolvedWarehouse,
    ReverseSync,
    ReverseSyncPreview,
    ReverseSyncRead,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    SourceUnreachable,
    StepFingerprintStore,
    StepOutcome,
    StepReport,
    StepStatus,
    StepTask,
    Sync,
    TargetUnreachable,
    Transform,
    TransformResult,
    WarehouseUnreachable,
    WriteFailed,
)
from adops.application.secrets import (
    resolve_connection,
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
from adops.domain.accepted_change_record import AcceptedChangeRecord, Coverage, cover
from adops.domain.assistant_access import DEV
from adops.domain.branch_environment import (
    STAGE,
    SchemaLayout,
    branch_environment_name,
    canonical_step_json,
    first_changed_step,
    raw_schema_var,
    saves_stage_fingerprint,
    step_fingerprint,
    uses_raw_schema_var,
)
from adops.domain.checks import (
    CheckName,
    CheckResult,
    CheckStatus,
    ReverseSyncFacts,
    SourceFacts,
    evaluate_declared_checks,
    evaluate_load_checks,
    evaluate_preflight_checks,
    evaluate_reverse_sync,
)
from adops.domain.environment import Environment
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    ensure_targets_defined,
    format_path,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import Pipeline, Step, SyncStep, SyncTable
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff, compute_diff, preview_sample, writes_allowed
from adops.domain.schema_snapshot import (
    BreakingChange,
    BreakingChangeKind,
    TableShape,
    find_breaking_changes,
)
from adops.domain.transform import TransformStep, evaluate_dbt_tests

NOT_IN_SOURCE = SourceFacts(table_exists=False, columns=frozenset())
URL_PATTERN = re.compile(r"[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
SNAPSHOT_STORE = "schema snapshot store"
CRASHED = TransformResult(tests=(), crashed=True, failed_models=())
_RECORD_HINT = " — add an accepted-change record for {table} to the PR"


class StepError(Exception):
    """A Step that did not pass; its message never contains a secret value."""


class CheckFailedError(StepError):
    def __init__(
        self, step: str, checks: Sequence[CheckResult], rows_loaded: Mapping[str, int]
    ) -> None:
        self.checks = tuple(checks)
        self.rows_loaded = dict(rows_loaded)
        failed = [check.message for check in self.checks if check.status is CheckStatus.FAIL]
        super().__init__(f"Step {step}: Checks failed: " + "; ".join(failed))


class EmptySourceTableError(CheckFailedError):
    def __init__(self, pipeline: str, step: str, table: str, checks: Sequence[CheckResult]) -> None:
        super().__init__(step, checks, {})
        self.table = table
        self.args = (f"Pipeline {pipeline}, Step {step}, table {table}: Source table is empty",)


class SyncFailedError(StepError):
    pass


class SourceUnreachableError(StepError):
    def __init__(self, pipeline: str, step: str, table: str) -> None:
        self.table = table
        super().__init__(f"Pipeline {pipeline}, Step {step}, table {table}: Source is unreachable")


class WarehouseUnreachableError(StepError):
    def __init__(self, pipeline: str, step: str) -> None:
        super().__init__(f"Pipeline {pipeline}, Step {step}: Warehouse is unreachable")


# The Server retries only an unreachable Source or Warehouse (ADR-0014). An unreachable Reverse
# Sync Target is deliberately not retried. Matched by exact type, so a new subclass is not retried
# until it is added here.
RETRYABLE_ERRORS = (SourceUnreachableError, WarehouseUnreachableError)


class ReverseSyncTargetUnreachableError(StepError):
    def __init__(self, pipeline: str, step: str, target: str) -> None:
        self.target = target
        super().__init__(f"Pipeline {pipeline}, Step {step}: Target {target} is unreachable")


class ReverseSyncWriteFailedError(StepError):
    def __init__(self, pipeline: str, step: str, target: str) -> None:
        self.target = target
        super().__init__(
            f"Pipeline {pipeline}, Step {step}: the write to Target {target} failed "
            "and was rolled back; the Target was left unchanged"
        )


class TransformFailedError(StepError):
    def __init__(self, pipeline: str, step: str, failed_models: Sequence[str]) -> None:
        self.failed_models = tuple(failed_models)
        models = f" (models: {', '.join(self.failed_models)})" if self.failed_models else ""
        super().__init__(f"Pipeline {pipeline}, Step {step}: dbt run failed{models}")


class BreakingChangeError(StepError):
    def __init__(
        self,
        step: str,
        changes: Mapping[str, Sequence[BreakingChange]],
        *,
        ask_for_record: bool = False,
    ) -> None:
        self.changes = {table: tuple(found) for table, found in changes.items()}
        super().__init__(
            f"Step {step}: "
            + "; ".join(
                f"Breaking Change in {table}: "
                + ", ".join(change.message for change in found)
                + (_RECORD_HINT.format(table=table) if ask_for_record else "")
                for table, found in self.changes.items()
            )
        )


class StageNotRunError(Exception):
    def __init__(self, pipeline: str) -> None:
        super().__init__(
            f"Pipeline {pipeline}: stage has no Step Fingerprints; run the Pipeline in stage first"
        )


class BranchSourcesNotParameterizedError(PipelineValidationError):
    def __init__(self, transform_step: str, source: str) -> None:
        self.source = source
        variable = raw_schema_var(source)
        schema = f"{{{{ var('{variable}', '{SchemaLayout.default().raw(source)}') }}}}"
        super().__init__(
            [
                f"Step {transform_step}: the Branch rebuilds Source {source}, but the dbt sources "
                f"do not read {variable}, so dbt would read stage's tables; set the schema of "
                f'Source {source} in sources.yml to "{schema}"'
            ]
        )


@dataclass(frozen=True)
class StepFingerprinting:
    store: StepFingerprintStore
    files: ProjectFiles


@dataclass(frozen=True)
class BranchRun:
    """A Branch Environment's run: Steps before `first_changed` are deferred to stage.

    `first_changed` is None when every Step matches stage, so nothing needs to run.
    """

    environment: str
    stage_fingerprints: Mapping[str, str]
    first_changed: int | None


@dataclass(frozen=True)
class DeployRun:
    """A stage or prod deploy's run: Steps before `start_index` are unchanged in the Environment.

    `start_index` is None when every Step is unchanged, so nothing needs to run.
    """

    start_index: int | None
    store: DeployStateStore


@dataclass(frozen=True)
class FingerprintSavingStep:
    """Saves the Step's fingerprint once the Step, Checks included, has passed."""

    task: StepTask
    pipeline: str
    fingerprint: str
    store: StepFingerprintStore

    @property
    def name(self) -> str:
        return self.task.name

    def __call__(self, sync: Sync) -> StepOutcome:
        outcome = self.task(sync)
        try:
            self.store.save(self.pipeline, self.name, self.fingerprint)
            return outcome
        except Exception as error:
            # Only the type: the store's error text may hold a Warehouse secret.
            failure = SyncFailedError(
                f"Step {self.name}: step fingerprint store: {type(error).__name__}"
            )
        raise failure


@dataclass(frozen=True)
class AcceptedChanges:
    changes: tuple[BreakingChange, ...]
    record_ids: tuple[str, ...]


@dataclass(frozen=True)
class PlannedStep:
    step: SyncStep
    source: ResolvedConnection
    warehouse: ResolvedWarehouse
    pipeline: str
    snapshot_store: SchemaSnapshotStore
    clock: Clock
    environment: str = DEV
    records: tuple[AcceptedChangeRecord, ...] = ()

    @property
    def name(self) -> str:
        return self.step.name

    def __call__(self, sync: Sync) -> StepOutcome:
        source_facts = self._guard(lambda: sync.preflight(self.step, self.source))
        checks: list[CheckResult] = []
        for table in self.step.tables:
            checks += evaluate_preflight_checks(table, source_facts.get(table.name, NOT_IN_SOURCE))
        _raise_if_only_empty_tables(self.pipeline, self.name, checks)
        _raise_if_failed(self.name, checks, {})
        shapes = {
            table.name: TableShape(source_facts[table.name].column_types)
            for table in self.step.tables
        }
        accepted = self._accept_breaking(shapes)
        self._relax_dropped(sync, accepted)

        result = self._guard(lambda: sync.sync(self.step, self.source, self.warehouse))
        now = self.clock.now()
        for table in self.step.tables:
            load_facts = result.load_facts.get(table.name)
            if load_facts is None:
                raise SyncFailedError(f"Step {self.name}: no load facts reported for {table.name}")
            checks += evaluate_load_checks(table, load_facts)
            checks += evaluate_declared_checks(table, load_facts, now)
        _raise_if_failed(self.name, checks, result.rows_loaded)
        self._save_shapes(shapes, accepted)
        applied = tuple(
            f"Applied accepted-change record {record_id} in {self.environment}"
            for found in accepted.values()
            for record_id in found.record_ids
        )
        return StepOutcome(tuple(checks), dict(result.rows_loaded), notes=applied)

    def _accept_breaking(self, shapes: Mapping[str, TableShape]) -> dict[str, AcceptedChanges]:
        """The changes records accept, per table; raises if any change is still breaking."""
        breaking: dict[str, tuple[BreakingChange, ...]] = {}
        accepted: dict[str, AcceptedChanges] = {}
        for table, current in shapes.items():
            previous = self._guard(
                lambda table=table: self.snapshot_store.load(
                    self.warehouse, self.pipeline, self.step.source, table
                ),
                SNAPSHOT_STORE,
            )
            if previous is None:
                continue
            changes = find_breaking_changes(previous, current)
            if not changes:
                continue
            coverage = self._cover(table, changes)
            if coverage.uncovered:
                breaking[table] = coverage.uncovered
            else:
                accepted[table] = AcceptedChanges(changes, coverage.used_ids)
        if breaking:
            raise BreakingChangeError(self.name, breaking, ask_for_record=self.environment != DEV)
        return accepted

    def _cover(self, table: str, changes: tuple[BreakingChange, ...]) -> Coverage:
        records = [record for record in self.records if record.table == table]
        if not records:
            return Coverage(changes, ())
        applied = self._guard(
            lambda: self.snapshot_store.applied_record_ids(self.warehouse), SNAPSHOT_STORE
        )
        return cover(changes, records, applied)

    def _relax_dropped(self, sync: Sync, accepted: Mapping[str, AcceptedChanges]) -> None:
        tables = {table.name: table for table in self.step.tables}
        for name, found in accepted.items():
            self._guard(
                lambda name=name, found=found: relax_dropped_columns(
                    sync, self.step, tables[name], self.warehouse, found.changes
                )
            )

    def _save_shapes(
        self, shapes: Mapping[str, TableShape], accepted: Mapping[str, AcceptedChanges]
    ) -> None:
        for table, shape in shapes.items():
            found = accepted.get(table)
            self._guard(
                lambda table=table, shape=shape, found=found: self._save_shape(table, shape, found),
                SNAPSHOT_STORE,
            )

    def _save_shape(self, table: str, shape: TableShape, found: AcceptedChanges | None) -> None:
        if found is None:
            self.snapshot_store.save(self.warehouse, self.pipeline, self.step.source, table, shape)
        else:
            self.snapshot_store.save(
                self.warehouse,
                self.pipeline,
                self.step.source,
                table,
                shape,
                applied_record_ids=found.record_ids,
            )

    def _guard[T](self, call: Callable[[], T], component: str | None = None) -> T:
        try:
            return call()
        except SourceUnreachable as marker:
            failure: StepError = SourceUnreachableError(self.pipeline, self.name, marker.table)
        except WarehouseUnreachable:
            failure = WarehouseUnreachableError(self.pipeline, self.name)
        except StepError as error:
            failure = error
        except Exception as error:
            where = f"{component}: " if component else ""
            message = self._redact(f"{type(error).__name__}: {error}")
            failure = SyncFailedError(f"Step {self.name}: {where}{message}")
        # Raised outside `except` so the original error, which may hold a secret, is not chained.
        raise failure

    def _redact(self, text: str) -> str:
        return _redact(text, (self.source, self.warehouse))


@dataclass(frozen=True)
class PlannedTransformStep:
    step: TransformStep
    warehouse: ResolvedWarehouse
    pipeline: str
    transform: Transform

    @property
    def name(self) -> str:
        return self.step.name

    def __call__(self, sync: Sync) -> StepOutcome:
        result = self._run()
        if result.crashed or result.failed_models:
            raise TransformFailedError(self.pipeline, self.name, result.failed_models)
        checks = evaluate_dbt_tests(result.tests)
        _raise_if_failed(self.name, checks, {})
        return StepOutcome(checks, {})

    def _run(self) -> TransformResult:
        try:
            return self.transform.run(self.step, self.warehouse)
        except Exception:
            # dbt's own error text may hold the Warehouse password, so none of it is kept.
            return CRASHED


@dataclass(frozen=True)
class PlannedReverseSyncStep:
    step: ReverseSyncStep
    warehouse: ResolvedWarehouse
    target: ResolvedConnection
    environment: str
    pipeline: str
    reverse_sync: ReverseSync

    @property
    def name(self) -> str:
        return self.step.name

    def __call__(self, sync: Sync) -> StepOutcome:
        read = self._guard(lambda: self.reverse_sync.read(self.step, self.warehouse, self.target))
        diff = compute_diff(
            read.result_rows,
            read.target_rows,
            self.step.key,
            self.step.mapping,
            self.step.delete_missing,
        )
        checks = evaluate_reverse_sync(self.step, _reverse_sync_facts(read, diff))
        _raise_if_failed(self.name, checks, {})
        written = writes_allowed(self.environment)
        if written:
            self._guard(lambda: self.reverse_sync.apply(self.step, self.target, diff))
        return StepOutcome(checks, {}, _preview(diff, written))

    def _guard[T](self, call: Callable[[], T]) -> T:
        try:
            return call()
        except TargetUnreachable:
            failure: StepError = ReverseSyncTargetUnreachableError(
                self.pipeline, self.name, self.step.target
            )
        except WriteFailed:
            failure = ReverseSyncWriteFailedError(self.pipeline, self.name, self.step.target)
        except WarehouseUnreachable:
            failure = WarehouseUnreachableError(self.pipeline, self.name)
        except Exception as error:
            message = _redact(f"{type(error).__name__}: {error}", (self.target, self.warehouse))
            failure = SyncFailedError(f"Step {self.name}: {message}")
        # Raised outside `except` so the original error, which may hold a secret, is not chained.
        raise failure


def relax_dropped_columns(
    sync: Sync,
    step: SyncStep,
    table: SyncTable,
    warehouse: ResolvedWarehouse,
    changes: Iterable[BreakingChange],
) -> tuple[str, ...]:
    """Drop NOT NULL from the accepted changes' dropped columns; returns those columns."""
    dropped = tuple(
        change.column for change in changes if change.kind is BreakingChangeKind.DROPPED
    )
    if dropped:
        # Loaded rows no longer carry these columns, so a NOT NULL copied from the Source would
        # reject every new row.
        sync.make_columns_nullable(step, table, warehouse, dropped)
    return dropped


def _reverse_sync_facts(read: ReverseSyncRead, diff: Diff) -> ReverseSyncFacts:
    return ReverseSyncFacts(
        target_table_exists=read.target_table_exists,
        missing_target_columns=read.missing_target_columns,
        result_rows=len(read.result_rows),
        duplicate_keys=read.duplicate_keys,
        null_keys=read.null_keys,
        target_rows=len(read.target_rows),
        diff_updates=len(diff.updates),
        diff_deletes=len(diff.deletes) + len(diff.kept),
    )


def _preview(diff: Diff, written: bool) -> ReverseSyncPreview:
    return ReverseSyncPreview(
        inserts=len(diff.inserts),
        updates=len(diff.updates),
        deletes=len(diff.deletes),
        kept=len(diff.kept),
        sample=preview_sample(diff),
        written=written,
    )


def _redact(text: str, connections: Iterable[ResolvedWarehouse]) -> str:
    for connection in connections:
        if isinstance(connection, ResolvedConnection):
            text = text.replace(connection.password, MASK)
    return URL_PATTERN.sub(MASK, text)


def _raise_if_only_empty_tables(pipeline: str, step: str, checks: Sequence[CheckResult]) -> None:
    failed = [check for check in checks if check.status is CheckStatus.FAIL]
    if failed and all(check.check == CheckName.SOURCE_NOT_EMPTY for check in failed):
        raise EmptySourceTableError(pipeline, step, failed[0].table, checks)


def _raise_if_failed(
    step: str, checks: Sequence[CheckResult], rows_loaded: Mapping[str, int]
) -> None:
    if any(check.status is CheckStatus.FAIL for check in checks):
        raise CheckFailedError(step, checks, rows_loaded)


class InOrderOrchestrator:
    """Runs Steps in Pipeline order; after the first failure every later Step is skipped."""

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        reports: list[StepReport] = []
        for task in plan.steps:
            if reports and reports[-1].status is not StepStatus.PASSED:
                reports.append(StepReport(task.name, StepStatus.SKIPPED))
            else:
                reports.append(run_step(task, sync))
        return RunResult(plan.pipeline, plan.environment, tuple(reports))


def run_step(task: StepTask, sync: Sync) -> StepReport:
    try:
        outcome = task(sync)
    except CheckFailedError as error:
        return StepReport(
            task.name, StepStatus.CHECK_FAILED, error.checks, error.rows_loaded, str(error)
        )
    except StepError as error:
        return StepReport(
            task.name,
            StepStatus.FAILED,
            error=str(error),
            retryable=type(error) in RETRYABLE_ERRORS,
        )
    return StepReport(
        task.name,
        StepStatus.PASSED,
        outcome.checks,
        outcome.rows_loaded,
        preview=outcome.preview,
        notes=outcome.notes,
    )


def run_pipeline(
    pipeline_text: str,
    settings_text: str,
    environment_name: str,
    env: Mapping[str, str],
    sync: Sync,
    snapshot_store: SchemaSnapshotStore,
    clock: Clock,
    orchestrator: Orchestrator | None = None,
    transform: Transform | None = None,
    reverse_sync: ReverseSync | None = None,
    fingerprinting: StepFingerprinting | None = None,
    branch: BranchRun | None = None,
    *,
    records: Sequence[AcceptedChangeRecord] = (),
    deploy: DeployRun | None = None,
) -> RunResult:
    """Run a Pipeline; a Branch run uses stage's settings under the Branch Environment's name.

    Outside dev, the Pipeline's accepted-change records let Sync Steps past the Breaking Changes
    they cover; dev accepts by hand with accept-schema. A deploy run reruns only from its start
    Step and, once it passes, marks the Pipeline's records seen in the Environment's Deploy State.
    """
    if branch is not None and deploy is not None:
        raise ValueError("a run is either a Branch run or a deploy run, not both")
    pipeline = load_pipeline(pipeline_text)
    settings = load_project_settings(settings_text)
    if branch is None:
        environment = pick_environment(settings.environments, environment_name)
        first: int | None = 0 if deploy is None else deploy.start_index
    else:
        if branch.environment != environment_name:
            raise ValueError("a Branch run must run under its Branch Environment's name")
        environment = stage_environment(settings.environments)
        first = branch.first_changed
    ensure_sources_defined(pipeline, environment)
    ensure_targets_defined(pipeline, environment)
    not_rerun = _deferred if deploy is None else _unchanged
    if first is None:
        return RunResult(pipeline.name, environment_name, not_rerun(pipeline.steps))
    pipeline_records = (
        () if environment_name == DEV else [r for r in records if r.pipeline == pipeline.name]
    )
    plan = _build_plan(
        pipeline,
        environment_name,
        environment,
        env,
        snapshot_store,
        clock,
        transform,
        reverse_sync,
        first,
        pipeline_records,
    )
    if fingerprinting is not None and saves_stage_fingerprint(environment_name):
        plan = _saving_fingerprints(plan, pipeline, fingerprinting)
    executed = (orchestrator or InOrderOrchestrator()).execute(plan, sync)
    result = RunResult(
        executed.pipeline, executed.environment, not_rerun(pipeline.steps[:first]) + executed.steps
    )
    if deploy is not None and result.succeeded:
        return _saving_seen_records(result, deploy.store, [r.id for r in pipeline_records])
    return result


def _saving_seen_records(
    result: RunResult, store: DeployStateStore, record_ids: Sequence[str]
) -> RunResult:
    try:
        store.save_seen_records(result.pipeline, record_ids)
        return result
    except Exception as error:
        last = result.steps[-1]
        # Only the type: the store's error text may hold a Warehouse secret.
        failed = replace(
            last,
            status=StepStatus.FAILED,
            error=f"Step {last.name}: deploy state store: {type(error).__name__}",
        )
    return RunResult(result.pipeline, result.environment, result.steps[:-1] + (failed,))


def plan_branch_run(
    pipeline_text: str, environment: str, fingerprinting: StepFingerprinting
) -> BranchRun:
    pipeline = load_pipeline(pipeline_text)
    stage = fingerprinting.store.load(pipeline.name)
    if not stage:
        raise StageNotRunError(pipeline.name)
    current = current_fingerprints(pipeline.steps, fingerprinting.files)
    first = first_changed_step(current, stage)
    if first is not None:
        _ensure_sources_parameterized(pipeline.steps[first:], fingerprinting.files)
    return BranchRun(environment, stage, first)


def current_fingerprints(
    steps: Sequence[Step], project_files: ProjectFiles
) -> tuple[tuple[str, str], ...]:
    """Each Step's name and fingerprint in Step order; a Transform's covers its dbt project."""
    return tuple(
        (step.name, step_fingerprint(canonical_step_json(step), _step_files(step, project_files)))
        for step in steps
    )


def _step_files(step: Step, project_files: ProjectFiles) -> Mapping[str, bytes]:
    if isinstance(step, TransformStep):
        return project_files.read_tree(step.project)
    return {}


def _ensure_sources_parameterized(running: Sequence[Step], project_files: ProjectFiles) -> None:
    rebuilt: dict[str, None] = {}
    for step in running:
        if isinstance(step, SyncStep):
            rebuilt[step.source] = None
        elif isinstance(step, TransformStep):
            files = project_files.read_tree(step.project)
            for source in rebuilt:
                if not uses_raw_schema_var(files, source):
                    raise BranchSourcesNotParameterizedError(step.name, source)


def _saving_fingerprints(
    plan: RunPlan, pipeline: Pipeline, fingerprinting: StepFingerprinting
) -> RunPlan:
    fingerprints = dict(current_fingerprints(pipeline.steps, fingerprinting.files))
    steps = tuple(
        FingerprintSavingStep(task, pipeline.name, fingerprints[task.name], fingerprinting.store)
        for task in plan.steps
    )
    return RunPlan(plan.pipeline, plan.environment, steps)


def _deferred(steps: Sequence[Step]) -> tuple[StepReport, ...]:
    return tuple(StepReport(step.name, StepStatus.DEFERRED) for step in steps)


def _unchanged(steps: Sequence[Step]) -> tuple[StepReport, ...]:
    return tuple(StepReport(step.name, StepStatus.UNCHANGED) for step in steps)


def stage_environment(environments: Mapping[str, Environment]) -> Environment:
    if STAGE not in environments:
        location = format_path(("environments", STAGE))
        raise PipelineValidationError(
            [f"{location}: a Branch run needs a stage Environment in adops.yaml, and none is set"]
        )
    return environments[STAGE]


def stage_branch_name(stage: Environment, branch: str) -> str:
    """The Branch Environment a Git branch gets, sized to fit stage's longest Source name."""
    longest_source = max(stage.sources, key=lambda name: len(name.encode()), default="")
    return branch_environment_name(branch, longest_source)


def pick_environment(environments: Mapping[str, Environment], name: str) -> Environment:
    if name not in environments:
        available = ", ".join(sorted(environments))
        location = format_path(("environments", name))
        raise PipelineValidationError(
            [f"{location}: no Environment with this name (available: {available})"]
        )
    return environments[name]


def _build_plan(
    pipeline: Pipeline,
    environment_name: str,
    environment: Environment,
    env: Mapping[str, str],
    snapshot_store: SchemaSnapshotStore,
    clock: Clock,
    transform: Transform | None,
    reverse_sync: ReverseSync | None,
    first: int = 0,
    records: Sequence[AcceptedChangeRecord] = (),
) -> RunPlan:
    running = pipeline.steps[first:]
    sources = [environment.sources[step.source] for step in running if isinstance(step, SyncStep)]
    targets = [
        environment.targets[step.target] for step in running if isinstance(step, ReverseSyncStep)
    ]
    secrets = resolve_secret_references(
        env, secret_references(environment.warehouse, sources, targets)
    )
    warehouse = resolve_warehouse(environment.warehouse, secrets)
    steps: list[StepTask] = []
    for index, step in enumerate(running, start=first):
        if isinstance(step, SyncStep):
            source = resolve_connection(environment.sources[step.source], secrets)
            tables = {table.name for table in step.tables}
            steps.append(
                PlannedStep(
                    step,
                    source,
                    warehouse,
                    pipeline.name,
                    snapshot_store,
                    clock,
                    environment_name,
                    tuple(record for record in records if record.table in tables),
                )
            )
        elif isinstance(step, TransformStep):
            if transform is None:
                raise _missing_engine(index, "a Transform Step needs a Transform engine")
            steps.append(PlannedTransformStep(step, warehouse, pipeline.name, transform))
        elif isinstance(step, ReverseSyncStep):
            if reverse_sync is None:
                raise _missing_engine(index, "a Reverse Sync Step needs a Reverse Sync engine")
            target = resolve_connection(environment.targets[step.target], secrets)
            steps.append(
                PlannedReverseSyncStep(
                    step, warehouse, target, environment_name, pipeline.name, reverse_sync
                )
            )
        else:
            assert_never(step)
    return RunPlan(pipeline.name, environment_name, tuple(steps))


def _missing_engine(index: int, need: str) -> PipelineValidationError:
    return PipelineValidationError([f"{format_path(('steps', index))}: {need}, and none is set"])
