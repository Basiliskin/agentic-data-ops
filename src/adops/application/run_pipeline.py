import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from adops.application.ports import (
    MASK,
    Clock,
    Orchestrator,
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    SourceUnreachable,
    StepOutcome,
    StepReport,
    StepStatus,
    StepTask,
    Sync,
    Transform,
    TransformResult,
    WarehouseUnreachable,
)
from adops.application.secrets import resolve_secret_references
from adops.domain.checks import (
    CheckName,
    CheckResult,
    CheckStatus,
    SourceFacts,
    evaluate_declared_checks,
    evaluate_load_checks,
    evaluate_preflight_checks,
)
from adops.domain.environment import DuckDbConnection, Environment, PostgresConnection
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    format_path,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.schema_snapshot import BreakingChange, TableShape, find_breaking_changes
from adops.domain.secret_reference import SecretReference
from adops.domain.transform import TransformStep, evaluate_dbt_tests

NOT_IN_SOURCE = SourceFacts(table_exists=False, columns=frozenset())
URL_PATTERN = re.compile(r"[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
SNAPSHOT_STORE = "schema snapshot store"
CRASHED = TransformResult(tests=(), crashed=True, failed_models=())


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


class TransformFailedError(StepError):
    def __init__(self, pipeline: str, step: str, failed_models: Sequence[str]) -> None:
        self.failed_models = tuple(failed_models)
        models = f" (models: {', '.join(self.failed_models)})" if self.failed_models else ""
        super().__init__(f"Pipeline {pipeline}, Step {step}: dbt run failed{models}")


class BreakingChangeError(StepError):
    def __init__(self, step: str, changes: Mapping[str, Sequence[BreakingChange]]) -> None:
        self.changes = {table: tuple(found) for table, found in changes.items()}
        super().__init__(
            f"Step {step}: "
            + "; ".join(
                f"Breaking Change in {table}: " + ", ".join(change.message for change in found)
                for table, found in self.changes.items()
            )
        )


@dataclass(frozen=True)
class PlannedStep:
    step: SyncStep
    source: ResolvedConnection
    warehouse: ResolvedWarehouse
    pipeline: str
    snapshot_store: SchemaSnapshotStore
    clock: Clock

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
        self._raise_if_breaking(shapes)

        result = self._guard(lambda: sync.sync(self.step, self.source, self.warehouse))
        now = self.clock.now()
        for table in self.step.tables:
            load_facts = result.load_facts.get(table.name)
            if load_facts is None:
                raise SyncFailedError(f"Step {self.name}: no load facts reported for {table.name}")
            checks += evaluate_load_checks(table, load_facts)
            checks += evaluate_declared_checks(table, load_facts, now)
        _raise_if_failed(self.name, checks, result.rows_loaded)
        self._save_shapes(shapes)
        return StepOutcome(tuple(checks), dict(result.rows_loaded))

    def _raise_if_breaking(self, shapes: Mapping[str, TableShape]) -> None:
        breaking: dict[str, tuple[BreakingChange, ...]] = {}
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
            if changes:
                breaking[table] = changes
        if breaking:
            raise BreakingChangeError(self.name, breaking)

    def _save_shapes(self, shapes: Mapping[str, TableShape]) -> None:
        for table, shape in shapes.items():
            self._guard(
                lambda table=table, shape=shape: self.snapshot_store.save(
                    self.warehouse, self.pipeline, self.step.source, table, shape
                ),
                SNAPSHOT_STORE,
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
        secrets = [self.source.password]
        if isinstance(self.warehouse, ResolvedConnection):
            secrets.append(self.warehouse.password)
        for secret in secrets:
            text = text.replace(secret, MASK)
        return URL_PATTERN.sub(MASK, text)


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
        return StepReport(task.name, StepStatus.FAILED, error=str(error))
    return StepReport(task.name, StepStatus.PASSED, outcome.checks, outcome.rows_loaded)


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
) -> RunResult:
    pipeline = load_pipeline(pipeline_text)
    settings = load_project_settings(settings_text)
    environment = pick_environment(settings.environments, environment_name)
    ensure_sources_defined(pipeline, environment)
    plan = _build_plan(
        pipeline, environment_name, environment, env, snapshot_store, clock, transform
    )
    return (orchestrator or InOrderOrchestrator()).execute(plan, sync)


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
) -> RunPlan:
    sync_steps = [step for step in pipeline.steps if isinstance(step, SyncStep)]
    connections = [environment.sources[step.source] for step in sync_steps]
    secrets = resolve_secret_references(env, secret_references(environment.warehouse, connections))
    warehouse = resolve_warehouse(environment.warehouse, secrets)
    steps: list[StepTask] = []
    for index, step in enumerate(pipeline.steps):
        if isinstance(step, SyncStep):
            source = resolve_connection(environment.sources[step.source], secrets)
            steps.append(PlannedStep(step, source, warehouse, pipeline.name, snapshot_store, clock))
        elif transform is None:
            location = format_path(("steps", index))
            raise PipelineValidationError(
                [f"{location}: a Transform Step needs a Transform engine, and none is set"]
            )
        else:
            steps.append(PlannedTransformStep(step, warehouse, pipeline.name, transform))
    return RunPlan(pipeline.name, environment_name, tuple(steps))


def secret_references(
    warehouse: PostgresConnection | DuckDbConnection, sources: Sequence[PostgresConnection]
) -> list[SecretReference]:
    connections = [warehouse, *sources] if isinstance(warehouse, PostgresConnection) else sources
    return [connection.password_env for connection in connections]


def resolve_warehouse(
    connection: PostgresConnection | DuckDbConnection, secrets: Mapping[str, str]
) -> ResolvedWarehouse:
    if isinstance(connection, DuckDbConnection):
        return ResolvedDuckDb(connection.path)
    return resolve_connection(connection, secrets)


def resolve_connection(
    connection: PostgresConnection, secrets: Mapping[str, str]
) -> ResolvedConnection:
    return ResolvedConnection(
        host=connection.host,
        port=connection.port,
        database=connection.database,
        user=connection.user,
        password=secrets[connection.password_env.name],
    )
