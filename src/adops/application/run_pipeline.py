from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from adops.application.ports import (
    MASK,
    Orchestrator,
    ResolvedConnection,
    RunPlan,
    RunResult,
    StepOutcome,
    StepReport,
    StepStatus,
    StepTask,
    Sync,
)
from adops.application.secrets import resolve_secret_references
from adops.domain.checks import (
    CheckResult,
    CheckStatus,
    SourceFacts,
    evaluate_load_checks,
    evaluate_preflight_checks,
)
from adops.domain.environment import Environment, PostgresConnection
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import (
    ensure_sources_defined,
    format_path,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import Pipeline, SyncStep

NOT_IN_SOURCE = SourceFacts(table_exists=False, columns=frozenset())


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


class SyncFailedError(StepError):
    pass


@dataclass(frozen=True)
class PlannedStep:
    step: SyncStep
    source: ResolvedConnection
    warehouse: ResolvedConnection

    @property
    def name(self) -> str:
        return self.step.name

    def __call__(self, sync: Sync) -> StepOutcome:
        source_facts = self._guard(lambda: sync.preflight(self.step, self.source))
        checks: list[CheckResult] = []
        for table in self.step.tables:
            checks += evaluate_preflight_checks(table, source_facts.get(table.name, NOT_IN_SOURCE))
        self._raise_if_failed(checks, {})

        result = self._guard(lambda: sync.sync(self.step, self.source, self.warehouse))
        for table in self.step.tables:
            load_facts = result.load_facts.get(table.name)
            if load_facts is None:
                raise SyncFailedError(f"Step {self.name}: no load facts reported for {table.name}")
            checks += evaluate_load_checks(table, load_facts)
        self._raise_if_failed(checks, result.rows_loaded)
        return StepOutcome(tuple(checks), dict(result.rows_loaded))

    def _raise_if_failed(self, checks: list[CheckResult], rows_loaded: Mapping[str, int]) -> None:
        if any(check.status is CheckStatus.FAIL for check in checks):
            raise CheckFailedError(self.name, checks, rows_loaded)

    def _guard[T](self, call: Callable[[], T]) -> T:
        try:
            return call()
        except Exception as error:
            message = self._redact(f"{type(error).__name__}: {error}")
        # Raised outside `except` so the original error, which may hold a secret, is not chained.
        raise SyncFailedError(f"Step {self.name}: {message}")

    def _redact(self, text: str) -> str:
        for secret in (self.source.password, self.warehouse.password):
            text = text.replace(secret, MASK)
        return text


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
    orchestrator: Orchestrator | None = None,
) -> RunResult:
    pipeline = load_pipeline(pipeline_text)
    settings = load_project_settings(settings_text)
    environment = _pick_environment(settings.environments, environment_name)
    ensure_sources_defined(pipeline, environment)
    plan = _build_plan(pipeline, environment_name, environment, env)
    return (orchestrator or InOrderOrchestrator()).execute(plan, sync)


def _pick_environment(environments: Mapping[str, Environment], name: str) -> Environment:
    if name not in environments:
        available = ", ".join(sorted(environments))
        location = format_path(("environments", name))
        raise PipelineValidationError(
            [f"{location}: no Environment with this name (available: {available})"]
        )
    return environments[name]


def _build_plan(
    pipeline: Pipeline, environment_name: str, environment: Environment, env: Mapping[str, str]
) -> RunPlan:
    connections = [environment.warehouse] + [
        environment.sources[step.source] for step in pipeline.steps
    ]
    secrets = resolve_secret_references(env, (c.password_env for c in connections))

    def resolve(connection: PostgresConnection) -> ResolvedConnection:
        return ResolvedConnection(
            host=connection.host,
            port=connection.port,
            database=connection.database,
            user=connection.user,
            password=secrets[connection.password_env.name],
        )

    warehouse = resolve(environment.warehouse)
    steps = tuple(
        PlannedStep(step, resolve(environment.sources[step.source]), warehouse)
        for step in pipeline.steps
    )
    return RunPlan(pipeline.name, environment_name, steps)
