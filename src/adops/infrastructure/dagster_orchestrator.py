import os
from collections.abc import Iterable, MutableMapping
from pathlib import Path

from dagster import (
    DagsterInstance,
    DagsterInvalidDefinitionError,
    DependencyDefinition,
    Failure,
    GraphDefinition,
    In,
    JobDefinition,
    MetadataValue,
    Nothing,
    OpDefinition,
    OpExecutionContext,
    Out,
    op,
)

from adops.application.ports import RunPlan, RunResult, StepReport, StepStatus, StepTask, Sync
from adops.application.run_pipeline import run_step
from adops.domain.checks import CheckResult
from adops.infrastructure.dagster_server import RELEASE_TAG

AFTER = "after"
# The CLI prints its own summary from the StepReports, so Dagster's console logger stays silent
# (a failed Step would otherwise print a full stack trace); the run's events are still stored.
QUIET_CONSOLE = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}


class JobCompileError(Exception):
    """A RunPlan that cannot become a Dagster job, such as a name Dagster does not accept."""


def compile_job(
    plan: RunPlan, sync: Sync, *, reports: MutableMapping[str, StepReport] | None = None
) -> JobDefinition:
    """One op per Step, each waiting for the one before it; every StepReport lands in `reports`.

    The Sync and the Steps' resolved connections reach the ops only through this closure, so no
    secret is ever part of Dagster run config.
    """
    sink: MutableMapping[str, StepReport] = {} if reports is None else reports
    try:
        ops = [_step_op(task, sync, sink) for task in plan.steps]
        dependencies = {
            later.name: {AFTER: DependencyDefinition(earlier.name)}
            for earlier, later in zip(ops, ops[1:], strict=False)
        }
        return GraphDefinition(
            name=plan.pipeline, node_defs=ops, dependencies=dependencies
        ).to_job()
    except DagsterInvalidDefinitionError as error:
        message = str(error)
    raise JobCompileError(f"Pipeline {plan.pipeline}: {message}")


class DagsterOrchestrator:
    """Runs a RunPlan as a Dagster job, recorded in the instance DAGSTER_HOME points to.

    Each run is tagged RELEASE_TAG with `release`, the Project folder the run reads.
    """

    def __init__(self, release: Path) -> None:
        self.release = release

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        reports: dict[str, StepReport] = {}
        job = compile_job(plan, sync, reports=reports)
        with _instance() as instance:
            job.execute_in_process(
                instance=instance,
                raise_on_error=False,
                run_config=QUIET_CONSOLE,
                tags={RELEASE_TAG: str(self.release)},
            )
        steps = tuple(
            reports.get(task.name, StepReport(task.name, StepStatus.SKIPPED)) for task in plan.steps
        )
        return RunResult(plan.pipeline, plan.environment, steps)


def _instance() -> DagsterInstance:
    if os.environ.get("DAGSTER_HOME"):
        return DagsterInstance.get()
    return DagsterInstance.ephemeral()


def _step_op(task: StepTask, sync: Sync, reports: MutableMapping[str, StepReport]) -> OpDefinition:
    @op(name=task.name, ins={AFTER: In(Nothing)}, out=Out(Nothing))
    def run(context: OpExecutionContext) -> None:
        report = _report(task, sync)
        reports[task.name] = report
        metadata = _check_metadata(report.checks)
        if report.status is not StepStatus.PASSED:
            raise Failure(description=report.error, metadata=metadata, allow_retries=False)
        context.add_output_metadata(metadata)

    return run


def _report(task: StepTask, sync: Sync) -> StepReport:
    try:
        return run_step(task, sync)
    except Exception as error:
        kind = type(error).__name__
    # Only the type is kept: an error that is not a StepError was never redacted.
    return StepReport(task.name, StepStatus.FAILED, error=f"Step {task.name}: unexpected {kind}")


def _check_metadata(checks: Iterable[CheckResult]) -> dict[str, MetadataValue]:
    return {
        f"{check.table}.{check.check}": MetadataValue.json(
            {
                "check": str(check.check),
                "table": check.table,
                "status": str(check.status),
                "message": check.message,
            }
        )
        for check in checks
    }
