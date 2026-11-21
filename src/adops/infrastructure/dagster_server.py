"""Dagster jobs and schedules for the Server, one job per Pipeline.

Accepted trade-off: a retry re-runs the whole Pipeline (every Step is safe to re-run) and keeps
holding the Server's single run slot while it waits, so other Pipelines queue behind it.
"""

from collections.abc import Callable, Sequence

from dagster import (
    DagsterRunStatus,
    DefaultScheduleStatus,
    Definitions,
    Failure,
    JobDefinition,
    MetadataValue,
    OpExecutionContext,
    RetryRequested,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    in_process_executor,
    job,
    op,
)

from adops.application.ports import SUCCEEDED, RunResult
from adops.domain.schedule import Schedule

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 300
IN_PROGRESS = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


def build_server_definitions(
    pipelines: Sequence[tuple[str, Schedule | None]],
    time_zone: str,
    run: Callable[[str], RunResult],
    on_failure: Callable[[RunResult], None],
    retry_delay_seconds: int = RETRY_DELAY_SECONDS,
) -> Definitions:
    """`run` reads any secret itself, inside the run worker; none goes into run config or tags."""
    jobs = [_pipeline_job(name, run, on_failure, retry_delay_seconds) for name, _ in pipelines]
    schedules = [
        _schedule(job_def, schedule, time_zone)
        for job_def, (_, schedule) in zip(jobs, pipelines, strict=True)
        if schedule is not None
    ]
    return Definitions(jobs=jobs, schedules=schedules)


def _pipeline_job(
    name: str,
    run: Callable[[str], RunResult],
    on_failure: Callable[[RunResult], None],
    retry_delay_seconds: int,
) -> JobDefinition:
    @op(name="run_pipeline")
    def run_pipeline(context: OpExecutionContext) -> None:
        result = run(name)
        metadata = {step.name: MetadataValue.text(str(step.status)) for step in result.steps}
        if result.succeeded:
            context.add_output_metadata(metadata)
            return
        if result.retryable and context.retry_number < MAX_RETRIES:
            raise RetryRequested(max_retries=MAX_RETRIES, seconds_to_wait=retry_delay_seconds)
        on_failure(result)
        raise Failure(description=_first_error(result), metadata=metadata, allow_retries=False)

    @job(name=name, executor_def=in_process_executor)
    def pipeline_job() -> None:
        run_pipeline()

    return pipeline_job


def _first_error(result: RunResult) -> str:
    failed = next(step for step in result.steps if step.status not in SUCCEEDED)
    return failed.error or f"Pipeline {result.pipeline}: Step {failed.name} {failed.status}"


def _schedule(job_def: JobDefinition, schedule: Schedule, time_zone: str) -> ScheduleDefinition:
    def evaluate(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        in_progress = context.instance.get_runs(
            filters=RunsFilter(job_name=job_def.name, statuses=IN_PROGRESS), limit=1
        )
        if in_progress:
            return SkipReason(f"Pipeline {job_def.name} is still running")
        return RunRequest()

    return ScheduleDefinition(
        name=f"{job_def.name}_schedule",
        job=job_def,
        cron_schedule=schedule.cron,
        execution_timezone=time_zone,
        default_status=DefaultScheduleStatus.RUNNING,
        execution_fn=evaluate,
    )
