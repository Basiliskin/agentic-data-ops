"""Dagster jobs and schedules for the Server, one job per Pipeline.

Accepted trade-off: a retry re-runs the whole Pipeline (every Step is safe to re-run) and keeps
holding the Server's single run slot while it waits, so other Pipelines queue behind it.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

from dagster import (
    DagsterInstance,
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
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES

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
STALE = [DagsterRunStatus.STARTING, DagsterRunStatus.STARTED]
STALE_RUN_FAILURE = "Marked failed when the Server started: the run was left running by a crash."
RELEASE_TAG = "adops/release"


def build_server_definitions(
    pipelines: Sequence[tuple[str, Schedule | None]],
    time_zone: str,
    run: Callable[[str], RunResult],
    on_failure: Callable[[RunResult], None],
    *,
    release: str,
    retry_delay_seconds: int = RETRY_DELAY_SECONDS,
) -> Definitions:
    """`run` reads any secret itself, inside the run worker; none goes into run config or tags.

    `release` is the Project folder `run` reads; each run is tagged RELEASE_TAG with it.
    """
    jobs = [
        _pipeline_job(name, run, on_failure, release, retry_delay_seconds) for name, _ in pipelines
    ]
    schedules = [
        _schedule(job_def, schedule, time_zone)
        for job_def, (_, schedule) in zip(jobs, pipelines, strict=True)
        if schedule is not None
    ]
    return Definitions(jobs=jobs, schedules=schedules)


def clear_stale_runs(instance: DagsterInstance, notify: Callable[[str], object]) -> list[str]:
    """Marks every STARTING/STARTED run of this instance FAILURE, then calls `notify` per run.

    Safe only before the daemon starts: runs execute only inside the daemon container
    (QueuedRunCoordinator with DefaultRunLauncher), so any such run at that moment is dead.
    QUEUED and NOT_STARTED runs are left for the coordinator to launch. Every run is marked
    before any alert is sent, so a failing `notify` never leaves a run STARTED.
    """
    stale = instance.get_runs(filters=RunsFilter(statuses=STALE))
    for run in stale:
        instance.report_run_failed(run, message=STALE_RUN_FAILURE)
    pipelines = [run.job_name for run in stale]
    for pipeline in pipelines:
        notify(pipeline)
    return pipelines


@dataclass(frozen=True)
class ReleasesInUse:
    names: frozenset[str]
    untagged_run_in_progress: bool


def in_use_release_folders(instance: DagsterInstance, releases: PurePosixPath) -> ReleasesInUse:
    """Names of the folders in `releases` that unfinished runs read, from their RELEASE_TAG.

    A run waiting for a retry stays STARTED, so it counts. A tag outside `releases` (a Branch run
    from a pull-request checkout) protects nothing. A run with no tag (started before runs were
    tagged, or not yet at its op) may read any folder, which `untagged_run_in_progress` reports.
    """
    names: set[str] = set()
    untagged = False
    for run in instance.get_runs(filters=RunsFilter(statuses=list(NOT_FINISHED_STATUSES))):
        tag = run.tags.get(RELEASE_TAG)
        if tag is None:
            untagged = True
        elif PurePosixPath(tag).parent == releases:
            names.add(PurePosixPath(tag).name)
    return ReleasesInUse(frozenset(names), untagged)


def _pipeline_job(
    name: str,
    run: Callable[[str], RunResult],
    on_failure: Callable[[RunResult], None],
    release: str,
    retry_delay_seconds: int,
) -> JobDefinition:
    @op(name="run_pipeline")
    def run_pipeline(context: OpExecutionContext) -> None:
        context.instance.add_run_tags(context.run_id, {RELEASE_TAG: release})
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
