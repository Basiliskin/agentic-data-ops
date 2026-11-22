import inspect
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    DefaultScheduleStatus,
    Definitions,
    JobDefinition,
    RunsFilter,
    ScheduleDefinition,
    build_schedule_context,
    in_process_executor,
)
from dagster._core.remote_origin import (
    InProcessCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.test_utils import create_run_for_test
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._utils.schedules import is_valid_cron_string
from pydantic import ValidationError

from adops.application.alerts import notify_stale_run
from adops.application.ports import RunResult, StepReport, StepStatus
from adops.domain.schedule import WEEKDAYS, Schedule
from adops.infrastructure.dagster_server import (
    STALE_RUN_FAILURE,
    build_server_definitions,
    clear_stale_runs,
)

SECRET = "s3cr3t-server-sentinel"
UNREACHABLE = "Step extract: the Source shop is unreachable"
CHECK_FAILED = "Step extract: Check not_null failed on orders.id"


def passed() -> RunResult:
    return RunResult(
        "orders",
        "prod",
        (
            StepReport("extract", StepStatus.PASSED),
            StepReport("model", StepStatus.PASSED),
        ),
    )


def unreachable() -> RunResult:
    return RunResult(
        "orders",
        "prod",
        (
            StepReport("extract", StepStatus.FAILED, error=UNREACHABLE, retryable=True),
            StepReport("model", StepStatus.SKIPPED),
        ),
    )


def check_failed(error: str | None = CHECK_FAILED) -> RunResult:
    return RunResult(
        "orders",
        "prod",
        (
            StepReport("extract", StepStatus.CHECK_FAILED, error=error),
            StepReport("model", StepStatus.SKIPPED),
        ),
    )


class Server:
    """Plays the run worker: hands out results in order and records every call."""

    def __init__(self, *results: RunResult) -> None:
        self.results = list(results)
        self.runs: list[str] = []
        self.alerts: list[RunResult] = []

    def run(self, name: str) -> RunResult:
        self.runs.append(name)
        return self.results.pop(0) if len(self.results) > 1 else self.results[0]

    def definitions(
        self, pipelines: list[tuple[str, Schedule | None]] | None = None
    ) -> Definitions:
        return build_server_definitions(
            pipelines or [("orders", Schedule.parse("daily at 02:00"))],
            "Europe/Berlin",
            run=self.run,
            on_failure=self.alerts.append,
            retry_delay_seconds=0,
        )


def execute(server: Server, instance: DagsterInstance | None = None):
    job = server.definitions().resolve_job_def("orders")
    return job.execute_in_process(
        instance=instance or DagsterInstance.ephemeral(), raise_on_error=False
    )


def two_pipelines() -> Definitions:
    return Server(passed()).definitions(
        [("orders", Schedule.parse("daily at 02:00")), ("customers", None)]
    )


def only_schedule(definitions: Definitions) -> ScheduleDefinition:
    (schedule,) = definitions.schedules or []
    assert isinstance(schedule, ScheduleDefinition)
    return schedule


def test_one_job_per_pipeline_and_one_schedule_per_scheduled_pipeline():
    definitions = two_pipelines()

    jobs = [job for job in definitions.jobs or [] if isinstance(job, JobDefinition)]
    assert sorted(job.name for job in jobs) == ["customers", "orders"]
    schedule = only_schedule(definitions)
    assert schedule.job_name == "orders"
    assert schedule.cron_schedule == "0 2 * * *"
    assert schedule.execution_timezone == "Europe/Berlin"
    assert schedule.default_status == DefaultScheduleStatus.RUNNING


def test_jobs_have_one_op_run_in_process_and_no_partitions():
    definitions = two_pipelines()

    for job in definitions.jobs or []:
        assert isinstance(job, JobDefinition)
        assert job.executor_def is in_process_executor
        assert [node.name for node in job.nodes] == ["run_pipeline"]
        assert job.partitions_def is None
    assert only_schedule(definitions).job.partitions_def is None


@pytest.fixture
def instance(tmp_path: Path) -> Iterator[DagsterInstance]:
    with DagsterInstance.local_temp(str(tmp_path)) as persistent:
        yield persistent


def add_run(instance: DagsterInstance, job_name: str, status: DagsterRunStatus) -> None:
    """A run as the daemon would store it; a QUEUED run needs the code location it came from."""
    location = InProcessCodeLocationOrigin(LoadableTargetOrigin(python_file=__file__))
    origin = RemoteJobOrigin(RemoteRepositoryOrigin(location, "__repository__"), job_name)
    create_run_for_test(instance, job_name=job_name, status=status, remote_job_origin=origin)


def tick(instance: DagsterInstance, definitions: Definitions):
    return only_schedule(definitions).evaluate_tick(build_schedule_context(instance=instance))


@pytest.mark.parametrize(
    "status",
    [
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
    ],
)
def test_a_due_run_is_skipped_while_the_pipeline_is_in_progress(
    instance: DagsterInstance, status: DagsterRunStatus
):
    add_run(instance, "orders", status)

    data = tick(instance, two_pipelines())

    assert data.run_requests == []
    assert data.skip_message == "Pipeline orders is still running"


@pytest.mark.parametrize("status", [None, DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE])
def test_a_due_run_is_requested_when_the_pipeline_is_not_in_progress(
    instance: DagsterInstance, status: DagsterRunStatus | None
):
    if status is not None:
        add_run(instance, "orders", status)

    data = tick(instance, two_pipelines())

    (request,) = data.run_requests or []
    assert data.skip_message is None
    assert request.run_config == {}
    assert all(key.startswith("dagster/") for key in request.tags)


def test_another_pipeline_in_progress_does_not_skip_this_one(instance: DagsterInstance):
    add_run(instance, "customers", DagsterRunStatus.STARTED)

    data = tick(instance, two_pipelines())

    assert data.skip_message is None
    assert len(data.run_requests or []) == 1


def test_an_unreachable_failure_retries_three_times_then_alerts_once():
    server = Server(unreachable())

    result = execute(server)

    assert not result.success
    assert server.runs == ["orders"] * 4
    assert server.alerts == [unreachable()]
    assert result.failure_data_for_node("run_pipeline").user_failure_data.description == (
        UNREACHABLE
    )


def test_a_retry_that_then_passes_never_alerts():
    server = Server(unreachable(), passed())

    result = execute(server)

    assert result.success
    assert server.runs == ["orders"] * 2
    assert server.alerts == []


def test_a_failed_check_is_not_retried_and_alerts_once():
    server = Server(check_failed())

    result = execute(server)

    assert not result.success
    assert server.runs == ["orders"]
    assert server.alerts == [check_failed()]


def test_a_passing_run_never_alerts_and_stores_each_step_status_as_metadata():
    server = Server(passed())

    result = execute(server)

    assert result.success
    assert server.alerts == []
    (output,) = [event for event in result.all_node_events if event.is_successful_output]
    stored = {key: value.value for key, value in output.step_output_data.metadata.items()}
    assert stored == {"extract": "passed", "model": "passed"}


def test_the_failure_carries_the_step_error_and_step_statuses_only():
    result = execute(Server(check_failed()))

    failure = result.failure_data_for_node("run_pipeline").user_failure_data
    assert failure.description == CHECK_FAILED
    stored = {key: value.value for key, value in failure.metadata.items()}
    assert stored == {"extract": "check_failed", "model": "skipped"}


def test_a_failure_without_an_error_message_names_the_failing_step():
    result = execute(Server(check_failed(error=None)))

    failure = result.failure_data_for_node("run_pipeline").user_failure_data
    assert failure.description == "Pipeline orders: Step extract check_failed"


@pytest.mark.parametrize("outcome", [passed, check_failed, unreachable])
def test_no_secret_reaches_dagster_storage(
    outcome: Callable[[], RunResult], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    secrets = {"SHOP_PASSWORD": SECRET}
    read: list[str] = []

    def run(name: str) -> RunResult:
        read.append(secrets["SHOP_PASSWORD"])
        return outcome()

    job = build_server_definitions(
        [("orders", Schedule.parse("hourly"))],
        "UTC",
        run=run,
        on_failure=lambda result: None,
        retry_delay_seconds=0,
    ).resolve_job_def("orders")
    with DagsterInstance.get() as instance:
        job.execute_in_process(instance=instance, raise_on_error=False)

    assert read
    files = [path.read_bytes() for path in home.rglob("*") if path.is_file()]
    assert files
    assert all(SECRET.encode() not in content for content in files)


def test_the_default_retry_delay_is_five_minutes():
    default = inspect.signature(build_server_definitions).parameters["retry_delay_seconds"].default

    assert default == 300


PLAIN_WORDS = [
    "hourly",
    "daily at 00:00",
    "daily at 02:00",
    "daily at 23:59",
    *(f"weekly on {day} at 06:30" for day in WEEKDAYS),
]
DOMAIN_CRONS = [
    "* * * * *",
    "*/15 * * * *",
    "0 */2 * * *",
    "0 9-17 * * 1-5",
    "0 9-17/2 * * *",
    "0,30 * * * *",
    "5,10,15 1,13 * * *",
    "0 0 1 * *",
    "0 0 1 1 *",
    "0 0 29 2 *",
    "0 0 31 1-12/2 *",
    "59 23 31 12 6",
    "0 0 1-7 * 0",
]
REJECTED_BY_BOTH = ["*/0 * * * *", "1,,2 * * * *", "0 0 30 2 *", "0 0 31 2 *"]


@pytest.mark.parametrize("text", PLAIN_WORDS + DOMAIN_CRONS)
def test_dagster_accepts_every_cron_the_domain_accepts(text: str):
    assert is_valid_cron_string(Schedule.parse(text).cron)


@pytest.mark.parametrize("cron", REJECTED_BY_BOTH)
def test_the_domain_and_dagster_both_reject_impossible_crons(cron: str):
    assert not is_valid_cron_string(cron)
    with pytest.raises(ValidationError):
        Schedule.parse(cron)


def status_of(instance: DagsterInstance, job_name: str) -> DagsterRunStatus:
    (run,) = instance.get_runs(filters=RunsFilter(job_name=job_name))
    return instance.get_run_by_id(run.run_id).status


def seed_every_status(instance: DagsterInstance) -> None:
    for status in DagsterRunStatus:
        add_run(instance, status.value.lower(), status)


def test_the_sweep_fails_only_starting_and_started_runs(instance: DagsterInstance):
    seed_every_status(instance)
    notified: list[str] = []

    cleared = clear_stale_runs(instance, notified.append)

    assert sorted(cleared) == ["started", "starting"]
    assert sorted(notified) == ["started", "starting"]
    for status in DagsterRunStatus:
        expected = (
            DagsterRunStatus.FAILURE
            if status in (DagsterRunStatus.STARTING, DagsterRunStatus.STARTED)
            else status
        )
        assert status_of(instance, status.value.lower()) == expected


def test_a_second_sweep_clears_nothing_and_alerts_nobody(instance: DagsterInstance):
    seed_every_status(instance)
    clear_stale_runs(instance, lambda pipeline: None)
    notified: list[str] = []

    assert clear_stale_runs(instance, notified.append) == []
    assert notified == []


def test_a_sweep_with_no_stale_runs_alerts_nobody(instance: DagsterInstance):
    add_run(instance, "orders", DagsterRunStatus.SUCCESS)
    add_run(instance, "customers", DagsterRunStatus.QUEUED)
    notified: list[str] = []

    assert clear_stale_runs(instance, notified.append) == []
    assert notified == []


def test_the_sweep_writes_a_run_failure_event_with_fixed_text(instance: DagsterInstance):
    add_run(instance, "orders", DagsterRunStatus.STARTED)

    clear_stale_runs(instance, lambda pipeline: None)

    (run,) = instance.get_runs()
    (failure,) = [
        entry.dagster_event
        for entry in instance.all_logs(run.run_id)
        if entry.is_dagster_event and entry.dagster_event.event_type_value == "PIPELINE_FAILURE"
    ]
    assert failure.message == STALE_RUN_FAILURE


def test_every_run_is_failed_before_the_first_alert(instance: DagsterInstance):
    add_run(instance, "orders", DagsterRunStatus.STARTED)
    add_run(instance, "customers", DagsterRunStatus.STARTING)
    seen: list[tuple[str, DagsterRunStatus, DagsterRunStatus]] = []

    def notify(pipeline: str) -> None:
        seen.append((pipeline, status_of(instance, "orders"), status_of(instance, "customers")))
        raise ConnectionRefusedError("https://hooks.example/token")

    with pytest.raises(ConnectionRefusedError):
        clear_stale_runs(instance, notify)

    assert len(seen) == 1
    assert seen[0][1:] == (DagsterRunStatus.FAILURE, DagsterRunStatus.FAILURE)
    assert status_of(instance, "orders") == DagsterRunStatus.FAILURE
    assert status_of(instance, "customers") == DagsterRunStatus.FAILURE


class SecretLeakingSender:
    def send(self, alert: object) -> None:
        raise ConnectionRefusedError(f"https://hooks.example/{SECRET}")


def test_no_secret_reaches_dagster_storage_or_the_line_after_a_sweep(tmp_path: Path):
    home = tmp_path / "dagster_home"
    home.mkdir()
    lines: list[str | None] = []
    with DagsterInstance.local_temp(str(home)) as instance:
        add_run(instance, "orders", DagsterRunStatus.STARTED)
        add_run(instance, "customers", DagsterRunStatus.STARTING)

        cleared = clear_stale_runs(
            instance,
            lambda pipeline: lines.append(
                notify_stale_run(pipeline, "prod", SecretLeakingSender())
            ),
        )

    assert sorted(cleared) == ["customers", "orders"]
    assert lines == ["alert could not be sent (ConnectionRefusedError)"] * 2
    files = [path.read_bytes() for path in home.rglob("*") if path.is_file()]
    assert files
    assert all(SECRET.encode() not in content for content in files)
    assert all(b"hooks.example" not in content for content in files)
