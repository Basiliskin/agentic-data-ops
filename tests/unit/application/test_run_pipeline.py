from datetime import datetime

import pytest
from fakes import (
    ENV,
    PIPELINE,
    SETTINGS,
    SOURCE_PASSWORD,
    WAREHOUSE_PASSWORD,
    RecordingSync,
    healthy_load,
)

from adops.application.ports import (
    Orchestrator,
    ResolvedConnection,
    RunPlan,
    RunResult,
    StepStatus,
    Sync,
)
from adops.application.run_pipeline import (
    CheckFailedError,
    InOrderOrchestrator,
    PlannedStep,
    SyncFailedError,
    run_pipeline,
)
from adops.application.secrets import MissingSecretError
from adops.domain.checks import CheckName, CheckResult, CheckStatus, SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline

SENTINELS = (SOURCE_PASSWORD, WAREHOUSE_PASSWORD)
CURSOR_SENTINEL = "cursor-sentinel-2031"


def run(sync: RecordingSync, env=ENV, environment="dev", orchestrator=None) -> RunResult:
    return run_pipeline(PIPELINE, SETTINGS, environment, env, sync, orchestrator)


def statuses(result: RunResult) -> list[tuple[str, StepStatus]]:
    return [(step.name, step.status) for step in result.steps]


def checks_of(result: RunResult, table: str) -> dict[CheckName, CheckResult]:
    return {c.check: c for step in result.steps for c in step.checks if c.table == table}


def assert_no_secret_in(*texts: str) -> None:
    for text in texts:
        for secret in SENTINELS:
            assert secret not in text


def test_healthy_run_syncs_every_step_in_pipeline_order():
    sync = RecordingSync()

    result = run(sync)

    assert sync.calls == [
        ("preflight", "customers"),
        ("sync", "customers"),
        ("preflight", "orders"),
        ("sync", "orders"),
        ("preflight", "products"),
        ("sync", "products"),
    ]
    assert result.succeeded
    assert (result.pipeline, result.environment) == ("shop", "dev")
    assert statuses(result) == [
        ("customers", StepStatus.PASSED),
        ("orders", StepStatus.PASSED),
        ("products", StepStatus.PASSED),
    ]
    assert result.steps[1].rows_loaded == {"orders": 3, "order_items": 3}
    assert {c.status for step in result.steps for c in step.checks} == {
        CheckStatus.PASS,
        CheckStatus.SKIPPED,
    }


def test_sync_receives_resolved_source_and_warehouse_connections():
    sync = RecordingSync()

    run(sync)

    source, warehouse = sync.connections[:2]
    assert (source.host, source.port, source.database, source.user, source.password) == (
        "localhost",
        5432,
        "shop",
        "shop",
        SOURCE_PASSWORD,
    )
    assert (warehouse.database, warehouse.password) == ("warehouse", WAREHOUSE_PASSWORD)


def test_failing_preflight_stops_before_any_sync_and_skips_later_steps():
    sync = RecordingSync(source_facts={"customers": SourceFacts(False, frozenset())})

    result = run(sync)

    assert sync.calls == [("preflight", "customers")]
    assert statuses(result) == [
        ("customers", StepStatus.CHECK_FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    failed = result.steps[0].checks
    assert [(c.check, c.table, c.status) for c in failed] == [
        (CheckName.SOURCE_TABLES_EXIST, "customers", CheckStatus.FAIL)
    ]
    assert "customers" in failed[0].message
    assert result.steps[0].rows_loaded == {}
    assert not result.succeeded


def test_table_missing_from_preflight_report_counts_as_not_in_source():
    sync = RecordingSync()
    original = sync.preflight

    def preflight_without_orders(step, source):
        return {k: v for k, v in original(step, source).items() if k != "orders"}

    sync.preflight = preflight_without_orders  # type: ignore[method-assign]

    result = run(sync)

    assert ("sync", "orders") not in sync.calls
    assert checks_of(result, "orders")[CheckName.SOURCE_TABLES_EXIST].status is CheckStatus.FAIL
    assert checks_of(result, "order_items")[CheckName.SOURCE_TABLES_EXIST].status is (
        CheckStatus.PASS
    )


def test_failing_load_check_marks_step_check_failed_and_skips_later_steps():
    sync = RecordingSync(load_facts={"customers": healthy_load(duplicate_keys=1)})

    result = run(sync)

    assert sync.calls == [("preflight", "customers"), ("sync", "customers")]
    assert statuses(result) == [
        ("customers", StepStatus.CHECK_FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    step = result.steps[0]
    assert checks_of(result, "customers")[CheckName.PRIMARY_KEY_UNIQUE].status is CheckStatus.FAIL
    assert step.rows_loaded == {"customers": 3}
    assert step.error is not None and "primary_key_unique failed" in step.error


def test_checks_are_evaluated_for_every_table_of_a_step():
    sync = RecordingSync(load_facts={"orders": healthy_load(null_keys=2)})

    result = run(sync)

    assert statuses(result)[1] == ("orders", StepStatus.CHECK_FAILED)
    assert checks_of(result, "orders")[CheckName.PRIMARY_KEY_NOT_NULL].status is CheckStatus.FAIL
    assert checks_of(result, "order_items")[CheckName.PRIMARY_KEY_UNIQUE].status is (
        CheckStatus.PASS
    )


def test_skipped_check_is_reported_with_its_reason():
    result = run(RecordingSync())

    skipped = checks_of(result, "order_items")[CheckName.ROW_COUNT_MATCHES]
    assert skipped.status is CheckStatus.SKIPPED
    assert "the Source table is appended to while it is copied" in skipped.message


def test_same_facts_give_the_same_check_results():
    def facts():
        return RecordingSync(load_facts={"orders": healthy_load(duplicate_keys=4)})

    first, second = run(facts()), run(facts())

    assert [step.checks for step in first.steps] == [step.checks for step in second.steps]
    assert first == second


def test_missing_load_facts_for_a_table_fails_the_step():
    sync = RecordingSync(omit_load_facts={"order_items"})

    result = run(sync)

    assert statuses(result)[1:] == [("orders", StepStatus.FAILED), ("products", StepStatus.SKIPPED)]
    assert result.steps[1].error == "Step orders: no load facts reported for order_items"


def test_sync_error_fails_step_skips_later_and_never_shows_secret():
    leak = RuntimeError(f"auth failed for password {SOURCE_PASSWORD} / {WAREHOUSE_PASSWORD}")
    sync = RecordingSync(fail_sync_of={"customers": leak})

    result = run(sync)

    assert sync.calls == [("preflight", "customers"), ("sync", "customers")]
    assert statuses(result) == [
        ("customers", StepStatus.FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    assert result.steps[0].error == (
        "Step customers: RuntimeError: auth failed for password *** / ***"
    )
    assert_no_secret_in(repr(result), str(result))


def test_preflight_error_fails_step_without_chaining_the_original():
    sync = RecordingSync(fail_preflight_of={"orders": ConnectionError(SOURCE_PASSWORD)})
    plan_step = PlannedStep(
        load_pipeline(PIPELINE).steps[1],
        ResolvedConnection("h", 1, "d", "u", SOURCE_PASSWORD),
        ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD),
    )

    with pytest.raises(SyncFailedError) as raised:
        plan_step(sync)

    assert str(raised.value) == "Step orders: ConnectionError: ***"
    assert raised.value.__cause__ is None and raised.value.__context__ is None
    assert ("sync", "orders") not in sync.calls


def test_check_failure_messages_and_reprs_hold_no_secret_or_cursor_value():
    sync = RecordingSync(
        load_facts={
            "orders": healthy_load(
                cursor_max_before=CURSOR_SENTINEL + "9", cursor_max_after=CURSOR_SENTINEL
            )
        }
    )
    plan_step = PlannedStep(
        load_pipeline(PIPELINE).steps[1],
        ResolvedConnection("h", 1, "d", "u", SOURCE_PASSWORD),
        ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD),
    )

    with pytest.raises(CheckFailedError) as raised:
        plan_step(sync)
    result = run(sync)

    texts = [str(raised.value), repr(raised.value), repr(result)]
    texts += [c.message for step in result.steps for c in step.checks]
    assert_no_secret_in(*texts)
    assert all(CURSOR_SENTINEL not in text for text in texts)
    assert "cursor_not_backwards failed" in str(raised.value)


def test_resolved_connection_and_plan_never_print_the_password():
    connection = ResolvedConnection("db", 5432, "shop", "shop", SOURCE_PASSWORD)
    captured: list[RunPlan] = []

    class CapturingOrchestrator:
        def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
            captured.append(plan)
            return InOrderOrchestrator().execute(plan, sync)

    run(RecordingSync(), orchestrator=CapturingOrchestrator())

    assert repr(connection) == (
        "ResolvedConnection(host='db', port=5432, database='shop', user='shop', password='***')"
    )
    assert_no_secret_in(repr(connection), str(connection), f"{connection}")
    assert_no_secret_in(repr(captured[0]), str(captured[0]))
    assert len(captured[0].steps) == 3


def test_missing_secrets_are_reported_together_before_any_step_runs():
    sync = RecordingSync()

    with pytest.raises(MissingSecretError) as raised:
        run(sync, env={"UNRELATED": "x"})

    assert raised.value.names == ("SHOP_DB_PASSWORD", "WAREHOUSE_PASSWORD")
    assert sync.calls == []


def test_only_secrets_of_sources_the_pipeline_uses_are_required():
    assert "CRM_DB_PASSWORD" not in ENV

    assert run(RecordingSync()).succeeded


def test_unknown_environment_names_it_and_lists_available_ones():
    sync = RecordingSync()

    with pytest.raises(PipelineValidationError) as raised:
        run(sync, environment="staging")

    assert raised.value.problems == (
        "environments.staging: no Environment with this name (available: dev, prod)",
    )
    assert sync.calls == []


def test_step_using_a_source_missing_from_the_environment_is_rejected():
    sync = RecordingSync()

    with pytest.raises(PipelineValidationError):
        run(sync, env={"PROD_WAREHOUSE_PASSWORD": "x"}, environment="prod")

    assert sync.calls == []


def test_orchestrator_port_is_used_when_given():
    calls: list[RunPlan] = []

    class Fixed:
        def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
            calls.append(plan)
            return RunResult(plan.pipeline, plan.environment, ())

    result = run(RecordingSync(), orchestrator=Fixed())

    assert result == RunResult("shop", "dev", ())
    assert [step.name for step in calls[0].steps] == ["customers", "orders", "products"]


def test_fakes_and_default_runner_satisfy_the_ports():
    assert isinstance(RecordingSync(), Sync)
    assert isinstance(InOrderOrchestrator(), Orchestrator)


def test_check_failed_error_keeps_checks_and_rows():
    check = CheckResult(CheckName.NO_ROW_LOSS, "orders", CheckStatus.FAIL, "orders: lost rows")

    error = CheckFailedError("orders", [check], {"orders": 2})

    assert (error.checks, error.rows_loaded) == ((check,), {"orders": 2})
    assert str(error) == "Step orders: Checks failed: orders: lost rows"


def test_cursor_values_may_be_datetimes():
    sync = RecordingSync(
        load_facts={
            "orders": healthy_load(
                cursor_max_before=datetime(2026, 1, 1), cursor_max_after=datetime(2026, 1, 2)
            )
        }
    )

    assert run(sync).succeeded
