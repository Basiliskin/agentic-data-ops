from datetime import UTC, datetime

import pytest
from fakes import (
    ENV,
    PIPELINE,
    SETTINGS,
    SOURCE_PASSWORD,
    TABLE_TYPES,
    WAREHOUSE_PASSWORD,
    FakeSnapshotStore,
    FixedClock,
    RecordingSync,
    facts_with_types,
    healthy_load,
)

from adops.application.ports import (
    Clock,
    Orchestrator,
    ResolvedConnection,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    StepStatus,
    Sync,
)
from adops.application.run_pipeline import (
    BreakingChangeError,
    CheckFailedError,
    InOrderOrchestrator,
    PlannedStep,
    StepError,
    SyncFailedError,
    run_pipeline,
)
from adops.application.secrets import MissingSecretError
from adops.domain.checks import CheckName, CheckResult, CheckStatus, SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline
from adops.domain.schema_snapshot import TableShape

SENTINELS = (SOURCE_PASSWORD, WAREHOUSE_PASSWORD)
CURSOR_SENTINEL = "cursor-sentinel-2031"


def run(
    sync: RecordingSync,
    env=ENV,
    environment="dev",
    orchestrator=None,
    snapshot_store=None,
    clock=None,
    pipeline=PIPELINE,
) -> RunResult:
    store = FakeSnapshotStore() if snapshot_store is None else snapshot_store
    clock = FixedClock() if clock is None else clock
    return run_pipeline(pipeline, SETTINGS, environment, env, sync, store, clock, orchestrator)


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
        "shop",
        FakeSnapshotStore(),
        FixedClock(),
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
        "shop",
        FakeSnapshotStore(),
        FixedClock(),
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
    assert isinstance(FakeSnapshotStore(), SchemaSnapshotStore)
    assert isinstance(FixedClock(), Clock)


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


def baseline_store() -> FakeSnapshotStore:
    return FakeSnapshotStore(
        {("shop", "shop", table): TableShape(types) for table, types in TABLE_TYPES.items()}
    )


def test_first_run_records_a_baseline_for_every_table():
    store = FakeSnapshotStore()

    result = run(RecordingSync(), snapshot_store=store)

    assert result.succeeded
    assert store.shapes == {
        ("shop", "shop", table): TableShape(types) for table, types in TABLE_TYPES.items()
    }
    assert {warehouse.password for warehouse in store.warehouses} == {WAREHOUSE_PASSWORD}


def test_unchanged_shape_passes_even_when_types_are_spelled_differently():
    store = baseline_store()
    store.shapes[("shop", "shop", "customers")] = TableShape({"id": "int4", "email": "varchar"})
    sync = RecordingSync()

    result = run(sync, snapshot_store=store)

    assert result.succeeded
    assert [call for call in sync.calls if call[0] == "sync"] == [
        ("sync", "customers"),
        ("sync", "orders"),
        ("sync", "products"),
    ]


def test_added_column_passes_and_becomes_the_new_snapshot():
    store = baseline_store()
    widened = {**TABLE_TYPES["customers"], "phone": "text"}
    sync = RecordingSync(source_facts={"customers": facts_with_types(widened)})

    result = run(sync, snapshot_store=store)

    assert result.succeeded
    assert ("sync", "customers") in sync.calls
    assert store.shapes[("shop", "shop", "customers")] == TableShape(widened)


def test_dropped_column_halts_the_step_before_sync_and_skips_later_steps():
    store = baseline_store()
    before = dict(store.shapes)
    sync = RecordingSync(source_facts={"customers": facts_with_types({"id": "integer"})})

    result = run(sync, snapshot_store=store)

    assert sync.calls == [("preflight", "customers")]
    assert statuses(result) == [
        ("customers", StepStatus.FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    assert result.steps[0].error == "Step customers: Breaking Change in customers: email dropped"
    assert store.shapes == before and store.saves == []


def test_every_broken_table_of_the_step_is_reported_at_once():
    store = baseline_store()
    sync = RecordingSync(
        source_facts={
            "orders": facts_with_types({"id": "integer", "updated_at": "date"}),
            "order_items": facts_with_types({"id": "integer", "item_order_id": "integer"}),
        }
    )

    result = run(sync, snapshot_store=store)

    assert ("sync", "orders") not in sync.calls
    assert statuses(result)[1:] == [("orders", StepStatus.FAILED), ("products", StepStatus.SKIPPED)]
    error = result.steps[1].error
    assert error == (
        "Step orders: Breaking Change in orders: updated_at retyped (timestamptz -> date); "
        "Breaking Change in order_items: order_id dropped"
    )
    assert_no_secret_in(error)
    assert all(value not in error for value in ("localhost", "5433", "adops", "warehouse"))


def test_breaking_change_error_is_a_step_error_raised_by_the_planned_step():
    sync = RecordingSync(source_facts={"products": facts_with_types({"sku": "integer"})})
    plan_step = PlannedStep(
        load_pipeline(PIPELINE).steps[2],
        ResolvedConnection("h", 1, "d", "u", SOURCE_PASSWORD),
        ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD),
        "shop",
        baseline_store(),
        FixedClock(),
    )

    with pytest.raises(BreakingChangeError) as raised:
        plan_step(sync)

    assert isinstance(raised.value, StepError)
    assert str(raised.value) == (
        "Step products: Breaking Change in products: sku retyped (text -> integer)"
    )
    (change,) = raised.value.changes["products"]
    assert change.message == "sku retyped (text -> integer)"


def test_failed_load_check_saves_no_snapshot():
    store = FakeSnapshotStore()
    sync = RecordingSync(load_facts={"customers": healthy_load(duplicate_keys=1)})

    result = run(sync, snapshot_store=store)

    assert statuses(result)[0] == ("customers", StepStatus.CHECK_FAILED)
    assert store.saves == []


def test_failed_load_check_on_a_later_table_saves_no_table_of_the_step():
    store = FakeSnapshotStore()
    sync = RecordingSync(load_facts={"order_items": healthy_load(null_keys=1)})

    result = run(sync, snapshot_store=store)

    assert statuses(result)[1] == ("orders", StepStatus.CHECK_FAILED)
    assert store.saves == [("shop", "shop", "customers")]


LEAKY_STORE_ERROR = (
    f"could not connect to postgresql://adops:{WAREHOUSE_PASSWORD}@warehouse.internal/warehouse "
    f"with password {WAREHOUSE_PASSWORD}"
)


def test_snapshot_store_load_error_is_redacted_and_names_the_store():
    sync = RecordingSync()
    store = FakeSnapshotStore(fail_load=RuntimeError(LEAKY_STORE_ERROR))

    result = run(sync, snapshot_store=store)

    assert sync.calls == [("preflight", "customers")]
    assert statuses(result)[0] == ("customers", StepStatus.FAILED)
    assert result.steps[0].error == (
        "Step customers: schema snapshot store: RuntimeError: could not connect to *** "
        "with password ***"
    )
    assert_no_secret_in(repr(result))
    assert "postgresql://" not in repr(result)


def test_snapshot_store_save_error_is_redacted_and_names_the_store():
    sync = RecordingSync()
    store = FakeSnapshotStore(fail_save=RuntimeError(LEAKY_STORE_ERROR))

    result = run(sync, snapshot_store=store)

    assert sync.calls == [("preflight", "customers"), ("sync", "customers")]
    assert statuses(result)[0] == ("customers", StepStatus.FAILED)
    error = result.steps[0].error
    assert error is not None and error.startswith("Step customers: schema snapshot store: ")
    assert_no_secret_in(repr(result))
    assert "postgresql://" not in repr(result)


def test_sync_error_messages_mask_connection_urls():
    leak = RuntimeError("cannot reach postgresql://shop@db:5432/shop")
    sync = RecordingSync(fail_sync_of={"customers": leak})

    result = run(sync)

    assert result.steps[0].error == "Step customers: RuntimeError: cannot reach ***"


DECLARED = """
name: shop
steps:
  - name: customers
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
        checks:
          - {type: not_null, column: email}
          - {type: min_rows, count: 2}
  - name: orders
    source: shop
    tables:
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
        checks:
          - {type: freshness, column: updated_at, max_age_hours: 24}
      - {name: order_items, mode: full_refresh, primary_key: id}
"""

MEASURED = {
    "customers": healthy_load(null_counts={"email": 0}),
    "orders": healthy_load(newest_values={"updated_at": datetime(2026, 11, 18, 6, tzinfo=UTC)}),
}


def test_declared_checks_are_reported_after_the_built_in_ones_with_their_names():
    result = run(RecordingSync(load_facts=MEASURED), pipeline=DECLARED)

    assert result.succeeded
    assert [(c.check, c.status) for c in result.steps[0].checks] == [
        (CheckName.SOURCE_TABLES_EXIST, CheckStatus.PASS),
        (CheckName.PRIMARY_KEY_UNIQUE, CheckStatus.PASS),
        (CheckName.PRIMARY_KEY_NOT_NULL, CheckStatus.PASS),
        (CheckName.ROW_COUNT_MATCHES, CheckStatus.PASS),
        ("not_null(email)", CheckStatus.PASS),
        ("min_rows", CheckStatus.PASS),
    ]
    assert checks_of(result, "orders")["freshness(updated_at)"].message == (
        "orders: freshness(updated_at) passed"
    )


def test_failing_declared_check_halts_the_step_before_the_snapshot_is_saved():
    store = FakeSnapshotStore()
    facts = {**MEASURED, "customers": healthy_load(null_counts={"email": 2})}

    result = run(RecordingSync(load_facts=facts), snapshot_store=store, pipeline=DECLARED)

    assert statuses(result) == [
        ("customers", StepStatus.CHECK_FAILED),
        ("orders", StepStatus.SKIPPED),
    ]
    assert checks_of(result, "customers")["not_null(email)"].status is CheckStatus.FAIL
    assert result.steps[0].error is not None
    assert "not_null(email) failed — 2 rows with no value" in result.steps[0].error
    assert result.steps[0].rows_loaded == {"customers": 3}
    assert store.saves == []


def test_freshness_is_judged_against_the_injected_clock():
    early = FixedClock(datetime(2026, 11, 18, 12, tzinfo=UTC))
    late = FixedClock(datetime(2026, 11, 19, 12, tzinfo=UTC))

    fresh = run(RecordingSync(load_facts=MEASURED), clock=early, pipeline=DECLARED)
    stale = run(RecordingSync(load_facts=MEASURED), clock=late, pipeline=DECLARED)

    assert fresh.succeeded
    assert statuses(stale)[1] == ("orders", StepStatus.CHECK_FAILED)
    assert checks_of(stale, "orders")["freshness(updated_at)"].message == (
        "orders: freshness(updated_at) failed — "
        "the newest value is 30.0 hours old, more than 24 hours"
    )


def test_the_clock_is_read_after_the_load_once_per_step():
    clock = FixedClock()
    sync = RecordingSync(load_facts=MEASURED)
    original = sync.sync

    def sync_then_note(step, source, warehouse):
        assert clock.reads == len([c for c in sync.calls if c[0] == "sync"])
        return original(step, source, warehouse)

    sync.sync = sync_then_note  # type: ignore[method-assign]

    assert run(sync, clock=clock, pipeline=DECLARED).succeeded
    assert clock.reads == 2


def test_declared_check_without_a_measured_fact_fails_instead_of_passing():
    store = FakeSnapshotStore()

    result = run(RecordingSync(), snapshot_store=store, pipeline=DECLARED)

    assert statuses(result)[0] == ("customers", StepStatus.CHECK_FAILED)
    assert checks_of(result, "customers")["not_null(email)"].message == (
        "customers: not_null(email) failed — the null count was not measured"
    )
    assert store.saves == []
