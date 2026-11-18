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
    FakeTransform,
    FixedClock,
    RecordingSync,
    dbt_test,
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
    SourceUnreachable,
    StepStatus,
    Sync,
    TransformResult,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import (
    BreakingChangeError,
    CheckFailedError,
    EmptySourceTableError,
    InOrderOrchestrator,
    PlannedStep,
    PlannedTransformStep,
    SourceUnreachableError,
    StepError,
    SyncFailedError,
    TransformFailedError,
    WarehouseUnreachableError,
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
    transform=None,
) -> RunResult:
    store = FakeSnapshotStore() if snapshot_store is None else snapshot_store
    clock = FixedClock() if clock is None else clock
    return run_pipeline(
        pipeline, SETTINGS, environment, env, sync, store, clock, orchestrator, transform
    )


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
        (CheckName.SOURCE_TABLES_EXIST, "customers", CheckStatus.FAIL),
        (CheckName.SOURCE_NOT_EMPTY, "customers", CheckStatus.PASS),
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


def empty(table: str) -> SourceFacts:
    types = TABLE_TYPES[table]
    return SourceFacts(True, frozenset(types), types, row_count=0)


def planned_orders_step(pipeline: str = PIPELINE) -> PlannedStep:
    return PlannedStep(
        load_pipeline(pipeline).steps[1],
        ResolvedConnection("h", 1, "d", "u", SOURCE_PASSWORD),
        ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD),
        "shop",
        FakeSnapshotStore(),
        FixedClock(),
    )


SKIP_EMPTY_ORDER_ITEMS = PIPELINE.replace(
    "checks: [row_count_matches]", "checks: [row_count_matches, source_not_empty]"
)


def test_an_empty_source_table_halts_its_step_before_any_sync_and_skips_later_steps():
    sync = RecordingSync(source_facts={"customers": empty("customers")})

    result = run(sync)

    assert sync.calls == [("preflight", "customers")]
    assert statuses(result) == [
        ("customers", StepStatus.CHECK_FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    assert checks_of(result, "customers")[CheckName.SOURCE_NOT_EMPTY].status is CheckStatus.FAIL
    assert (
        result.steps[0].error
        == "Pipeline shop, Step customers, table customers: Source table is empty"
    )


def test_empty_source_table_error_is_a_sanitized_check_failure():
    sync = RecordingSync(source_facts={"order_items": empty("order_items")})

    with pytest.raises(EmptySourceTableError) as raised:
        planned_orders_step()(sync)

    error = raised.value
    assert str(error) == "Pipeline shop, Step orders, table order_items: Source table is empty"
    assert error.__cause__ is None and error.__context__ is None
    assert isinstance(error, CheckFailedError)
    assert (error.table, error.rows_loaded) == ("order_items", {})
    assert [c.check for c in error.checks if c.status is CheckStatus.FAIL] == [
        CheckName.SOURCE_NOT_EMPTY
    ]
    assert_no_secret_in(str(error), repr(error), *(c.message for c in error.checks))


def test_a_skipped_source_not_empty_lets_an_empty_table_pass_and_reports_the_skip():
    sync = RecordingSync(source_facts={"order_items": empty("order_items")})

    result = run(sync, pipeline=SKIP_EMPTY_ORDER_ITEMS)

    assert result.succeeded
    assert ("sync", "orders") in sync.calls
    skipped = checks_of(result, "order_items")[CheckName.SOURCE_NOT_EMPTY]
    assert skipped.status is CheckStatus.SKIPPED
    assert skipped.message == (
        "order_items: source_not_empty skipped — the Source table is appended to while it is copied"
    )


def test_skipping_source_not_empty_without_a_reason_is_rejected_before_any_step_runs():
    no_reason = SKIP_EMPTY_ORDER_ITEMS.replace(
        "reason: the Source table is appended to while it is copied", 'reason: ""'
    )
    sync = RecordingSync(source_facts={"order_items": empty("order_items")})

    with pytest.raises(PipelineValidationError) as raised:
        run(sync, pipeline=no_reason)

    assert "skip_checks.reason" in str(raised.value)
    assert sync.calls == []


@pytest.mark.parametrize("row_count", [1, None])
def test_a_non_empty_or_uncounted_source_table_passes_source_not_empty(row_count):
    types = TABLE_TYPES["orders"]
    sync = RecordingSync(
        source_facts={"orders": SourceFacts(True, frozenset(types), types, row_count)}
    )

    result = run(sync)

    assert result.succeeded
    assert checks_of(result, "orders")[CheckName.SOURCE_NOT_EMPTY].status is CheckStatus.PASS


def test_an_empty_table_beside_a_missing_one_gives_the_general_check_failure():
    sync = RecordingSync(
        source_facts={"orders": empty("orders"), "order_items": SourceFacts(False, frozenset())}
    )

    with pytest.raises(CheckFailedError) as raised:
        planned_orders_step()(sync)

    assert type(raised.value) is CheckFailedError
    assert str(raised.value) == (
        "Step orders: Checks failed: orders: source_not_empty failed — table orders is empty in"
        " the Source; order_items: source_tables_exist failed — table not found in the Source"
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
        (CheckName.SOURCE_NOT_EMPTY, CheckStatus.PASS),
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


SYNC_TRANSFORM_SYNC = """
name: shop
steps:
  - name: customers
    source: shop
    tables:
      - {name: customers, mode: full_refresh, primary_key: id}
  - name: transform
    type: transform
    project: dbt
  - name: products
    source: shop
    tables:
      - {name: products, mode: full_refresh, primary_key: sku}
"""

TRANSFORM_ONLY = """
name: shop
steps:
  - {name: transform, type: transform, project: dbt}
"""

WAREHOUSE_ONLY_ENV = {"WAREHOUSE_PASSWORD": WAREHOUSE_PASSWORD}


def transform_result(*tests, crashed=False, failed_models=()) -> TransformResult:
    return TransformResult(tests=tests, crashed=crashed, failed_models=failed_models)


def planned_transform(transform: FakeTransform) -> PlannedTransformStep:
    step = load_pipeline(TRANSFORM_ONLY).steps[0]
    warehouse = ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD)
    return PlannedTransformStep(step, warehouse, "shop", transform)  # type: ignore[arg-type]


def test_passing_dbt_tests_are_reported_as_checks_of_the_transform_step():
    transform = FakeTransform(
        transform_result(dbt_test("not_null_customer_orders_id"), dbt_test("unique_orders_id"))
    )

    result = run(RecordingSync(), pipeline=SYNC_TRANSFORM_SYNC, transform=transform)

    assert result.succeeded
    transform_step = result.steps[1]
    assert transform_step.status is StepStatus.PASSED
    assert [(c.table, c.check, c.status) for c in transform_step.checks] == [
        ("customer_orders", "not_null_customer_orders_id", CheckStatus.PASS),
        ("customer_orders", "unique_orders_id", CheckStatus.PASS),
    ]
    assert transform_step.rows_loaded == {}


def test_the_transform_engine_gets_the_resolved_warehouse_of_the_environment():
    transform = FakeTransform()

    run(RecordingSync(), pipeline=SYNC_TRANSFORM_SYNC, transform=transform)

    [(step, warehouse)] = transform.calls
    assert step == "transform"
    assert (warehouse.host, warehouse.port, warehouse.database) == ("localhost", 5433, "warehouse")
    assert warehouse.password == WAREHOUSE_PASSWORD


def test_failing_dbt_test_fails_its_checks_and_skips_every_later_step():
    sync = RecordingSync()
    transform = FakeTransform(
        transform_result(dbt_test("not_null_customer_orders_id", status="fail", failures=2))
    )

    result = run(sync, pipeline=SYNC_TRANSFORM_SYNC, transform=transform)

    assert statuses(result) == [
        ("customers", StepStatus.PASSED),
        ("transform", StepStatus.CHECK_FAILED),
        ("products", StepStatus.SKIPPED),
    ]
    [check] = result.steps[1].checks
    assert check.status is CheckStatus.FAIL
    assert check.message == (
        "customer_orders: dbt test not_null_customer_orders_id failed — 2 failing rows"
    )
    assert result.steps[1].error == "Step transform: Checks failed: " + check.message
    assert result.steps[0].rows_loaded == {"customers": 3}
    assert ("preflight", "products") not in sync.calls
    assert ("sync", "products") not in sync.calls


@pytest.mark.parametrize(
    ("measured", "error"),
    [
        (transform_result(crashed=True), "Pipeline shop, Step transform: dbt run failed"),
        (
            transform_result(dbt_test("unique_orders_id"), failed_models=("customer_orders",)),
            "Pipeline shop, Step transform: dbt run failed (models: customer_orders)",
        ),
    ],
    ids=["crash-without-tests", "broken-model-with-passing-tests"],
)
def test_crash_or_broken_model_fails_the_step_and_skips_every_later_step(measured, error):
    sync = RecordingSync()

    result = run(sync, pipeline=SYNC_TRANSFORM_SYNC, transform=FakeTransform(measured))

    assert statuses(result) == [
        ("customers", StepStatus.PASSED),
        ("transform", StepStatus.FAILED),
        ("products", StepStatus.SKIPPED),
    ]
    assert result.steps[1].error == error
    assert result.steps[1].checks == ()
    assert ("sync", "products") not in sync.calls


@pytest.mark.parametrize(
    ("tests", "crashed", "failed_models", "expected"),
    [
        ((), False, (), None),
        (("pass",), False, (), None),
        (("warn",), False, (), None),
        (("fail",), False, (), CheckFailedError),
        (("error",), False, (), CheckFailedError),
        (("skipped",), False, (), CheckFailedError),
        ((), True, (), TransformFailedError),
        (("pass",), True, (), TransformFailedError),
        (("fail",), True, (), TransformFailedError),
        ((), False, ("customer_orders",), TransformFailedError),
        (("pass",), False, ("customer_orders",), TransformFailedError),
        (("fail",), False, ("customer_orders",), TransformFailedError),
        (("fail",), True, ("customer_orders",), TransformFailedError),
    ],
)
def test_a_crash_or_broken_model_outranks_whatever_the_tests_say(
    tests, crashed, failed_models, expected
):
    facts = tuple(dbt_test(f"t{i}", status=status) for i, status in enumerate(tests))
    step = planned_transform(
        FakeTransform(transform_result(*facts, crashed=crashed, failed_models=failed_models))
    )

    if expected is None:
        assert len(step(RecordingSync()).checks) == len(tests)
    else:
        with pytest.raises(expected):
            step(RecordingSync())


def test_an_engine_error_fails_with_the_fixed_message_and_keeps_none_of_its_text():
    transform = FakeTransform(fail_with=RuntimeError(f"password {WAREHOUSE_PASSWORD} rejected"))

    with pytest.raises(TransformFailedError) as raised:
        planned_transform(transform)(RecordingSync())

    assert str(raised.value) == "Pipeline shop, Step transform: dbt run failed"
    assert raised.value.__cause__ is None and raised.value.__context__ is None


def test_a_failed_transform_run_reports_no_warehouse_password_anywhere():
    transforms = [
        FakeTransform(fail_with=RuntimeError(f"auth failed: {WAREHOUSE_PASSWORD}")),
        FakeTransform(transform_result(dbt_test("not_null_id", status="fail"))),
        FakeTransform(transform_result(failed_models=("customer_orders",))),
    ]

    for transform in transforms:
        result = run(RecordingSync(), pipeline=SYNC_TRANSFORM_SYNC, transform=transform)

        assert not result.succeeded
        texts = [repr(result), str(result)]
        texts += [step.error or "" for step in result.steps]
        texts += [check.message for step in result.steps for check in step.checks]
        assert_no_secret_in(*texts)


def test_a_transform_step_without_an_engine_is_rejected_before_any_step_runs():
    sync = RecordingSync()

    with pytest.raises(PipelineValidationError) as raised:
        run(sync, pipeline=SYNC_TRANSFORM_SYNC, transform=None)

    assert raised.value.problems == (
        "steps[1]: a Transform Step needs a Transform engine, and none is set",
    )
    assert sync.calls == []


def test_a_transform_step_needs_no_source_secret_and_no_earlier_sync():
    sync = RecordingSync()
    transform = FakeTransform(transform_result(dbt_test("unique_orders_id")))

    result = run(sync, env=WAREHOUSE_ONLY_ENV, pipeline=TRANSFORM_ONLY, transform=transform)

    assert statuses(result) == [("transform", StepStatus.PASSED)]
    assert sync.calls == []
    assert [step for step, _ in transform.calls] == ["transform"]


def test_a_transform_step_first_runs_before_the_sync_that_follows_it():
    sync = RecordingSync()
    transform = FakeTransform()
    pipeline = (
        TRANSFORM_ONLY
        + """  - name: products
    source: shop
    tables:
      - {name: products, mode: full_refresh, primary_key: sku}
"""
    )

    result = run(sync, pipeline=pipeline, transform=transform)

    assert statuses(result) == [
        ("transform", StepStatus.PASSED),
        ("products", StepStatus.PASSED),
    ]
    assert transform.calls and sync.calls[0] == ("preflight", "products")


def orders_step(store: FakeSnapshotStore | None = None) -> PlannedStep:
    return PlannedStep(
        load_pipeline(PIPELINE).steps[1],
        ResolvedConnection("h", 1, "d", "u", SOURCE_PASSWORD),
        ResolvedConnection("h", 1, "d", "u", WAREHOUSE_PASSWORD),
        "shop",
        store or FakeSnapshotStore(),
        FixedClock(),
    )


def assert_unchained(error: Exception) -> None:
    assert error.__cause__ is None and error.__context__ is None


@pytest.mark.parametrize("stage", ["preflight", "sync"])
def test_source_unreachable_marker_becomes_source_unreachable_error(stage):
    marker = {"orders": SourceUnreachable("order_items")}
    sync = RecordingSync(**{f"fail_{stage}_of": marker})

    with pytest.raises(SourceUnreachableError) as raised:
        orders_step()(sync)

    assert type(raised.value) is SourceUnreachableError
    assert str(raised.value) == (
        "Pipeline shop, Step orders, table order_items: Source is unreachable"
    )
    assert raised.value.table == "order_items"
    assert_unchained(raised.value)


@pytest.mark.parametrize(
    ("sync_kwargs", "store_kwargs"),
    [
        ({"fail_sync_of": {"orders": WarehouseUnreachable()}}, {}),
        ({}, {"fail_load": WarehouseUnreachable()}),
        ({}, {"fail_save": WarehouseUnreachable()}),
    ],
    ids=["sync", "snapshot load", "snapshot save"],
)
def test_warehouse_unreachable_marker_becomes_warehouse_unreachable_error(
    sync_kwargs, store_kwargs
):
    with pytest.raises(WarehouseUnreachableError) as raised:
        orders_step(FakeSnapshotStore(**store_kwargs))(RecordingSync(**sync_kwargs))

    assert type(raised.value) is WarehouseUnreachableError
    assert str(raised.value) == "Pipeline shop, Step orders: Warehouse is unreachable"
    assert_unchained(raised.value)


def test_an_unreachable_source_fails_its_step_and_skips_later_steps():
    sync = RecordingSync(fail_preflight_of={"customers": SourceUnreachable("customers")})

    result = run(sync)

    assert statuses(result) == [
        ("customers", StepStatus.FAILED),
        ("orders", StepStatus.SKIPPED),
        ("products", StepStatus.SKIPPED),
    ]
    assert result.steps[0].error == (
        "Pipeline shop, Step customers, table customers: Source is unreachable"
    )


def test_a_step_error_from_the_sync_keeps_its_own_message():
    failed = SyncFailedError("Pipeline shop, Step orders, table orders: load failed")

    with pytest.raises(SyncFailedError) as raised:
        orders_step()(RecordingSync(fail_sync_of={"orders": failed}))

    assert raised.value is failed
    assert str(raised.value) == "Pipeline shop, Step orders, table orders: load failed"
