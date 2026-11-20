from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
from fakes import (
    CRM_TARGET,
    DUCKDB_PATH,
    DUCKDB_SETTINGS,
    ENV,
    PIPELINE,
    REVERSE_ENV,
    REVERSE_PIPELINE,
    REVERSE_SETTINGS,
    SETTINGS,
    SOURCE_PASSWORD,
    STAGE_WAREHOUSE_PASSWORD,
    TABLE_TYPES,
    TARGET_PASSWORD,
    TARGET_ROWS,
    WAREHOUSE_PASSWORD,
    FakeFingerprintStore,
    FakeProjectFiles,
    FakeReverseSync,
    FakeSnapshotStore,
    FakeTransform,
    FixedClock,
    RecordingSync,
    StrictEnv,
    dbt_test,
    facts_with_types,
    healthy_load,
)

from adops.application import run_pipeline as run_pipeline_module
from adops.application.ports import (
    Clock,
    Orchestrator,
    ProjectFiles,
    ResolvedConnection,
    ResolvedDuckDb,
    ReverseSync,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    SourceUnreachable,
    StepFingerprintStore,
    StepReport,
    StepStatus,
    Sync,
    TargetUnreachable,
    TransformResult,
    WarehouseUnreachable,
    WriteFailed,
)
from adops.application.run_pipeline import (
    BranchRun,
    BranchSourcesNotParameterizedError,
    BreakingChangeError,
    CheckFailedError,
    EmptySourceTableError,
    InOrderOrchestrator,
    PlannedReverseSyncStep,
    PlannedStep,
    PlannedTransformStep,
    ReverseSyncTargetUnreachableError,
    ReverseSyncWriteFailedError,
    SourceUnreachableError,
    StageNotRunError,
    StepError,
    StepFingerprinting,
    SyncFailedError,
    TransformFailedError,
    WarehouseUnreachableError,
    _build_plan,
    current_fingerprints,
    plan_branch_run,
    run_pipeline,
)
from adops.application.secrets import MissingSecretError
from adops.domain.branch_environment import STAGE
from adops.domain.checks import CheckName, CheckResult, CheckStatus, SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline
from adops.domain.reverse_sync_diff import Diff, PreviewSample, compute_diff, writes_allowed
from adops.domain.schema_snapshot import TableShape

SENTINELS = (SOURCE_PASSWORD, WAREHOUSE_PASSWORD, TARGET_PASSWORD, STAGE_WAREHOUSE_PASSWORD)
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
    settings=SETTINGS,
) -> RunResult:
    store = FakeSnapshotStore() if snapshot_store is None else snapshot_store
    clock = FixedClock() if clock is None else clock
    return run_pipeline(
        pipeline, settings, environment, env, sync, store, clock, orchestrator, transform
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
    assert isinstance(FakeReverseSync(), ReverseSync)


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


SOURCE_ONLY_ENV = StrictEnv({"SHOP_DB_PASSWORD": SOURCE_PASSWORD})
DUCKDB_WAREHOUSE = ResolvedDuckDb(path=DUCKDB_PATH)


def run_on_duckdb(sync: RecordingSync, **options) -> RunResult:
    return run(sync, env=SOURCE_ONLY_ENV, settings=DUCKDB_SETTINGS, **options)


def test_a_duckdb_run_needs_only_the_source_secrets():
    sync = RecordingSync()
    store = FakeSnapshotStore()
    transform = FakeTransform(transform_result(dbt_test("unique_orders_id")))

    result = run_on_duckdb(
        sync, snapshot_store=store, pipeline=SYNC_TRANSFORM_SYNC, transform=transform
    )

    assert statuses(result) == [
        ("customers", StepStatus.PASSED),
        ("transform", StepStatus.PASSED),
        ("products", StepStatus.PASSED),
    ]


def test_a_duckdb_transform_only_run_needs_no_secret_at_all():
    transform = FakeTransform()

    result = run(
        RecordingSync(),
        env=StrictEnv({}),
        settings=DUCKDB_SETTINGS,
        pipeline=TRANSFORM_ONLY,
        transform=transform,
    )

    assert result.succeeded
    assert transform.calls == [("transform", DUCKDB_WAREHOUSE)]


def test_a_duckdb_run_hands_every_warehouse_call_the_path_as_written():
    sync = RecordingSync()
    store = FakeSnapshotStore()
    transform = FakeTransform()

    run_on_duckdb(sync, snapshot_store=store, pipeline=SYNC_TRANSFORM_SYNC, transform=transform)

    sources, warehouses = sync.connections[0::2], sync.connections[1::2]
    assert [type(source) for source in sources] == [ResolvedConnection, ResolvedConnection]
    assert warehouses == [DUCKDB_WAREHOUSE, DUCKDB_WAREHOUSE]
    assert store.warehouses == [DUCKDB_WAREHOUSE] * 4
    assert transform.calls == [("transform", DUCKDB_WAREHOUSE)]


def test_a_duckdb_run_still_masks_the_source_password_in_errors():
    leak = RuntimeError(f"auth failed for password {SOURCE_PASSWORD}")
    sync = RecordingSync(fail_sync_of={"customers": leak})

    result = run_on_duckdb(sync)

    assert result.steps[0].error == "Step customers: RuntimeError: auth failed for password ***"
    assert_no_secret_in(repr(result), str(result))


def test_a_duckdb_run_leaves_error_text_without_secrets_unchanged():
    sync = RecordingSync(fail_sync_of={"customers": RuntimeError("disk full")})

    result = run_on_duckdb(sync)

    assert result.steps[0].error == "Step customers: RuntimeError: disk full"


def run_reverse(
    engine: FakeReverseSync | None,
    environment: str = "dev",
    env=REVERSE_ENV,
    orchestrator=None,
    pipeline: str = REVERSE_PIPELINE,
    settings: str = REVERSE_SETTINGS,
) -> RunResult:
    return run_pipeline(
        pipeline,
        settings,
        environment,
        env,
        RecordingSync(),
        FakeSnapshotStore(),
        FixedClock(),
        orchestrator,
        reverse_sync=engine,
    )


def planned_reverse(engine: FakeReverseSync, environment: str = "stage") -> PlannedReverseSyncStep:
    return PlannedReverseSyncStep(
        load_pipeline(REVERSE_PIPELINE).steps[0],
        ResolvedConnection("w", 5432, "warehouse_stage", "adops", STAGE_WAREHOUSE_PASSWORD),
        ResolvedConnection("t", 5440, "crm", "crm_writer", TARGET_PASSWORD),
        environment,
        "crm_push",
        engine,
    )


SINGLE_REVERSE_SYNC = REVERSE_PIPELINE.split("  - type: reverse_sync\n    name: push_again")[0]
EXPECTED_SAMPLE = PreviewSample(inserts=(3,), updates=((2, ("orders",)),), deletes=(), kept=(4, 5))


@pytest.mark.parametrize("environment", ["dev", "staging"])
def test_a_non_writing_environment_gets_a_preview_and_never_reaches_apply(environment):
    engine = FakeReverseSync()

    result = run_reverse(engine, environment)

    assert result.succeeded
    assert engine.applied == []
    preview = result.steps[0].preview
    assert preview is not None
    assert (preview.inserts, preview.updates, preview.deletes, preview.kept) == (1, 1, 0, 2)
    assert preview.sample == EXPECTED_SAMPLE
    assert preview.written is False
    assert [check.status for check in result.steps[0].checks] == [CheckStatus.PASS] * 5


def test_stage_applies_the_very_diff_the_preview_reports(monkeypatch: pytest.MonkeyPatch):
    computed: list[Diff] = []

    def spy(*args, **kwargs) -> Diff:
        computed.append(compute_diff(*args, **kwargs))
        return computed[-1]

    monkeypatch.setattr(run_pipeline_module, "compute_diff", spy)
    engine = FakeReverseSync()

    result = run_reverse(engine, "stage", pipeline=SINGLE_REVERSE_SYNC)

    assert result.succeeded
    assert len(computed) == 1
    assert engine.applied_diff is computed[0]
    preview = result.steps[0].preview
    assert preview is not None and preview.written is True
    assert (preview.inserts, preview.updates, preview.deletes, preview.kept) == (
        len(computed[0].inserts),
        len(computed[0].updates),
        len(computed[0].deletes),
        len(computed[0].kept),
    )
    assert preview.sample == EXPECTED_SAMPLE


def test_the_engine_reads_from_the_selected_environments_own_warehouse_and_target():
    dev, stage = FakeReverseSync(), FakeReverseSync()

    run_reverse(dev, "dev")
    run_reverse(stage, "stage")

    _, dev_warehouse, dev_target = dev.reads[0]
    _, stage_warehouse, stage_target = stage.reads[0]
    assert dev_warehouse == ResolvedDuckDb("dev.duckdb")
    assert stage_warehouse == ResolvedConnection(
        "warehouse-stage.internal", 5432, "warehouse_stage", "adops", STAGE_WAREHOUSE_PASSWORD
    )
    assert dev_target == stage_target == ResolvedConnection(
        "crm.internal", 5440, "crm", "crm_writer", TARGET_PASSWORD
    )
    assert stage.applied[0][1] == stage_target


def test_a_failing_reverse_sync_check_writes_nothing_and_raises_check_failed():
    every_row_changes = tuple({**row, "orders": 99} for row in TARGET_ROWS[:2])
    engine = FakeReverseSync(target_rows=every_row_changes)

    with pytest.raises(CheckFailedError) as raised:
        planned_reverse(engine)(RecordingSync())

    assert engine.applied == []
    assert "max_change failed" in str(raised.value)


def test_a_failed_reverse_sync_check_stops_every_later_step():
    every_row_changes = tuple({**row, "orders": 99} for row in TARGET_ROWS[:2])
    engine = FakeReverseSync(target_rows=every_row_changes)

    result = run_reverse(engine, "stage")

    assert statuses(result) == [
        ("push_stats", StepStatus.CHECK_FAILED),
        ("push_again", StepStatus.SKIPPED),
    ]
    assert engine.applied == []
    assert [name for name, _, _ in engine.reads] == ["push_stats"]


@pytest.mark.parametrize(
    ("measured", "failing"),
    [
        ({"duplicate_keys": 2}, CheckName.KEY_UNIQUE),
        ({"null_keys": 1}, CheckName.KEY_NOT_NULL),
        ({"target_table_exists": False}, CheckName.TARGET_COLUMNS_EXIST),
        ({"missing_target_columns": ("total",)}, CheckName.TARGET_COLUMNS_EXIST),
        ({"result_rows": ()}, CheckName.RESULT_NOT_EMPTY),
    ],
)
def test_every_measured_fact_reaches_its_check_before_anything_is_written(measured, failing):
    engine = FakeReverseSync(**measured)

    result = run_reverse(engine, "stage")

    failed = [c.check for c in result.steps[0].checks if c.status is CheckStatus.FAIL]
    assert failed == [failing]
    assert engine.applied == []


def test_a_delete_missing_step_previews_absent_target_rows_as_deletes():
    pipeline = REVERSE_PIPELINE.replace(
        "      total_amount: total\n",
        "      total_amount: total\n    delete_missing: true\n    max_change_share: 1\n",
    )

    result = run_reverse(FakeReverseSync(), "dev", pipeline=pipeline)

    preview = result.steps[0].preview
    assert preview is not None
    assert (preview.deletes, preview.kept, preview.sample.deletes) == (2, 0, (4, 5))


@pytest.mark.parametrize(
    ("engine", "error", "message"),
    [
        (
            FakeReverseSync(fail_read=TargetUnreachable()),
            ReverseSyncTargetUnreachableError,
            "Pipeline crm_push, Step push_stats: Target crm is unreachable",
        ),
        (
            FakeReverseSync(fail_apply=TargetUnreachable()),
            ReverseSyncTargetUnreachableError,
            "Pipeline crm_push, Step push_stats: Target crm is unreachable",
        ),
        (
            FakeReverseSync(fail_apply=WriteFailed()),
            ReverseSyncWriteFailedError,
            "Pipeline crm_push, Step push_stats: the write to Target crm failed "
            "and was rolled back; the Target was left unchanged",
        ),
        (
            FakeReverseSync(fail_read=WarehouseUnreachable()),
            WarehouseUnreachableError,
            "Pipeline crm_push, Step push_stats: Warehouse is unreachable",
        ),
    ],
    ids=["target on read", "target on apply", "write failed", "warehouse on read"],
)
def test_engine_markers_become_fixed_unchained_errors(engine, error, message):
    with pytest.raises(error) as raised:
        planned_reverse(engine)(RecordingSync())

    assert type(raised.value) is error
    assert str(raised.value) == message
    assert_unchained(raised.value)
    assert_no_secret_in(str(raised.value), repr(raised.value))


def leaky_engine_error(environment: str) -> RuntimeError:
    text = f"could not connect to postgresql://crm_writer:{TARGET_PASSWORD}@crm.internal/crm"
    if environment == "stage":
        text += f" (warehouse password={STAGE_WAREHOUSE_PASSWORD})"
    return RuntimeError(text)


@pytest.mark.parametrize(
    ("environment", "stage"), [("dev", "read"), ("stage", "read"), ("stage", "apply")]
)
def test_an_unexpected_engine_error_fails_the_step_with_secrets_masked(environment, stage):
    engine = FakeReverseSync(**{f"fail_{stage}": leaky_engine_error(environment)})

    result = run_reverse(engine, environment)

    report = result.steps[0]
    assert statuses(result) == [
        ("push_stats", StepStatus.FAILED),
        ("push_again", StepStatus.SKIPPED),
    ]
    assert report.error is not None and report.error.startswith("Step push_stats: RuntimeError")
    assert_no_secret_in(report.error, repr(result))


def test_a_write_failure_fails_the_run_with_no_secret_and_skips_later_steps():
    result = run_reverse(FakeReverseSync(fail_apply=WriteFailed()), "stage")

    assert statuses(result) == [
        ("push_stats", StepStatus.FAILED),
        ("push_again", StepStatus.SKIPPED),
    ]
    assert result.steps[0].error is not None
    assert "left unchanged" in result.steps[0].error
    assert result.steps[0].preview is None
    assert_no_secret_in(repr(result))


def test_a_reverse_sync_step_without_an_engine_is_rejected_before_any_step_runs():
    with pytest.raises(PipelineValidationError) as raised:
        run_reverse(None)

    assert raised.value.problems == (
        "steps[0]: a Reverse Sync Step needs a Reverse Sync engine, and none is set",
    )


def test_a_reverse_sync_step_needs_its_target_defined_in_the_environment():
    engine = FakeReverseSync()

    with pytest.raises(PipelineValidationError) as raised:
        run_reverse(engine, settings=SETTINGS, env=ENV)

    assert raised.value.problems[0].startswith("steps[0].target: Step push_stats writes to Target")
    assert engine.reads == []


def test_a_missing_target_password_is_reported_by_name_before_any_step_runs():
    engine = FakeReverseSync()

    with pytest.raises(MissingSecretError) as raised:
        run_reverse(engine, "stage", env={"STAGE_WAREHOUSE_PASSWORD": "x"})

    assert raised.value.names == ("CRM_TARGET_PASSWORD",)
    assert engine.reads == []


def test_the_reverse_sync_plan_never_prints_a_password():
    captured: list[RunPlan] = []

    class CapturingOrchestrator:
        def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
            captured.append(plan)
            return InOrderOrchestrator().execute(plan, sync)

    run_reverse(FakeReverseSync(), "stage", orchestrator=CapturingOrchestrator())

    assert_no_secret_in(repr(captured[0]), str(captured[0]))


def test_a_step_of_an_unknown_kind_is_a_programming_error():
    settings = load_project_settings(REVERSE_SETTINGS)
    pipeline = Pipeline.model_construct(name="odd", steps=[object()])

    with pytest.raises(AssertionError):
        _build_plan(
            pipeline,
            "dev",
            settings.environments["dev"],
            REVERSE_ENV,
            FakeSnapshotStore(),
            FixedClock(),
            None,
            None,
        )


SHOP_SOURCE = """
      shop:
        type: postgres
        host: localhost
        port: 5432
        database: shop
        user: shop
        password_env: SHOP_DB_PASSWORD
"""

BRANCH_SETTINGS = f"""
environments:
  dev:
    warehouse:
      type: duckdb
      path: dev.duckdb
    sources:{SHOP_SOURCE}
    targets:{CRM_TARGET}
  stage:
    warehouse:
      type: duckdb
      path: stage.duckdb
    sources:{SHOP_SOURCE}
    targets:{CRM_TARGET}
"""

NO_STAGE_SETTINGS = BRANCH_SETTINGS.split("  stage:")[0]

BRANCH_ENV = {"SHOP_DB_PASSWORD": SOURCE_PASSWORD, "CRM_TARGET_PASSWORD": TARGET_PASSWORD}

BRANCH_PIPELINE = """
name: shop
steps:
  - name: customers
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
  - {name: build, type: transform, project: dbt}
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping:
      order_count: orders
      total_amount: total
"""

BRANCH = "br_feat"
STEP_NAMES = ["customers", "build", "push_stats"]
PARAMETERIZED_SOURCES = b"schema: \"{{ var('adops_raw_schema_shop', 'raw_shop') }}\""
HARD_CODED_SOURCES = b"schema: raw_shop"


def dbt_files(
    sources: bytes = PARAMETERIZED_SOURCES, model: bytes = b"select 1"
) -> FakeProjectFiles:
    return FakeProjectFiles(
        {"dbt": {"models/sources.yml": sources, "models/customer_orders.sql": model}}
    )


def fingerprints_of(files: ProjectFiles) -> dict[str, str]:
    return dict(current_fingerprints(load_pipeline(BRANCH_PIPELINE).steps, files))


def store_matching(files: ProjectFiles, **changed: str) -> FakeFingerprintStore:
    return FakeFingerprintStore({"shop": {**fingerprints_of(files), **changed}})


@dataclass
class CountingOrchestrator:
    plans: list[RunPlan] = field(default_factory=list)

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        self.plans.append(plan)
        return InOrderOrchestrator().execute(plan, sync)


def run_with_fingerprints(
    store: FakeFingerprintStore,
    files: FakeProjectFiles,
    environment: str = STAGE,
    transform: FakeTransform | None = None,
    engine: FakeReverseSync | None = None,
) -> RunResult:
    return run_pipeline(
        BRANCH_PIPELINE,
        BRANCH_SETTINGS,
        environment,
        BRANCH_ENV,
        RecordingSync(),
        FakeSnapshotStore(),
        FixedClock(),
        None,
        transform or FakeTransform(),
        engine or FakeReverseSync(),
        StepFingerprinting(store, files),
    )


def run_branch(
    store: FakeFingerprintStore,
    files: FakeProjectFiles,
    sync: RecordingSync | None = None,
    orchestrator: CountingOrchestrator | None = None,
    engine: FakeReverseSync | None = None,
    settings: str = BRANCH_SETTINGS,
    env=BRANCH_ENV,
) -> RunResult:
    fingerprinting = StepFingerprinting(store, files)
    branch = plan_branch_run(BRANCH_PIPELINE, BRANCH, fingerprinting)
    return run_pipeline(
        BRANCH_PIPELINE,
        settings,
        BRANCH,
        env,
        sync or RecordingSync(),
        FakeSnapshotStore(),
        FixedClock(),
        orchestrator,
        FakeTransform(),
        engine or FakeReverseSync(),
        fingerprinting,
        branch,
    )


def test_fingerprint_fakes_satisfy_their_ports():
    assert isinstance(FakeFingerprintStore(), StepFingerprintStore)
    assert isinstance(FakeProjectFiles(), ProjectFiles)


def test_a_passing_stage_run_saves_every_steps_fingerprint_in_step_order():
    store, files = FakeFingerprintStore(), dbt_files()

    result = run_with_fingerprints(store, files)

    assert result.succeeded
    assert store.saves == [("shop", name) for name in STEP_NAMES]
    assert store.fingerprints["shop"] == fingerprints_of(files)


def test_a_failed_check_saves_the_earlier_steps_fingerprint_only():
    store = FakeFingerprintStore()
    failing = FakeTransform(transform_result(dbt_test("unique_id", status="fail")))

    result = run_with_fingerprints(store, dbt_files(), transform=failing)

    assert statuses(result) == [
        ("customers", StepStatus.PASSED),
        ("build", StepStatus.CHECK_FAILED),
        ("push_stats", StepStatus.SKIPPED),
    ]
    assert store.saves == [("shop", "customers")]


def test_a_raised_step_error_saves_no_fingerprint_for_that_step():
    store = FakeFingerprintStore()

    result = run_with_fingerprints(store, dbt_files(), transform=FakeTransform(fail_with=OSError()))

    assert result.steps[1].status is StepStatus.FAILED
    assert store.saves == [("shop", "customers")]


@pytest.mark.parametrize("environment", ["dev", BRANCH])
def test_dev_and_branch_runs_never_save_a_fingerprint(environment):
    files = dbt_files()
    store = store_matching(files, build="old")

    if environment == BRANCH:
        result = run_branch(store, files)
    else:
        result = run_with_fingerprints(store, files, environment)

    assert result.succeeded
    assert store.saves == []


def test_the_fingerprint_stage_saves_is_the_one_a_branch_compares():
    store, files = FakeFingerprintStore(), dbt_files()
    run_with_fingerprints(store, files)

    branch = plan_branch_run(BRANCH_PIPELINE, BRANCH, StepFingerprinting(store, files))

    assert branch == BranchRun(BRANCH, fingerprints_of(files), None)


def test_a_changed_dbt_file_changes_only_the_transform_steps_fingerprint():
    store = FakeFingerprintStore()
    run_with_fingerprints(store, dbt_files())

    edited = dbt_files(model=b"select 2")
    branch = plan_branch_run(BRANCH_PIPELINE, BRANCH, StepFingerprinting(store, edited))

    assert branch.first_changed == 1
    assert edited.reads == ["dbt", "dbt"]


def test_a_branch_defers_steps_before_the_first_changed_one_and_runs_every_later_one():
    files = dbt_files()
    sync, orchestrator = RecordingSync(), CountingOrchestrator()

    result = run_branch(store_matching(files, build="old"), files, sync, orchestrator)

    [plan] = orchestrator.plans
    assert [task.name for task in plan.steps] == ["build", "push_stats"]
    assert sync.calls == []
    assert statuses(result) == [
        ("customers", StepStatus.DEFERRED),
        ("build", StepStatus.PASSED),
        ("push_stats", StepStatus.PASSED),
    ]
    assert result.environment == BRANCH
    assert result.succeeded


def test_a_branch_that_matches_stage_never_calls_the_orchestrator():
    files = dbt_files()
    sync, orchestrator = RecordingSync(), CountingOrchestrator()

    result = run_branch(store_matching(files), files, sync, orchestrator)

    assert orchestrator.plans == []
    assert sync.calls == []
    assert result == RunResult(
        "shop", BRANCH, tuple(StepReport(name, StepStatus.DEFERRED) for name in STEP_NAMES)
    )
    assert result.succeeded


def test_a_deferred_sync_needs_no_source_secret():
    files = dbt_files()
    env = StrictEnv({"CRM_TARGET_PASSWORD": TARGET_PASSWORD})

    result = run_branch(store_matching(files, build="old"), files, env=env)

    assert result.succeeded


def test_a_failed_step_after_a_deferred_one_fails_the_run():
    result = RunResult(
        "shop",
        BRANCH,
        (StepReport("a", StepStatus.DEFERRED), StepReport("b", StepStatus.CHECK_FAILED)),
    )

    assert not result.succeeded


def test_a_branch_before_any_stage_run_is_told_to_run_stage_first():
    with pytest.raises(StageNotRunError, match="run the Pipeline in stage first"):
        plan_branch_run(
            BRANCH_PIPELINE, BRANCH, StepFingerprinting(FakeFingerprintStore(), dbt_files())
        )


def test_a_branch_run_without_a_stage_environment_is_rejected_by_name():
    files = dbt_files()

    with pytest.raises(PipelineValidationError) as raised:
        run_branch(store_matching(files, build="old"), files, settings=NO_STAGE_SETTINGS)

    assert raised.value.problems == (
        "environments.stage: a Branch run needs a stage Environment in adops.yaml, "
        "and none is set",
    )


def test_a_branch_run_must_run_under_its_branch_environments_name():
    branch = BranchRun(BRANCH, {"customers": "x"}, 0)

    with pytest.raises(ValueError, match="Branch Environment's name"):
        run_pipeline(
            BRANCH_PIPELINE,
            BRANCH_SETTINGS,
            STAGE,
            BRANCH_ENV,
            RecordingSync(),
            FakeSnapshotStore(),
            FixedClock(),
            branch=branch,
        )


def test_a_branch_reverse_sync_previews_even_though_stage_settings_write():
    files = dbt_files()
    stage_engine, branch_engine = FakeReverseSync(), FakeReverseSync()
    orchestrator = CountingOrchestrator()
    run_with_fingerprints(FakeFingerprintStore(), files, engine=stage_engine)

    result = run_branch(
        store_matching(files, build="old"), files, orchestrator=orchestrator, engine=branch_engine
    )

    assert writes_allowed(STAGE)
    assert len(stage_engine.applied) == 1
    reverse_task = orchestrator.plans[0].steps[-1]
    assert isinstance(reverse_task, PlannedReverseSyncStep)
    assert reverse_task.environment.startswith("br_")
    assert branch_engine.applied == []
    assert len(branch_engine.reads) == 1
    preview = result.steps[-1].preview
    assert preview is not None and preview.written is False


def test_a_rebuilt_sync_with_hard_coded_dbt_sources_fails_before_any_step_runs():
    files = dbt_files(sources=HARD_CODED_SOURCES)
    sync, orchestrator = RecordingSync(), CountingOrchestrator()

    with pytest.raises(BranchSourcesNotParameterizedError) as raised:
        run_branch(store_matching(files, customers="old"), files, sync, orchestrator)

    message = str(raised.value)
    assert "shop" in message and "adops_raw_schema_shop" in message
    assert "{{ var('adops_raw_schema_shop', 'raw_shop') }}" in message
    assert isinstance(raised.value, PipelineValidationError)
    assert raised.value.source == "shop"
    assert orchestrator.plans == [] and sync.calls == []


def test_a_rebuilt_sync_with_parameterized_dbt_sources_runs_every_step():
    files = dbt_files()

    result = run_branch(store_matching(files, customers="old"), files)

    assert [status for _, status in statuses(result)] == [StepStatus.PASSED] * 3


def test_hard_coded_dbt_sources_are_fine_when_the_sync_is_deferred():
    files = dbt_files(sources=HARD_CODED_SOURCES)

    result = run_branch(store_matching(files, build="old"), files)

    assert result.succeeded


def test_a_fingerprint_store_failure_fails_the_step_without_its_text():
    store = FakeFingerprintStore(fail_save=RuntimeError(f"connect {WAREHOUSE_PASSWORD}"))

    result = run_with_fingerprints(store, dbt_files())

    assert statuses(result)[:2] == [("customers", StepStatus.FAILED), ("build", StepStatus.SKIPPED)]
    assert result.steps[0].error == "Step customers: step fingerprint store: RuntimeError"
