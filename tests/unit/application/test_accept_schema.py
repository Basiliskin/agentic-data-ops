from datetime import UTC, datetime, timedelta, timezone

import pytest
from fakes import (
    DUCKDB_PATH,
    DUCKDB_SETTINGS,
    ENV,
    PIPELINE,
    SETTINGS,
    SOURCE_PASSWORD,
    WAREHOUSE_PASSWORD,
    FakeSnapshotStore,
    FixedClock,
    RecordingSync,
    StrictEnv,
    facts_with_types,
)

from adops.application.accept_schema import (
    AcceptedSchema,
    AlreadyCovered,
    FoundSchemaChanges,
    NewRecord,
    NothingToRecord,
    accept_schema,
    apply_schema_acceptance,
    find_schema_changes,
    plan_accepted_record,
)
from adops.application.ports import ResolvedDuckDb
from adops.application.run_pipeline import StepError, run_pipeline
from adops.application.secrets import MissingSecretError
from adops.domain.accepted_change_record import AcceptedChangeRecord
from adops.domain.checks import SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.schema_snapshot import BreakingChange, BreakingChangeKind, TableShape

ORDERS_BEFORE = {"id": "integer", "updated_at": "timestamptz", "customer_id": "integer"}
CHANGED_ORDERS = {"id": "integer", "updated_at": "timestamptz", "note": "text"}

TWO_SOURCES = """
name: shop
steps:
  - name: orders
    source: shop
    tables:
      - {name: orders, mode: incremental, primary_key: id, cursor: updated_at}
  - name: crm_contacts
    source: crm
    tables:
      - {name: contacts, mode: full_refresh, primary_key: id}
"""

ORDERS_TWICE = """
name: shop
steps:
  - name: first
    source: shop
    tables:
      - {name: orders, mode: full_refresh, primary_key: id}
  - name: second
    source: shop
    tables:
      - {name: orders, mode: full_refresh, primary_key: id}
"""


def accept(
    table="orders",
    pipeline=PIPELINE,
    environment="dev",
    env=ENV,
    sync=None,
    store=None,
    settings=SETTINGS,
):
    return accept_schema(
        pipeline,
        settings,
        environment,
        table,
        env,
        sync or RecordingSync(),
        store if store is not None else FakeSnapshotStore(),
    )


def test_saves_the_measured_shape_under_the_steps_pipeline_and_source():
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})
    store = FakeSnapshotStore()

    accepted = accept(sync=sync, store=store)

    assert accepted == AcceptedSchema("shop", "dev", "orders", "orders", TableShape(CHANGED_ORDERS))
    assert store.shapes == {("shop", "shop", "orders"): TableShape(CHANGED_ORDERS)}
    assert [w.password for w in store.warehouses] == [WAREHOUSE_PASSWORD, WAREHOUSE_PASSWORD]
    assert [c.password for c in sync.connections] == [SOURCE_PASSWORD]


def test_preflights_only_the_accepted_table():
    sync = RecordingSync()
    preflighted = []
    original = sync.preflight

    def preflight(step, source):
        preflighted.append([table.name for table in step.tables])
        return original(step, source)

    sync.preflight = preflight

    accept("order_items", sync=sync)

    assert preflighted == [["order_items"]]


def test_the_next_run_passes_against_the_accepted_shape():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    changed = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})

    halted = run_pipeline(PIPELINE, SETTINGS, "dev", ENV, changed, store, FixedClock())
    accept(sync=changed, store=store)
    passed = run_pipeline(PIPELINE, SETTINGS, "dev", ENV, changed, store, FixedClock())

    assert "Breaking Change in orders: customer_id dropped" in (halted.steps[1].error or "")
    assert changed.nullable == [("orders", "orders", ("customer_id",))]
    assert passed.succeeded


def test_a_secret_of_another_source_is_not_needed():
    env = {"SHOP_DB_PASSWORD": SOURCE_PASSWORD, "WAREHOUSE_PASSWORD": WAREHOUSE_PASSWORD}
    store = FakeSnapshotStore()

    accept(pipeline=TWO_SOURCES, env=env, store=store)

    assert set(store.shapes) == {("shop", "shop", "orders")}


def test_missing_secret_of_the_accepted_source_is_named():
    with pytest.raises(MissingSecretError, match="SHOP_DB_PASSWORD"):
        accept(env={"WAREHOUSE_PASSWORD": WAREHOUSE_PASSWORD})


def test_unknown_table_is_a_validation_error_and_saves_nothing():
    store = FakeSnapshotStore()

    with pytest.raises(PipelineValidationError) as error:
        accept("refunds", store=store)

    assert error.value.problems == ("refunds: no Step in Pipeline shop copies it",)
    assert store.saves == []


def test_table_in_two_steps_is_ambiguous_and_saves_nothing():
    store = FakeSnapshotStore()

    with pytest.raises(PipelineValidationError) as error:
        accept(pipeline=ORDERS_TWICE, store=store)

    assert error.value.problems == ("orders: copied by more than one Step (first, second)",)
    assert store.saves == []


def test_table_missing_in_the_source_is_refused_and_saves_nothing():
    sync = RecordingSync(source_facts={"orders": SourceFacts(False, frozenset())})
    store = FakeSnapshotStore()

    with pytest.raises(StepError, match="orders not found in Source shop; nothing accepted"):
        accept(sync=sync, store=store)

    assert store.saves == []


def test_unknown_environment_names_the_available_ones():
    with pytest.raises(PipelineValidationError, match=r"available: dev, prod"):
        accept(environment="staging")


def test_step_source_missing_from_the_environment_is_a_validation_error():
    with pytest.raises(PipelineValidationError, match=r"steps\[0\]\.source"):
        accept(environment="prod")


def test_dropped_columns_are_made_nullable_before_the_shape_is_saved():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})
    order: list[str] = []
    record_nullable = sync.make_columns_nullable

    def make_columns_nullable(step, table, warehouse, columns):
        order.append("nullable")
        record_nullable(step, table, warehouse, columns)

    sync.make_columns_nullable = make_columns_nullable
    store.fail_save = RuntimeError("save after nullable")

    with pytest.raises(RuntimeError):
        accept(sync=sync, store=store)

    assert order == ["nullable"]
    assert [w.password for w in sync.connections] == [SOURCE_PASSWORD, WAREHOUSE_PASSWORD]


def test_accepted_result_names_the_columns_made_nullable():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})

    accepted = accept(sync=sync, store=store)

    assert accepted.nullable_columns == ("customer_id",)
    assert store.shapes[("shop", "shop", "orders")] == TableShape(CHANGED_ORDERS)


@pytest.mark.parametrize(
    "previous",
    [None, {"id": "integer", "updated_at": "timestamp"}],
    ids=["first-accept", "retype-only"],
)
def test_nothing_is_made_nullable_without_a_dropped_column(previous):
    shapes = {} if previous is None else {("shop", "shop", "orders"): TableShape(previous)}
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})

    accepted = accept(sync=sync, store=FakeSnapshotStore(shapes))

    assert sync.nullable == []
    assert accepted.nullable_columns == ()


def test_a_transform_step_in_the_pipeline_is_not_a_candidate():
    pipeline = PIPELINE + """  - {name: transform, type: transform, project: dbt}
"""

    accepted = accept(table="products", pipeline=pipeline)

    assert accepted.step == "products"


def test_a_duckdb_warehouse_needs_no_warehouse_secret():
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})

    accepted = accept(
        env=StrictEnv({"SHOP_DB_PASSWORD": SOURCE_PASSWORD}),
        settings=DUCKDB_SETTINGS,
        sync=sync,
        store=store,
    )

    duckdb = ResolvedDuckDb(path=DUCKDB_PATH)
    assert accepted.nullable_columns == ("customer_id",)
    assert store.shapes[("shop", "shop", "orders")] == TableShape(CHANGED_ORDERS)
    assert store.warehouses == [duckdb, duckdb]
    assert sync.connections[1:] == [duckdb]


STAGE_SETTINGS = SETTINGS.replace(
    "  prod:\n",
    """  stage:
    warehouse:
      type: postgres
      host: warehouse-stage.internal
      port: 5432
      database: warehouse
      user: adops
      password_env: WAREHOUSE_PASSWORD
    sources:
      shop:
        type: postgres
        host: shop-stage.internal
        port: 5432
        database: shop
        user: shop
        password_env: SHOP_DB_PASSWORD
  prod:
""",
)

CUSTOMER_ID_DROPPED = BreakingChange("customer_id", BreakingChangeKind.DROPPED, "integer")
UPDATED_AT_RETYPED = BreakingChange(
    "updated_at", BreakingChangeKind.RETYPED, "timestamp", "timestamptz"
)
NOW = datetime(2026, 11, 22, 9, 30, 5, tzinfo=UTC)


def find(store, sync, environment="dev", settings=SETTINGS, environment_name=None):
    return find_schema_changes(
        PIPELINE, settings, environment, "orders", ENV, sync, store, environment_name
    )


def test_find_reports_the_changes_and_changes_nothing():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})

    found = find(store, sync)

    assert found.had_previous_snapshot
    assert found.changes == (CUSTOMER_ID_DROPPED,)
    assert (found.pipeline, found.environment, found.step.name, found.source, found.table) == (
        "shop",
        "dev",
        "orders",
        "shop",
        "orders",
    )
    assert [t.name for t in found.step.tables] == ["orders"]
    assert found.shape == TableShape(CHANGED_ORDERS)
    assert found.warehouse.password == WAREHOUSE_PASSWORD
    assert store.saves == []
    assert sync.nullable == []


def test_find_without_a_previous_snapshot_reports_no_changes():
    found = find(FakeSnapshotStore(), RecordingSync())

    assert not found.had_previous_snapshot
    assert found.changes == ()


def test_found_changes_are_frozen():
    found = find(FakeSnapshotStore(), RecordingSync())

    with pytest.raises(AttributeError):
        found.changes = (CUSTOMER_ID_DROPPED,)  # type: ignore[misc]
    assert isinstance(found, FoundSchemaChanges)


def test_apply_relaxes_and_saves_what_find_found():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})
    found = find(store, sync)

    accepted = apply_schema_acceptance(found, sync, store)

    assert accepted == AcceptedSchema(
        "shop",
        "dev",
        "orders",
        "orders",
        TableShape(CHANGED_ORDERS),
        ("customer_id",),
        (CUSTOMER_ID_DROPPED,),
    )
    assert sync.nullable == [("orders", "orders", ("customer_id",))]
    assert store.shapes[("shop", "shop", "orders")] == TableShape(CHANGED_ORDERS)


def test_a_branch_is_reported_under_its_name_with_stage_settings():
    store = FakeSnapshotStore({("shop", "shop", "orders"): TableShape(ORDERS_BEFORE)})
    sync = RecordingSync(source_facts={"orders": facts_with_types(CHANGED_ORDERS)})

    accepted = accept_schema(
        PIPELINE,
        STAGE_SETTINGS,
        "stage",
        "orders",
        ENV,
        sync,
        store,
        environment_name="br_feat",
    )

    assert accepted.environment == "br_feat"
    assert accepted.changes == (CUSTOMER_ID_DROPPED,)
    assert sync.connections[0].host == "shop-stage.internal"
    assert [w.host for w in store.warehouses] == ["warehouse-stage.internal"] * 2


def record(record_id, *changes, pipeline="shop", table="orders"):
    return AcceptedChangeRecord(record_id, pipeline, table, changes)


def test_no_changes_need_no_record():
    existing = [record("old", CUSTOMER_ID_DROPPED)]

    assert plan_accepted_record("shop", "orders", (), existing, NOW) == NothingToRecord()


def test_changes_a_record_already_accepts_need_no_new_record():
    existing = [
        record("a", CUSTOMER_ID_DROPPED),
        record("b", UPDATED_AT_RETYPED),
        record("c", BreakingChange("note", BreakingChangeKind.DROPPED, "text")),
    ]

    plan = plan_accepted_record(
        "shop", "orders", (CUSTOMER_ID_DROPPED, UPDATED_AT_RETYPED), existing, NOW
    )

    assert plan == AlreadyCovered(("a", "b"))


def test_a_new_record_lists_every_found_change_even_when_some_are_covered():
    found = (CUSTOMER_ID_DROPPED, UPDATED_AT_RETYPED)

    plan = plan_accepted_record("shop", "orders", found, [record("a", CUSTOMER_ID_DROPPED)], NOW)

    assert plan == NewRecord(record("20261122T093005Z-shop-orders", *found))


@pytest.mark.parametrize(
    "other",
    [
        record("other-table", CUSTOMER_ID_DROPPED, table="order_items"),
        record("other-pipeline", CUSTOMER_ID_DROPPED, pipeline="crm"),
    ],
    ids=["other-table", "other-pipeline"],
)
def test_a_record_for_another_table_does_not_cover(other):
    plan = plan_accepted_record("shop", "orders", (CUSTOMER_ID_DROPPED,), [other], NOW)

    assert plan == NewRecord(record("20261122T093005Z-shop-orders", CUSTOMER_ID_DROPPED))


def test_the_record_id_uses_utc_time():
    local = NOW.astimezone(timezone(timedelta(hours=2)))

    plan = plan_accepted_record("shop", "orders", (CUSTOMER_ID_DROPPED,), [], local)

    assert isinstance(plan, NewRecord)
    assert plan.record.id == "20261122T093005Z-shop-orders"


def test_a_table_name_that_is_not_plain_cannot_name_a_record():
    with pytest.raises(PipelineValidationError) as error:
        plan_accepted_record("shop", "../orders", (CUSTOMER_ID_DROPPED,), [], NOW)

    assert error.value.problems == (
        "../orders: an accepted-change record needs a plain table name (a-z, 0-9, _)",
    )
