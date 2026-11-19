from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from adops.application.ports import (
    ResolvedConnection,
    ResolvedWarehouse,
    ReverseSyncRead,
    SyncResult,
    TransformResult,
)
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep, SyncTable
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff, Row
from adops.domain.schema_snapshot import TableShape
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep

SOURCE_PASSWORD = "s3cr3t-source-sentinel"
WAREHOUSE_PASSWORD = "s3cr3t-warehouse-sentinel"

ENV = {"SHOP_DB_PASSWORD": SOURCE_PASSWORD, "WAREHOUSE_PASSWORD": WAREHOUSE_PASSWORD}

SETTINGS = """
environments:
  dev:
    warehouse:
      type: postgres
      host: localhost
      port: 5433
      database: warehouse
      user: adops
      password_env: WAREHOUSE_PASSWORD
    sources:
      shop:
        type: postgres
        host: localhost
        port: 5432
        database: shop
        user: shop
        password_env: SHOP_DB_PASSWORD
      crm:
        type: postgres
        host: localhost
        port: 5434
        database: crm
        user: crm
        password_env: CRM_DB_PASSWORD
  prod:
    warehouse:
      type: postgres
      host: warehouse.internal
      port: 5432
      database: warehouse
      user: adops
      password_env: PROD_WAREHOUSE_PASSWORD
    sources: {}
"""

DUCKDB_PATH = "warehouse.duckdb"

DUCKDB_SETTINGS = f"""
environments:
  dev:
    warehouse:
      type: duckdb
      path: {DUCKDB_PATH}
    sources:
      shop:
        type: postgres
        host: localhost
        port: 5432
        database: shop
        user: shop
        password_env: SHOP_DB_PASSWORD
"""


class StrictEnv(Mapping[str, str]):
    """An environment that fails the test the moment anything reads a variable it does not hold."""

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def __getitem__(self, name: str) -> str:
        if name not in self._values:
            raise AssertionError(f"unexpected lookup of environment variable {name}")
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


PIPELINE = """
name: shop
steps:
  - name: customers
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
  - name: orders
    source: shop
    tables:
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
      - name: order_items
        mode: full_refresh
        primary_key: id
        skip_checks:
          checks: [row_count_matches]
          reason: the Source table is appended to while it is copied
  - name: products
    source: shop
    tables:
      - name: products
        mode: full_refresh
        primary_key: sku
"""

TABLE_TYPES = {
    "customers": {"id": "integer", "email": "character varying"},
    "orders": {"id": "integer", "updated_at": "timestamp with time zone"},
    "order_items": {"id": "integer", "order_id": "integer"},
    "products": {"sku": "text"},
}

TABLE_COLUMNS = {table: frozenset(types) for table, types in TABLE_TYPES.items()}


def facts_with_types(column_types: Mapping[str, str]) -> SourceFacts:
    return SourceFacts(True, frozenset(column_types), column_types)


def healthy_load(rows: int = 3, **overrides: object) -> LoadFacts:
    values: dict[str, object] = {
        "warehouse_rows_before": 0,
        "warehouse_rows_after": rows,
        "duplicate_keys": 0,
        "null_keys": 0,
        "source_rows": rows,
    }
    values.update(overrides)
    return LoadFacts(**values)  # type: ignore[arg-type]


@dataclass
class RecordingSync:
    """A Sync whose every table exists and loads cleanly unless told otherwise."""

    source_facts: dict[str, SourceFacts] = field(default_factory=dict)
    load_facts: dict[str, LoadFacts] = field(default_factory=dict)
    omit_load_facts: set[str] = field(default_factory=set)
    fail_preflight_of: dict[str, Exception] = field(default_factory=dict)
    fail_sync_of: dict[str, Exception] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)
    connections: list[ResolvedWarehouse] = field(default_factory=list)
    nullable: list[tuple[str, str, tuple[str, ...]]] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.calls.append(("preflight", step.name))
        self.connections.append(source)
        if step.name in self.fail_preflight_of:
            raise self.fail_preflight_of[step.name]
        return {
            table.name: self.source_facts.get(table.name, facts_with_types(TABLE_TYPES[table.name]))
            for table in step.tables
        }

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedWarehouse
    ) -> SyncResult:
        self.calls.append(("sync", step.name))
        self.connections.append(warehouse)
        if step.name in self.fail_sync_of:
            raise self.fail_sync_of[step.name]
        facts = {
            table.name: self.load_facts.get(table.name, healthy_load())
            for table in step.tables
            if table.name not in self.omit_load_facts
        }
        return SyncResult(
            rows_loaded={name: fact.warehouse_rows_after for name, fact in facts.items()},
            load_facts=facts,
        )

    def make_columns_nullable(
        self,
        step: SyncStep,
        table: SyncTable,
        warehouse: ResolvedWarehouse,
        columns: Collection[str],
    ) -> None:
        self.connections.append(warehouse)
        self.nullable.append((step.name, table.name, tuple(columns)))


SnapshotKey = tuple[str, str, str]


@dataclass
class FakeSnapshotStore:
    """Holds TableShapes in memory, keyed by (pipeline, source, table)."""

    shapes: dict[SnapshotKey, TableShape] = field(default_factory=dict)
    fail_load: Exception | None = None
    fail_save: Exception | None = None
    saves: list[SnapshotKey] = field(default_factory=list)
    warehouses: list[ResolvedWarehouse] = field(default_factory=list)

    def load(
        self, warehouse: ResolvedWarehouse, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        self.warehouses.append(warehouse)
        if self.fail_load is not None:
            raise self.fail_load
        return self.shapes.get((pipeline, source, table))

    def save(
        self,
        warehouse: ResolvedWarehouse,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
    ) -> None:
        self.warehouses.append(warehouse)
        if self.fail_save is not None:
            raise self.fail_save
        self.saves.append((pipeline, source, table))
        self.shapes[(pipeline, source, table)] = shape


NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


@dataclass
class FixedClock:
    moment: datetime = NOW
    reads: int = 0

    def now(self) -> datetime:
        self.reads += 1
        return self.moment


def dbt_test(
    name: str, table: str = "customer_orders", status: str = "pass", failures: int | None = 0
) -> DbtTestFacts:
    return DbtTestFacts(f"test.demo.{name}", name, table, DbtTestStatus(status), failures)


@dataclass
class FakeTransform:
    """A Transform that reports a fixed result, or raises, and records what it was given."""

    result: TransformResult = field(
        default_factory=lambda: TransformResult(tests=(), crashed=False, failed_models=())
    )
    fail_with: Exception | None = None
    calls: list[tuple[str, ResolvedWarehouse]] = field(default_factory=list)

    def run(self, step: TransformStep, warehouse: ResolvedWarehouse) -> TransformResult:
        self.calls.append((step.name, warehouse))
        if self.fail_with is not None:
            raise self.fail_with
        return self.result


TARGET_PASSWORD = "s3cr3t-target-sentinel"
STAGE_WAREHOUSE_PASSWORD = "s3cr3t-stage-warehouse-sentinel"
REVERSE_ENV = {
    "CRM_TARGET_PASSWORD": TARGET_PASSWORD,
    "STAGE_WAREHOUSE_PASSWORD": STAGE_WAREHOUSE_PASSWORD,
}

CRM_TARGET = """
      crm:
        type: postgres
        host: crm.internal
        port: 5440
        database: crm
        user: crm_writer
        password_env: CRM_TARGET_PASSWORD
"""

REVERSE_SETTINGS = f"""
environments:
  dev:
    warehouse:
      type: duckdb
      path: dev.duckdb
    sources: {{}}
    targets:{CRM_TARGET}
  staging:
    warehouse:
      type: duckdb
      path: staging.duckdb
    sources: {{}}
    targets:{CRM_TARGET}
  stage:
    warehouse:
      type: postgres
      host: warehouse-stage.internal
      port: 5432
      database: warehouse_stage
      user: adops
      password_env: STAGE_WAREHOUSE_PASSWORD
    sources: {{}}
    targets:{CRM_TARGET}
"""

REVERSE_PIPELINE = """
name: crm_push
steps:
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping:
      order_count: orders
      total_amount: total
  - type: reverse_sync
    name: push_again
    model: customer_stats
    target: crm
    target_table: customer_stats_copy
    key: customer_id
    mapping:
      order_count: orders
"""

RESULT_ROWS: tuple[Row, ...] = (
    {"customer_id": 1, "order_count": 3, "total_amount": 30},
    {"customer_id": 2, "order_count": 1, "total_amount": 10},
    {"customer_id": 3, "order_count": 2, "total_amount": 25},
)

TARGET_ROWS: tuple[Row, ...] = (
    {"customer_id": 1, "orders": 3, "total": 30},
    {"customer_id": 2, "orders": 0, "total": 10},
    {"customer_id": 4, "orders": 1, "total": 5},
    {"customer_id": 5, "orders": 1, "total": 5},
)


@dataclass
class FakeReverseSync:
    """A Reverse Sync engine that reports fixed rows and facts and records every read and apply."""

    result_rows: tuple[Row, ...] = RESULT_ROWS
    target_rows: tuple[Row, ...] = TARGET_ROWS
    target_table_exists: bool = True
    missing_target_columns: tuple[str, ...] = ()
    duplicate_keys: int = 0
    null_keys: int = 0
    fail_read: Exception | None = None
    fail_apply: Exception | None = None
    reads: list[tuple[str, ResolvedWarehouse, ResolvedConnection]] = field(default_factory=list)
    applied: list[tuple[str, ResolvedConnection, Diff]] = field(default_factory=list)

    @property
    def applied_diff(self) -> Diff:
        (applied,) = self.applied
        return applied[2]

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead:
        self.reads.append((step.name, warehouse, target))
        if self.fail_read is not None:
            raise self.fail_read
        return ReverseSyncRead(
            result_rows=self.result_rows,
            target_rows=self.target_rows,
            target_table_exists=self.target_table_exists,
            missing_target_columns=self.missing_target_columns,
            duplicate_keys=self.duplicate_keys,
            null_keys=self.null_keys,
        )

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        self.applied.append((step.name, target, diff))
        if self.fail_apply is not None:
            raise self.fail_apply
