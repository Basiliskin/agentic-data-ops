from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from adops.application.ports import ResolvedConnection, SyncResult
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep, SyncTable
from adops.domain.schema_snapshot import TableShape

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
    connections: list[ResolvedConnection] = field(default_factory=list)
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
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
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
        warehouse: ResolvedConnection,
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
    warehouses: list[ResolvedConnection] = field(default_factory=list)

    def load(
        self, warehouse: ResolvedConnection, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        self.warehouses.append(warehouse)
        if self.fail_load is not None:
            raise self.fail_load
        return self.shapes.get((pipeline, source, table))

    def save(
        self,
        warehouse: ResolvedConnection,
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
