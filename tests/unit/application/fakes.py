from collections.abc import Mapping
from dataclasses import dataclass, field

from adops.application.ports import ResolvedConnection, SyncResult
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep

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

TABLE_COLUMNS = {
    "customers": frozenset({"id", "email"}),
    "orders": frozenset({"id", "updated_at"}),
    "order_items": frozenset({"id", "order_id"}),
    "products": frozenset({"sku"}),
}


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

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.calls.append(("preflight", step.name))
        self.connections.append(source)
        if step.name in self.fail_preflight_of:
            raise self.fail_preflight_of[step.name]
        return {
            table.name: self.source_facts.get(
                table.name, SourceFacts(table_exists=True, columns=TABLE_COLUMNS[table.name])
            )
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
