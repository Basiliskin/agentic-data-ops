import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
import sqlalchemy as sa

from adops.application.ports import ResolvedConnection, Sync
from adops.application.run_pipeline import SyncFailedError
from adops.domain.pipeline import SyncStep
from adops.domain.schema_snapshot import (
    TableShape,
    find_breaking_changes,
    normalize_column_type,
)
from adops.infrastructure.dlt_sync import (
    DltSync,
    UnknownConnectionKindError,
    source_engine,
    warehouse_destination,
)

SENTINEL_PASSWORD = "s3cr3t-dlt-sentinel"
SENTINEL_CURSOR = 918273645


def file_connection(path: Path) -> ResolvedConnection:
    return ResolvedConnection(host="", port=0, database=str(path), user="", password="")


def unreachable_postgres(password: str = SENTINEL_PASSWORD) -> ResolvedConnection:
    return ResolvedConnection(
        host="127.0.0.1", port=1, database="shop", user="shop", password=password
    )


def step(*tables: dict[str, object], name: str = "orders") -> SyncStep:
    return SyncStep.model_validate({"name": name, "source": "shop", "tables": list(tables)})


FULL_REFRESH_CUSTOMERS = {"name": "customers", "mode": "full_refresh", "primary_key": "id"}
INCREMENTAL_ORDERS = {
    "name": "orders",
    "mode": "incremental",
    "primary_key": "id",
    "cursor": "updated_at",
}


class Shop:
    """A SQLite Source and a DuckDB Warehouse in a temporary Project directory."""

    def __init__(self, project: Path) -> None:
        self.project = project
        self.source_path = project / "shop.db"
        self.warehouse_path = project / "warehouse.duckdb"
        self.source = file_connection(self.source_path)
        self.warehouse = file_connection(self.warehouse_path)
        self.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT)",
            "INSERT INTO customers VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x')",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, updated_at INTEGER, amount INTEGER)",
            "INSERT INTO orders VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)",
            "CREATE TABLE empty_orders (id INTEGER PRIMARY KEY, updated_at INTEGER)",
            "CREATE TABLE no_cursor (id INTEGER PRIMARY KEY)",
            "CREATE TABLE internal_notes (id INTEGER PRIMARY KEY, note TEXT)",
            "INSERT INTO internal_notes VALUES (1, 'not for the Warehouse')",
        )

    def execute(self, *statements: str) -> None:
        with sqlite3.connect(self.source_path) as connection:
            for statement in statements:
                connection.execute(statement)

    def source_rows(self, sql: str) -> list[tuple]:
        with sqlite3.connect(self.source_path) as connection:
            return connection.execute(sql).fetchall()

    def warehouse_rows(self, sql: str) -> list[tuple]:
        with duckdb.connect(str(self.warehouse_path)) as connection:
            return connection.execute(sql).fetchall()

    def warehouse_execute(self, *statements: str) -> None:
        with duckdb.connect(str(self.warehouse_path)) as connection:
            for statement in statements:
                connection.execute(statement)

    def adapter(self) -> DltSync:
        return DltSync(self.project, "shop", source_kind="sqlite", warehouse_kind="duckdb")

    def sync(self, sync_step: SyncStep):
        return self.adapter().sync(sync_step, self.source, self.warehouse)


@pytest.fixture
def shop(tmp_path: Path) -> Shop:
    return Shop(tmp_path)


def test_adapter_implements_the_sync_port(shop):
    assert isinstance(shop.adapter(), Sync)


def test_full_refresh_replaces_the_table_including_deletions_and_updates(shop):
    customers = step(FULL_REFRESH_CUSTOMERS, name="customers")
    shop.sync(customers)
    shop.execute(
        "DELETE FROM customers WHERE id = 1", "UPDATE customers SET email = 'new@x' WHERE id = 2"
    )

    result = shop.sync(customers)

    assert sorted(shop.warehouse_rows("SELECT id, email FROM raw_shop.customers")) == sorted(
        shop.source_rows("SELECT id, email FROM customers")
    )
    assert result.rows_loaded == {"customers": 2}
    assert result.load_facts["customers"].source_rows == 2
    assert result.load_facts["customers"].warehouse_rows_before == 3
    assert result.load_facts["customers"].warehouse_rows_after == 2


def test_full_refresh_source_row_count_is_measured_on_the_source(shop):
    result = shop.sync(step(FULL_REFRESH_CUSTOMERS, name="customers"))

    [(count,)] = shop.source_rows("SELECT count(*) FROM customers")
    assert result.load_facts["customers"].source_rows == count == 3
    assert result.load_facts["customers"].cursor_max_after is None


def test_only_allowlisted_tables_land_in_the_raw_source_dataset(shop):
    shop.sync(step(FULL_REFRESH_CUSTOMERS, name="customers"))

    tables = shop.warehouse_rows(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_name NOT LIKE '\\_dlt%' ESCAPE '\\'"
    )
    assert tables == [("raw_shop", "customers")]


def test_second_incremental_sync_loads_only_advanced_rows_with_a_fresh_adapter(shop):
    orders = step(INCREMENTAL_ORDERS)
    first = shop.sync(orders)
    shop.execute(
        "INSERT INTO orders VALUES (4, 4, 40)",
        "UPDATE orders SET updated_at = 5, amount = 99 WHERE id = 2",
    )

    second = shop.adapter().sync(orders, shop.source, shop.warehouse)

    assert first.rows_loaded == {"orders": 3}
    assert second.rows_loaded == {"orders": 2}
    assert shop.warehouse_rows("SELECT count(*) FROM raw_shop.orders") == [(4,)]
    assert (
        shop.warehouse_rows("SELECT id FROM raw_shop.orders GROUP BY id HAVING count(*) > 1") == []
    )
    assert shop.warehouse_rows("SELECT amount FROM raw_shop.orders WHERE id = 2") == [(99,)]
    assert (shop.project / ".adops" / "dlt" / "adops_shop_orders").is_dir()


def test_incremental_facts_carry_before_and_after_counts_and_cursor(shop):
    orders = step(INCREMENTAL_ORDERS)
    first = shop.sync(orders).load_facts["orders"]
    shop.execute("INSERT INTO orders VALUES (4, 4, 40)")

    second = shop.sync(orders).load_facts["orders"]

    assert (first.warehouse_rows_before, first.warehouse_rows_after) == (0, 3)
    assert (first.cursor_max_before, first.cursor_max_after) == (None, 3)
    assert second.warehouse_rows_before == first.warehouse_rows_after
    assert second.cursor_max_before == first.cursor_max_after
    assert (second.warehouse_rows_after, second.cursor_max_after) == (4, 4)
    assert second.source_rows is None


def test_empty_source_table_reports_zero_rows_and_no_cursor(shop):
    empty = step({**INCREMENTAL_ORDERS, "name": "empty_orders"})

    result = shop.sync(empty)

    facts = result.load_facts["empty_orders"]
    assert result.rows_loaded == {"empty_orders": 0}
    assert (facts.warehouse_rows_before, facts.cursor_max_before) == (0, None)
    assert (facts.warehouse_rows_after, facts.cursor_max_after) == (0, None)


def test_clean_load_reports_no_duplicate_or_null_keys(shop):
    facts = shop.sync(step(INCREMENTAL_ORDERS)).load_facts["orders"]

    assert (facts.duplicate_keys, facts.null_keys) == (0, 0)


def test_duplicated_key_in_the_warehouse_is_counted(shop):
    orders = step(INCREMENTAL_ORDERS)
    shop.sync(orders)
    shop.warehouse_execute("INSERT INTO raw_shop.orders SELECT * FROM raw_shop.orders WHERE id = 1")

    facts = shop.sync(orders).load_facts["orders"]

    assert (facts.duplicate_keys, facts.null_keys) == (1, 0)


def test_null_key_in_the_warehouse_is_counted_and_not_as_a_duplicate(shop):
    orders = step(INCREMENTAL_ORDERS)
    shop.sync(orders)
    shop.warehouse_execute(
        "ALTER TABLE raw_shop.orders ALTER COLUMN id DROP NOT NULL",
        "INSERT INTO raw_shop.orders SELECT * REPLACE (NULL AS id) FROM raw_shop.orders "
        "WHERE id = 1",
    )

    facts = shop.sync(orders).load_facts["orders"]

    assert (facts.duplicate_keys, facts.null_keys) == (0, 1)


ROW_VALUE = "row-value-sentinel"


def events_table(*checks: dict[str, object]) -> dict[str, object]:
    return {
        "name": "events",
        "mode": "incremental",
        "primary_key": "id",
        "cursor": "id",
        "checks": list(checks),
    }


def with_events(shop: Shop) -> Shop:
    shop.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, priority INTEGER, "
        "happened_at TIMESTAMP, day DATE, note TEXT)",
        "INSERT INTO events VALUES "
        "(1, 'open', 1, '2026-01-01 00:00:00', '2026-01-01', 'a'), "
        "(2, NULL, 2, '2026-01-03 06:00:00', '2026-01-05', NULL), "
        "(3, 'lost', 3, NULL, NULL, 'b'), "
        f"(4, 'it''s', NULL, '2026-01-02 00:00:00', '2026-01-02', '{ROW_VALUE}')",
    )
    return shop


def test_declared_check_facts_are_measured_in_the_warehouse(shop):
    events = events_table(
        {"type": "not_null", "column": "status"},
        {"type": "not_null", "column": "note"},
        {"type": "not_null", "column": "id"},
        {"type": "accepted_values", "column": "status", "values": ["open", "it's"]},
        {"type": "accepted_values", "column": "priority", "values": [1, 2]},
        {"type": "freshness", "column": "happened_at", "max_age_hours": 24},
        {"type": "freshness", "column": "day", "max_age_hours": 24},
        {"type": "min_rows", "count": 1},
    )

    facts = with_events(shop).sync(step(events)).load_facts["events"]

    assert facts.null_counts == {"status": 1, "note": 1, "id": 0}
    assert facts.unexpected_value_counts == {"status": 1, "priority": 1}
    assert facts.newest_values == {
        "happened_at": datetime(2026, 1, 3, 6),
        "day": datetime(2026, 1, 5),
    }


def test_accepted_values_does_not_count_null_as_outside_the_list(shop):
    events = events_table({"type": "accepted_values", "column": "status", "values": ["open"]})

    facts = with_events(shop).sync(step(events)).load_facts["events"]

    # 'lost' and "it's" are outside the list; the NULL status is left to not_null.
    assert facts.unexpected_value_counts == {"status": 2}


def test_freshness_of_a_timestamptz_column_is_time_zone_aware(shop):
    events = step(events_table({"type": "freshness", "column": "happened_at", "max_age_hours": 1}))
    with_events(shop).sync(events)
    shop.warehouse_execute(
        "SET TimeZone = 'UTC'",
        "ALTER TABLE raw_shop.events ALTER happened_at TYPE TIMESTAMPTZ",
    )

    newest = shop.sync(events).load_facts["events"].newest_values["happened_at"]

    assert newest.tzinfo is not None
    assert newest == datetime(2026, 1, 3, 6, tzinfo=UTC)


def test_a_table_without_declared_checks_leaves_the_declared_facts_empty(shop):
    facts = with_events(shop).sync(step(events_table())).load_facts["events"]

    assert (facts.null_counts, facts.unexpected_value_counts, facts.newest_values) == ({}, {}, {})


def test_a_table_never_loaded_reports_no_nulls_and_no_newest_value(shop):
    empty = {
        **INCREMENTAL_ORDERS,
        "name": "empty_orders",
        "checks": [
            {"type": "not_null", "column": "updated_at"},
            {"type": "accepted_values", "column": "updated_at", "values": [1]},
            {"type": "freshness", "column": "updated_at", "max_age_hours": 1},
        ],
    }

    facts = shop.sync(step(empty)).load_facts["empty_orders"]

    assert facts.null_counts == {"updated_at": 0}
    assert facts.unexpected_value_counts == {"updated_at": 0}
    assert facts.newest_values == {"updated_at": None}


def test_freshness_of_a_text_column_fails_without_showing_a_value(shop):
    events = events_table({"type": "freshness", "column": "note", "max_age_hours": 1})

    with pytest.raises(SyncFailedError) as raised:
        with_events(shop).sync(step(events))

    assert str(raised.value) == (
        "Step orders, table events: UnsupportedFreshnessColumnError: "
        "freshness column note holds str values, not timestamps"
    )
    assert ROW_VALUE not in str(raised.value)


def test_preflight_reports_missing_tables_and_columns_without_loading(shop):
    allowlist = step(
        INCREMENTAL_ORDERS,
        {**INCREMENTAL_ORDERS, "name": "no_cursor"},
        {**INCREMENTAL_ORDERS, "name": "refunds"},
    )

    facts = shop.adapter().preflight(allowlist, shop.source)

    assert facts["orders"].table_exists
    assert facts["orders"].columns == {"id", "updated_at", "amount"}
    assert facts["no_cursor"].table_exists
    assert "updated_at" not in facts["no_cursor"].columns
    assert not facts["refunds"].table_exists
    assert facts["refunds"].columns == frozenset()
    assert not shop.warehouse_path.exists()
    assert not (shop.project / ".adops").exists()


def test_preflight_reports_normalized_column_types_compiled_by_the_dialect(shop):
    shop.execute(
        "CREATE TABLE typed (id INTEGER PRIMARY KEY, name VARCHAR(40), price NUMERIC(10,2), "
        "seen_at TIMESTAMP, _dlt_id TEXT)"
    )

    facts = shop.adapter().preflight(step({**FULL_REFRESH_CUSTOMERS, "name": "typed"}), shop.source)

    types = facts["typed"].column_types
    assert types == {
        "id": "integer",
        "name": "varchar(40)",
        "price": "numeric(10, 2)",
        "seen_at": "timestamp",
    }
    assert all(normalize_column_type(kind) == kind for kind in types.values())
    assert set(types) == facts["typed"].columns
    assert "_dlt_id" not in facts["typed"].columns


def test_postgres_timestamptz_keeps_its_time_zone_when_compiled():
    engine = source_engine("postgres", unreachable_postgres())
    compiled = sa.TIMESTAMP(timezone=True).compile(dialect=engine.dialect)

    assert normalize_column_type(compiled) == "timestamptz"


def test_preflight_twice_on_an_unchanged_source_gives_equal_shapes(shop):
    allowlist = step(FULL_REFRESH_CUSTOMERS, INCREMENTAL_ORDERS)

    first = shop.adapter().preflight(allowlist, shop.source)
    second = shop.adapter().preflight(allowlist, shop.source)

    for table in ("customers", "orders"):
        assert first[table].column_types
        assert TableShape(first[table].column_types) == TableShape(second[table].column_types)
    assert (
        find_breaking_changes(
            TableShape(first["orders"].column_types), TableShape(second["orders"].column_types)
        )
        == ()
    )


def test_sync_of_a_missing_table_fails_naming_step_and_table(shop):
    with pytest.raises(SyncFailedError) as raised:
        shop.sync(step({**FULL_REFRESH_CUSTOMERS, "name": "refunds"}, name="money"))

    assert str(raised.value).startswith("Step money, table refunds: ")


def assert_no_secret(error: SyncFailedError) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None
    for text in (str(error), repr(error)):
        assert SENTINEL_PASSWORD not in text
        assert "postgresql://" not in text


def assert_project_holds_no_secret(project: Path) -> None:
    files = [path for path in project.rglob("*") if path.is_file()]
    assert not [path for path in files if path.name in {"secrets.toml", "config.toml"}]
    assert not [path for path in files if SENTINEL_PASSWORD.encode() in path.read_bytes()]


def test_preflight_connection_failure_hides_the_password(shop):
    adapter = DltSync(shop.project, "shop", warehouse_kind="duckdb")

    with pytest.raises(SyncFailedError) as raised:
        adapter.preflight(step(INCREMENTAL_ORDERS), unreachable_postgres())

    assert str(raised.value).startswith("Step orders, table orders: OperationalError")
    assert_no_secret(raised.value)


def test_sync_source_failure_hides_the_password_everywhere(shop):
    adapter = DltSync(shop.project, "shop", warehouse_kind="duckdb")

    with pytest.raises(SyncFailedError) as raised:
        adapter.sync(step(INCREMENTAL_ORDERS), unreachable_postgres(), shop.warehouse)

    assert str(raised.value).startswith("Step orders, table orders: ")
    assert_no_secret(raised.value)
    assert_project_holds_no_secret(shop.project)


def test_sync_full_refresh_source_count_failure_hides_the_password(shop):
    adapter = DltSync(shop.project, "shop", warehouse_kind="duckdb")

    with pytest.raises(SyncFailedError) as raised:
        adapter.sync(step(FULL_REFRESH_CUSTOMERS), unreachable_postgres(), shop.warehouse)

    assert str(raised.value).startswith("Step orders, table customers: OperationalError")
    assert_no_secret(raised.value)


def test_sync_warehouse_failure_hides_the_password_everywhere(shop):
    adapter = DltSync(shop.project, "shop", source_kind="sqlite")

    with pytest.raises(SyncFailedError) as raised:
        adapter.sync(step(INCREMENTAL_ORDERS), shop.source, unreachable_postgres())

    assert str(raised.value).startswith("Step orders, table orders: ")
    assert_no_secret(raised.value)
    assert_project_holds_no_secret(shop.project)


def test_connection_urls_in_error_text_are_masked(shop, monkeypatch):
    def leak(*_):
        raise RuntimeError("could not reach postgresql://shop:hunter2@db:5432/shop")

    monkeypatch.setattr("adops.infrastructure.dlt_sync._source_facts", leak)

    with pytest.raises(SyncFailedError) as raised:
        shop.adapter().preflight(step(INCREMENTAL_ORDERS), shop.source)

    assert str(raised.value) == "Step orders, table orders: RuntimeError: could not reach ***"


def test_cursor_values_are_never_logged(shop, caplog):
    shop.execute(f"INSERT INTO orders VALUES (9, {SENTINEL_CURSOR}, 90)")
    caplog.set_level(logging.DEBUG)

    result = shop.sync(step(INCREMENTAL_ORDERS))

    assert result.load_facts["orders"].cursor_max_after == SENTINEL_CURSOR
    assert not [r for r in caplog.records if str(SENTINEL_CURSOR) in r.getMessage()]


@pytest.mark.parametrize(
    ("kind", "destination_type"),
    [("postgres", "dlt.destinations.postgres"), ("duckdb", "dlt.destinations.duckdb")],
)
def test_warehouse_kind_maps_to_a_dlt_destination(kind, destination_type):
    destination = warehouse_destination(kind, unreachable_postgres())

    assert destination.destination_type == destination_type


def test_unknown_warehouse_kind_is_rejected_clearly():
    with pytest.raises(UnknownConnectionKindError, match="unknown Warehouse kind 'mysql'"):
        warehouse_destination("mysql", unreachable_postgres())


def test_postgres_source_engine_keeps_the_password_out_of_its_url_text():
    engine = source_engine("postgres", unreachable_postgres())

    assert engine.url.drivername == "postgresql+psycopg"
    assert engine.url.password == SENTINEL_PASSWORD
    assert SENTINEL_PASSWORD not in repr(engine.url)
    assert SENTINEL_PASSWORD not in str(engine.url)


def test_unknown_source_kind_is_rejected_clearly():
    with pytest.raises(UnknownConnectionKindError, match=r"unknown Source kind 'oracle' \(known"):
        source_engine("oracle", unreachable_postgres())


def not_null_columns(shop: Shop, table: str, schema: str = "raw_shop") -> set[str]:
    rows = shop.warehouse_rows(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' AND is_nullable = 'NO'"
    )
    return {name for (name,) in rows}


def test_make_columns_nullable_drops_not_null_only_on_named_existing_columns(shop):
    orders = step(INCREMENTAL_ORDERS)
    shop.sync(orders)
    shop.warehouse_execute(
        *(
            f"ALTER TABLE {schema}.orders ALTER COLUMN {column} SET NOT NULL"
            for schema in ("raw_shop", "raw_shop_staging")
            for column in ("amount", "updated_at")
        )
    )
    before = not_null_columns(shop, "orders")
    staging_before = not_null_columns(shop, "orders", "raw_shop_staging")

    shop.adapter().make_columns_nullable(
        orders, orders.tables[0], shop.warehouse, ["amount", "never_loaded"]
    )

    assert {"amount", "updated_at"} <= before
    assert not_null_columns(shop, "orders") == before - {"amount"}
    assert not_null_columns(shop, "orders", "raw_shop_staging") == staging_before - {"amount"}


def test_make_columns_nullable_before_any_load_does_nothing(shop):
    orders = step(INCREMENTAL_ORDERS)

    shop.adapter().make_columns_nullable(orders, orders.tables[0], shop.warehouse, ["amount"])

    assert not shop.warehouse_path.exists() or not_null_columns(shop, "orders") == set()


def test_make_columns_nullable_failure_is_redacted(shop):
    orders = step(INCREMENTAL_ORDERS)
    adapter = DltSync(shop.project, "shop", source_kind="sqlite", warehouse_kind="postgres")

    with pytest.raises(SyncFailedError) as error:
        adapter.make_columns_nullable(
            orders, orders.tables[0], unreachable_postgres(), ["amount"]
        )

    assert str(error.value).startswith("Step orders, table orders: ")
    assert SENTINEL_PASSWORD not in str(error.value)
