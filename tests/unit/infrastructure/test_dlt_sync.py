import logging
import sqlite3
from pathlib import Path

import duckdb
import pytest

from adops.application.ports import ResolvedConnection, Sync
from adops.application.run_pipeline import SyncFailedError
from adops.domain.pipeline import SyncStep
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
