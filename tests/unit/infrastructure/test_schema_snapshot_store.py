from pathlib import Path

import duckdb
import pytest

from adops.application.ports import ResolvedConnection, SchemaSnapshotStore
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore

ORDERS = TableShape({"id": "integer", "amount": "numeric(10, 2)"})
ORDERS_RETYPED = TableShape({"id": "bigint", "amount": "numeric(12, 2)", "note": "text"})


class Warehouse:
    def __init__(self, project: Path) -> None:
        self.project = project
        self.path = project / "warehouse.duckdb"
        self.connection = ResolvedConnection(
            host="", port=0, database=str(self.path), user="", password=""
        )

    def store(self) -> WarehouseSnapshotStore:
        return WarehouseSnapshotStore(self.project, "duckdb")

    def rows(self, sql: str, *parameters: object) -> list[tuple]:
        with duckdb.connect(str(self.path)) as connection:
            return connection.execute(sql, list(parameters)).fetchall()


@pytest.fixture
def warehouse(tmp_path: Path) -> Warehouse:
    return Warehouse(tmp_path)


def test_store_implements_the_snapshot_store_port(warehouse):
    assert isinstance(warehouse.store(), SchemaSnapshotStore)


def test_first_use_creates_the_table_in_the_adops_schema_and_finds_nothing(warehouse):
    assert warehouse.store().load(warehouse.connection, "shop", "shop", "orders") is None

    assert warehouse.rows(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name = '_adops_schema_snapshots'"
    ) == [("adops",)]
    assert warehouse.rows("SELECT count(*) FROM adops._adops_schema_snapshots") == [(0,)]


def test_save_then_load_round_trips_the_shape(warehouse):
    store = warehouse.store()

    store.save(warehouse.connection, "shop", "shop", "orders", ORDERS)

    assert store.load(warehouse.connection, "shop", "shop", "orders") == ORDERS


def test_saving_again_replaces_the_row(warehouse):
    store = warehouse.store()

    store.save(warehouse.connection, "shop", "shop", "orders", ORDERS)
    store.save(warehouse.connection, "shop", "shop", "orders", ORDERS_RETYPED)

    assert warehouse.rows(
        "SELECT count(*) FROM adops._adops_schema_snapshots "
        "WHERE pipeline = ? AND source = ? AND table_name = ?",
        "shop",
        "shop",
        "orders",
    ) == [(1,)]
    assert store.load(warehouse.connection, "shop", "shop", "orders") == ORDERS_RETYPED


def test_a_second_store_instance_reuses_the_existing_table(warehouse):
    warehouse.store().save(warehouse.connection, "shop", "shop", "orders", ORDERS)

    later_run = warehouse.store()
    later_run.save(warehouse.connection, "shop", "shop", "customers", ORDERS_RETYPED)

    assert later_run.load(warehouse.connection, "shop", "shop", "orders") == ORDERS
    assert later_run.load(warehouse.connection, "shop", "shop", "customers") == ORDERS_RETYPED


def test_keys_differing_in_one_part_are_kept_apart(warehouse):
    store = warehouse.store()
    shapes = {
        ("shop", "shop", "orders"): ORDERS,
        ("other", "shop", "orders"): TableShape({"id": "text"}),
        ("shop", "crm", "orders"): TableShape({"id": "uuid"}),
        ("shop", "shop", "refunds"): TableShape({"id": "smallint"}),
    }

    for key, shape in shapes.items():
        store.save(warehouse.connection, *key, shape)

    assert {key: store.load(warehouse.connection, *key) for key in shapes} == shapes


def test_quotes_in_keys_are_stored_as_values(warehouse):
    store = warehouse.store()

    store.save(warehouse.connection, "shop", "shop", "o'rders", ORDERS)

    assert store.load(warehouse.connection, "shop", "shop", "o'rders") == ORDERS
    assert store.load(warehouse.connection, "shop", "shop", "orders") is None


def test_stored_row_holds_only_names_and_types_and_no_credential(tmp_path):
    secret = "s3cr3t-snapshot-sentinel"
    path = tmp_path / "warehouse.duckdb"
    connection = ResolvedConnection(host="", port=0, database=str(path), user="", password=secret)
    store = WarehouseSnapshotStore(tmp_path, "duckdb")

    store.save(connection, "shop", "shop", "orders", ORDERS)

    with duckdb.connect(str(path)) as db:
        (row,) = db.execute("SELECT * FROM adops._adops_schema_snapshots").fetchall()
    assert row[:4] == (
        "shop",
        "shop",
        "orders",
        '{"amount": "numeric(10, 2)", "id": "integer"}',
    )
    assert all(secret not in str(value) and "://" not in str(value) for value in row)
    assert vars(store) == {
        "_pipelines_dir": tmp_path / ".adops" / "dlt",
        "_warehouse_kind": "duckdb",
    }
