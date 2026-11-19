from pathlib import Path

import duckdb
import pytest
from dlt.destinations.exceptions import DatabaseException

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    SchemaSnapshotStore,
    WarehouseUnreachable,
)
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore

ORDERS = TableShape({"id": "integer", "amount": "numeric(10, 2)"})
ORDERS_RETYPED = TableShape({"id": "bigint", "amount": "numeric(12, 2)", "note": "text"})


class Warehouse:
    def __init__(self, project: Path) -> None:
        self.project = project
        self.path = project / "warehouse.duckdb"
        self.connection = ResolvedDuckDb(str(self.path))

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


def test_stored_row_holds_only_names_and_types_and_no_connection_details(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    connection = ResolvedDuckDb(str(path))
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
    assert all(str(path) not in str(value) and "://" not in str(value) for value in row)
    assert vars(store) == {
        "_project": tmp_path,
        "_pipelines_dir": tmp_path / ".adops" / "dlt",
        "_warehouse_kind": "duckdb",
    }


UNREACHABLE = ResolvedConnection(
    host="127.0.0.1", port=1, database="warehouse", user="adops", password="s3cr3t-store-sentinel"
)


def assert_holds_no_driver_text(error: Exception) -> None:
    assert error.__cause__ is None and error.__context__ is None
    for text in (str(error), repr(error)):
        for leak in ("s3cr3t-store-sentinel", "127.0.0.1", "port 1", "OperationalError"):
            assert leak not in text


def test_load_from_an_unreachable_warehouse_raises_warehouse_unreachable(tmp_path):
    store = WarehouseSnapshotStore(tmp_path, "postgres")

    with pytest.raises(WarehouseUnreachable) as raised:
        store.load(UNREACHABLE, "shop", "shop", "orders")

    assert_holds_no_driver_text(raised.value)


def test_save_to_an_unreachable_warehouse_raises_warehouse_unreachable(tmp_path):
    store = WarehouseSnapshotStore(tmp_path, "postgres")

    with pytest.raises(WarehouseUnreachable) as raised:
        store.save(UNREACHABLE, "shop", "shop", "orders", ORDERS)

    assert_holds_no_driver_text(raised.value)


def other_cwd(tmp_path: Path, monkeypatch) -> Path:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


def test_a_relative_path_is_saved_and_loaded_in_the_project_from_any_cwd(
    warehouse, tmp_path, monkeypatch
):
    relative = ResolvedDuckDb("wh.duckdb")
    save_cwd = other_cwd(tmp_path, monkeypatch)
    warehouse.store().save(relative, "shop", "shop", "orders", ORDERS)
    monkeypatch.chdir(warehouse.project)

    assert warehouse.store().load(relative, "shop", "shop", "orders") == ORDERS
    assert (warehouse.project / "wh.duckdb").is_file()
    assert not (save_cwd / "wh.duckdb").exists()


def test_an_absolute_path_is_used_exactly(warehouse, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    warehouse.store().save(
        ResolvedDuckDb(str(elsewhere / "wh.duckdb")), "shop", "shop", "orders", ORDERS
    )

    assert (elsewhere / "wh.duckdb").is_file()
    assert not (warehouse.project / "wh.duckdb").exists()


def assert_writable(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        db.execute("CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER)")


def test_save_and_load_release_the_duckdb_file(warehouse):
    warehouse.store().save(warehouse.connection, "shop", "shop", "orders", ORDERS)
    assert_writable(warehouse.path)

    warehouse.store().load(warehouse.connection, "shop", "shop", "orders")
    assert_writable(warehouse.path)


def test_a_save_failing_inside_its_transaction_releases_the_duckdb_file(warehouse, monkeypatch):
    warehouse.store().save(warehouse.connection, "shop", "shop", "orders", ORDERS)
    monkeypatch.setattr(
        "adops.infrastructure.schema_snapshot_store.json.dumps", lambda *_, **__: object()
    )

    with pytest.raises(DatabaseException):
        warehouse.store().save(warehouse.connection, "shop", "shop", "orders", ORDERS)

    assert_writable(warehouse.path)


def test_a_duckdb_file_in_a_missing_folder_is_unreachable_and_not_created(warehouse):
    with pytest.raises(WarehouseUnreachable):
        warehouse.store().save(
            ResolvedDuckDb("missing/dir/wh.duckdb"), "shop", "shop", "orders", ORDERS
        )

    assert not (warehouse.project / "missing").exists()


@pytest.mark.parametrize("call", ["save", "load"])
def test_a_duckdb_file_locked_by_another_process_is_unreachable(
    warehouse, duckdb_held_by_another_process, call
):
    store = warehouse.store()
    store.save(warehouse.connection, "shop", "shop", "orders", ORDERS)

    with (
        duckdb_held_by_another_process(warehouse.path),
        pytest.raises(WarehouseUnreachable),
    ):
        if call == "save":
            store.save(warehouse.connection, "shop", "shop", "orders", ORDERS)
        else:
            store.load(warehouse.connection, "shop", "shop", "orders")
