from pathlib import Path

import duckdb
import pytest

from adops.application.ports import (
    DeployStateStore,
    ResolvedConnection,
    ResolvedDuckDb,
    WarehouseUnreachable,
)
from adops.domain.deploy_rerun import DeployState
from adops.infrastructure.deploy_state_store import WarehouseDeployStateStore

SENTINEL_PASSWORD = "s3cr3t-deploy-state-sentinel"
TABLE_EXISTS = (
    "SELECT count(*) FROM information_schema.tables "
    "WHERE table_schema = 'adops' AND table_name = '_adops_deploy_state'"
)
ADOPS_SCHEMA_EXISTS = "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'adops'"
EMPTY = DeployState(settings_marker=None, seen_record_ids=frozenset())


class DuckDbWarehouse:
    def __init__(self, project: Path, name: str = "warehouse.duckdb") -> None:
        self.project = project
        self.path = project / name
        self.connection = ResolvedDuckDb(str(self.path))

    def rows(self, query: str, *parameters: object) -> list[tuple]:
        with duckdb.connect(str(self.path)) as connection:
            return connection.execute(query.replace("%s", "?"), list(parameters)).fetchall()

    def execute(self, *statements: str) -> None:
        with duckdb.connect(str(self.path)) as connection:
            for statement in statements:
                connection.execute(statement)


class Warehouses:
    def __init__(self, kind: str, project: Path, warehouse) -> None:
        self.kind = kind
        self.project = project
        self.warehouse = warehouse

    def store(self, environment: str = "stage") -> WarehouseDeployStateStore:
        return WarehouseDeployStateStore(
            self.project, self.kind, self.warehouse.connection, environment=environment
        )

    def rows(self, query: str, *parameters: object) -> list[tuple]:
        return self.warehouse.rows(query, *parameters)


@pytest.fixture(params=["duckdb", "postgres"])
def warehouses(request, tmp_path: Path) -> Warehouses:
    if request.param == "duckdb":
        return Warehouses("duckdb", tmp_path, DuckDbWarehouse(tmp_path))
    return Warehouses("postgres", tmp_path, request.getfixturevalue("postgres_warehouse"))


def test_store_implements_the_deploy_state_store_port(tmp_path):
    store = WarehouseDeployStateStore(
        tmp_path, "duckdb", ResolvedDuckDb("wh.duckdb"), environment="stage"
    )

    assert isinstance(store, DeployStateStore)


def test_load_on_an_empty_warehouse_is_the_empty_state_and_creates_nothing(warehouses):
    assert warehouses.store().load() == EMPTY

    assert warehouses.rows(TABLE_EXISTS) == [(0,)]
    assert warehouses.rows(ADOPS_SCHEMA_EXISTS) == [(0,)]


def test_load_with_the_schema_but_no_table_is_the_empty_state(warehouses):
    warehouses.warehouse.execute("CREATE SCHEMA adops")

    assert warehouses.store().load() == EMPTY

    assert warehouses.rows(TABLE_EXISTS) == [(0,)]


def test_load_from_an_existing_empty_table_is_the_empty_state(warehouses):
    store = warehouses.store()
    store.save_seen_records("shop", [])

    assert warehouses.rows(TABLE_EXISTS) == [(1,)]
    assert store.load() == EMPTY


def test_a_saved_marker_loads_back(warehouses):
    warehouses.store().save_marker("marker-1")

    assert warehouses.store().load() == DeployState(settings_marker="marker-1")


def test_saving_a_marker_again_overwrites_its_one_row(warehouses):
    store = warehouses.store()

    store.save_marker("marker-1")
    store.save_marker("marker-2")
    store.save_marker("marker-2")

    assert store.load().settings_marker == "marker-2"
    assert warehouses.rows(
        "SELECT count(*) FROM adops._adops_deploy_state WHERE kind = %s", "settings_marker"
    ) == [(1,)]


def test_seen_ids_accumulate_across_pipelines(warehouses):
    store = warehouses.store()

    store.save_seen_records("shop", ["shop_drop_email"])
    store.save_seen_records("crm", ["crm_rename_id", "crm_drop_phone"])
    store.save_seen_records("shop", ["shop_retype_total"])

    assert store.load().seen_record_ids == frozenset(
        {"shop_drop_email", "shop_retype_total", "crm_rename_id", "crm_drop_phone"}
    )


def test_saving_a_seen_id_twice_keeps_one_row(warehouses):
    store = warehouses.store()

    store.save_seen_records("shop", ["shop_drop_email"])
    store.save_seen_records("shop", ["shop_drop_email", "shop_drop_email"])

    assert store.load().seen_record_ids == frozenset({"shop_drop_email"})
    assert warehouses.rows(
        "SELECT count(*) FROM adops._adops_deploy_state WHERE pipeline = %s AND value = %s",
        "shop",
        "shop_drop_email",
    ) == [(1,)]


def test_the_marker_and_seen_ids_are_kept_apart(warehouses):
    store = warehouses.store()

    store.save_seen_records("shop", ["marker-1"])
    store.save_marker("marker-1")
    store.save_marker("marker-2")

    assert store.load() == DeployState(
        settings_marker="marker-2", seen_record_ids=frozenset({"marker-1"})
    )


def test_quotes_in_values_are_stored_as_values(warehouses):
    store = warehouses.store()

    store.save_marker("mar'ker")
    store.save_seen_records("sh'op", ["rec'ord"])

    assert store.load() == DeployState(
        settings_marker="mar'ker", seen_record_ids=frozenset({"rec'ord"})
    )


def test_another_environments_warehouse_sees_none_of_the_state(tmp_path):
    stage = DuckDbWarehouse(tmp_path, "stage.duckdb")
    prod = DuckDbWarehouse(tmp_path, "prod.duckdb")
    stage_store = WarehouseDeployStateStore(
        tmp_path, "duckdb", stage.connection, environment="stage"
    )
    prod_store = WarehouseDeployStateStore(tmp_path, "duckdb", prod.connection, environment="prod")

    stage_store.save_marker("stage-marker")
    stage_store.save_seen_records("shop", ["shop_drop_email"])

    assert prod_store.load() == EMPTY
    assert prod.rows(TABLE_EXISTS) == [(0,)]


def test_saving_leaves_the_applied_records_table_untouched(warehouses):
    warehouses.warehouse.execute(
        "CREATE SCHEMA adops",
        "CREATE TABLE adops._adops_applied_records (record_id TEXT)",
        "INSERT INTO adops._adops_applied_records VALUES ('applied')",
    )
    store = warehouses.store()

    store.save_marker("marker-1")
    store.save_seen_records("shop", ["shop_drop_email"])

    assert warehouses.rows("SELECT record_id FROM adops._adops_applied_records") == [("applied",)]
    assert store.load().seen_record_ids == frozenset({"shop_drop_email"})


UNREACHABLE = ResolvedConnection(
    host="127.0.0.1", port=1, database="warehouse", user="adops", password=SENTINEL_PASSWORD
)


def assert_holds_no_driver_text(error: Exception) -> None:
    assert error.__cause__ is None and error.__context__ is None
    for text in (str(error), repr(error)):
        for leak in (SENTINEL_PASSWORD, "127.0.0.1", "port 1", "OperationalError", ".duckdb"):
            assert leak not in text


def call_store(store: WarehouseDeployStateStore, call: str) -> None:
    if call == "load":
        store.load()
    elif call == "save_marker":
        store.save_marker("marker")
    else:
        store.save_seen_records("shop", ["record"])


@pytest.mark.parametrize("call", ["load", "save_marker", "save_seen_records"])
def test_an_unreachable_postgres_raises_warehouse_unreachable_without_driver_text(tmp_path, call):
    store = WarehouseDeployStateStore(tmp_path, "postgres", UNREACHABLE, environment="stage")

    with pytest.raises(WarehouseUnreachable) as raised:
        call_store(store, call)

    assert_holds_no_driver_text(raised.value)


def test_a_failing_statement_raises_warehouse_unreachable_without_driver_text(
    tmp_path, monkeypatch
):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseDeployStateStore(tmp_path, "duckdb", warehouse.connection, environment="stage")

    def failing_statement(*_):
        raise RuntimeError(f"OperationalError at postgresql://adops:{SENTINEL_PASSWORD}@db/wh")

    monkeypatch.setattr("adops.infrastructure.deploy_state_store._save_marker", failing_statement)

    with pytest.raises(WarehouseUnreachable) as raised:
        store.save_marker("marker")

    assert_holds_no_driver_text(raised.value)


@pytest.mark.parametrize("call", ["load", "save_marker", "save_seen_records"])
def test_a_duckdb_file_locked_by_another_process_is_unreachable(
    tmp_path, duckdb_held_by_another_process, call
):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseDeployStateStore(tmp_path, "duckdb", warehouse.connection, environment="stage")
    store.save_marker("marker")

    with (
        duckdb_held_by_another_process(warehouse.path),
        pytest.raises(WarehouseUnreachable) as raised,
    ):
        call_store(store, call)

    assert_holds_no_driver_text(raised.value)


def test_saves_and_load_release_the_duckdb_file(tmp_path):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseDeployStateStore(tmp_path, "duckdb", warehouse.connection, environment="stage")

    store.save_marker("marker")
    store.save_seen_records("shop", ["record"])
    store.load()

    with duckdb.connect(str(warehouse.path)) as db:
        db.execute("CREATE TABLE lock_probe (id INTEGER)")
