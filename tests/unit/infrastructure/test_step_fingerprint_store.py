from pathlib import Path

import duckdb
import pytest

from adops.application.ports import (
    ProjectFiles,
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    StepFingerprintStore,
    WarehouseUnreachable,
)
from adops.domain.branch_environment import canonical_step_json, step_fingerprint
from adops.domain.transform import TransformStep
from adops.infrastructure.step_fingerprint_store import (
    ProjectTree,
    WarehouseFingerprintStore,
    read_project_tree,
)

SENTINEL_PASSWORD = "s3cr3t-fingerprint-sentinel"
TABLE_EXISTS = (
    "SELECT count(*) FROM information_schema.tables "
    "WHERE table_schema = 'adops' AND table_name = '_adops_step_fingerprints'"
)
ADOPS_SCHEMA_EXISTS = "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'adops'"


class DuckDbWarehouse:
    def __init__(self, project: Path) -> None:
        self.project = project
        self.path = project / "warehouse.duckdb"
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

    @property
    def connection(self) -> ResolvedWarehouse:
        return self.warehouse.connection

    def store(self, environment: str = "stage") -> WarehouseFingerprintStore:
        return WarehouseFingerprintStore(
            self.project, self.kind, self.connection, environment=environment
        )

    def rows(self, query: str, *parameters: object) -> list[tuple]:
        return self.warehouse.rows(query, *parameters)


@pytest.fixture(params=["duckdb", "postgres"])
def warehouses(request, tmp_path: Path) -> Warehouses:
    if request.param == "duckdb":
        return Warehouses("duckdb", tmp_path, DuckDbWarehouse(tmp_path))
    return Warehouses("postgres", tmp_path, request.getfixturevalue("postgres_warehouse"))


def test_store_implements_the_fingerprint_store_port(tmp_path):
    store = WarehouseFingerprintStore(
        tmp_path, "duckdb", ResolvedDuckDb("wh.duckdb"), environment="stage"
    )

    assert isinstance(store, StepFingerprintStore)


def test_save_then_load_round_trips_in_step_order(warehouses):
    store = warehouses.store()

    for step, fingerprint in [("load_orders", "f1"), ("transform", "f2"), ("checks", "f3")]:
        store.save("shop", step, fingerprint)

    loaded = warehouses.store().load("shop")
    assert list(loaded.items()) == [("load_orders", "f1"), ("transform", "f2"), ("checks", "f3")]


def test_saving_a_step_again_overwrites_its_one_row(warehouses):
    store = warehouses.store()

    store.save("shop", "step", "a")
    store.save("shop", "step", "b")

    assert store.load("shop") == {"step": "b"}
    assert warehouses.rows(
        "SELECT count(*) FROM adops._adops_step_fingerprints WHERE pipeline = %s AND step = %s",
        "shop",
        "step",
    ) == [(1,)]


def test_a_stage_run_with_reordered_steps_loads_them_in_the_new_order(warehouses):
    store = warehouses.store()
    store.save("shop", "a", "1")
    store.save("shop", "b", "2")

    store.save("shop", "b", "2")
    store.save("shop", "a", "1")

    assert list(store.load("shop").items()) == [("b", "2"), ("a", "1")]


def test_pipelines_are_kept_apart(warehouses):
    store = warehouses.store()

    store.save("shop", "load", "shop-fp")
    store.save("crm", "load", "crm-fp")
    store.save("crm", "transform", "crm-tf")

    assert store.load("shop") == {"load": "shop-fp"}
    assert list(store.load("crm").items()) == [("load", "crm-fp"), ("transform", "crm-tf")]
    assert store.load("billing") == {}


def test_quotes_in_values_are_stored_as_values(warehouses):
    store = warehouses.store()

    store.save("sh'op", "st'ep", "f'p")

    assert store.load("sh'op") == {"st'ep": "f'p"}
    assert store.load("shop") == {}


def test_load_on_an_empty_warehouse_finds_nothing_and_creates_nothing(warehouses):
    assert warehouses.store().load("shop") == {}

    assert warehouses.rows(TABLE_EXISTS) == [(0,)]
    assert warehouses.rows(ADOPS_SCHEMA_EXISTS) == [(0,)]


def test_load_with_the_schema_but_no_table_finds_nothing_and_creates_nothing(warehouses):
    warehouses.warehouse.execute("CREATE SCHEMA adops")

    assert warehouses.store().load("shop") == {}

    assert warehouses.rows(TABLE_EXISTS) == [(0,)]


def test_a_branch_environment_reads_what_stage_saved(warehouses):
    warehouses.store("stage").save("shop", "load", "fp")

    assert warehouses.store("br_feature").load("shop") == {"load": "fp"}


def test_stored_rows_hold_no_connection_details(warehouses):
    warehouses.store().save("shop", "load", "fp")

    rows = warehouses.rows("SELECT * FROM adops._adops_step_fingerprints")
    assert [row[:3] for row in rows] == [("shop", "load", "fp")]
    assert all("://" not in str(value) and "password" not in str(value) for value in rows[0])


UNREACHABLE = ResolvedConnection(
    host="127.0.0.1", port=1, database="warehouse", user="adops", password=SENTINEL_PASSWORD
)


def assert_holds_no_driver_text(error: Exception) -> None:
    assert error.__cause__ is None and error.__context__ is None
    for text in (str(error), repr(error)):
        for leak in (SENTINEL_PASSWORD, "127.0.0.1", "port 1", "OperationalError", ".duckdb"):
            assert leak not in text


@pytest.mark.parametrize("call", ["save", "load"])
def test_an_unreachable_postgres_raises_warehouse_unreachable_without_driver_text(tmp_path, call):
    store = WarehouseFingerprintStore(tmp_path, "postgres", UNREACHABLE, environment="stage")

    with pytest.raises(WarehouseUnreachable) as raised:
        if call == "save":
            store.save("shop", "load", "fp")
        else:
            store.load("shop")

    assert_holds_no_driver_text(raised.value)


def test_a_failing_statement_raises_warehouse_unreachable_without_driver_text(
    tmp_path, monkeypatch
):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseFingerprintStore(tmp_path, "duckdb", warehouse.connection, environment="stage")

    def failing_statement(*_):
        raise RuntimeError(f"OperationalError at postgresql://adops:{SENTINEL_PASSWORD}@db/wh")

    monkeypatch.setattr("adops.infrastructure.step_fingerprint_store._save", failing_statement)

    with pytest.raises(WarehouseUnreachable) as raised:
        store.save("shop", "load", "fp")

    assert_holds_no_driver_text(raised.value)


@pytest.mark.parametrize("call", ["save", "load"])
def test_a_duckdb_file_locked_by_another_process_is_unreachable(
    tmp_path, duckdb_held_by_another_process, call
):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseFingerprintStore(tmp_path, "duckdb", warehouse.connection, environment="stage")
    store.save("shop", "load", "fp")

    with (
        duckdb_held_by_another_process(warehouse.path),
        pytest.raises(WarehouseUnreachable) as raised,
    ):
        if call == "save":
            store.save("shop", "load", "fp")
        else:
            store.load("shop")

    assert_holds_no_driver_text(raised.value)


def test_save_and_load_release_the_duckdb_file(tmp_path):
    warehouse = DuckDbWarehouse(tmp_path)
    store = WarehouseFingerprintStore(tmp_path, "duckdb", warehouse.connection, environment="stage")

    store.save("shop", "load", "fp")
    store.load("shop")

    with duckdb.connect(str(warehouse.path)) as db:
        db.execute("CREATE TABLE lock_probe (id INTEGER)")


def write(root: Path, relative: str, content: bytes = b"x") -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_project_tree_skips_what_dbt_and_adops_generate(tmp_path):
    for relative in [
        "transform/models/a.sql",
        "transform/dbt_project.yml",
        "transform/models/target_customers.sql",
        "transform/models/logs_daily.sql",
        "transform/target/manifest.json",
        "transform/logs/dbt.log",
        "transform/dbt_packages/x/y.sql",
        "transform/dbt_packages/x/target/run.sql",
        "transform/.adops/s",
        "transform/macros/__pycache__/m.cpython-313.pyc",
    ]:
        write(tmp_path, relative)

    assert list(read_project_tree(tmp_path, "transform")) == [
        "dbt_project.yml",
        "models/a.sql",
        "models/logs_daily.sql",
        "models/target_customers.sql",
    ]


def test_project_tree_keys_are_relative_posix_paths_with_their_bytes(tmp_path):
    write(tmp_path, "transform/models/staging/b.sql", b"select 2")
    write(tmp_path, "transform/models/a.sql", b"select 1")

    tree = read_project_tree(tmp_path, "transform")

    assert tree == {"models/a.sql": b"select 1", "models/staging/b.sql": b"select 2"}
    assert all(not key.startswith("/") and "\\" not in key for key in tree)
    assert list(tree) == sorted(tree)


def test_a_missing_project_folder_reads_as_empty(tmp_path):
    assert read_project_tree(tmp_path, "missing") == {}


def test_a_dbt_run_does_not_change_a_transform_fingerprint(tmp_path):
    write(tmp_path, "transform/models/a.sql", b"select 1")
    step = TransformStep.model_validate(
        {"type": "transform", "name": "transform", "project": "transform"}
    )
    files = ProjectTree(tmp_path)

    before = step_fingerprint(canonical_step_json(step), files.read_tree("transform"))
    write(tmp_path, "transform/target/manifest.json", b"{}")
    write(tmp_path, "transform/logs/dbt.log", b"ran")
    after = step_fingerprint(canonical_step_json(step), files.read_tree("transform"))
    write(tmp_path, "transform/models/a.sql", b"select 2")
    edited = step_fingerprint(canonical_step_json(step), files.read_tree("transform"))

    assert before == after
    assert edited != before


def test_project_tree_implements_the_project_files_port(tmp_path):
    assert isinstance(ProjectTree(tmp_path), ProjectFiles)
