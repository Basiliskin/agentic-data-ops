from pathlib import Path

import duckdb
import pytest

from adops.application.drop_branch import BranchState, WarehouseSchemas, drop_branch
from adops.application.ports import ResolvedConnection, ResolvedDuckDb, WarehouseUnreachable
from adops.infrastructure.warehouse_schemas import LocalBranchState, WarehouseSchemaAdmin

SENTINEL_PASSWORD = "s3cr3t-drop-branch-sentinel"

STAGE_SCHEMAS = {"raw_shop", "raw_shop_staging", "analytics", "adops"}
NEIGHBOURS = {
    "br_feat2__analytics",
    "br_fea__analytics",
    "br_featx_analytics",
    "br_feat_%_analytics",
    "br_other__adops",
}
FEAT = {
    "br_feat__raw_shop",
    "br_feat__raw_shop_staging",
    "br_feat__analytics",
    'br_feat__we"ird%',
}


class DuckDbWarehouse:
    def __init__(self, project: Path) -> None:
        self.path = project / "stage.duckdb"
        self.connection = ResolvedDuckDb(str(self.path))

    def execute(self, *statements: str) -> None:
        with duckdb.connect(str(self.path)) as connection:
            for statement in statements:
                connection.execute(statement)

    def schemas(self) -> set[str]:
        with duckdb.connect(str(self.path)) as connection:
            rows = connection.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE catalog_name = current_database()"
            ).fetchall()
        return {name for (name,) in rows}


class Warehouses:
    def __init__(self, kind: str, project: Path, warehouse) -> None:
        self.kind = kind
        self.project = project
        self.warehouse = warehouse

    @property
    def connection(self):
        return self.warehouse.connection

    def admin(self) -> WarehouseSchemaAdmin:
        return WarehouseSchemaAdmin(self.project, self.kind)

    def create(self, schemas: set[str]) -> None:
        statements = []
        for name in sorted(schemas):
            quoted = '"' + name.replace('"', '""') + '"'
            statements += [
                f"CREATE SCHEMA {quoted}",
                f"CREATE TABLE {quoted}.orders (id INTEGER)",
            ]
        self.warehouse.execute(*statements)

    def settings(self) -> str:
        if isinstance(self.connection, ResolvedDuckDb):
            warehouse = f"{{type: duckdb, path: '{self.connection.path}'}}"
        else:
            c = self.connection
            warehouse = (
                f"{{type: postgres, host: {c.host}, port: {c.port}, database: {c.database}, "
                f"user: {c.user}, password_env: STAGE_WAREHOUSE_PASSWORD}}"
            )
        return f"""
environments:
  dev:
    warehouse: {{type: duckdb, path: dev.duckdb}}
    sources: {{}}
  stage:
    warehouse: {warehouse}
    sources:
      shop: {{type: postgres, host: src, port: 5432, database: s, user: s,
             password_env: STAGE_SHOP_PASSWORD}}
"""

    def env(self) -> dict[str, str]:
        if isinstance(self.connection, ResolvedConnection):
            return {"STAGE_WAREHOUSE_PASSWORD": self.connection.password}
        return {}


@pytest.fixture(params=["duckdb", "postgres"])
def warehouses(request, tmp_path: Path) -> Warehouses:
    if request.param == "duckdb":
        return Warehouses("duckdb", tmp_path, DuckDbWarehouse(tmp_path))
    return Warehouses("postgres", tmp_path, request.getfixturevalue("postgres_warehouse"))


def test_drop_branch_removes_exactly_the_branchs_schemas(warehouses):
    warehouses.create(STAGE_SCHEMAS | NEIGHBOURS | FEAT)
    warehouses.warehouse.execute(
        "CREATE VIEW br_feat__analytics.orders_view AS SELECT * FROM br_feat__raw_shop.orders",
        "CREATE VIEW br_feat__analytics.stage_orders AS SELECT * FROM raw_shop.orders",
    )
    before = warehouses.warehouse.schemas()

    dropped = drop_branch(
        warehouses.settings(),
        "feat",
        warehouses.env(),
        warehouses.admin(),
        LocalBranchState(warehouses.project),
    )

    assert set(dropped.schemas) == FEAT
    assert warehouses.warehouse.schemas() == before - FEAT
    assert STAGE_SCHEMAS | NEIGHBOURS <= warehouses.warehouse.schemas()


def test_dropping_a_branch_twice_succeeds(warehouses):
    warehouses.create(STAGE_SCHEMAS | FEAT)
    state = LocalBranchState(warehouses.project)

    def drop():
        return drop_branch(
            warehouses.settings(), "feat", warehouses.env(), warehouses.admin(), state
        )

    first, second = drop(), drop()

    assert set(first.schemas) == FEAT and second.schemas == ()
    assert STAGE_SCHEMAS <= warehouses.warehouse.schemas()


def test_list_schemas_returns_every_schema_in_the_warehouse(warehouses):
    warehouses.create({"br_feat__analytics", "analytics"})

    listed = warehouses.admin().list_schemas(warehouses.connection)

    assert {"br_feat__analytics", "analytics"} <= set(listed)
    assert set(listed) == warehouses.warehouse.schemas()


def test_dropping_a_schema_that_is_gone_succeeds(warehouses):
    warehouses.create({"analytics"})
    admin = warehouses.admin()

    admin.drop(warehouses.connection, "br_feat__raw_shop_staging")

    assert "analytics" in warehouses.warehouse.schemas()


def test_a_drop_takes_the_schemas_tables_with_it(warehouses):
    warehouses.create({"br_feat__raw_shop"})

    warehouses.admin().drop(warehouses.connection, "br_feat__raw_shop")

    assert "br_feat__raw_shop" not in warehouses.warehouse.schemas()


def test_the_admin_leaves_the_branchs_state_folder_alone(warehouses):
    warehouses.create({"br_feat__analytics"})

    warehouses.admin().drop(warehouses.connection, "br_feat__analytics")

    assert not (warehouses.project / ".adops" / "dlt" / "br_feat").exists()


def test_the_duckdb_file_is_released_after_a_drop(tmp_path):
    warehouse = DuckDbWarehouse(tmp_path)
    warehouse.execute("CREATE SCHEMA br_feat__analytics")
    admin = WarehouseSchemaAdmin(tmp_path, "duckdb")

    admin.list_schemas(warehouse.connection)
    admin.drop(warehouse.connection, "br_feat__analytics")

    warehouse.execute("CREATE TABLE lock_probe (id INTEGER)")


UNREACHABLE = ResolvedConnection(
    host="127.0.0.1", port=1, database="warehouse", user="adops", password=SENTINEL_PASSWORD
)


def assert_holds_no_driver_text(error: Exception) -> None:
    assert error.__cause__ is None and error.__context__ is None
    for text in (str(error), repr(error)):
        for leak in (SENTINEL_PASSWORD, "127.0.0.1", "port 1", "OperationalError", ".duckdb"):
            assert leak not in text


def call(admin: WarehouseSchemaAdmin, method: str, warehouse) -> None:
    if method == "list":
        admin.list_schemas(warehouse)
    else:
        admin.drop(warehouse, "br_feat__analytics")


@pytest.mark.parametrize("method", ["list", "drop"])
def test_an_unreachable_postgres_raises_warehouse_unreachable_without_driver_text(tmp_path, method):
    with pytest.raises(WarehouseUnreachable) as raised:
        call(WarehouseSchemaAdmin(tmp_path, "postgres"), method, UNREACHABLE)

    assert_holds_no_driver_text(raised.value)


@pytest.mark.parametrize("method", ["list", "drop"])
def test_a_duckdb_file_locked_by_another_process_is_unreachable(
    tmp_path, duckdb_held_by_another_process, method
):
    warehouse = DuckDbWarehouse(tmp_path)
    warehouse.execute("CREATE SCHEMA br_feat__analytics")

    with (
        duckdb_held_by_another_process(warehouse.path),
        pytest.raises(WarehouseUnreachable) as raised,
    ):
        call(WarehouseSchemaAdmin(tmp_path, "duckdb"), method, warehouse.connection)

    assert_holds_no_driver_text(raised.value)
    assert "br_feat__analytics" in warehouse.schemas()


def test_a_failing_statement_raises_warehouse_unreachable_without_driver_text(
    tmp_path, monkeypatch
):
    warehouse = DuckDbWarehouse(tmp_path)
    warehouse.execute("CREATE SCHEMA analytics")

    def failing_statement(*_):
        raise RuntimeError(f"OperationalError at postgresql://adops:{SENTINEL_PASSWORD}@db/wh")

    monkeypatch.setattr("adops.infrastructure.warehouse_schemas._drop_schema", failing_statement)

    with pytest.raises(WarehouseUnreachable) as raised:
        WarehouseSchemaAdmin(tmp_path, "duckdb").drop(warehouse.connection, "analytics")

    assert_holds_no_driver_text(raised.value)


def make_dirs(project: Path, *relative: str) -> None:
    for path in relative:
        (project / path).mkdir(parents=True)
        (project / path / "state.json").write_text("{}")


def test_only_the_branchs_state_folder_is_deleted(tmp_path):
    make_dirs(tmp_path, ".adops/dlt/br_feat/pipe", ".adops/dlt/br_feat2", ".adops/dlt/stage")
    warehouse = DuckDbWarehouse(tmp_path)
    warehouse.execute("CREATE SCHEMA analytics")

    LocalBranchState(tmp_path).delete("br_feat")

    assert not (tmp_path / ".adops/dlt/br_feat").exists()
    assert (tmp_path / ".adops/dlt/br_feat2/state.json").is_file()
    assert (tmp_path / ".adops/dlt/stage/state.json").is_file()
    assert warehouse.path.is_file()


def test_a_branch_without_a_state_folder_is_fine(tmp_path):
    make_dirs(tmp_path, ".adops/dlt/stage")

    LocalBranchState(tmp_path).delete("br_feat")

    assert (tmp_path / ".adops/dlt/stage/state.json").is_file()


@pytest.mark.parametrize(
    "name", ["", "br_", "stage", "dev", "br_../stage", "../br_feat", "br_feat/..", "br_Feat"]
)
def test_a_name_that_is_not_a_branch_environment_deletes_nothing(tmp_path, name):
    make_dirs(tmp_path, ".adops/dlt/stage", ".adops/dlt/br_feat")

    with pytest.raises(ValueError):
        LocalBranchState(tmp_path).delete(name)

    assert (tmp_path / ".adops/dlt/stage/state.json").is_file()
    assert (tmp_path / ".adops/dlt/br_feat/state.json").is_file()


def test_the_adapters_satisfy_the_ports(tmp_path):
    assert isinstance(WarehouseSchemaAdmin(tmp_path, "duckdb"), WarehouseSchemas)
    assert isinstance(LocalBranchState(tmp_path), BranchState)
