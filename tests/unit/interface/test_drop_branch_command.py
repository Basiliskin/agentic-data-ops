from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from adops.application.drop_branch import BranchState, WarehouseSchemas
from adops.application.ports import ResolvedConnection, ResolvedWarehouse, WarehouseLocked
from adops.infrastructure.warehouse_schemas import LocalBranchState, WarehouseSchemaAdmin
from adops.interface import cli, drop_branch_command

runner = CliRunner()

SENTINEL = "s3cr3t-drop-branch-command-sentinel"
SECRETS = ("DEV_WAREHOUSE_PASSWORD", "STAGE_WAREHOUSE_PASSWORD", "PROD_WAREHOUSE_PASSWORD")

SOURCES = """
    sources:
      shop: {type: postgres, host: src, port: 5432, database: s, user: s,
             password_env: SHOP_DB_PASSWORD}
"""

DUCKDB_SETTINGS = f"""
environments:
  dev:
    warehouse: {{type: duckdb, path: dev.duckdb}}{SOURCES}
  stage:
    warehouse: {{type: duckdb, path: stage.duckdb}}{SOURCES}
"""


def postgres_settings(stage_host: str = "127.0.0.1", stage_port: int = 1) -> str:
    def warehouse(name: str, host: str, port: int) -> str:
        return (
            f"  {name}:\n    warehouse: {{type: postgres, host: {host}, port: {port}, "
            f"database: wh, user: adops, password_env: {name.upper()}_WAREHOUSE_PASSWORD}}"
            f"{SOURCES}"
        )

    return "environments:\n" + "\n".join(
        [
            warehouse("dev", "localhost", 5433),
            warehouse("stage", stage_host, stage_port),
            warehouse("prod", "prod-db", 5432),
        ]
    )


STAGE_SCHEMAS = {"raw_shop", "analytics", "adops"}
FEAT = {"br_feat__raw_shop", "br_feat__raw_shop_staging", "br_feat__analytics"}
NEIGHBOURS = {"br_feat2__analytics", "br_other__adops"}


def schemas(path: Path) -> set[str]:
    with duckdb.connect(str(path)) as connection:
        rows = connection.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = current_database()"
        ).fetchall()
    return {name for (name,) in rows}


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(DUCKDB_SETTINGS)
    for name in SECRETS:
        monkeypatch.delenv(name, raising=False)
    with duckdb.connect(str(tmp_path / "stage.duckdb")) as connection:
        for name in sorted(STAGE_SCHEMAS | FEAT | NEIGHBOURS):
            connection.execute(f'CREATE SCHEMA "{name}"')
            connection.execute(f'CREATE TABLE "{name}".orders (id INTEGER)')
    for state in ("br_feat", "br_feat2", "stage"):
        (tmp_path / ".adops" / "dlt" / state).mkdir(parents=True)
    return tmp_path


def invoke(project: Path, branch: str = "feat"):
    return runner.invoke(cli.app, ["drop-branch", branch, "--project", str(project)])


def test_drop_branch_drops_the_branchs_schemas_and_state(project):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert schemas(project / "stage.duckdb") >= STAGE_SCHEMAS | NEIGHBOURS
    assert not FEAT & schemas(project / "stage.duckdb")
    assert not (project / ".adops/dlt/br_feat").exists()
    assert (project / ".adops/dlt/br_feat2").is_dir()
    assert (project / ".adops/dlt/stage").is_dir()
    assert (project / "stage.duckdb").is_file()
    assert result.output == (
        "Dropped Branch Environment br_feat: "
        "br_feat__analytics, br_feat__raw_shop, br_feat__raw_shop_staging\n"
    )


def test_dropping_twice_succeeds_both_times(project):
    first = invoke(project)
    second = invoke(project)

    assert first.exit_code == 0 and second.exit_code == 0, second.output
    assert second.output == "Branch Environment br_feat has no schemas left to drop.\n"
    assert schemas(project / "stage.duckdb") >= STAGE_SCHEMAS | NEIGHBOURS


def test_a_branch_name_without_letters_or_digits_is_a_usage_error(project):
    before = schemas(project / "stage.duckdb")

    result = invoke(project, "///")

    assert result.exit_code == 1
    assert "--branch: the branch name has no letters or digits" in result.output
    assert schemas(project / "stage.duckdb") == before
    assert (project / ".adops/dlt/br_feat").is_dir()


@dataclass
class RecordingSchemas:
    warehouses: list[ResolvedWarehouse] = field(default_factory=list)

    def list_schemas(self, warehouse: ResolvedWarehouse) -> list[str]:
        self.warehouses.append(warehouse)
        return ["br_feat__analytics", "analytics"]

    def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
        self.warehouses.append(warehouse)


@pytest.mark.usefixtures("locks")
def test_only_stages_secret_is_needed(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(postgres_settings("stage-db", 5432))
    for name in SECRETS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("STAGE_WAREHOUSE_PASSWORD", SENTINEL)
    recording = RecordingSchemas()
    monkeypatch.setattr(drop_branch_command, "build_warehouse_schemas", lambda *_: recording)

    result = invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert set(recording.warehouses) == {
        ResolvedConnection(
            host="stage-db", port=5432, database="wh", user="adops", password=SENTINEL
        )
    }
    assert SENTINEL not in result.output


def test_a_missing_stage_secret_names_the_variable(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(postgres_settings())
    for name in SECRETS:
        monkeypatch.delenv(name, raising=False)

    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert "STAGE_WAREHOUSE_PASSWORD" in result.output


def test_an_unreachable_stage_warehouse_prints_no_secret(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(postgres_settings())
    monkeypatch.setenv("STAGE_WAREHOUSE_PASSWORD", SENTINEL)

    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert result.output == (
        "Branch feat: stage's Warehouse is unreachable or locked by another process\n"
    )
    assert SENTINEL not in result.output


def test_a_locked_duckdb_warehouse_gets_a_plain_message(project, duckdb_held_by_another_process):
    with duckdb_held_by_another_process(project / "stage.duckdb"):
        result = invoke(project)

    assert result.exit_code == 1
    assert result.output == (
        "Branch feat: stage's Warehouse is unreachable or locked by another process\n"
    )
    assert FEAT <= schemas(project / "stage.duckdb")
    assert (project / ".adops/dlt/br_feat").is_dir()


def test_an_unexpected_error_prints_only_its_type(project, monkeypatch):
    class Exploding:
        def list_schemas(self, warehouse):
            raise KeyError(f"postgresql://adops:{SENTINEL}@db/wh")

    monkeypatch.setattr(drop_branch_command, "build_warehouse_schemas", lambda *_: Exploding())

    result = invoke(project)

    assert result.exit_code == 1
    assert result.output == "unexpected KeyError while dropping Branch feat\n"


STAGE_WAREHOUSE = ResolvedConnection(
    host="stage-db", port=5432, database="wh", user="adops", password=SENTINEL
)


@dataclass
class MarkingSchemas:
    locks: object
    names: tuple[str, ...] = ("br_feat__analytics", "analytics")
    error: Exception | None = None

    def list_schemas(self, warehouse: ResolvedWarehouse) -> list[str]:
        self.locks.mark("list")
        if self.error is not None:
            raise self.error
        return list(self.names)

    def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
        self.locks.mark(f"drop {schema}")


@pytest.fixture
def stage_postgres(tmp_path, monkeypatch, locks):
    (tmp_path / "adops.yaml").write_text(postgres_settings("stage-db", 5432))
    monkeypatch.setenv("STAGE_WAREHOUSE_PASSWORD", SENTINEL)
    marking = MarkingSchemas(locks)
    monkeypatch.setattr(drop_branch_command, "build_warehouse_schemas", lambda *_: marking)
    return marking


def test_the_drop_holds_stages_warehouse_lock(tmp_path, stage_postgres, locks):
    result = invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert locks.events == [
        ("enter", "stage"),
        ("list",),
        ("drop br_feat__analytics",),
        ("exit", "stage"),
    ]
    assert locks.warehouses == [STAGE_WAREHOUSE]


def test_nothing_to_drop_still_takes_the_lock(tmp_path, stage_postgres, locks):
    stage_postgres.names = ("analytics",)

    result = invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert locks.events == [("enter", "stage"), ("list",), ("exit", "stage")]


def test_a_failing_drop_still_releases_the_lock(tmp_path, stage_postgres, locks):
    stage_postgres.error = KeyError(SENTINEL)

    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert locks.events == [("enter", "stage"), ("list",), ("exit", "stage")]


def test_a_lock_timeout_is_one_plain_line(tmp_path, stage_postgres, locks):
    locks.error = WarehouseLocked("stage", 30 * 60)

    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert result.output == (
        "Another run is using the stage Warehouse; gave up after 30 minutes.\n"
    )
    for leaked in (SENTINEL, "stage-db", str(tmp_path)):
        assert leaked not in result.output
    assert locks.events == []


def test_the_real_lock_is_taken_on_a_duckdb_stage(project):
    lock_file = project / "stage.duckdb.lock"

    assert invoke(project).exit_code == 0
    assert lock_file.is_file()


def test_a_project_without_a_stage_environment_is_rejected(tmp_path):
    (tmp_path / "adops.yaml").write_text(DUCKDB_SETTINGS.split("  stage:")[0])

    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert "environments.stage: a Branch run needs a stage Environment" in result.output


def test_a_folder_without_adops_yaml_is_rejected(tmp_path):
    result = invoke(tmp_path)

    assert result.exit_code == 1
    assert result.output == "adops.yaml: file not found\n"


def test_the_builders_return_the_real_adapters(tmp_path):
    schemas_port = drop_branch_command.build_warehouse_schemas(tmp_path, "duckdb")
    state = drop_branch_command.build_branch_state(tmp_path)

    assert isinstance(schemas_port, WarehouseSchemaAdmin)
    assert isinstance(schemas_port, WarehouseSchemas)
    assert isinstance(state, LocalBranchState) and isinstance(state, BranchState)


def test_drop_branch_is_listed_in_the_help():
    result = runner.invoke(cli.app, ["--help"])

    assert "drop-branch" in result.output
