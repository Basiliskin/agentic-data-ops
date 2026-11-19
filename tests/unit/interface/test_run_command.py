import os
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from adops.application.ports import (
    Clock,
    Orchestrator,
    ResolvedConnection,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    Sync,
    SyncResult,
    Transform,
    TransformResult,
)
from adops.application.run_pipeline import InOrderOrchestrator
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep
from adops.domain.schema_snapshot import TableShape
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep
from adops.infrastructure.clock import SystemClock
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dbt_transform import (
    DbtTransform,
    render_duckdb_profile,
    render_postgres_profile,
)
from adops.infrastructure.dlt_sync import DltSync
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore
from adops.interface import cli, run_command

runner = CliRunner()

REAL_BUILD_SNAPSHOT_STORE = run_command.build_snapshot_store

SENTINEL = "s3cr3t-run-command-sentinel"

SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""

PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
"""

TRANSFORM_STEP = """
  - name: transform_shop
    type: transform
    project: dbt
"""

COLUMNS = {"customers": frozenset({"id"}), "orders": frozenset({"id", "updated_at"})}


@dataclass
class FakeSync:
    missing_tables: set[str] = field(default_factory=set)
    sync_error: Exception | None = None
    sources: list[ResolvedConnection] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.sources.append(source)
        return {
            table.name: SourceFacts(
                table_exists=table.name not in self.missing_tables, columns=COLUMNS[table.name]
            )
            for table in step.tables
        }

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
    ) -> SyncResult:
        if self.sync_error is not None:
            raise self.sync_error
        facts = {
            "customers": LoadFacts(0, 20, 0, 0, source_rows=20),
            "orders": LoadFacts(98, 100, 0, 0, cursor_max_before=1, cursor_max_after=2),
        }
        return SyncResult({"customers": 20, "orders": 2}, facts)


@dataclass
class RecordingOrchestrator:
    error: Exception | None = None
    environ: list[dict[str, str]] = field(default_factory=list)

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        self.environ.append(dict(os.environ))
        if self.error is not None:
            raise self.error
        return InOrderOrchestrator().execute(plan, sync)


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(SETTINGS)
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "shop_sync.yaml").write_text(PIPELINE)
    monkeypatch.setenv("ADOPS_TEST_SHOP_PASSWORD", SENTINEL)
    monkeypatch.setenv("ADOPS_TEST_WAREHOUSE_PASSWORD", SENTINEL)
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    return tmp_path


@pytest.fixture
def sync(monkeypatch):
    fake = FakeSync()
    monkeypatch.setattr(run_command, "build_sync", lambda project, pipeline, kind: fake)
    return fake


@dataclass
class InMemorySnapshotStore:
    shapes: dict[tuple[str, str, str], TableShape] = field(default_factory=dict)

    def load(
        self, warehouse: ResolvedConnection, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        return self.shapes.get((pipeline, source, table))

    def save(
        self,
        warehouse: ResolvedConnection,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
    ) -> None:
        self.shapes[(pipeline, source, table)] = shape


@pytest.fixture(autouse=True)
def snapshot_store(monkeypatch):
    store = InMemorySnapshotStore()
    monkeypatch.setattr(run_command, "build_snapshot_store", lambda project, kind: store)
    return store


@pytest.fixture
def orchestrator(monkeypatch):
    fake = RecordingOrchestrator()
    monkeypatch.setattr(run_command, "build_orchestrator", lambda: fake)
    return fake


@dataclass
class FakeTransform:
    tests: tuple[DbtTestFacts, ...] = ()
    warehouses: list[ResolvedConnection] = field(default_factory=list)

    def run(self, step: TransformStep, warehouse: ResolvedConnection) -> TransformResult:
        self.warehouses.append(warehouse)
        return TransformResult(tests=self.tests, crashed=False, failed_models=())


def dbt_test(name: str, table: str, status: DbtTestStatus, failures: int) -> DbtTestFacts:
    return DbtTestFacts(f"test.shop.{name}.0", name, table, status, failures)


@pytest.fixture
def transform(project, monkeypatch):
    (project / "pipelines" / "shop_sync.yaml").write_text(PIPELINE + TRANSFORM_STEP)
    fake = FakeTransform()
    monkeypatch.setattr(run_command, "build_transform", lambda project_dir, kind: fake)
    return fake


def invoke(project, *args):
    return runner.invoke(cli.app, ["run", "shop_sync", "--project", str(project), *args])


def assert_clean_failure(result):
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert SENTINEL not in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_success_prints_steps_rows_and_checks(project):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0] == "step sync_shop: passed — customers 20 rows, orders 2 rows"
    assert "  check customers source_tables_exist: pass" in lines
    assert "  check customers row_count_matches: pass" in lines
    assert "  check orders cursor_not_backwards: pass" in lines
    assert lines[-1] == "Pipeline shop_sync passed in Environment dev."


@pytest.mark.usefixtures("orchestrator")
def test_missing_source_table_fails_its_check_and_names_the_table(project, sync):
    sync.missing_tables.add("customers")

    result = invoke(project)

    assert_clean_failure(result)
    assert "step sync_shop: check_failed" in result.output
    assert "  check customers source_tables_exist: fail" in result.output
    assert "customers: source_tables_exist failed — table not found" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_dbt_checks_print_in_the_sync_check_line_format(project, transform):
    transform.tests = (
        dbt_test("unique_customers_id", "customers", DbtTestStatus.PASS, 0),
        dbt_test("relationships_orders_customer_id", "orders", DbtTestStatus.FAIL, 1),
    )

    result = invoke(project)

    assert_clean_failure(result)
    lines = result.output.splitlines()
    assert "step sync_shop: passed — customers 20 rows, orders 2 rows" in lines
    assert "step transform_shop: check_failed" in lines
    assert "  check customers unique_customers_id: pass" in lines
    assert "  check orders relationships_orders_customer_id: fail" in lines
    check_lines = [line for line in lines if line.startswith("  check ")]
    assert all(re.match(r"^  check \S+ \S+: (pass|fail)$", line) for line in check_lines)
    assert "orders: dbt test relationships_orders_customer_id failed — 1 failing rows" in (
        result.output
    )
    assert [w.password for w in transform.warehouses] == [SENTINEL]


@pytest.mark.usefixtures("sync", "orchestrator")
def test_passing_transform_step_follows_the_sync_step(project, transform):
    transform.tests = (dbt_test("not_null_customers_id", "customers", DbtTestStatus.PASS, 0),)

    result = invoke(project)

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines.index("step transform_shop: passed") > lines.index(
        "step sync_shop: passed — customers 20 rows, orders 2 rows"
    )
    assert "  check customers not_null_customers_id: pass" in lines


@pytest.mark.usefixtures("orchestrator")
def test_sync_failure_is_reported_without_the_secret(project, sync):
    sync.sync_error = RuntimeError(f"connection refused for password {SENTINEL}")

    result = invoke(project)

    assert_clean_failure(result)
    assert "step sync_shop: failed" in result.output
    assert "Step sync_shop: RuntimeError: connection refused for password ***" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_invalid_pipeline_lists_problems(project):
    (project / "pipelines" / "shop_sync.yaml").write_text("name: shop_sync\nsteps: []\n")

    result = invoke(project)

    assert_clean_failure(result)
    assert "steps" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_unknown_environment_names_the_available_ones(project):
    result = invoke(project, "--env", "prod")

    assert_clean_failure(result)
    assert "environments.prod: no Environment with this name (available: dev)" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_missing_pipeline_file_is_one_line(project):
    result = runner.invoke(cli.app, ["run", "nope", "--project", str(project)])

    assert_clean_failure(result)
    assert result.output.strip() == "pipelines/nope.yaml: file not found"


@pytest.mark.usefixtures("sync", "orchestrator")
def test_missing_secret_names_the_variable(project, monkeypatch):
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    result = invoke(project)

    assert_clean_failure(result)
    assert "ADOPS_TEST_SHOP_PASSWORD" in result.output


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (JobCompileError("Pipeline shop_sync: bad name"), "Pipeline shop_sync: bad name"),
        (RuntimeError(f"boom {SENTINEL}"), "unexpected RuntimeError while running shop_sync"),
    ],
)
@pytest.mark.usefixtures("sync")
def test_orchestrator_errors_exit_without_traceback(project, orchestrator, error, expected):
    orchestrator.error = error

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.strip() == expected


@pytest.mark.usefixtures("orchestrator")
def test_process_environment_wins_over_dotenv(project, sync, monkeypatch):
    (project / ".env").write_text(
        "ADOPS_TEST_SHOP_PASSWORD=from_dotenv\nADOPS_TEST_WAREHOUSE_PASSWORD=from_dotenv\n"
    )
    monkeypatch.setenv("ADOPS_TEST_SHOP_PASSWORD", "from_process")
    monkeypatch.delenv("ADOPS_TEST_WAREHOUSE_PASSWORD")

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert [source.password for source in sync.sources] == ["from_process"]


@pytest.mark.usefixtures("orchestrator")
def test_dotenv_entries_without_value_are_ignored(project, sync, monkeypatch):
    (project / ".env").write_text("ADOPS_TEST_SHOP_PASSWORD\n")
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    result = invoke(project)

    assert_clean_failure(result)
    assert "ADOPS_TEST_SHOP_PASSWORD" in result.output


@pytest.mark.usefixtures("sync")
def test_dagster_yaml_scopes_dagster_home_and_dotenv_to_the_run(project, orchestrator):
    (project / "dagster.yaml").write_text("")
    (project / ".env").write_text("ADOPS_TEST_DAGSTER_PG_HOST=from_dotenv\n")
    before = dict(os.environ)

    result = invoke(project)

    assert result.exit_code == 0, result.output
    [during] = orchestrator.environ
    assert during["DAGSTER_HOME"] == str(project.resolve())
    assert during["ADOPS_TEST_DAGSTER_PG_HOST"] == "from_dotenv"
    assert dict(os.environ) == before


@pytest.mark.usefixtures("sync")
def test_dagster_home_unset_without_dagster_yaml(project, orchestrator):
    before = dict(os.environ)

    result = invoke(project)

    assert result.exit_code == 0, result.output
    [during] = orchestrator.environ
    assert "DAGSTER_HOME" not in during
    assert dict(os.environ) == before


@pytest.mark.usefixtures("sync", "orchestrator")
def test_run_passes_the_project_snapshot_store_to_the_pipeline(project, monkeypatch):
    sentinel = InMemorySnapshotStore()
    built_for: list[Path] = []
    received: list[object] = []

    def build(project_dir: Path, kind: str) -> InMemorySnapshotStore:
        built_for.append(project_dir)
        return sentinel

    def capture(*args: object) -> RunResult:
        received.append(args[5])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "build_snapshot_store", build)
    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    assert received == [sentinel] and received[0] is sentinel
    assert built_for == [project.resolve()]


@pytest.mark.usefixtures("sync", "orchestrator", "snapshot_store")
def test_run_passes_the_built_clock_to_the_pipeline(project, monkeypatch):
    clock = SystemClock()
    received: list[object] = []

    def capture(*args: object) -> RunResult:
        received.append(args[6])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "build_clock", lambda: clock)
    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    assert received == [clock] and received[0] is clock


@pytest.mark.usefixtures("sync", "orchestrator", "snapshot_store")
def test_run_passes_the_transform_built_for_the_project_to_the_pipeline(project, monkeypatch):
    built: list[FakeTransform] = []
    built_for: list[Path] = []
    received: list[object] = []

    def build(project_dir: Path, kind: str) -> FakeTransform:
        built_for.append(project_dir)
        built.append(FakeTransform())
        return built[-1]

    def capture(*args: object) -> RunResult:
        received.append(args[8])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "build_transform", build)
    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    assert received == built and received[0] is built[0]
    assert built_for == [project.resolve()]


def test_first_run_records_a_snapshot_for_every_table(project, sync, orchestrator, snapshot_store):
    assert invoke(project).exit_code == 0

    assert set(snapshot_store.shapes) == {
        ("shop_sync", "shop", "customers"),
        ("shop_sync", "shop", "orders"),
    }


def test_real_adapters_are_built_for_the_project(tmp_path, monkeypatch):
    monkeypatch.undo()
    sync = run_command.build_sync(tmp_path, "shop_sync", "postgres")
    orchestrator = run_command.build_orchestrator()
    store = run_command.build_snapshot_store(tmp_path, "postgres")
    clock = run_command.build_clock()
    transform = run_command.build_transform(tmp_path, "postgres")

    assert isinstance(transform, DbtTransform)
    assert isinstance(transform, Transform)
    assert transform.project_root == tmp_path
    assert transform.render_profile is render_postgres_profile
    assert isinstance(sync, DltSync)
    assert isinstance(sync, Sync)
    assert isinstance(orchestrator, DagsterOrchestrator)
    assert isinstance(orchestrator, Orchestrator)
    assert isinstance(store, WarehouseSnapshotStore)
    assert isinstance(store, SchemaSnapshotStore)
    assert isinstance(clock, SystemClock)
    assert isinstance(clock, Clock)
    assert vars(store) == {
        "_project": tmp_path,
        "_pipelines_dir": tmp_path / ".adops" / "dlt",
        "_warehouse_kind": "postgres",
    }
    assert vars(sync) == {
        "_project_dir": tmp_path,
        "_pipelines_dir": tmp_path / ".adops" / "dlt",
        "_pipeline": "shop_sync",
        "_source_kind": "postgres",
        "_warehouse_kind": "postgres",
    }


TWO_KINDS_SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
  ci:
    warehouse: {type: duckdb, path: warehouse.duckdb}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""


@pytest.mark.usefixtures("orchestrator")
@pytest.mark.parametrize(("env", "kind"), [("dev", "postgres"), ("ci", "duckdb")])
def test_every_adapter_is_built_for_the_chosen_environments_kind(
    project, sync, snapshot_store, monkeypatch, env, kind
):
    (project / "adops.yaml").write_text(TWO_KINDS_SETTINGS)
    built: list[tuple[str, str]] = []

    def build_sync(project_dir: Path, pipeline: str, kind: str) -> FakeSync:
        built.append(("sync", kind))
        return sync

    def build_store(project_dir: Path, kind: str) -> InMemorySnapshotStore:
        built.append(("store", kind))
        return snapshot_store

    def build_transform(project_dir: Path, kind: str) -> FakeTransform:
        built.append(("transform", kind))
        return FakeTransform()

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)
    monkeypatch.setattr(run_command, "build_transform", build_transform)

    result = invoke(project, "--env", env)

    assert result.exit_code == 0, result.output
    assert built == [("sync", kind), ("store", kind), ("transform", kind)]


def test_real_adapters_are_built_for_a_duckdb_warehouse(tmp_path):
    sync = run_command.build_sync(tmp_path, "shop_sync", "duckdb")
    store = REAL_BUILD_SNAPSHOT_STORE(tmp_path, "duckdb")
    transform = run_command.build_transform(tmp_path, "duckdb")

    assert vars(sync)["_warehouse_kind"] == "duckdb"
    assert vars(store)["_warehouse_kind"] == "duckdb"
    assert isinstance(transform, DbtTransform)
    assert transform.render_profile is render_duckdb_profile


DUCKDB_SETTINGS = """
environments:
  dev:
    warehouse: {{type: duckdb, path: {path}}}
    sources:
      shop: {{type: postgres, host: unused, port: 5432, database: "{source}", user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}}
"""

CUSTOMERS_PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - {name: customers, mode: full_refresh, primary_key: id}
"""


@pytest.fixture
def duckdb_project(project, monkeypatch):
    """A Project whose Source is a SQLite file and whose dev Warehouse is a DuckDB file."""
    source = project / "shop.db"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT)")
        connection.execute("INSERT INTO customers VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x')")
    (project / "pipelines" / "shop_sync.yaml").write_text(CUSTOMERS_PIPELINE)
    kinds: list[str] = []

    def sqlite_source_sync(project_dir: Path, pipeline: str, kind: str) -> DltSync:
        kinds.append(kind)
        return DltSync(project_dir, pipeline, source_kind="sqlite", warehouse_kind=kind)

    monkeypatch.setattr(run_command, "build_sync", sqlite_source_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", REAL_BUILD_SNAPSHOT_STORE)

    def use_warehouse_path(path: str) -> list[str]:
        (project / "adops.yaml").write_text(DUCKDB_SETTINGS.format(path=path, source=source))
        return kinds

    return use_warehouse_path


def test_run_on_duckdb_loads_the_project_warehouse_under_the_real_orchestrator(
    project, duckdb_project
):
    kinds = duckdb_project("warehouse.duckdb")

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert kinds == ["duckdb"]
    assert "step sync_shop: passed — customers 3 rows" in result.output
    with duckdb.connect(str(project / "warehouse.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM raw_shop.customers").fetchone() == (3,)


def test_unopenable_duckdb_file_fails_with_the_dedicated_message(project, duckdb_project):
    duckdb_project("missing/dir/wh.duckdb")

    result = invoke(project)

    assert_clean_failure(result)
    assert "Pipeline shop_sync, Step sync_shop: Warehouse is unreachable" in result.output
    for leaked in ("missing/dir", str(project), "IOException", "unexpected"):
        assert leaked not in result.output
    assert not (project / "missing").exists()
