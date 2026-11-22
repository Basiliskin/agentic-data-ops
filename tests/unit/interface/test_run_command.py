import os
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn

import duckdb
import pytest
from typer.testing import CliRunner

from adops.application.ports import (
    Clock,
    Orchestrator,
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    ReverseSyncRead,
    RunPlan,
    RunResult,
    SchemaSnapshotStore,
    StepReport,
    StepStatus,
    Sync,
    SyncResult,
    Transform,
    TransformResult,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import (
    InOrderOrchestrator,
    StepFingerprinting,
    current_fingerprints,
)
from adops.domain.accepted_change_record import AcceptedChangeRecord
from adops.domain.branch_environment import SchemaLayout
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline
from adops.domain.pipeline import SyncStep, SyncTable
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff, Row
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
from adops.infrastructure.postgres_reverse_sync import PostgresReverseSync
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore
from adops.infrastructure.step_fingerprint_store import ProjectTree, WarehouseFingerprintStore
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.interface import cli, run_command
from adops.interface.run_command import ProjectFileError

runner = CliRunner()

REAL_BUILD_SNAPSHOT_STORE = run_command.build_snapshot_store
REAL_BUILD_FINGERPRINT_STORE = run_command.build_fingerprint_store
REAL_BUILD_WAREHOUSE_LOCK = run_command.build_warehouse_lock
DEFAULT_LAYOUT = SchemaLayout.default()

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
    column_types: dict[str, dict[str, str]] = field(default_factory=dict)
    made_nullable: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.sources.append(source)
        return {
            table.name: SourceFacts(
                table_exists=table.name not in self.missing_tables,
                columns=COLUMNS[table.name],
                column_types=self.column_types.get(table.name, {}),
            )
            for table in step.tables
        }

    def make_columns_nullable(
        self,
        step: SyncStep,
        table: SyncTable,
        warehouse: ResolvedWarehouse,
        columns: tuple[str, ...],
    ) -> None:
        self.made_nullable.append((table.name, tuple(columns)))

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
    monkeypatch.setattr(run_command, "build_sync", lambda *_: fake)
    return fake


@dataclass
class InMemorySnapshotStore:
    shapes: dict[tuple[str, str, str], TableShape] = field(default_factory=dict)
    applied: set[str] = field(default_factory=set)

    def load(
        self, warehouse: ResolvedConnection, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        return self.shapes.get((pipeline, source, table))

    def applied_record_ids(self, warehouse: ResolvedConnection) -> frozenset[str]:
        return frozenset(self.applied)

    def save(
        self,
        warehouse: ResolvedConnection,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
        *,
        applied_record_ids: tuple[str, ...] = (),
    ) -> None:
        self.shapes[(pipeline, source, table)] = shape
        self.applied.update(applied_record_ids)


@pytest.fixture(autouse=True)
def snapshot_store(monkeypatch):
    store = InMemorySnapshotStore()
    monkeypatch.setattr(run_command, "build_snapshot_store", lambda *_: store)
    return store


@dataclass
class InMemoryFingerprintStore:
    saved: dict[str, dict[str, str]] = field(default_factory=dict)
    built_for: list[tuple[str, str, ResolvedWarehouse]] = field(default_factory=list)

    def load(self, pipeline: str) -> Mapping[str, str]:
        return dict(self.saved.get(pipeline, {}))

    def save(self, pipeline: str, step: str, fingerprint: str) -> None:
        self.saved.setdefault(pipeline, {})[step] = fingerprint


@pytest.fixture(autouse=True)
def fingerprint_store(monkeypatch):
    store = InMemoryFingerprintStore()

    def build(
        project_dir: Path, environment: str, kind: str, warehouse: ResolvedWarehouse
    ) -> InMemoryFingerprintStore:
        store.built_for.append((environment, kind, warehouse))
        return store

    monkeypatch.setattr(run_command, "build_fingerprint_store", build)
    return store


@pytest.fixture(autouse=True)
def lock(locks):
    return locks


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
def test_adops_run_goes_through_run_in_environment_with_the_built_orchestrator(
    project, orchestrator, monkeypatch
):
    calls: list[tuple[object, ...]] = []

    def capture(*args: object, lock_timeout: float) -> RunResult:
        calls.append((*args, lock_timeout))
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "run_in_environment", capture)

    assert invoke(project).exit_code == 0
    assert calls == [(project.resolve(), "shop_sync", "dev", orchestrator, DEFAULT_LOCK_TIMEOUT)]


def test_run_passes_the_project_snapshot_store_to_the_pipeline(project, monkeypatch):
    sentinel = InMemorySnapshotStore()
    built_for: list[Path] = []
    received: list[object] = []

    def build(project_dir: Path, environment: str, kind: str) -> InMemorySnapshotStore:
        built_for.append(project_dir)
        return sentinel

    def capture(*args: object, **kwargs: object) -> RunResult:
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

    def capture(*args: object, **kwargs: object) -> RunResult:
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

    def capture(*args: object, **kwargs: object) -> RunResult:
        received.append(args[8])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "build_transform", build)
    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    assert received == built and received[0] is built[0]
    assert built_for == [project.resolve()]


def test_run_passes_the_reverse_sync_built_for_the_project_to_the_pipeline(project, monkeypatch):
    built: list[FakeReverseSync] = []
    received: list[object] = []

    def build(project_dir: Path, environment: str, kind: str) -> FakeReverseSync:
        assert (project_dir, environment, kind) == (project.resolve(), "dev", "postgres")
        built.append(FakeReverseSync())
        return built[-1]

    def capture(*args: object, **kwargs: object) -> RunResult:
        received.append(args[9])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "build_reverse_sync", build)
    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    assert len(built) == 1 and received[0] is built[0]


def test_first_run_records_a_snapshot_for_every_table(project, sync, orchestrator, snapshot_store):
    assert invoke(project).exit_code == 0

    assert set(snapshot_store.shapes) == {
        ("shop_sync", "shop", "customers"),
        ("shop_sync", "shop", "orders"),
    }


def test_real_adapters_are_built_for_the_project(tmp_path, monkeypatch):
    monkeypatch.undo()
    sync = run_command.build_sync(tmp_path, "shop_sync", "stage", "postgres")
    orchestrator = run_command.build_orchestrator()
    store = run_command.build_snapshot_store(tmp_path, "stage", "postgres")
    clock = run_command.build_clock()
    transform = run_command.build_transform(tmp_path, "postgres")
    reverse_sync = run_command.build_reverse_sync(tmp_path, "stage", "postgres")

    assert isinstance(reverse_sync, PostgresReverseSync)
    assert vars(reverse_sync) == {
        "_project_dir": tmp_path,
        "_pipelines_dir": tmp_path / ".adops" / "dlt" / "stage",
        "_warehouse_kind": "postgres",
        "_analytics_schema": "analytics",
        "_result_rows": {},
    }
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
        "_pipelines_dir": tmp_path / ".adops" / "dlt" / "stage",
        "_warehouse_kind": "postgres",
        "_layout": SchemaLayout.default(),
    }
    assert vars(sync) == {
        "_project_dir": tmp_path,
        "_layout": SchemaLayout.default(),
        "_pipelines_dir": tmp_path / ".adops" / "dlt" / "stage",
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
    built: list[tuple[str, ...]] = []

    def build_sync(project_dir: Path, pipeline: str, environment: str, kind: str) -> FakeSync:
        built.append(("sync", environment, kind))
        return sync

    def build_store(project_dir: Path, environment: str, kind: str) -> InMemorySnapshotStore:
        built.append(("store", environment, kind))
        return snapshot_store

    def build_transform(project_dir: Path, kind: str) -> FakeTransform:
        built.append(("transform", kind))
        return FakeTransform()

    def build_reverse_sync(project_dir: Path, environment: str, kind: str) -> FakeReverseSync:
        built.append(("reverse_sync", environment, kind))
        return FakeReverseSync()

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)
    monkeypatch.setattr(run_command, "build_transform", build_transform)
    monkeypatch.setattr(run_command, "build_reverse_sync", build_reverse_sync)

    result = invoke(project, "--env", env)

    assert result.exit_code == 0, result.output
    assert built == [
        ("sync", env, kind),
        ("store", env, kind),
        ("transform", kind),
        ("reverse_sync", env, kind),
    ]


def test_real_adapters_are_built_for_a_duckdb_warehouse(tmp_path):
    sync = run_command.build_sync(tmp_path, "shop_sync", "dev", "duckdb")
    store = REAL_BUILD_SNAPSHOT_STORE(tmp_path, "dev", "duckdb")
    transform = run_command.build_transform(tmp_path, "duckdb")
    reverse_sync = run_command.build_reverse_sync(tmp_path, "dev", "duckdb")

    assert isinstance(reverse_sync, PostgresReverseSync)
    assert vars(reverse_sync)["_warehouse_kind"] == "duckdb"
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

    def sqlite_source_sync(project_dir: Path, pipeline: str, environment: str, kind: str):
        kinds.append(kind)
        return DltSync(
            project_dir,
            pipeline,
            environment=environment,
            source_kind="sqlite",
            warehouse_kind=kind,
        )

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


VALUE_SENTINEL = "nonkey-value-sentinel"
TARGET_SENTINEL = "s3cr3t-crm-target-sentinel"

CRM_TARGET = """
    targets:
      crm: {type: postgres, host: crm.internal, port: 5440, database: crm, user: crm_writer,
            password_env: ADOPS_TEST_CRM_PASSWORD}
"""

REVERSE_SETTINGS = f"""
environments:
  dev:
    warehouse: {{type: duckdb, path: dev.duckdb}}
    sources: {{}}{CRM_TARGET}
  stage:
    warehouse: {{type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}}
    sources: {{}}{CRM_TARGET}
"""


def reverse_pipeline(delete_missing: bool = False) -> str:
    return f"""
name: shop_sync
steps:
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping: {{order_count: orders, total_amount: total}}
    delete_missing: {str(delete_missing).lower()}
    max_change_share: 1
"""


RESULT_ROWS: tuple[Row, ...] = (
    {"customer_id": 1, "order_count": 3, "total_amount": VALUE_SENTINEL},
    {"customer_id": 2, "order_count": 9, "total_amount": "b"},
    {"customer_id": 3, "order_count": 2, "total_amount": "c"},
)

TARGET_ROWS: tuple[Row, ...] = (
    {"customer_id": 1, "orders": 3, "total": VALUE_SENTINEL},
    {"customer_id": 2, "orders": 0, "total": "b"},
    {"customer_id": 4, "orders": 1, "total": VALUE_SENTINEL},
    {"customer_id": 5, "orders": 1, "total": "e"},
)


@dataclass
class FakeReverseSync:
    result_rows: tuple[Row, ...] = RESULT_ROWS
    target_rows: tuple[Row, ...] = TARGET_ROWS
    null_keys: int = 0
    targets: list[ResolvedConnection] = field(default_factory=list)
    applied: list[Diff] = field(default_factory=list)

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead:
        self.targets.append(target)
        return ReverseSyncRead(
            result_rows=self.result_rows,
            target_rows=self.target_rows,
            target_table_exists=True,
            missing_target_columns=(),
            duplicate_keys=0,
            null_keys=self.null_keys,
        )

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        self.applied.append(diff)


@pytest.fixture
def reverse_sync(project, monkeypatch):
    (project / "adops.yaml").write_text(REVERSE_SETTINGS)
    (project / "pipelines" / "shop_sync.yaml").write_text(reverse_pipeline())
    monkeypatch.setenv("ADOPS_TEST_CRM_PASSWORD", TARGET_SENTINEL)
    fake = FakeReverseSync()
    monkeypatch.setattr(run_command, "build_reverse_sync", lambda *_: fake)
    return fake


def reverse_lines(result) -> list[str]:
    assert result.exit_code == 0, result.output
    for leaked in (VALUE_SENTINEL, TARGET_SENTINEL, SENTINEL, "Diff(", "RowUpdate("):
        assert leaked not in result.output
    return result.output.splitlines()


@pytest.mark.usefixtures("sync", "orchestrator")
def test_dev_prints_a_preview_with_kept_rows_and_writes_nothing(project, reverse_sync):
    lines = reverse_lines(invoke(project))

    assert lines[0] == "step push_stats: passed"
    preview = lines.index(
        "  Preview: would insert 1, would update 1, kept 2 (absent from dbt result)"
    )
    assert lines[preview + 1 :] == [
        "  sample insert keys: 3",
        "  sample update keys: 2 (orders)",
        "  sample kept keys: 4, 5",
        "Pipeline shop_sync passed in Environment dev.",
    ]
    assert not any("wrote" in line or "would delete" in line for line in lines)
    assert reverse_sync.applied == []
    assert reverse_sync.targets[0].password == TARGET_SENTINEL


@pytest.mark.usefixtures("sync", "orchestrator")
def test_dev_preview_names_deletes_when_the_step_deletes_missing_rows(project, reverse_sync):
    (project / "pipelines" / "shop_sync.yaml").write_text(reverse_pipeline(delete_missing=True))

    lines = reverse_lines(invoke(project))

    assert "  Preview: would insert 1, would update 1, would delete 2" in lines
    assert "  sample delete keys: 4, 5" in lines
    assert not any("wrote" in line or "kept" in line for line in lines)
    assert reverse_sync.applied == []


@pytest.mark.usefixtures("sync", "orchestrator")
@pytest.mark.parametrize(
    ("delete_missing", "summary"),
    [
        (True, "  wrote: inserted 1, updated 1, deleted 2"),
        (False, "  wrote: inserted 1, updated 1, kept 2 (absent from dbt result)"),
    ],
)
def test_stage_prints_the_write_summary_and_no_preview(
    project, reverse_sync, delete_missing, summary
):
    (project / "pipelines" / "shop_sync.yaml").write_text(reverse_pipeline(delete_missing))

    lines = reverse_lines(invoke(project, "--env", "stage"))

    assert summary in lines
    assert not any("Preview" in line or "would" in line for line in lines)
    assert len(reverse_sync.applied) == 1
    assert lines[-1] == "Pipeline shop_sync passed in Environment stage."


@pytest.mark.usefixtures("sync", "orchestrator")
def test_stage_run_stopped_by_a_check_prints_neither_wrote_nor_preview(project, reverse_sync):
    reverse_sync.null_keys = 1

    result = invoke(project, "--env", "stage")

    assert_clean_failure(result)
    assert VALUE_SENTINEL not in result.output
    assert not any(word in result.output for word in ("wrote", "Preview", "sample"))
    assert reverse_sync.applied == []


@pytest.mark.usefixtures("sync", "orchestrator")
def test_preview_sample_shows_at_most_five_keys_per_kind(project, reverse_sync):
    reverse_sync.result_rows = tuple(
        {"customer_id": key, "order_count": key, "total_amount": VALUE_SENTINEL}
        for key in range(10, 17)
    )
    reverse_sync.target_rows = ()

    lines = reverse_lines(invoke(project))

    assert "  Preview: would insert 7, would update 0, would delete 0" in lines
    assert "  sample insert keys: 10, 11, 12, 13, 14" in lines
    assert not any(line.startswith(("  sample update", "  sample kept")) for line in lines)


@pytest.mark.usefixtures("sync", "orchestrator")
def test_update_sample_lists_every_changed_column_name_without_values(project, reverse_sync):
    reverse_sync.result_rows = (
        {"customer_id": 1, "order_count": 4, "total_amount": "changed-total"},
        {"customer_id": 2, "order_count": 0, "total_amount": "b"},
    )
    reverse_sync.target_rows = (
        {"customer_id": 1, "orders": 3, "total": VALUE_SENTINEL},
        {"customer_id": 2, "orders": 0, "total": "c"},
    )

    lines = reverse_lines(invoke(project))

    assert "  sample update keys: 1 (orders, total); 2 (total)" in lines
    assert "changed-total" not in "\n".join(lines)


STAGE_SECRET = "S3cr3t-xyz-stage-warehouse"

BRANCH_SETTINGS = f"""
environments:
  dev:
    warehouse: {{type: duckdb, path: dev.duckdb}}
    sources:
      shop: {{type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}}{CRM_TARGET}
  stage:
    warehouse: {{type: postgres, host: stage-warehouse, port: 5434, database: w, user: w,
                password_env: ADOPS_TEST_STAGE_PASSWORD}}
    sources:
      shop: {{type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}}{CRM_TARGET}
"""

REVERSE_STEP = """
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping: {order_count: orders, total_amount: total}
    max_change_share: 1
"""

BRANCH_PIPELINE = PIPELINE + TRANSFORM_STEP + REVERSE_STEP

SOURCES_YML = """
sources:
  - name: shop
    schema: "{{ var('adops_raw_schema_shop', 'raw_shop') }}"
"""


@dataclass
class BranchBuilds:
    sync: list[tuple[str, str, SchemaLayout]] = field(default_factory=list)
    store: list[tuple[str, str, SchemaLayout]] = field(default_factory=list)
    transform: list[tuple[str, str, Mapping[str, str] | None]] = field(default_factory=list)
    reverse_sync: list[tuple[str, str, str]] = field(default_factory=list)
    transforms: list[FakeTransform] = field(default_factory=list)
    reverse_syncs: list[FakeReverseSync] = field(default_factory=list)


def stage_fingerprints(project: Path, pipeline_text: str) -> dict[str, str]:
    steps = load_pipeline(pipeline_text).steps
    return dict(current_fingerprints(steps, ProjectTree(project)))


@pytest.fixture
def branch_project(project, sync, orchestrator, snapshot_store, fingerprint_store, monkeypatch):
    (project / "adops.yaml").write_text(BRANCH_SETTINGS)
    (project / "pipelines" / "shop_sync.yaml").write_text(BRANCH_PIPELINE)
    (project / "dbt" / "models").mkdir(parents=True)
    (project / "dbt" / "models" / "sources.yml").write_text(SOURCES_YML)
    (project / "dbt" / "models" / "customer_stats.sql").write_text("select 1 as customer_id\n")
    monkeypatch.setenv("ADOPS_TEST_STAGE_PASSWORD", STAGE_SECRET)
    monkeypatch.setenv("ADOPS_TEST_CRM_PASSWORD", TARGET_SENTINEL)
    builds = BranchBuilds()

    def build_sync(
        project_dir: Path,
        pipeline: str,
        environment: str,
        kind: str,
        *,
        layout: SchemaLayout = DEFAULT_LAYOUT,
    ) -> FakeSync:
        builds.sync.append((environment, kind, layout))
        return sync

    def build_store(
        project_dir: Path,
        environment: str,
        kind: str,
        *,
        layout: SchemaLayout = DEFAULT_LAYOUT,
    ) -> InMemorySnapshotStore:
        builds.store.append((environment, kind, layout))
        return snapshot_store

    def build_transform(
        project_dir: Path,
        kind: str,
        *,
        target_schema: str = "analytics",
        raw_schemas: Mapping[str, str] | None = None,
    ) -> FakeTransform:
        builds.transform.append((kind, target_schema, raw_schemas))
        builds.transforms.append(FakeTransform())
        return builds.transforms[-1]

    def build_reverse_sync(
        project_dir: Path, environment: str, kind: str, *, analytics_schema: str = "analytics"
    ) -> FakeReverseSync:
        builds.reverse_sync.append((environment, kind, analytics_schema))
        builds.reverse_syncs.append(FakeReverseSync())
        return builds.reverse_syncs[-1]

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)
    monkeypatch.setattr(run_command, "build_transform", build_transform)
    monkeypatch.setattr(run_command, "build_reverse_sync", build_reverse_sync)
    return builds


def assert_no_secret(result):
    for leaked in (STAGE_SECRET, SENTINEL, TARGET_SENTINEL, "Traceback"):
        assert leaked not in result.output


def stage_ran(project: Path, store: InMemoryFingerprintStore, pipeline_text: str) -> None:
    store.saved["shop_sync"] = stage_fingerprints(project, pipeline_text)


def test_branch_with_a_changed_transform_rebuilds_from_it_reading_stage_raw(
    project, branch_project, fingerprint_store
):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    stage_saved = dict(fingerprint_store.saved["shop_sync"])
    (project / "dbt" / "models" / "customer_stats.sql").write_text("select 2 as customer_id\n")

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert_no_secret(result)
    branch = SchemaLayout.for_branch("br_feat")
    assert branch_project.sync == [("br_feat", "postgres", branch)]
    assert branch_project.store == [("br_feat", "postgres", branch)]
    assert branch_project.transform == [("postgres", "br_feat__analytics", {"shop": "raw_shop"})]
    assert branch_project.reverse_sync == [("br_feat", "postgres", "br_feat__analytics")]
    [(store_env, store_kind, store_warehouse)] = fingerprint_store.built_for
    assert (store_env, store_kind) == ("stage", "postgres")
    assert isinstance(store_warehouse, ResolvedConnection)
    assert (store_warehouse.host, store_warehouse.port) == ("stage-warehouse", 5434)
    [transform] = branch_project.transforms
    [warehouse] = transform.warehouses
    assert isinstance(warehouse, ResolvedConnection)
    assert (warehouse.host, warehouse.port) == ("stage-warehouse", 5434)
    assert branch_project.reverse_syncs[0].applied == []
    lines = result.output.splitlines()
    assert lines[0] == "step sync_shop: deferred to stage"
    assert lines[1] == "step transform_shop: passed"
    assert "  Preview: would insert 1, would update 1, kept 2 (absent from dbt result)" in lines
    assert lines[-1] == "Pipeline shop_sync passed in Environment br_feat."
    assert "uses stage" not in result.output
    assert fingerprint_store.saved["shop_sync"] == stage_saved


def test_branch_with_a_changed_sync_reads_its_own_raw_schema_downstream(
    project, branch_project, fingerprint_store
):
    edited = BRANCH_PIPELINE.replace("mode: full_refresh", "mode: incremental\n        cursor: x")
    stage_ran(project, fingerprint_store, edited)

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert_no_secret(result)
    assert branch_project.sync == [("br_feat", "postgres", SchemaLayout.for_branch("br_feat"))]
    assert branch_project.transform == [
        ("postgres", "br_feat__analytics", {"shop": "br_feat__raw_shop"})
    ]
    assert branch_project.reverse_sync == [("br_feat", "postgres", "br_feat__analytics")]
    assert result.output.splitlines()[0] == (
        "step sync_shop: passed — customers 20 rows, orders 2 rows"
    )
    assert "deferred to stage" not in result.output


def test_branch_with_only_the_reverse_sync_changed_reads_stage_analytics(
    project, branch_project, fingerprint_store
):
    edited = BRANCH_PIPELINE.replace("max_change_share: 1", "max_change_share: 0.5")
    stage_ran(project, fingerprint_store, edited)

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert branch_project.transform == []
    assert branch_project.reverse_sync == [("br_feat", "postgres", "analytics")]


def test_branch_matching_stage_defers_every_step_and_exits_zero(
    project, branch_project, fingerprint_store, orchestrator
):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)

    result = invoke(project, "--branch", "feature/Feat")

    assert result.exit_code == 0, result.output
    assert_no_secret(result)
    assert result.output.splitlines() == [
        "step sync_shop: deferred to stage",
        "step transform_shop: deferred to stage",
        "step push_stats: deferred to stage",
        "Nothing changed since stage's last run; br_feature_feat uses stage.",
        "Pipeline shop_sync passed in Environment br_feature_feat.",
    ]
    assert orchestrator.environ == []
    assert branch_project.transform == [] and branch_project.reverse_sync == []


def test_branch_before_any_stage_run_asks_for_a_stage_run(project, branch_project):
    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert_no_secret(result)
    assert result.output.strip() == (
        "Pipeline shop_sync: stage has no Step Fingerprints; run the Pipeline in stage first"
    )


@pytest.mark.usefixtures("sync", "orchestrator")
def test_branch_without_a_stage_environment_says_so(project):
    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert result.output.strip() == (
        "environments.stage: a Branch run needs a stage Environment in adops.yaml, and none is set"
    )


def test_branch_name_without_letters_or_digits_is_rejected(project, branch_project):
    result = invoke(project, "--branch", "///")

    assert_clean_failure(result)
    assert "--branch: the branch name has no letters or digits" in result.output


def test_branch_names_the_missing_stage_warehouse_secret(project, branch_project, monkeypatch):
    monkeypatch.delenv("ADOPS_TEST_STAGE_PASSWORD")

    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert "ADOPS_TEST_STAGE_PASSWORD" in result.output


def test_branch_unexpected_error_prints_only_its_type(
    project, branch_project, fingerprint_store, monkeypatch
):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    (project / "dbt" / "models" / "customer_stats.sql").write_text("select 2 as customer_id\n")

    def explode(*args: object, **kwargs: object) -> FakeTransform:
        raise RuntimeError(f"cannot reach postgres://w:{STAGE_SECRET}@stage-warehouse")

    monkeypatch.setattr(run_command, "build_transform", explode)

    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert_no_secret(result)
    assert result.output.strip() == "unexpected RuntimeError while running shop_sync"


@pytest.mark.parametrize("env", ["stage", "dev"])
def test_branch_cannot_be_combined_with_env(project, branch_project, env):
    result = invoke(project, "--env", env, "--branch", "feat")

    assert result.exit_code == 2
    assert "--branch" in result.output and "--env" in result.output
    assert_no_secret(result)
    assert branch_project.sync == []


def test_stage_run_saves_fingerprints_in_step_order(project, branch_project, fingerprint_store):
    result = invoke(project, "--env", "stage")

    assert result.exit_code == 0, result.output
    [(environment, kind, warehouse)] = fingerprint_store.built_for
    assert (environment, kind) == ("stage", "postgres")
    assert isinstance(warehouse, ResolvedConnection) and warehouse.host == "stage-warehouse"
    assert list(fingerprint_store.saved["shop_sync"].items()) == list(
        stage_fingerprints(project, BRANCH_PIPELINE).items()
    )
    assert branch_project.sync == [("stage", "postgres", SchemaLayout.default())]


def test_dev_run_builds_a_fingerprint_store_but_saves_nothing(
    project, branch_project, fingerprint_store
):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert [(env, kind) for env, kind, _ in fingerprint_store.built_for] == [("dev", "duckdb")]
    assert fingerprint_store.saved == {}


@pytest.mark.usefixtures("sync", "orchestrator", "snapshot_store")
def test_run_passes_fingerprinting_for_the_project_to_the_pipeline(
    project, fingerprint_store, monkeypatch
):
    received: list[object] = []

    def capture(*args: object, **kwargs: object) -> RunResult:
        received.append(args[10])
        return RunResult("shop_sync", "dev", ())

    monkeypatch.setattr(run_command, "run_pipeline", capture)

    assert invoke(project).exit_code == 0
    [fingerprinting] = received
    assert isinstance(fingerprinting, StepFingerprinting)
    assert fingerprinting.store is fingerprint_store
    assert fingerprinting.files == ProjectTree(project.resolve())


def test_real_adapters_take_branch_schemas(tmp_path, monkeypatch):
    monkeypatch.undo()
    layout = SchemaLayout.for_branch("br_feat")
    warehouse = ResolvedDuckDb("w.duckdb")

    sync = run_command.build_sync(tmp_path, "shop_sync", "br_feat", "duckdb", layout=layout)
    store = REAL_BUILD_SNAPSHOT_STORE(tmp_path, "br_feat", "duckdb", layout=layout)
    transform = run_command.build_transform(
        tmp_path, "duckdb", target_schema=layout.analytics, raw_schemas={"shop": "raw_shop"}
    )
    reverse_sync = run_command.build_reverse_sync(
        tmp_path, "br_feat", "duckdb", analytics_schema=layout.analytics
    )
    fingerprints = REAL_BUILD_FINGERPRINT_STORE(tmp_path, "stage", "duckdb", warehouse)

    assert vars(sync)["_pipelines_dir"] == tmp_path / ".adops" / "dlt" / "br_feat"
    assert vars(sync)["_layout"] == layout
    assert vars(store)["_layout"] == layout
    assert isinstance(transform, DbtTransform)
    assert (transform.target_schema, transform.raw_schemas) == (
        "br_feat__analytics",
        {"shop": "raw_shop"},
    )
    assert vars(reverse_sync)["_analytics_schema"] == "br_feat__analytics"
    assert isinstance(fingerprints, WarehouseFingerprintStore)
    assert vars(fingerprints)["_warehouse"] == warehouse
    assert vars(fingerprints)["_pipelines_dir"] == tmp_path / ".adops" / "dlt" / "stage"
    assert run_command.build_project_files(tmp_path) == ProjectTree(tmp_path)


def test_step_reverse_syncs_apply_with_the_steps_own_engine():
    engine = FakeReverseSync()
    step = load_pipeline(BRANCH_PIPELINE).steps[2]
    assert isinstance(step, ReverseSyncStep)
    diff = Diff("customer_id", inserts=(), updates=(), deletes=(), kept=(), delete_missing=False)
    target = ResolvedConnection("crm.internal", 5440, "crm", "crm_writer", "unused")

    run_command.StepReverseSyncs({"push_stats": engine}).apply(step, target, diff)

    assert engine.applied == [diff]


ALL_PIPELINES = ("c_pipe", "a_pipe", "b_pipe")


@pytest.fixture
def run_project_calls(project, monkeypatch):
    for name in ALL_PIPELINES:
        (project / "pipelines" / f"{name}.yaml").write_text(PIPELINE)
    (project / "pipelines" / "shop_sync.yaml").unlink()
    (project / "pipelines" / "README.md").write_text("not a Pipeline")
    (project / "pipelines" / "old.yml").write_text(PIPELINE)
    calls: list[tuple[Path, str, str, str | None]] = []
    failing: set[str] = set()

    def fake_run_project(
        project_dir: Path, pipeline: str, environment: str, branch: str | None = None
    ) -> RunResult:
        calls.append((project_dir, pipeline, environment, branch))
        status = StepStatus.FAILED if pipeline in failing else StepStatus.PASSED
        return RunResult(pipeline, branch or environment, (StepReport("sync_shop", status),))

    monkeypatch.setattr(run_command, "run_project", fake_run_project)
    return calls, failing


WIDE = {"COLUMNS": "200"}


def invoke_all(project, *args):
    return runner.invoke(cli.app, ["run", "--all", "--project", str(project), *args], env=WIDE)


def test_run_all_runs_every_pipeline_in_name_order(project, run_project_calls):
    calls, _ = run_project_calls

    result = invoke_all(project)

    assert result.exit_code == 0, result.output
    assert [pipeline for _, pipeline, _, _ in calls] == ["a_pipe", "b_pipe", "c_pipe"]
    for name in ("a_pipe", "b_pipe", "c_pipe"):
        assert f"Pipeline {name} passed in Environment dev." in result.output


def test_run_all_stops_at_the_first_failed_pipeline(project, run_project_calls):
    calls, failing = run_project_calls
    failing.add("b_pipe")

    result = invoke_all(project)

    assert result.exit_code == 1
    assert [pipeline for _, pipeline, _, _ in calls] == ["a_pipe", "b_pipe"]
    passed = result.output.index("Pipeline a_pipe passed in Environment dev.")
    assert passed < result.output.index("Pipeline b_pipe failed in Environment dev.")
    assert "c_pipe" not in result.output


def test_run_all_stops_at_a_pipeline_that_raises(project, run_project_calls, monkeypatch):
    calls, _ = run_project_calls
    fake = run_command.run_project

    def raise_on_b(project_dir: Path, pipeline: str, environment: str, branch=None) -> RunResult:
        if pipeline == "b_pipe":
            calls.append((project_dir, pipeline, environment, branch))
            raise ProjectFileError("pipelines/b_pipe.yaml: file not found")
        return fake(project_dir, pipeline, environment, branch)

    monkeypatch.setattr(run_command, "run_project", raise_on_b)

    result = invoke_all(project)

    assert_clean_failure(result)
    assert "pipelines/b_pipe.yaml: file not found" in result.output
    assert [pipeline for _, pipeline, _, _ in calls] == ["a_pipe", "b_pipe"]


def test_run_all_passes_env_to_every_pipeline(project, run_project_calls):
    calls, _ = run_project_calls

    result = invoke_all(project, "--env", "stage")

    assert result.exit_code == 0, result.output
    assert [(env, branch) for _, _, env, branch in calls] == [("stage", None)] * 3


def test_run_all_passes_branch_to_every_pipeline(project, run_project_calls):
    calls, _ = run_project_calls

    result = invoke_all(project, "--branch", "feat-x")

    assert result.exit_code == 0, result.output
    assert [(env, branch) for _, _, env, branch in calls] == [("dev", "feat-x")] * 3
    assert {project_dir for project_dir, _, _, _ in calls} == {project.resolve()}


def test_run_all_uses_the_single_pipeline_default_environment(project, run_project_calls):
    calls, _ = run_project_calls

    assert invoke_all(project).exit_code == 0
    assert runner.invoke(cli.app, ["run", "a_pipe", "--project", str(project)]).exit_code == 0

    defaults = {(env, branch) for _, _, env, branch in calls}
    assert defaults == {("dev", None)}
    assert len(calls) == 4


def test_run_all_with_a_pipeline_name_is_rejected(project, run_project_calls):
    calls, _ = run_project_calls

    result = runner.invoke(cli.app, ["run", "a_pipe", "--all", "--project", str(project)], env=WIDE)

    assert result.exit_code != 0
    assert "--all" in result.output
    assert "cannot be combined with a Pipeline name" in result.output
    assert calls == []


def test_run_without_a_pipeline_or_all_is_rejected(project, run_project_calls):
    calls, _ = run_project_calls

    result = runner.invoke(cli.app, ["run", "--project", str(project)], env=WIDE)

    assert result.exit_code != 0
    assert "give a Pipeline name, or --all to run every Pipeline" in result.output
    assert "Traceback" not in result.output
    assert calls == []


def test_run_all_with_env_and_branch_keeps_the_conflict_error(project, run_project_calls):
    calls, _ = run_project_calls

    result = invoke_all(project, "--env", "stage", "--branch", "x")

    assert result.exit_code != 0
    assert "cannot be combined with --env" in result.output
    assert calls == []


@pytest.mark.parametrize("folder", ["empty", "missing"])
def test_run_all_without_pipelines_fails_plainly(project, run_project_calls, folder):
    calls, _ = run_project_calls
    for path in (project / "pipelines").iterdir():
        path.unlink()
    if folder == "missing":
        (project / "pipelines").rmdir()

    result = invoke_all(project)

    assert_clean_failure(result)
    assert "No Pipelines in pipelines/" in result.output
    assert calls == []


def test_pipeline_files_are_the_yaml_files_in_name_order(tmp_path):
    (tmp_path / "pipelines").mkdir()
    for name in ("b.yaml", "a_b.yaml", "a.yaml", "c.yml", "notes.md"):
        (tmp_path / "pipelines" / name).write_text("")

    files = run_command.pipeline_files(tmp_path)

    assert [path.name for path in files] == ["a.yaml", "a_b.yaml", "b.yaml"]


STAGE_WAREHOUSE = ResolvedConnection(
    host="stage-warehouse", port=5434, database="w", user="w", password=STAGE_SECRET
)


@dataclass
class MarkingOrchestrator:
    """Marks where the Warehouse work happens relative to the fake lock's entry and exit."""

    locks: object
    error: Exception | None = None

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        self.locks.mark("work")
        if self.error is not None:
            raise self.error
        return InOrderOrchestrator().execute(plan, sync)


@pytest.fixture
def marking(lock, monkeypatch):
    fake = MarkingOrchestrator(lock)
    monkeypatch.setattr(run_command, "build_orchestrator", lambda: fake)
    return fake


@pytest.fixture
def stage_project(project, monkeypatch):
    (project / "adops.yaml").write_text(BRANCH_SETTINGS)
    monkeypatch.setenv("ADOPS_TEST_STAGE_PASSWORD", STAGE_SECRET)
    return project


@pytest.mark.usefixtures("sync")
def test_a_run_holds_the_lock_on_its_environments_warehouse_around_the_work(
    stage_project, lock, marking
):
    result = invoke(stage_project, "--env", "stage")

    assert result.exit_code == 0, result.output
    assert lock.events == [("enter", "stage"), ("work",), ("exit", "stage")]
    assert lock.warehouses == [STAGE_WAREHOUSE]


@pytest.mark.usefixtures("sync")
def test_a_failing_run_still_releases_the_lock(stage_project, lock, marking):
    marking.error = RuntimeError(STAGE_SECRET)

    result = invoke(stage_project, "--env", "stage")

    assert_clean_failure(result)
    assert lock.events == [("enter", "stage"), ("work",), ("exit", "stage")]


def test_a_branch_run_locks_stages_warehouse(project, branch_project, fingerprint_store, lock):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    (project / "dbt" / "models" / "customer_stats.sql").write_text("select 2 as customer_id\n")

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert lock.events == [("enter", "stage"), ("exit", "stage")]
    assert lock.warehouses == [STAGE_WAREHOUSE]


def test_a_failing_branch_run_still_releases_the_lock(
    project, branch_project, fingerprint_store, lock, monkeypatch
):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    (project / "dbt" / "models" / "customer_stats.sql").write_text("select 2 as customer_id\n")

    def explode(*args: object, **kwargs: object) -> FakeTransform:
        lock.mark("work")
        raise RuntimeError(STAGE_SECRET)

    monkeypatch.setattr(run_command, "build_transform", explode)

    assert_clean_failure(invoke(project, "--branch", "feat"))
    assert lock.events == [("enter", "stage"), ("work",), ("exit", "stage")]


def test_a_branch_run_reads_stages_fingerprints_inside_the_lock(
    project, branch_project, fingerprint_store, lock, monkeypatch
):
    def load(pipeline: str) -> Mapping[str, str]:
        lock.mark("load fingerprints")
        return {}

    monkeypatch.setattr(fingerprint_store, "load", load)

    assert_clean_failure(invoke(project, "--branch", "feat"))
    assert lock.events == [("enter", "stage"), ("load fingerprints",), ("exit", "stage")]


LOCKED_MESSAGE = "Another run is using the stage Warehouse; gave up after 30 minutes."


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_lock_timeout_is_one_plain_line_and_exit_one(stage_project, lock):
    lock.error = WarehouseLocked("stage", 30 * 60)

    result = invoke(stage_project, "--env", "stage")

    assert_clean_failure(result)
    assert result.output == LOCKED_MESSAGE + "\n"
    for leaked in (STAGE_SECRET, "stage-warehouse", str(stage_project)):
        assert leaked not in result.output


def test_a_branch_lock_timeout_names_stage(project, branch_project, fingerprint_store, lock):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    lock.error = WarehouseLocked("stage", 30 * 60)

    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert result.output == LOCKED_MESSAGE + "\n"


def test_warehouse_locked_is_a_run_error():
    assert WarehouseLocked in run_command.RUN_ERRORS


@pytest.mark.usefixtures("sync", "orchestrator")
def test_an_unreachable_warehouse_at_lock_time_fails_the_first_step_retryably(
    stage_project, lock, monkeypatch
):
    lock.error = WarehouseUnreachable()
    (stage_project / "pipelines" / "shop_sync.yaml").write_text(PIPELINE + TRANSFORM_STEP)

    result = run_command.run_project(stage_project, "shop_sync", "stage")

    assert [(step.name, step.status) for step in result.steps] == [
        ("sync_shop", StepStatus.FAILED),
        ("transform_shop", StepStatus.SKIPPED),
    ]
    assert result.steps[0].error == "Pipeline shop_sync, Step sync_shop: Warehouse is unreachable"
    assert result.retryable


def test_an_unreachable_stage_warehouse_fails_a_branch_run_retryably(project, branch_project, lock):
    lock.error = WarehouseUnreachable()

    result = run_command.run_project(project, "shop_sync", "dev", "feat")

    assert result.environment == "br_feat"
    assert result.steps[0].status is StepStatus.FAILED
    assert result.retryable


def test_the_real_warehouse_lock_is_built(tmp_path):
    held = REAL_BUILD_WAREHOUSE_LOCK(ResolvedDuckDb("stage.duckdb"), tmp_path, "stage")

    with held:
        assert (tmp_path / "stage.duckdb.lock").is_file()


def run_log_texts(project: Path, environment: str = "dev") -> list[str]:
    directory = project / ".adops" / "logs" / environment
    if not directory.exists():
        return []
    return [path.read_text(encoding="utf-8") for path in sorted(directory.iterdir())]


def the_run_log(project: Path, environment: str = "dev") -> str:
    texts = run_log_texts(project, environment)
    assert len(texts) == 1, texts
    return texts[0]


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_successful_run_logs_exactly_what_the_cli_printed(project):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert the_run_log(project) == result.output
    assert result.output.splitlines()[-1] == "Pipeline shop_sync passed in Environment dev."


@pytest.mark.usefixtures("orchestrator")
def test_a_failed_step_logs_exactly_what_the_cli_printed(project, sync):
    sync.sync_error = RuntimeError(f"connection refused for password {SENTINEL}")

    result = invoke(project)

    assert_clean_failure(result)
    log = the_run_log(project)
    assert log == result.output
    assert "Step sync_shop: RuntimeError: connection refused for password ***" in log
    assert log.endswith("Pipeline shop_sync failed in Environment dev.\n")


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_preview_logs_exactly_what_the_cli_printed(project, reverse_sync):
    result = invoke(project)

    reverse_lines(result)
    log = the_run_log(project)
    assert log == result.output
    assert "  Preview: would insert 1, would update 1, kept 2 (absent from dbt result)\n" in log
    assert TARGET_SENTINEL not in log


@dataclass
class LeakingOrchestrator:
    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        error = f"Step sync_shop: password {SENTINEL} rejected"
        failed = StepReport("sync_shop", StepStatus.FAILED, error=error)
        return RunResult(plan.pipeline, plan.environment, (failed,))


@pytest.mark.usefixtures("sync")
def test_a_run_log_masks_every_project_secret_value(project, monkeypatch):
    monkeypatch.setattr(run_command, "build_orchestrator", LeakingOrchestrator)

    run_command.run_project(project, "shop_sync", "dev")

    log = the_run_log(project)
    assert SENTINEL not in log
    assert "  Step sync_shop: password *** rejected\n" in log


EARLY_FAILURES = {
    "missing secret": "not set or are empty: ADOPS_TEST_SHOP_PASSWORD",
    "invalid yaml": "(root): not valid YAML (line 2, column 1)",
    "unreachable warehouse": "Pipeline shop_sync, Step sync_shop: Warehouse is unreachable",
    "held lock": "Another run is using the dev Warehouse; gave up after 0 minutes.",
}


@pytest.mark.parametrize("failure", sorted(EARLY_FAILURES))
@pytest.mark.usefixtures("sync", "orchestrator")
def test_an_early_failure_writes_one_run_log_with_the_clean_message(
    project, lock, monkeypatch, failure
):
    if failure == "missing secret":
        monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")
    elif failure == "invalid yaml":
        (project / "adops.yaml").write_text("environments: [dev\n")
    elif failure == "unreachable warehouse":
        lock.error = WarehouseUnreachable()
    else:
        lock.error = WarehouseLocked("dev", 0)

    result = invoke(project)

    assert_clean_failure(result)
    log = the_run_log(project)
    assert log == result.output
    assert EARLY_FAILURES[failure] in log
    assert SENTINEL not in log


@pytest.mark.usefixtures("sync", "orchestrator")
def test_run_all_writes_one_run_log_per_pipeline(project):
    (project / "pipelines" / "shop_sync.yaml").unlink()
    for name in ALL_PIPELINES:
        pipeline = PIPELINE.replace("name: shop_sync", f"name: {name}")
        (project / "pipelines" / f"{name}.yaml").write_text(pipeline)

    result = invoke_all(project)

    assert result.exit_code == 0, result.output
    names = sorted(path.name for path in (project / ".adops" / "logs" / "dev").iterdir())
    assert [name.split("-", 1)[1] for name in names] == sorted(f"{n}.log" for n in ALL_PIPELINES)


def test_a_branch_run_logs_under_its_branch_environment(project, branch_project, fingerprint_store):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert the_run_log(project, "br_feat") == result.output
    assert sorted(path.name for path in (project / ".adops" / "logs").iterdir()) == ["br_feat"]


def test_a_failed_branch_run_logs_under_its_branch_environment(project, branch_project):
    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert the_run_log(project, "br_feat") == result.output
    assert sorted(path.name for path in (project / ".adops" / "logs").iterdir()) == ["br_feat"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "../x"],
        ["run", "shop_sync", "--env", "../x"],
        ["run", "shop_sync", "--branch", "///"],
    ],
)
@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_run_with_an_unsafe_name_writes_no_run_log(project, arguments):
    result = runner.invoke(cli.app, [*arguments, "--project", str(project)])

    assert_clean_failure(result)
    assert not (project / ".adops").exists()


@pytest.mark.parametrize("option", ["--env", "--branch"])
@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_run_whose_environment_cannot_be_named_writes_no_run_log(project, option):
    (project / "adops.yaml").write_text("environments: [dev\n")

    result = invoke(project, option, "../x")

    assert_clean_failure(result)
    assert not (project / ".adops").exists()


def failing_writer(*args: object) -> None:
    return None


def raising_writer(*args: object) -> NoReturn:
    raise OSError("disk full")


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_run_log_that_cannot_be_written_leaves_a_success_unchanged(project, monkeypatch):
    baseline = invoke(project)
    monkeypatch.setattr(run_command, "build_run_log_writer", lambda: failing_writer)

    result = invoke(project)

    assert result.exit_code == 0
    assert result.output == baseline.output


@pytest.mark.parametrize("writer", [failing_writer, raising_writer])
@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_run_log_problem_keeps_the_original_error(project, lock, monkeypatch, writer):
    lock.error = WarehouseLocked("dev", 0)
    baseline = invoke(project)
    monkeypatch.setattr(run_command, "build_run_log_writer", lambda: writer)

    result = invoke(project)
    with pytest.raises(WarehouseLocked) as raised:
        run_command.run_project(project, "shop_sync", "dev")

    assert raised.value is lock.error
    assert (result.exit_code, result.output) == (1, baseline.output)


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_clock_failure_returns_the_run_result_unchanged(project, monkeypatch):
    clocks = iter([SystemClock(), None])
    monkeypatch.setattr(run_command, "build_clock", lambda: next(clocks))

    result = run_command.run_project(project, "shop_sync", "dev")

    assert result.succeeded
    assert run_log_texts(project) == []


def test_a_step_prints_each_applied_record_under_its_status_line():
    passed = StepReport(
        "sync_shop",
        StepStatus.PASSED,
        notes=("Applied accepted-change record r1 in stage",),
    )

    lines = run_command.result_lines(RunResult("shop_sync", "stage", (passed,)))

    assert lines[:2] == [
        "step sync_shop: passed",
        "  Applied accepted-change record r1 in stage",
    ]


ORDERS_BEFORE = {"id": "bigint", "updated_at": "timestamp", "customer_id": "bigint"}
ORDERS_AFTER = {"id": "bigint", "updated_at": "timestamp"}
ACCEPT_ORDERS_DROP = """
pipeline: shop_sync
table: orders
changes: [customer_id dropped]
"""


def write_record(project: Path, name: str, text: str = ACCEPT_ORDERS_DROP) -> None:
    folder = project / "pipelines" / "accepted"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(text)


def test_pipeline_files_ignore_accepted_change_records(project):
    write_record(project, "x.yaml")

    files = run_command.pipeline_files(project)

    assert [path.name for path in files] == ["shop_sync.yaml"]


def test_a_project_without_records_loads_none(project):
    assert run_command.load_project_records(project) == ()


def test_records_load_in_file_name_order_with_the_file_stem_as_id(project):
    write_record(project, "2026-x-orders.yaml")
    write_record(project, "2026-a-orders.yaml")
    write_record(project, "notes.md", "not a record")

    records = run_command.load_project_records(project)

    assert [record.id for record in records] == ["2026-a-orders", "2026-x-orders"]
    assert records[0].pipeline == "shop_sync" and records[0].table == "orders"


def test_a_malformed_record_names_its_file_and_hides_its_values(project):
    text = f"pipeline: shop_sync\ntable: orders\nchanges: [{SENTINEL}]\n"
    write_record(project, "bad.yaml", text)

    with pytest.raises(PipelineValidationError) as raised:
        run_command.load_project_records(project)

    [problem] = raised.value.problems
    assert problem.startswith("pipelines/accepted/bad.yaml: changes[0]:")
    assert SENTINEL not in problem


def test_a_record_file_ending_in_yml_is_refused_by_name(project):
    write_record(project, "r1.yml")

    with pytest.raises(PipelineValidationError) as raised:
        run_command.load_project_records(project)

    assert raised.value.problems == ("pipelines/accepted/r1.yml: a record file must end in .yaml",)


@pytest.mark.usefixtures("sync", "orchestrator")
def test_a_malformed_record_fails_the_run_cleanly(project):
    write_record(project, "bad.yaml", "pipeline: [")

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.startswith("pipelines/accepted/bad.yaml: ")


def test_a_record_file_leaves_every_step_fingerprint_unchanged(project, branch_project):
    before = stage_fingerprints(project, BRANCH_PIPELINE)

    write_record(project, "r1.yaml")

    assert stage_fingerprints(project, BRANCH_PIPELINE) == before


def test_a_record_file_leaves_an_unchanged_branch_fully_deferred(
    project, branch_project, fingerprint_store
):
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)
    write_record(project, "r1.yaml")

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[:3] == [
        "step sync_shop: deferred to stage",
        "step transform_shop: deferred to stage",
        "step push_stats: deferred to stage",
    ]


def test_records_are_loaded_inside_the_warehouse_lock(project, branch_project, lock, monkeypatch):
    real = run_command.load_project_records

    def marked(project_dir: Path) -> tuple[AcceptedChangeRecord, ...]:
        lock.mark("records")
        return real(project_dir)

    monkeypatch.setattr(run_command, "load_project_records", marked)

    stage = invoke(project, "--env", "stage")

    assert stage.exit_code == 0, stage.output
    assert lock.events == [("enter", "stage"), ("records",), ("exit", "stage")]


def test_branch_records_are_loaded_inside_stages_warehouse_lock(
    project, branch_project, fingerprint_store, lock, monkeypatch
):
    real = run_command.load_project_records

    def marked(project_dir: Path) -> tuple[AcceptedChangeRecord, ...]:
        lock.mark("records")
        return real(project_dir)

    monkeypatch.setattr(run_command, "load_project_records", marked)
    stage_ran(project, fingerprint_store, BRANCH_PIPELINE)

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert lock.events == [("enter", "stage"), ("records",), ("exit", "stage")]


def test_stage_applies_a_covering_record_once(project, branch_project, sync, snapshot_store):
    sync.column_types = {"orders": ORDERS_BEFORE}
    assert invoke(project, "--env", "stage").exit_code == 0
    sync.column_types = {"orders": ORDERS_AFTER}

    halted = invoke(project, "--env", "stage")

    assert halted.exit_code == 1
    assert "Breaking Change in orders: customer_id dropped" in halted.output
    assert "add an accepted-change record for orders to the PR" in halted.output

    write_record(project, "r1.yaml")
    applied = invoke(project, "--env", "stage")

    assert applied.exit_code == 0, applied.output
    assert "  Applied accepted-change record r1 in stage" in applied.output.splitlines()
    assert sync.made_nullable == [("orders", ("customer_id",))]
    assert snapshot_store.applied == {"r1"}

    again = invoke(project, "--env", "stage")

    assert again.exit_code == 0, again.output
    assert "Applied accepted-change record" not in again.output


def test_a_halted_branch_passes_once_a_covering_record_is_added(
    project, branch_project, sync, fingerprint_store, snapshot_store
):
    edited = BRANCH_PIPELINE.replace("mode: full_refresh", "mode: incremental\n        cursor: x")
    stage_ran(project, fingerprint_store, edited)
    sync.column_types = {"orders": ORDERS_BEFORE}
    assert invoke(project, "--branch", "feat").exit_code == 0
    sync.column_types = {"orders": ORDERS_AFTER}

    halted = invoke(project, "--branch", "feat")

    assert halted.exit_code == 1
    assert "Breaking Change in orders: customer_id dropped" in halted.output

    write_record(project, "r1.yaml")
    passed = invoke(project, "--branch", "feat")

    assert passed.exit_code == 0, passed.output
    assert "  Applied accepted-change record r1 in br_feat" in passed.output.splitlines()
    assert (project / "pipelines" / "shop_sync.yaml").read_text() == BRANCH_PIPELINE
