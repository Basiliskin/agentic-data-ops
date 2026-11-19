from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from typer.testing import CliRunner

from adops.application.ports import ResolvedConnection
from adops.domain.checks import SourceFacts
from adops.domain.pipeline import SyncStep
from adops.domain.schema_snapshot import TableShape
from adops.interface import accept_schema_command, cli, run_command

runner = CliRunner()

REAL_BUILD_SNAPSHOT_STORE = run_command.build_snapshot_store

SENTINEL = "s3cr3t-accept-command-sentinel"

SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
  staging:
    warehouse: {type: postgres, host: staging, port: 5432, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: staging, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""

PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - {name: customers, mode: full_refresh, primary_key: id}
      - {name: orders, mode: incremental, primary_key: id, cursor: updated_at}
"""

ORDERS = {"id": "integer", "amount": "numeric(10,2)", "updated_at": "timestamptz"}


@dataclass
class FakeSync:
    error: Exception | None = None
    sources: list[ResolvedConnection] = field(default_factory=list)
    nullable: list[str] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.sources.append(source)
        if self.error is not None:
            raise self.error
        return {table.name: SourceFacts(True, frozenset(ORDERS), ORDERS) for table in step.tables}

    def sync(self, step, source, warehouse):
        raise AssertionError("accept-schema must not copy data")

    def make_columns_nullable(self, step, table, warehouse, columns):
        self.nullable.extend(columns)


@dataclass
class InMemorySnapshotStore:
    shapes: dict[tuple[str, str, str], TableShape] = field(default_factory=dict)
    warehouses: list[ResolvedConnection] = field(default_factory=list)

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
        self.warehouses.append(warehouse)
        self.shapes[(pipeline, source, table)] = shape


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(SETTINGS)
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "shop_sync.yaml").write_text(PIPELINE)
    monkeypatch.setenv("ADOPS_TEST_SHOP_PASSWORD", SENTINEL)
    monkeypatch.setenv("ADOPS_TEST_WAREHOUSE_PASSWORD", SENTINEL)
    return tmp_path


@pytest.fixture
def sync(monkeypatch):
    fake = FakeSync()
    monkeypatch.setattr(run_command, "build_sync", lambda project, pipeline, kind: fake)
    return fake


@pytest.fixture
def store(monkeypatch):
    fake = InMemorySnapshotStore()
    monkeypatch.setattr(run_command, "build_snapshot_store", lambda project, kind: fake)
    return fake


def invoke(project, *args, pipeline="shop_sync", table="orders"):
    return runner.invoke(
        cli.app, ["accept-schema", pipeline, table, "--project", str(project), *args]
    )


def assert_clean_failure(result):
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert SENTINEL not in result.output


@pytest.mark.usefixtures("sync")
def test_success_names_the_table_step_pipeline_and_environment(project, store):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        "Accepted the current shape of orders (3 columns, Step sync_shop) "
        "for Pipeline shop_sync in Environment dev."
    )
    assert store.shapes == {("shop_sync", "shop", "orders"): TableShape(ORDERS)}
    assert SENTINEL not in result.output


def test_success_names_the_columns_made_nullable(project, sync, store):
    store.shapes[("shop_sync", "shop", "orders")] = TableShape({**ORDERS, "customer_id": "integer"})

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert sync.nullable == ["customer_id"]
    assert result.output.splitlines()[-1] == (
        "These columns are no longer in the Source and may now be NULL in the Warehouse: "
        "customer_id"
    )


@pytest.mark.usefixtures("sync")
def test_env_option_targets_only_that_environment(project, store):
    result = invoke(project, "--env", "staging")

    assert result.exit_code == 0, result.output
    assert result.output.strip().endswith("in Environment staging.")
    assert [warehouse.host for warehouse in store.warehouses] == ["staging"]


def test_dotenv_supplies_secrets_the_shell_does_not_set(project, sync, store, monkeypatch):
    (project / ".env").write_text("ADOPS_TEST_SHOP_PASSWORD=from_dotenv\n")
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    assert invoke(project).exit_code == 0
    assert [source.password for source in sync.sources] == ["from_dotenv"]


@pytest.mark.usefixtures("sync", "store")
def test_unknown_table_is_a_validation_error(project):
    result = invoke(project, table="refunds")

    assert_clean_failure(result)
    assert result.output.strip() == "refunds: no Step in Pipeline shop_sync copies it"


@pytest.mark.usefixtures("sync", "store")
def test_unknown_environment_names_the_available_ones(project):
    result = invoke(project, "--env", "prod")

    assert_clean_failure(result)
    assert "available: dev, staging" in result.output


@pytest.mark.usefixtures("sync", "store")
def test_missing_secret_names_the_variable(project, monkeypatch):
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    result = invoke(project)

    assert_clean_failure(result)
    assert "ADOPS_TEST_SHOP_PASSWORD" in result.output


@pytest.mark.usefixtures("sync", "store")
def test_missing_pipeline_file_is_one_line(project):
    result = invoke(project, pipeline="nope")

    assert_clean_failure(result)
    assert result.output.strip() == "pipelines/nope.yaml: file not found"


@pytest.mark.usefixtures("store")
def test_table_missing_in_source_is_refused(project, monkeypatch):
    @dataclass
    class MissingTableSync(FakeSync):
        def preflight(self, step, source):
            return {table.name: SourceFacts(False, frozenset()) for table in step.tables}

    monkeypatch.setattr(
        run_command, "build_sync", lambda project, pipeline, kind: MissingTableSync()
    )

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.strip() == (
        "Step sync_shop: orders not found in Source shop; nothing accepted"
    )


@pytest.mark.usefixtures("store")
def test_unexpected_error_shows_only_its_type(project, sync):
    sync.error = RuntimeError(f"postgresql://u:{SENTINEL}@h/db")

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.strip() == "unexpected RuntimeError while accepting orders"
    assert "postgresql://" not in result.output


def test_builds_sync_and_store_through_run_commands_factories(project, monkeypatch):
    built: list[tuple[str, object]] = []
    store = InMemorySnapshotStore()

    def build_sync(project_dir, pipeline, kind):
        built.append(("sync", (project_dir, pipeline, kind)))
        return FakeSync()

    def build_store(project_dir, kind):
        built.append(("store", (project_dir, kind)))
        return store

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)

    accepted = accept_schema_command.accept_project_schema(project, "shop_sync", "orders", "dev")

    assert accepted.environment == "dev"
    assert built == [("sync", (project, "shop_sync", "postgres")), ("store", (project, "postgres"))]


DUCKDB_ENVIRONMENT = """
  ci:
    warehouse: {type: duckdb, path: missing/dir/wh.duckdb}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""


@pytest.mark.parametrize(("env", "kind"), [("dev", "postgres"), ("ci", "duckdb")])
def test_adapters_are_built_for_the_chosen_environments_kind(project, monkeypatch, env, kind):
    (project / "adops.yaml").write_text(SETTINGS + DUCKDB_ENVIRONMENT)
    built: list[tuple[str, str]] = []

    def build_sync(project_dir: Path, pipeline: str, kind: str) -> FakeSync:
        built.append(("sync", kind))
        return FakeSync()

    def build_store(project_dir: Path, kind: str) -> InMemorySnapshotStore:
        built.append(("store", kind))
        return InMemorySnapshotStore()

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)

    accept_schema_command.accept_project_schema(project, "shop_sync", "orders", env)

    assert built == [("sync", kind), ("store", kind)]


@pytest.mark.usefixtures("sync")
def test_unopenable_duckdb_file_fails_with_the_dedicated_message(project, monkeypatch):
    (project / "adops.yaml").write_text(SETTINGS + DUCKDB_ENVIRONMENT)
    monkeypatch.setattr(run_command, "build_snapshot_store", REAL_BUILD_SNAPSHOT_STORE)

    result = invoke(project, "--env", "ci")

    assert_clean_failure(result)
    assert result.output.strip() == "Pipeline shop_sync, table orders: Warehouse is unreachable"
    for leaked in ("missing/dir", str(Path.home()), "IOException", "unexpected"):
        assert leaked not in result.output
    assert not (project / "missing").exists()
