import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.domain.branch_environment import SchemaLayout
from adops.domain.checks import SourceFacts
from adops.domain.pipeline import SyncStep
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.interface import accept_schema_command, cli, run_command
from adops.interface.accept_schema_command import RecordExists
from adops.interface.run_command import BRANCH_WITH_ENV, ProjectFileError, load_project_records
from adops.interface.validate_command import validate_project

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
WITH_CUSTOMER_ID = TableShape({**ORDERS, "customer_id": "integer"})

NOW = datetime(2026, 11, 23, 8, 0, 0, tzinfo=UTC)
RECORD_ID = "20261123T080000Z-shop_sync-orders"
RECORD_PATH = f"pipelines/accepted/{RECORD_ID}.yaml"
KEY = ("shop_sync", "shop", "orders")


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
    saved_record_ids: list[str] = field(default_factory=list)

    def load(
        self, warehouse: ResolvedConnection, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        return self.shapes.get((pipeline, source, table))

    def applied_record_ids(self, warehouse: ResolvedConnection) -> frozenset[str]:
        return frozenset()

    def save(
        self,
        warehouse: ResolvedConnection,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
        *,
        applied_record_ids: Sequence[str] = (),
    ) -> None:
        self.warehouses.append(warehouse)
        self.saved_record_ids.extend(applied_record_ids)
        self.shapes[(pipeline, source, table)] = shape


@dataclass(frozen=True)
class FixedClock:
    def now(self) -> datetime:
        return NOW


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr(run_command, "build_clock", FixedClock)


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
    monkeypatch.setattr(run_command, "build_sync", lambda *_, **__: fake)
    return fake


@pytest.fixture
def store_options() -> list[dict[str, object]]:
    return []


@pytest.fixture
def store(store_options, monkeypatch):
    fake = InMemorySnapshotStore()

    def build_store(*_, **kwargs):
        store_options.append(kwargs)
        return fake

    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)
    return fake


def invoke(project, *args, pipeline="shop_sync", table="orders"):
    return runner.invoke(
        cli.app, ["accept-schema", pipeline, table, "--project", str(project), *args]
    )


def assert_clean_failure(result):
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert SENTINEL not in result.output


def record_files(project: Path) -> list[str]:
    return sorted(
        path.relative_to(project).as_posix()
        for path in (project / "pipelines" / "accepted").glob("*.yaml")
    )


@pytest.mark.usefixtures("sync")
def test_success_names_the_table_step_pipeline_and_environment(project, store):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        "Accepted the current shape of orders (3 columns, Step sync_shop) "
        "for Pipeline shop_sync in Environment dev."
    )
    assert store.shapes == {KEY: TableShape(ORDERS)}
    assert record_files(project) == []
    assert SENTINEL not in result.output


def test_success_names_the_columns_made_nullable(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert sync.nullable == ["customer_id"]
    assert result.output.splitlines()[1] == (
        "These columns are no longer in the Source and may now be NULL in the Warehouse: "
        "customer_id"
    )


def test_dev_writes_one_record_that_loads_and_validates(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert record_files(project) == [RECORD_PATH]
    assert result.output.splitlines()[-1] == (
        f"Wrote accepted-change record {RECORD_PATH} (customer_id dropped). "
        "Commit it so stage and prod accept the change on their next run."
    )
    [record] = load_project_records(project)
    assert (record.id, record.pipeline, record.table) == (RECORD_ID, "shop_sync", "orders")
    assert [change.message for change in record.changes] == ["customer_id dropped"]
    assert validate_project(project, None).problems == []
    assert store.shapes[KEY] == TableShape(ORDERS)
    assert store.saved_record_ids == []


def test_dev_creates_the_missing_accepted_folder(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID
    assert not (project / "pipelines" / "accepted").exists()

    assert invoke(project).exit_code == 0

    assert (project / "pipelines" / "accepted").is_dir()
    assert record_files(project) == [RECORD_PATH]


def test_record_path_is_the_projects_even_from_another_folder(
    project, sync, store, tmp_path_factory, monkeypatch
):
    store.shapes[KEY] = WITH_CUSTOMER_ID
    monkeypatch.chdir(tmp_path_factory.mktemp("elsewhere"))

    assert invoke(project).exit_code == 0

    assert record_files(project) == [RECORD_PATH]
    assert not Path("pipelines").exists()


def test_second_run_on_the_same_change_prints_the_existing_record(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID
    assert invoke(project).exit_code == 0
    store.shapes[KEY] = WITH_CUSTOMER_ID

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert record_files(project) == [RECORD_PATH]
    assert result.output.splitlines()[-1] == (
        f"Already accepted by {RECORD_PATH}; no record written."
    )


CLASHING_RECORD = "pipeline: shop_sync\ntable: orders\nchanges:\n- amount dropped\n"


def test_record_name_clash_refuses_and_leaves_dev_unchanged(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID
    (project / "pipelines" / "accepted").mkdir()
    (project / RECORD_PATH).write_text(CLASHING_RECORD)

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.strip() == (
        f"{RECORD_PATH}: a record with this name already exists; nothing accepted"
    )
    assert (project / RECORD_PATH).read_text() == CLASHING_RECORD
    assert store.warehouses == []
    assert store.shapes[KEY] == WITH_CUSTOMER_ID
    assert sync.nullable == []


def test_record_exists_is_a_project_file_error():
    assert issubclass(RecordExists, ProjectFileError)


DASHED_PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - {name: order-lines, mode: full_refresh, primary_key: id}
"""


def test_non_plain_table_is_refused_and_leaves_dev_unchanged(project, sync, store):
    (project / "pipelines" / "shop_sync.yaml").write_text(DASHED_PIPELINE)
    store.shapes[("shop_sync", "shop", "order-lines")] = WITH_CUSTOMER_ID

    result = invoke(project, table="order-lines")

    assert_clean_failure(result)
    assert result.output.strip() == (
        "order-lines: an accepted-change record needs a plain table name (a-z, 0-9, _)"
    )
    assert store.warehouses == []
    assert sync.nullable == []
    assert record_files(project) == []


def test_non_dev_environment_writes_only_the_record(project, sync, store, store_options):
    store.shapes[KEY] = WITH_CUSTOMER_ID

    result = invoke(project, "--env", "staging")

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        f"Wrote accepted-change record {RECORD_PATH} (customer_id dropped). "
        "Commit it so stage and prod accept the change on their next run."
    )
    assert record_files(project) == [RECORD_PATH]
    assert store_options == [{"read_only": True}]
    assert store.warehouses == []
    assert store.saved_record_ids == []
    assert store.shapes[KEY] == WITH_CUSTOMER_ID
    assert sync.nullable == []
    assert [source.host for source in sync.sources] == ["staging"]


def test_non_dev_already_covered_writes_nothing(project, sync, store):
    store.shapes[KEY] = WITH_CUSTOMER_ID
    assert invoke(project, "--env", "staging").exit_code == 0

    result = invoke(project, "--env", "staging")

    assert result.exit_code == 0, result.output
    assert result.output.strip() == f"Already accepted by {RECORD_PATH}; no record written."
    assert record_files(project) == [RECORD_PATH]
    assert store.warehouses == []


@pytest.mark.parametrize(
    ("previous", "reason"),
    [
        (None, "no previous snapshot to compare with"),
        (TableShape(ORDERS), "no Breaking Change found"),
    ],
)
def test_non_dev_with_nothing_to_record_says_so(project, sync, store, previous, reason):
    if previous is not None:
        store.shapes[KEY] = previous

    result = invoke(project, "--env", "staging")

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        f"Pipeline shop_sync, table orders in Environment staging: {reason}; no record written."
    )
    assert record_files(project) == []
    assert store.warehouses == []
    assert sync.nullable == []


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

    monkeypatch.setattr(run_command, "build_sync", lambda *_, **__: MissingTableSync())

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

    def build_sync(project_dir, pipeline, environment, kind):
        built.append(("sync", (project_dir, pipeline, environment, kind)))
        return FakeSync()

    def build_store(project_dir, environment, kind, *, read_only):
        built.append(("store", (project_dir, environment, kind, read_only)))
        return store

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)

    acceptance = accept_schema_command.accept_project_schema(project, "shop_sync", "orders", "dev")

    assert acceptance.accepted is not None
    assert acceptance.accepted.environment == "dev"
    assert built == [
        ("sync", (project, "shop_sync", "dev", "postgres")),
        ("store", (project, "dev", "postgres", False)),
    ]


DUCKDB_ENVIRONMENT = """
  ci:
    warehouse: {type: duckdb, path: missing/dir/wh.duckdb}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""


@pytest.mark.parametrize(
    ("env", "kind", "read_only"), [("dev", "postgres", False), ("ci", "duckdb", True)]
)
def test_adapters_are_built_for_the_chosen_environments_kind(
    project, monkeypatch, env, kind, read_only
):
    (project / "adops.yaml").write_text(SETTINGS + DUCKDB_ENVIRONMENT)
    built: list[tuple[str, str, str, bool | None]] = []

    def build_sync(project_dir: Path, pipeline: str, environment: str, kind: str) -> FakeSync:
        built.append(("sync", environment, kind, None))
        return FakeSync()

    def build_store(
        project_dir: Path, environment: str, kind: str, *, read_only: bool
    ) -> InMemorySnapshotStore:
        built.append(("store", environment, kind, read_only))
        return InMemorySnapshotStore()

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)

    accept_schema_command.accept_project_schema(project, "shop_sync", "orders", env)

    assert built == [("sync", env, kind, None), ("store", env, kind, read_only)]


def test_ci_writes_only_the_record(project, sync, store, store_options):
    (project / "adops.yaml").write_text(SETTINGS + DUCKDB_ENVIRONMENT)
    store.shapes[KEY] = WITH_CUSTOMER_ID

    result = invoke(project, "--env", "ci")

    assert result.exit_code == 0, result.output
    assert record_files(project) == [RECORD_PATH]
    assert store_options == [{"read_only": True}]
    assert store.warehouses == []
    assert sync.nullable == []


@pytest.mark.usefixtures("sync")
def test_unopenable_duckdb_file_fails_with_the_dedicated_message(project, monkeypatch):
    (project / "adops.yaml").write_text(SETTINGS + DUCKDB_ENVIRONMENT)
    monkeypatch.setattr(run_command, "build_snapshot_store", REAL_BUILD_SNAPSHOT_STORE)
    (project / "missing" / "dir").mkdir(parents=True)
    (project / "missing" / "dir" / "wh.duckdb").write_text("not a duckdb file")

    result = invoke(project, "--env", "ci")

    assert_clean_failure(result)
    assert result.output.strip() == "Pipeline shop_sync, table orders: Warehouse is unreachable"
    for leaked in ("missing/dir", str(Path.home()), "IOException", "unexpected"):
        assert leaked not in result.output


STAGE_DUCKDB_SETTINGS = """
environments:
  dev:
    warehouse: {type: duckdb, path: dev.duckdb}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
  stage:
    warehouse: {type: duckdb, path: stage.duckdb}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""


def test_accept_schema_in_stage_reads_stages_snapshot_and_writes_only_the_record(
    project, sync, monkeypatch
):
    (project / "adops.yaml").write_text(STAGE_DUCKDB_SETTINGS)
    stage_shape = TableShape({**ORDERS, "stage_only": "text"})
    for environment, shape in (
        ("dev", TableShape({**ORDERS, "dev_only": "text"})),
        ("stage", stage_shape),
    ):
        REAL_BUILD_SNAPSHOT_STORE(project, environment, "duckdb").save(
            ResolvedDuckDb(str(project / f"{environment}.duckdb")), *KEY, shape
        )
    shutil.rmtree(project / ".adops")
    stage_bytes = (project / "stage.duckdb").read_bytes()
    monkeypatch.setattr(run_command, "build_snapshot_store", REAL_BUILD_SNAPSHOT_STORE)

    result = invoke(project, "--env", "stage")

    assert result.exit_code == 0, result.output
    assert record_files(project) == [RECORD_PATH]
    [record] = load_project_records(project)
    assert [change.message for change in record.changes] == ["stage_only dropped"]
    assert sync.nullable == []
    assert not (project / ".adops").exists()
    assert (project / "stage.duckdb").read_bytes() == stage_bytes
    stage = REAL_BUILD_SNAPSHOT_STORE(project, "stage", "duckdb", read_only=True)
    warehouse = ResolvedDuckDb(str(project / "stage.duckdb"))
    assert stage.load(warehouse, *KEY) == stage_shape
    assert stage.applied_record_ids(warehouse) == frozenset()


BRANCH_SETTINGS = (
    SETTINGS
    + """
  stage:
    warehouse: {type: postgres, host: stage-db, port: 5432, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: stage-shop, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""
)
BRANCH = "br_feat"


@dataclass
class MarkingSnapshotStore(InMemorySnapshotStore):
    locks: object = None

    def save(self, warehouse, pipeline, source, table, shape, *, applied_record_ids=()):
        self.locks.mark("save")
        super().save(warehouse, pipeline, source, table, shape)


@dataclass
class BranchAdapters:
    sync: FakeSync
    store: MarkingSnapshotStore
    built: list[tuple[str, str, str, SchemaLayout]] = field(default_factory=list)


@pytest.fixture
def branch(project, monkeypatch, locks):
    (project / "adops.yaml").write_text(BRANCH_SETTINGS)
    adapters = BranchAdapters(FakeSync(), MarkingSnapshotStore(locks=locks))
    adapters.store.shapes[KEY] = WITH_CUSTOMER_ID

    def build_sync(project_dir, pipeline, environment, kind, *, layout):
        adapters.built.append(("sync", environment, kind, layout))
        return adapters.sync

    def build_store(project_dir, environment, kind, *, layout):
        adapters.built.append(("store", environment, kind, layout))
        return adapters.store

    monkeypatch.setattr(run_command, "build_sync", build_sync)
    monkeypatch.setattr(run_command, "build_snapshot_store", build_store)
    return adapters


def test_branch_saves_its_baseline_while_holding_stages_lock(project, branch, locks):
    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert locks.events == [("enter", "stage"), ("save",), ("exit", "stage")]
    assert locks.timeouts == [DEFAULT_LOCK_TIMEOUT]
    assert [warehouse.host for warehouse in locks.warehouses] == ["stage-db"]
    assert branch.store.shapes == {KEY: TableShape(ORDERS)}


def test_branch_uses_its_layout_and_stages_settings(project, branch):
    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    layout = SchemaLayout.for_branch(BRANCH)
    assert branch.built == [
        ("sync", BRANCH, "postgres", layout),
        ("store", BRANCH, "postgres", layout),
    ]
    assert [source.host for source in branch.sync.sources] == ["stage-shop"]
    assert [warehouse.host for warehouse in branch.store.warehouses] == ["stage-db"]
    assert branch.sync.nullable == ["customer_id"]
    assert result.output.splitlines() == [
        "Accepted the current shape of orders (3 columns, Step sync_shop) "
        f"for Pipeline shop_sync in Environment {BRANCH}.",
        "These columns are no longer in the Source and may now be NULL in the Warehouse: "
        "customer_id",
    ]
    assert SENTINEL not in result.output


def test_branch_name_is_normalised_like_a_branch_run(project, branch):
    result = invoke(project, "--branch", "Feat/Login")

    assert result.exit_code == 0, result.output
    names = {environment for _, environment, _, _ in branch.built}
    assert names == {"br_feat_login"}


def test_branch_acceptance_writes_no_record(project, branch, monkeypatch):
    def no_record(*_):
        raise AssertionError("a Branch acceptance must not plan a record")

    monkeypatch.setattr(accept_schema_command, "plan_accepted_record", no_record)

    result = invoke(project, "--branch", "feat")

    assert result.exit_code == 0, result.output
    assert record_files(project) == []
    assert "record" not in result.output


def test_branch_waits_out_a_locked_stage_with_one_plain_line(project, branch, locks):
    locks.error = WarehouseLocked("stage", DEFAULT_LOCK_TIMEOUT)

    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert result.output == "Another run is using the stage Warehouse; gave up after 30 minutes.\n"
    assert branch.built == []
    assert branch.store.warehouses == []


def test_branch_with_unreachable_warehouse_is_one_line(project, branch, locks):
    branch.sync.error = WarehouseUnreachable()

    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert result.output == "Pipeline shop_sync, table orders: Warehouse is unreachable\n"
    assert locks.events == [("enter", "stage"), ("exit", "stage")]


@pytest.mark.usefixtures("sync", "store")
def test_branch_needs_a_stage_environment(project):
    result = invoke(project, "--branch", "feat")

    assert_clean_failure(result)
    assert "environments.stage" in result.output


def _error_text(output: str) -> str:
    return " ".join(re.sub(r"[│╭╮╰╯─]", " ", output).split())


@pytest.mark.usefixtures("branch")
def test_env_with_branch_is_refused_with_runs_message(project):
    result = invoke(project, "--branch", "feat", "--env", "stage")
    run = runner.invoke(
        cli.app,
        ["run", "shop_sync", "--project", str(project), "--branch", "feat", "--env", "stage"],
    )

    assert result.exit_code == run.exit_code == 2
    assert _error_text(BRANCH_WITH_ENV) in _error_text(result.output)
    assert _error_text(BRANCH_WITH_ENV) in _error_text(run.output)
    assert record_files(project) == []
