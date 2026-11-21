import ast
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dagster import DagsterInstance, DagsterRunStatus

import adops
from adops.application.ports import (
    Orchestrator,
    ResolvedConnection,
    ResolvedWarehouse,
    ReverseSyncRead,
    RunPlan,
    RunResult,
    StepOutcome,
    StepStatus,
    SyncResult,
)
from adops.application.run_pipeline import InOrderOrchestrator, run_pipeline
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure import dagster_orchestrator
from adops.infrastructure.dagster_orchestrator import (
    DagsterOrchestrator,
    JobCompileError,
    compile_job,
)

SOURCE_PASSWORD = "s3cr3t-dagster-source-sentinel"
WAREHOUSE_PASSWORD = "s3cr3t-dagster-warehouse-sentinel"
CURSOR_SENTINEL = "cursor-sentinel-2031"
SENTINELS = (SOURCE_PASSWORD, WAREHOUSE_PASSWORD, CURSOR_SENTINEL)
ENV = {"SHOP_DB_PASSWORD": SOURCE_PASSWORD, "WAREHOUSE_PASSWORD": WAREHOUSE_PASSWORD}

SETTINGS = """
environments:
  dev:
    warehouse:
      type: postgres
      host: localhost
      port: 5433
      database: warehouse
      user: adops
      password_env: WAREHOUSE_PASSWORD
    sources:
      shop:
        type: postgres
        host: localhost
        port: 5432
        database: shop
        user: shop
        password_env: SHOP_DB_PASSWORD
"""

PIPELINE = """
name: shop
steps:
  - name: customers
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
  - name: orders
    source: shop
    tables:
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
      - name: order_items
        mode: full_refresh
        primary_key: id
        skip_checks:
          checks: [row_count_matches]
          reason: the Source table is appended to while it is copied
"""

COLUMNS = {
    "customers": frozenset({"id"}),
    "orders": frozenset({"id", "updated_at"}),
    "order_items": frozenset({"id"}),
}

EXPECTED_CHECKS = {
    "customers.source_tables_exist",
    "customers.source_not_empty",
    "customers.primary_key_unique",
    "customers.primary_key_not_null",
    "customers.row_count_matches",
    "orders.source_tables_exist",
    "orders.source_not_empty",
    "orders.primary_key_unique",
    "orders.primary_key_not_null",
    "orders.no_row_loss",
    "orders.cursor_not_backwards",
    "order_items.source_tables_exist",
    "order_items.source_not_empty",
    "order_items.primary_key_unique",
    "order_items.primary_key_not_null",
    "order_items.row_count_matches",
}


def load(**overrides: object) -> LoadFacts:
    values: dict[str, object] = {
        "warehouse_rows_before": 0,
        "warehouse_rows_after": 3,
        "duplicate_keys": 0,
        "null_keys": 0,
        "source_rows": 3,
    }
    values.update(overrides)
    return LoadFacts(**values)  # type: ignore[arg-type]


@dataclass
class RecordingSync:
    load_facts: dict[str, LoadFacts] = field(default_factory=dict)
    fail_preflight_of: dict[str, Exception] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.calls.append(("preflight", step.name))
        if step.name in self.fail_preflight_of:
            raise self.fail_preflight_of[step.name]
        return {
            t.name: SourceFacts(table_exists=True, columns=COLUMNS[t.name]) for t in step.tables
        }

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
    ) -> SyncResult:
        self.calls.append(("sync", step.name))
        facts = {t.name: self.load_facts.get(t.name, load()) for t in step.tables}
        return SyncResult({n: f.warehouse_rows_after for n, f in facts.items()}, facts)


def cursor_facts() -> dict[str, LoadFacts]:
    return {
        "orders": load(
            warehouse_rows_before=3,
            warehouse_rows_after=5,
            cursor_max_before=CURSOR_SENTINEL,
            cursor_max_after=CURSOR_SENTINEL,
        )
    }


@dataclass
class FakeSnapshotStore:
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


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 11, 18, tzinfo=UTC)


def run(sync: RecordingSync, orchestrator: Orchestrator | None = None) -> RunResult:
    return run_pipeline(
        PIPELINE,
        SETTINGS,
        "dev",
        ENV,
        sync,
        FakeSnapshotStore(),
        FixedClock(),
        orchestrator or DagsterOrchestrator(),
    )


@pytest.fixture(autouse=True)
def dagster_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    return home


def only_run_id() -> str:
    with DagsterInstance.get() as instance:
        (run,) = instance.get_runs()
        return run.run_id


def stored_texts(run_id: str) -> list[str]:
    with DagsterInstance.get() as instance:
        stored = instance.get_run_by_id(run_id)
        assert stored is not None
        return [str(stored.run_config), str(stored.tags)] + [
            str(record) for record in instance.all_logs(run_id)
        ]


def stored_check_metadata(run_id: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    with DagsterInstance.get() as instance:
        records = instance.all_logs(run_id)
    for record in records:
        event = record.dagster_event
        if event is None:
            continue
        if event.is_successful_output:
            metadata = event.step_output_data.metadata
        elif event.is_step_failure and event.step_failure_data.user_failure_data:
            metadata = event.step_failure_data.user_failure_data.metadata
        else:
            continue
        found.update({key: value.value for key, value in metadata.items()})
    return found


@dataclass(frozen=True)
class NamedTask:
    name: str

    def __call__(self, sync: object) -> StepOutcome:
        return StepOutcome((), {})


def test_dagster_orchestrator_is_an_orchestrator():
    assert isinstance(DagsterOrchestrator(), Orchestrator)


def test_compiled_job_has_one_op_per_step_chained_in_pipeline_order():
    plan = RunPlan("shop", "dev", (NamedTask("customers"), NamedTask("orders"), NamedTask("items")))

    job = compile_job(plan, RecordingSync())

    assert job.name == "shop"
    assert job.graph.node_names() == ["customers", "orders", "items"]
    upstream = {
        name: [
            out.node_name
            for out in job.graph.dependency_structure.all_upstream_outputs_from_node(name)
        ]
        for name in job.graph.node_names()
    }
    assert upstream == {"customers": [], "orders": ["customers"], "items": ["orders"]}


@pytest.mark.parametrize(
    ("pipeline", "step", "bad"),
    [
        ("shop", "order-items", "order-items"),
        ("my-shop", "orders", "my-shop"),
        ("shop", "input", "input"),
    ],
)
def test_name_dagster_rejects_is_a_clear_compile_error(pipeline: str, step: str, bad: str):
    plan = RunPlan(pipeline, "dev", (NamedTask(step),))

    with pytest.raises(JobCompileError, match=f'Pipeline {pipeline}: "{bad}" is not a valid name'):
        compile_job(plan, RecordingSync())


def test_healthy_run_passes_every_step_and_is_recorded_in_dagster_home():
    sync = RecordingSync()

    result = run(sync)

    assert result.succeeded
    assert [(s.name, s.status) for s in result.steps] == [
        ("customers", StepStatus.PASSED),
        ("orders", StepStatus.PASSED),
    ]
    assert result.steps[1].rows_loaded == {"orders": 3, "order_items": 3}
    with DagsterInstance.get() as instance:
        (stored,) = instance.get_runs()
    assert stored.job_name == "shop"
    assert stored.status is DagsterRunStatus.SUCCESS


def test_failing_first_step_skips_the_rest_with_the_sanitized_error():
    sync = RecordingSync(fail_preflight_of={"customers": ConnectionError("connection refused")})

    result = run(sync)

    assert sync.calls == [("preflight", "customers")]
    assert [(s.name, s.status) for s in result.steps] == [
        ("customers", StepStatus.FAILED),
        ("orders", StepStatus.SKIPPED),
    ]
    error = result.steps[0].error or ""
    assert error == "Step customers: ConnectionError: connection refused"
    assert "dagster._core" not in error
    assert "Traceback" not in error
    with DagsterInstance.get() as instance:
        (stored,) = instance.get_runs()
    assert stored.status is DagsterRunStatus.FAILURE


def test_execute_returns_a_result_instead_of_raising(monkeypatch: pytest.MonkeyPatch):
    seen: list[bool] = []
    original = dagster_orchestrator.JobDefinition.execute_in_process

    def spy(self, *args, **kwargs):
        seen.append(kwargs["raise_on_error"])
        return original(self, *args, **kwargs)

    monkeypatch.setattr(dagster_orchestrator.JobDefinition, "execute_in_process", spy)

    result = run(RecordingSync(fail_preflight_of={"customers": RuntimeError("boom")}))

    assert seen == [False]
    assert not result.succeeded


@pytest.mark.parametrize(
    "fail_preflight_of",
    [{}, {"customers": RuntimeError(f"auth failed for password={SOURCE_PASSWORD}")}],
    ids=["success", "failing-first-step"],
)
def test_no_secret_or_cursor_value_reaches_dagster_storage(
    fail_preflight_of: dict[str, Exception], dagster_home: Path
):
    result = run(RecordingSync(load_facts=cursor_facts(), fail_preflight_of=fail_preflight_of))

    assert result.succeeded is not fail_preflight_of
    run_id = only_run_id()
    texts = stored_texts(run_id) + [str(stored_check_metadata(run_id))]
    files = [path.read_bytes() for path in dagster_home.rglob("*") if path.is_file()]
    assert files
    for secret in SENTINELS:
        assert all(secret not in text for text in texts)
        assert all(secret.encode() not in content for content in files)


def test_every_check_result_is_stored_as_op_metadata_on_success():
    run(RecordingSync())

    metadata = stored_check_metadata(only_run_id())

    assert set(metadata) == EXPECTED_CHECKS
    statuses = {key: entry["status"] for key, entry in metadata.items()}
    assert statuses.pop("order_items.row_count_matches") == "skipped"
    assert set(statuses.values()) == {"pass"}
    assert metadata["customers.primary_key_unique"] == {
        "check": "primary_key_unique",
        "table": "customers",
        "status": "pass",
        "message": "customers: primary_key_unique passed",
    }


def test_skipped_check_metadata_carries_its_reason():
    run(RecordingSync())

    entry = stored_check_metadata(only_run_id())["order_items.row_count_matches"]

    assert entry["status"] == "skipped"
    assert "the Source table is appended to while it is copied" in entry["message"]


def test_failed_check_is_stored_and_stops_later_steps():
    sync = RecordingSync(load_facts={"customers": load(duplicate_keys=2)})

    result = run(sync)

    assert [(s.name, s.status) for s in result.steps] == [
        ("customers", StepStatus.CHECK_FAILED),
        ("orders", StepStatus.SKIPPED),
    ]
    assert ("preflight", "orders") not in sync.calls
    metadata = stored_check_metadata(only_run_id())
    assert metadata["customers.primary_key_unique"]["status"] == "fail"
    assert metadata["customers.primary_key_unique"]["message"] == (
        "customers: primary_key_unique failed — 2 duplicate primary key values in the Warehouse"
    )
    assert not any(key.startswith("orders.") for key in metadata)


@pytest.mark.parametrize(
    "make_sync",
    [
        lambda: RecordingSync(load_facts=cursor_facts()),
        lambda: RecordingSync(load_facts={"customers": load(duplicate_keys=2)}),
        lambda: RecordingSync(load_facts={"order_items": load(null_keys=1)}),
        lambda: RecordingSync(fail_preflight_of={"orders": TimeoutError("timed out")}),
    ],
    ids=["healthy", "check-failed-first", "check-failed-last", "sync-failed"],
)
def test_result_matches_the_in_order_orchestrator(make_sync):
    assert run(make_sync()) == run(make_sync(), InOrderOrchestrator())


def test_unexpected_error_fails_the_step_without_its_message(dagster_home: Path):
    sync = RecordingSync(
        load_facts={
            "orders": load(
                warehouse_rows_before=3, cursor_max_before=SOURCE_PASSWORD, cursor_max_after=7
            )
        }
    )

    result = run(sync)

    assert [(s.name, s.status, s.error) for s in result.steps] == [
        ("customers", StepStatus.PASSED, None),
        ("orders", StepStatus.FAILED, "Step orders: unexpected TypeError"),
    ]
    for content in (p.read_bytes() for p in dagster_home.rglob("*") if p.is_file()):
        assert SOURCE_PASSWORD.encode() not in content


TARGET_PASSWORD = "s3cr3t-dagster-target-sentinel"
KEY_SENTINEL = "key-sentinel-4417"

REVERSE_SETTINGS = """
environments:
  dev:
    warehouse:
      type: duckdb
      path: dev.duckdb
    sources: {}
  stage:
    warehouse:
      type: duckdb
      path: stage.duckdb
    sources: {}
    targets:
      crm:
        type: postgres
        host: crm.internal
        port: 5440
        database: crm
        user: crm_writer
        password_env: CRM_TARGET_PASSWORD
"""

REVERSE_PIPELINE = """
name: crm_push
steps:
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping:
      order_count: orders
"""


@dataclass
class KeySentinelReverseSync:
    applied: list[Diff] = field(default_factory=list)

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead:
        return ReverseSyncRead(
            result_rows=({"customer_id": KEY_SENTINEL, "order_count": 1},),
            target_rows=(),
            target_table_exists=True,
            missing_target_columns=(),
            duplicate_keys=0,
            null_keys=0,
        )

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        self.applied.append(diff)


def test_no_target_password_or_preview_key_reaches_dagster_storage(dagster_home: Path):
    engine = KeySentinelReverseSync()

    result = run_pipeline(
        REVERSE_PIPELINE,
        REVERSE_SETTINGS,
        "stage",
        {"CRM_TARGET_PASSWORD": TARGET_PASSWORD},
        RecordingSync(),
        FakeSnapshotStore(),
        FixedClock(),
        DagsterOrchestrator(),
        reverse_sync=engine,
    )

    preview = result.steps[0].preview
    assert result.succeeded and len(engine.applied) == 1
    assert preview is not None and preview.sample.inserts == (KEY_SENTINEL,)
    run_id = only_run_id()
    texts = stored_texts(run_id) + [str(stored_check_metadata(run_id))]
    files = [path.read_bytes() for path in dagster_home.rglob("*") if path.is_file()]
    for sentinel in (TARGET_PASSWORD, KEY_SENTINEL):
        assert all(sentinel not in text for text in texts)
        assert all(sentinel.encode() not in content for content in files)


def test_without_dagster_home_an_ephemeral_instance_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    workdir = tmp_path / "project"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    monkeypatch.delenv("DAGSTER_HOME")
    ephemeral_calls: list[None] = []
    original = dagster_orchestrator.DagsterInstance.ephemeral

    def spy(*args, **kwargs):
        ephemeral_calls.append(None)
        return original(*args, **kwargs)

    monkeypatch.setattr(dagster_orchestrator.DagsterInstance, "ephemeral", spy)

    result = run(RecordingSync())

    assert result.succeeded
    assert ephemeral_calls == [None]
    assert list(workdir.iterdir()) == []


def test_execute_does_not_change_the_environment():
    before = dict(os.environ)

    run(RecordingSync())

    assert dict(os.environ) == before


def test_only_the_dagster_adapter_imports_dagster():
    package = Path(adops.__file__).parent
    importers = set()
    for source in package.rglob("*.py"):
        for node in ast.walk(ast.parse(source.read_text())):
            names = (
                [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else []
            )
            if any(name.split(".")[0] == "dagster" for name in names):
                importers.add(source.relative_to(package).as_posix())

    assert importers == {
        "infrastructure/dagster_orchestrator.py",
        "infrastructure/dagster_server.py",
    }
