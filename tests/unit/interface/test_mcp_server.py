import io
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import anyio
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CallToolResult, TextContent
from mcp_harness import (
    PIPELINE,
    SETTINGS,
    assert_no_secret,
    call_tool,
    error_text,
    secret_values,
    write_project,
)
from typer.testing import CliRunner

from adops.application.ports import (
    PreviewSample,
    ResolvedConnection,
    ReverseSyncPreview,
    RunPlan,
    RunResult,
    SourceUnreachable,
    StepReport,
    StepStatus,
    Sync,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import (
    WarehouseUnreachableError,
    stage_branch_name,
    stage_environment,
)
from adops.domain.assistant_access import (
    ONLY_DEV_OR_BRANCH,
    OUTSIDE_PROJECT_FILES,
    WRITES_ONLY_UNDER_ROOTS,
)
from adops.domain.checks import CheckResult, CheckStatus, SourceFacts
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_project_settings
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.connection_probe import DriverConnectionProbe
from adops.infrastructure.run_log_store import write_run_log
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.interface import cli, drop_branch_command, mcp_server, run_command
from adops.interface.assistant_files import (
    MAX_READ_BYTES,
    MAX_WRITE_BYTES,
    NOT_A_FILE_TARGET,
    NOT_READABLE_TEXT,
    NOT_WRITABLE_TEXT,
)
from adops.interface.mcp_server import ONLY_RUN_LOGS
from adops.interface.run_command import error_lines
from adops.interface.validate_command import validate_project

pytestmark = pytest.mark.anyio

runner = CliRunner()

TOOLS = {
    "run",
    "preview",
    "validate",
    "drop_branch",
    "accept_schema",
    "project_view",
    "list_files",
    "read_file",
    "edit_file",
    "read_logs",
    "test_connection",
}

SEAMS = (
    "build_sync",
    "build_snapshot_store",
    "build_clock",
    "build_orchestrator",
    "build_transform",
    "build_reverse_sync",
    "build_fingerprint_store",
    "build_project_files",
    "build_run_log_writer",
    "build_connection_probe",
)
MCP_SEAMS = {"build_connection_probe"}
RUN_LOG_SEAMS = ["build_run_log_writer", "build_clock"]
NOW = datetime(2026, 11, 22, 9, 30, tzinfo=UTC)

DEV_SHARES_STAGE_WAREHOUSE = SETTINGS.replace(
    "host: localhost, port: 5433, database: w_dev", "host: 127.0.0.1, port: 5433, database: w_stage"
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def passing_steps() -> tuple[StepReport, ...]:
    check = CheckResult("row_count_matches", "customers", CheckStatus.PASS, "20 rows")
    return (StepReport("sync_shop", StepStatus.PASSED, (check,), {"customers": 20}),)


@dataclass
class FakeOrchestrator:
    steps: tuple[StepReport, ...] = field(default_factory=passing_steps)
    plans: list[RunPlan] = field(default_factory=list)

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        self.plans.append(plan)
        return RunResult(plan.pipeline, plan.environment, self.steps)


@dataclass
class StageFingerprints:
    saved: dict[str, str] = field(default_factory=lambda: {"sync_shop": "from-stage"})

    def load(self, pipeline: str) -> dict[str, str]:
        return dict(self.saved)

    def save(self, pipeline: str, step: str, fingerprint: str) -> None:
        self.saved[step] = fingerprint


CUSTOMERS = {"id": "integer", "email": "text"}


@dataclass
class AcceptSync:
    error: Exception | None = None
    table_exists: bool = True
    nullable: list[str] = field(default_factory=list)

    def preflight(self, step, source: ResolvedConnection) -> dict[str, SourceFacts]:
        if self.error is not None:
            raise self.error
        columns = CUSTOMERS if self.table_exists else {}
        facts = SourceFacts(self.table_exists, frozenset(columns), columns)
        return {table.name: facts for table in step.tables}

    def make_columns_nullable(self, step, table, warehouse, columns) -> None:
        self.nullable.extend(columns)


@dataclass
class InMemorySnapshots:
    shapes: dict[tuple[str, str, str], TableShape] = field(default_factory=dict)
    error: Exception | None = None

    def load(self, warehouse, pipeline: str, source: str, table: str) -> TableShape | None:
        if self.error is not None:
            raise self.error
        return self.shapes.get((pipeline, source, table))

    def save(self, warehouse, pipeline: str, source: str, table: str, shape: TableShape) -> None:
        self.shapes[(pipeline, source, table)] = shape


@dataclass
class FakeProbe:
    unreachable: set[str] = field(default_factory=set)
    postgres_calls: list[ResolvedConnection] = field(default_factory=list)
    duckdb_calls: list[str] = field(default_factory=list)
    error: Exception | None = None

    def postgres(self, connection: ResolvedConnection) -> bool:
        if self.error is not None:
            raise self.error
        self.postgres_calls.append(connection)
        return connection.database not in self.unreachable

    def duckdb(self, path: str) -> bool:
        self.duckdb_calls.append(path)
        return path not in self.unreachable

    @property
    def databases(self) -> list[str]:
        return [connection.database for connection in self.postgres_calls]


@dataclass
class Seams:
    orchestrator: FakeOrchestrator = field(default_factory=FakeOrchestrator)
    fingerprints: StageFingerprints = field(default_factory=StageFingerprints)
    sync: AcceptSync = field(default_factory=AcceptSync)
    snapshots: InMemorySnapshots = field(default_factory=InMemorySnapshots)
    probe: FakeProbe = field(default_factory=FakeProbe)
    calls: list[str] = field(default_factory=list)
    errors: dict[str, Exception] = field(default_factory=dict)
    run_logs: list[tuple[str, str, str]] = field(default_factory=list)

    def write_run_log(
        self, project: Path, environment: str, pipeline: str, timestamp: datetime, text: str
    ) -> Path:
        self.run_logs.append((environment, pipeline, text))
        return project / "run.log"


@dataclass
class FixedClock:
    def now(self) -> datetime:
        return NOW


@pytest.fixture
def seams(monkeypatch):
    fake = Seams()
    real_project_files = run_command.build_project_files
    results = {
        "build_orchestrator": lambda *_: fake.orchestrator,
        "build_fingerprint_store": lambda *_: fake.fingerprints,
        "build_sync": lambda *_: fake.sync,
        "build_snapshot_store": lambda *_: fake.snapshots,
        "build_project_files": real_project_files,
        "build_run_log_writer": lambda *_: fake.write_run_log,
        "build_clock": lambda *_: FixedClock(),
        "build_connection_probe": lambda *_: fake.probe,
    }

    def spy(name):
        def build(*args, **kwargs):
            fake.calls.append(name)
            if name in fake.errors:
                raise fake.errors[name]
            return results.get(name, lambda *_: object())(*args)

        return build

    for name in SEAMS:
        monkeypatch.setattr(mcp_server if name in MCP_SEAMS else run_command, name, spy(name))
    return fake


@pytest.fixture
def project(tmp_path, monkeypatch):
    secrets = write_project(tmp_path)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    return tmp_path


async def run_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "run", arguments)
    assert_no_secret(result)
    return result


def assert_nothing_touched(seams, locks):
    assert seams.calls == []
    assert locks.events == []
    assert locks.timeouts == []


async def test_run_in_dev_returns_the_steps_as_plain_json(project, seams, locks):
    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    assert not result.isError
    reply = result.structuredContent
    assert json.loads(json.dumps(reply)) == {
        "pipeline": "shop_sync",
        "environment": "dev",
        "succeeded": True,
        "steps": [
            {
                "name": "sync_shop",
                "status": "passed",
                "checks": [
                    {
                        "table": "customers",
                        "check": "row_count_matches",
                        "status": "pass",
                        "message": "20 rows",
                    }
                ],
                "rows_loaded": {"customers": 20},
                "error": None,
                "preview": None,
            }
        ],
    }
    assert locks.events == [("enter", "dev"), ("exit", "dev")]
    assert locks.timeouts == [0]


async def test_run_in_a_branch_builds_it_from_stage_under_stages_lock(project, seams, locks):
    result = await run_tool(project, pipeline="shop_sync", branch="feature/x")

    assert not result.isError
    assert result.structuredContent["environment"] == "br_feature_x"
    assert [plan.environment for plan in seams.orchestrator.plans] == ["br_feature_x"]
    assert locks.events == [("enter", "stage"), ("exit", "stage")]
    assert locks.timeouts == [0]
    assert locks.warehouses[0].database == "w_stage"


async def test_a_preview_reports_counts_and_sample_keys_only(project, seams, locks):
    sample = PreviewSample(inserts=(7,), updates=((8, ("email",)),), deletes=(9,), kept=())
    preview = ReverseSyncPreview(1, 1, 1, 0, sample, written=False)
    seams.orchestrator.steps = (StepReport("push_crm", StepStatus.PASSED, preview=preview),)

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    assert result.structuredContent["steps"][0]["preview"] == {
        "would_insert": 1,
        "would_update": 1,
        "would_delete": 1,
        "kept": 0,
        "sample_insert_keys": ["7"],
        "sample_update_keys": [{"key": "8", "columns": ["email"]}],
        "sample_delete_keys": ["9"],
        "sample_kept_keys": [],
    }


async def test_a_step_that_reports_a_write_is_refused(project, seams, locks):
    sample = PreviewSample(inserts=(), updates=(), deletes=(), kept=())
    written = ReverseSyncPreview(3, 0, 0, 0, sample, written=True)
    seams.orchestrator.steps = (StepReport("push_crm", StepStatus.PASSED, preview=written),)

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    text = error_text(result)
    assert "reported a write in dev" in text
    assert "passed" not in text
    assert result.structuredContent is None


@pytest.mark.parametrize(
    "arguments",
    [
        {"environment": "stage"},
        {"environment": "prod"},
        {"environment": "qa"},
        {"environment": "Dev"},
        {"environment": "dev", "branch": "feat"},
        {},
    ],
)
async def test_every_other_environment_is_refused_before_anything_runs(
    project, seams, locks, arguments
):
    result = await run_tool(project, pipeline="shop_sync", **arguments)

    assert "exactly `dev`" in error_text(result)
    assert_nothing_touched(seams, locks)


async def test_dev_sharing_stages_warehouse_is_refused_before_anything_runs(tmp_path, seams, locks):
    write_project(tmp_path, DEV_SHARES_STAGE_WAREHOUSE)

    result = await call_tool(tmp_path, "run", {"pipeline": "shop_sync", "environment": "dev"})

    assert_no_secret(result, secret_values(DEV_SHARES_STAGE_WAREHOUSE))
    assert "dev uses the same database as stage" in error_text(result)
    assert_nothing_touched(seams, locks)


async def test_dev_sharing_a_duckdb_file_with_stage_is_refused(tmp_path, seams, locks):
    settings = """
environments:
  dev:
    warehouse: {type: duckdb, path: data/../warehouse.duckdb}
    sources: {}
  stage:
    warehouse: {type: duckdb, path: warehouse.duckdb}
    sources: {}
"""
    write_project(tmp_path, settings)

    result = await call_tool(tmp_path, "run", {"pipeline": "shop_sync", "environment": "dev"})

    assert_no_secret(result, secret_values(settings))
    assert "dev uses the same database as stage" in error_text(result)
    assert_nothing_touched(seams, locks)


async def test_adops_yaml_is_read_again_on_every_call(project, seams, locks):
    first = await run_tool(project, pipeline="shop_sync", environment="dev")
    (project / "adops.yaml").write_text(DEV_SHARES_STAGE_WAREHOUSE)
    seams.calls.clear()
    locks.events.clear()
    locks.timeouts.clear()

    second = await call_tool(project, "run", {"pipeline": "shop_sync", "environment": "dev"})

    assert not first.isError
    assert_no_secret(second)
    assert "dev uses the same database as stage" in error_text(second)
    assert_nothing_touched(seams, locks)


async def test_a_held_dev_lock_replies_busy_at_once(project, seams, locks):
    locks.error = WarehouseLocked("dev", 0)

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    assert error_text(result) == (
        "Error executing tool run: The dev Warehouse is busy with another run; try again later."
    )
    assert locks.timeouts == [0]
    assert "build_orchestrator" in seams.calls
    assert seams.orchestrator.plans == []


async def test_a_held_stage_lock_replies_busy_for_a_branch_run(project, seams, locks):
    locks.error = WarehouseLocked("stage", 0)

    result = await run_tool(project, pipeline="shop_sync", branch="feat")

    assert "The stage Warehouse is busy with another run; try again later." in error_text(result)
    assert locks.timeouts == [0]
    assert seams.calls == RUN_LOG_SEAMS
    assert seams.run_logs == [
        (
            "br_feat",
            "shop_sync",
            "Another run is using the stage Warehouse; gave up after 0 minutes.\n",
        )
    ]


@pytest.mark.parametrize("tool", ["run", "preview"])
async def test_an_mcp_run_writes_one_run_log_of_the_cli_output(project, seams, locks, tool):
    result = await call_tool(project, tool, {"pipeline": "shop_sync", "environment": "dev"})

    assert not result.isError
    assert seams.run_logs == [
        (
            "dev",
            "shop_sync",
            "step sync_shop: passed — customers 20 rows\n"
            "  check customers row_count_matches: pass\n"
            "Pipeline shop_sync passed in Environment dev.\n",
        )
    ]
    for _, _, text in seams.run_logs:
        for value in secret_values().values():
            assert value not in text


def test_run_project_keeps_the_cli_wait_by_default(project, seams, locks):
    run_command.run_project(project, "shop_sync", "dev")

    assert locks.timeouts == [DEFAULT_LOCK_TIMEOUT]


def test_adops_run_keeps_the_cli_wait(project, seams, locks):
    result = runner.invoke(cli.app, ["run", "shop_sync", "--project", str(project)])

    assert result.exit_code == 0, result.output
    assert locks.timeouts == [DEFAULT_LOCK_TIMEOUT]


async def test_a_branch_run_without_stages_secret_names_the_secret_reference(project, seams, locks):
    env = (project / ".env").read_text().splitlines()
    kept = [line for line in env if not line.startswith("ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD=")]
    (project / ".env").write_text("\n".join(kept) + "\n")

    result = await run_tool(project, pipeline="shop_sync", branch="feat")

    assert "ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD" in error_text(result)
    assert locks.events == []


async def test_a_branch_run_before_stage_ran_says_so(project, seams, locks):
    seams.fingerprints.saved.clear()

    result = await run_tool(project, pipeline="shop_sync", branch="feat")

    assert "run the Pipeline in stage first" in error_text(result)


async def test_invalid_adops_yaml_returns_the_validation_lines(project, seams, locks):
    broken = SETTINGS.replace("port: 5433, database: w_dev", "port: nope, database: w_dev")
    (project / "adops.yaml").write_text(broken)
    with pytest.raises(PipelineValidationError) as caught:
        load_project_settings(broken)

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    assert error_text(result) == "Error executing tool run: " + "\n".join(error_lines(caught.value))
    assert_nothing_touched(seams, locks)


async def test_a_missing_pipeline_file_is_named(project, seams, locks):
    result = await run_tool(project, pipeline="nope", environment="dev")

    assert error_text(result) == "Error executing tool run: pipelines/nope.yaml: file not found"


async def test_an_unreachable_warehouse_fails_the_first_step(project, seams, locks):
    locks.error = WarehouseUnreachable()

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    reply = result.structuredContent
    assert reply["succeeded"] is False
    assert reply["steps"][0]["status"] == "failed"
    assert reply["steps"][0]["error"] == str(WarehouseUnreachableError("shop_sync", "sync_shop"))


async def test_an_unexpected_error_is_reduced_to_its_type(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_DEV_WAREHOUSE_PASSWORD"]
    seams.errors["build_sync"] = RuntimeError(f"connect failed with password {secret}")

    result = await run_tool(project, pipeline="shop_sync", environment="dev")

    assert error_text(result) == "Error executing tool run: unexpected RuntimeError"


def test_assert_no_secret_catches_a_secret_in_structured_content():
    secret = secret_values()["ADOPS_MCP_ALERT_WEBHOOK"]
    leaking = CallToolResult(
        content=[TextContent(type="text", text="fine")], structuredContent={"error": secret}
    )

    with pytest.raises(AssertionError, match="ADOPS_MCP_ALERT_WEBHOOK"):
        assert_no_secret(leaking)


def test_stdout_goes_to_stderr_while_the_protocol_keeps_real_stdout(capfd):
    with mcp_server.stdout_to_stderr() as protocol:
        print("from-print")
        sys.stdout.flush()
        os.write(1, b"from-fd-1\n")
        os.write(protocol, b"protocol\n")

    captured = capfd.readouterr()
    assert captured.out == "protocol\n"
    assert "from-print" in captured.err
    assert "from-fd-1" in captured.err


def test_serve_writes_the_protocol_to_real_stdout_only(capfd, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"")))

    async def fake_serve_streams(server, stdin, stdout):
        print("engine noise")
        await stdout.write("protocol\n")
        await stdout.flush()

    monkeypatch.setattr(mcp_server, "serve_streams", fake_serve_streams)

    mcp_server.serve(mcp_server.build_server(tmp_path))

    captured = capfd.readouterr()
    assert captured.out == "protocol\n"
    assert "engine noise" in captured.err


async def test_serve_streams_answers_json_rpc_over_the_given_streams(tmp_path):
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    ]
    stdout = io.StringIO()

    def reply(request_id: int) -> dict | None:
        replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
        return next((r for r in replies if r.get("id") == request_id), None)

    async def stdin():
        # The server cancels requests still in flight when stdin ends, so hold
        # stdin open until the tools/list reply is written.
        for request in requests:
            yield json.dumps(request) + "\n"
        while reply(2) is None:
            await anyio.sleep(0.01)

    with anyio.fail_after(10):
        await mcp_server.serve_streams(
            mcp_server.build_server(tmp_path), stdin(), anyio.wrap_file(stdout)
        )

    tools = reply(2)["result"]["tools"]
    assert {tool["name"] for tool in tools} == TOOLS


def test_adops_mcp_serves_the_project(monkeypatch, tmp_path):
    served = []
    monkeypatch.setattr(mcp_server, "serve", served.append)

    result = runner.invoke(cli.app, ["mcp", "--project", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert len(served) == 1


async def list_tools(project: Path) -> dict[str, dict]:
    server = mcp_server.build_server(project)
    async with create_connected_server_and_client_session(server) as session:
        listed = await session.list_tools()
    return {tool.name: tool.inputSchema for tool in listed.tools}


async def test_the_server_lists_exactly_the_built_tools(tmp_path):
    assert set(await list_tools(tmp_path)) == TOOLS


async def validate_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "validate", arguments)
    assert_no_secret(result)
    return result


async def test_validate_takes_only_an_optional_pipeline(tmp_path):
    schema = (await list_tools(tmp_path))["validate"]

    assert set(schema["properties"]) == {"pipeline"}
    assert schema.get("required", []) == []


@pytest.mark.parametrize("arguments", [{}, {"pipeline": "shop_sync"}])
async def test_validate_passes_a_valid_project_without_connecting(project, seams, locks, arguments):
    result = await validate_tool(project, **arguments)

    assert result.structuredContent == {
        "valid": True,
        "problems": [],
        "skipped": [],
        "schedules": ["shop_sync: no schedule"],
    }
    assert_nothing_touched(seams, locks)


async def test_validate_returns_the_problems_of_a_failing_pipeline(project, seams, locks):
    (project / "pipelines" / "shop_sync.yaml").write_text(PIPELINE_WITH_UNKNOWN_SOURCE)

    result = await validate_tool(project, pipeline="shop_sync")

    reply = result.structuredContent
    assert reply["valid"] is False
    assert reply["problems"] == [
        "pipelines/shop_sync.yaml: steps[0].source: no Source with this name in the Environment "
        f"(Environment {name})"
        for name in ("dev", "prod", "stage")
    ]
    assert reply["schedules"] == ["shop_sync: no schedule"]
    assert_nothing_touched(seams, locks)


async def test_validate_returns_the_problems_of_a_bad_accepted_change_record(project, seams, locks):
    accepted = project / "pipelines" / "accepted"
    accepted.mkdir()
    (accepted / "r1.yaml").write_text("pipeline: nope\ntable: orders\nchanges: [id dropped]\n")

    result = await validate_tool(project)

    reply = result.structuredContent
    assert reply["valid"] is False
    assert reply["problems"] == ["pipelines/accepted/r1.yaml: pipeline: no Pipeline with this name"]
    assert_nothing_touched(seams, locks)


async def test_validate_names_a_missing_pipeline(project, seams, locks):
    result = await validate_tool(project, pipeline="nope")

    assert result.structuredContent["problems"] == ["pipelines/nope.yaml: file not found"]
    assert_nothing_touched(seams, locks)


async def test_validate_turns_broken_adops_yaml_into_its_problem_line(project, seams, locks):
    (project / "adops.yaml").write_text("environments: [dev\n")

    result = await validate_tool(project)

    reply = result.structuredContent
    assert reply["valid"] is False
    assert reply["problems"] == ["adops.yaml: (root): not valid YAML (line 2, column 1)"]
    assert reply["schedules"] == []
    assert_nothing_touched(seams, locks)


async def test_validate_reduces_an_unexpected_error_to_its_type(project, seams, locks, monkeypatch):
    secret = secret_values()["ADOPS_MCP_DEV_SHOP_PASSWORD"]

    def failing(*_):
        raise RuntimeError(f"connect to shop with {secret}")

    monkeypatch.setattr(mcp_server, "validate_project", failing)

    result = await validate_tool(project)

    assert error_text(result) == "Error executing tool validate: unexpected RuntimeError"


PIPELINE_WITH_UNKNOWN_SOURCE = """
name: shop_sync
steps:
  - name: sync_shop
    source: crm_db
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
"""

ROW_VALUE = "ROWVALUE-zz9"


async def preview_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "preview", arguments)
    assert_no_secret(result)
    return result


def wide_preview() -> ReverseSyncPreview:
    keys = tuple(range(1, 9))
    sample = PreviewSample(
        inserts=(*keys[:7], ROW_VALUE),
        updates=tuple((key, ("email",)) for key in keys),
        deletes=keys,
        kept=keys,
    )
    return ReverseSyncPreview(8, 8, 8, 8, sample, written=False)


def steps_with_a_reverse_sync() -> tuple[StepReport, ...]:
    check = CheckResult("row_count_matches", "customers", CheckStatus.PASS, ROW_VALUE)
    sync = StepReport("sync_shop", StepStatus.PASSED, (check,), {"customers": 20})
    return (sync, StepReport("push_crm", StepStatus.PASSED, preview=wide_preview()))


async def test_preview_returns_counts_and_capped_sample_keys_only(project, seams, locks):
    seams.orchestrator.steps = steps_with_a_reverse_sync()

    result = await preview_tool(project, pipeline="shop_sync", environment="dev")

    assert not result.isError
    capped = ["1", "2", "3", "4", "5"]
    assert result.structuredContent == {
        "pipeline": "shop_sync",
        "environment": "dev",
        "succeeded": True,
        "previews": [
            {
                "step": "push_crm",
                "preview": {
                    "would_insert": 8,
                    "would_update": 8,
                    "would_delete": 8,
                    "kept": 8,
                    "sample_insert_keys": capped,
                    "sample_update_keys": [{"key": key, "columns": ["email"]} for key in capped],
                    "sample_delete_keys": capped,
                    "sample_kept_keys": capped,
                },
            }
        ],
        "errors": [],
    }
    serialized = result.model_dump_json()
    assert ROW_VALUE not in serialized
    assert "rows_loaded" not in serialized
    assert "checks" not in serialized
    assert locks.events == [("enter", "dev"), ("exit", "dev")]
    assert locks.timeouts == [0]


async def test_preview_in_a_branch_previews_from_stages_lock(project, seams, locks):
    seams.orchestrator.steps = steps_with_a_reverse_sync()

    result = await preview_tool(project, pipeline="shop_sync", branch="feature/x")

    reply = result.structuredContent
    assert reply["environment"] == "br_feature_x"
    assert [entry["step"] for entry in reply["previews"]] == ["push_crm"]
    assert [plan.environment for plan in seams.orchestrator.plans] == ["br_feature_x"]
    assert locks.events == [("enter", "stage"), ("exit", "stage")]
    assert locks.timeouts == [0]


async def test_preview_refuses_a_step_that_reports_a_write(project, seams, locks):
    sample = PreviewSample(inserts=(), updates=(), deletes=(), kept=())
    written = ReverseSyncPreview(3, 0, 0, 0, sample, written=True)
    seams.orchestrator.steps = (StepReport("push_crm", StepStatus.PASSED, preview=written),)

    result = await preview_tool(project, pipeline="shop_sync", environment="dev")

    assert "reported a write in dev" in error_text(result)
    assert result.structuredContent is None


@pytest.mark.parametrize(
    "arguments",
    [{"environment": "stage"}, {"environment": "prod"}, {"environment": "qa"}, {}],
)
async def test_preview_refuses_every_other_environment_before_anything_runs(
    project, seams, locks, arguments
):
    result = await preview_tool(project, pipeline="shop_sync", **arguments)

    assert "exactly `dev`" in error_text(result)
    assert_nothing_touched(seams, locks)


async def test_preview_refuses_dev_sharing_stages_warehouse(tmp_path, seams, locks):
    write_project(tmp_path, DEV_SHARES_STAGE_WAREHOUSE)

    result = await call_tool(tmp_path, "preview", {"pipeline": "shop_sync", "environment": "dev"})

    assert_no_secret(result, secret_values(DEV_SHARES_STAGE_WAREHOUSE))
    assert "dev uses the same database as stage" in error_text(result)
    assert_nothing_touched(seams, locks)


@pytest.mark.parametrize(
    ("environment", "arguments"),
    [("dev", {"environment": "dev"}), ("stage", {"branch": "feat"})],
)
async def test_a_held_lock_gives_preview_the_same_busy_reply_as_run(
    project, seams, locks, environment, arguments
):
    locks.error = WarehouseLocked(environment, 0)

    previewed = await preview_tool(project, pipeline="shop_sync", **arguments)
    ran = await run_tool(project, pipeline="shop_sync", **arguments)

    busy = f"The {environment} Warehouse is busy with another run; try again later."
    assert error_text(previewed) == f"Error executing tool preview: {busy}"
    assert error_text(ran) == f"Error executing tool run: {busy}"
    assert locks.timeouts == [0, 0]


async def test_preview_with_an_unreachable_target_returns_the_steps_error(project, seams, locks):
    locks.error = WarehouseUnreachable()

    result = await preview_tool(project, pipeline="shop_sync", environment="dev")

    reply = result.structuredContent
    assert reply["succeeded"] is False
    assert reply["previews"] == []
    assert reply["errors"] == [
        {"step": "sync_shop", "error": str(WarehouseUnreachableError("shop_sync", "sync_shop"))}
    ]


async def test_preview_maps_invalid_adops_yaml_to_its_problem_lines(project, seams, locks):
    (project / "adops.yaml").write_text("environments: [dev\n")

    result = await preview_tool(project, pipeline="shop_sync", environment="dev")

    assert error_text(result) == (
        "Error executing tool preview: (root): not valid YAML (line 2, column 1)"
    )
    assert_nothing_touched(seams, locks)


STAGE_SCHEMAS = ("analytics", "raw_shop", "adops")
FEAT_SCHEMAS = ("br_feat__raw_shop", "br_feat__analytics")


@dataclass
class FakeBranchWarehouse:
    names: tuple[str, ...] = STAGE_SCHEMAS + FEAT_SCHEMAS + ("br_feat2__analytics",)
    error: Exception | None = None
    listed: list[object] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    deleted_state: list[str] = field(default_factory=list)

    def list_schemas(self, warehouse) -> list[str]:
        self.listed.append(warehouse)
        if self.error is not None:
            raise self.error
        return list(self.names)

    def drop(self, warehouse, schema: str) -> None:
        self.dropped.append(schema)

    def delete(self, environment: str) -> None:
        self.deleted_state.append(environment)


@pytest.fixture
def branch_warehouse(monkeypatch):
    fake = FakeBranchWarehouse()
    monkeypatch.setattr(drop_branch_command, "build_warehouse_schemas", lambda *_: fake)
    monkeypatch.setattr(drop_branch_command, "build_branch_state", lambda *_: fake)
    return fake


async def drop_branch_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "drop_branch", arguments)
    assert_no_secret(result)
    return result


def assert_nothing_dropped(branch_warehouse, locks):
    assert branch_warehouse.listed == []
    assert branch_warehouse.dropped == []
    assert branch_warehouse.deleted_state == []
    assert locks.events == []


async def test_drop_branch_takes_only_a_branch(tmp_path):
    schema = (await list_tools(tmp_path))["drop_branch"]

    assert set(schema["properties"]) == {"branch"}
    assert schema["required"] == ["branch"]


async def test_drop_branch_drops_only_the_branchs_schemas_under_stages_lock(
    project, branch_warehouse, locks
):
    result = await drop_branch_tool(project, branch="feat")

    assert result.structuredContent == {
        "environment": "br_feat",
        "dropped_schemas": ["br_feat__analytics", "br_feat__raw_shop"],
    }
    assert branch_warehouse.dropped == ["br_feat__analytics", "br_feat__raw_shop"]
    assert branch_warehouse.deleted_state == ["br_feat"]
    assert locks.events == [("enter", "stage"), ("exit", "stage")]
    assert locks.timeouts == [mcp_server.ASSISTANT_LOCK_TIMEOUT]
    assert [warehouse.database for warehouse in locks.warehouses] == ["w_stage"]


async def test_drop_branch_with_nothing_left_succeeds(project, branch_warehouse, locks):
    branch_warehouse.names = STAGE_SCHEMAS

    result = await drop_branch_tool(project, branch="feat")

    assert result.structuredContent == {"environment": "br_feat", "dropped_schemas": []}
    assert branch_warehouse.dropped == []


async def test_drop_branch_refuses_an_empty_branch_before_anything_runs(
    project, branch_warehouse, locks
):
    result = await drop_branch_tool(project, branch="")

    assert error_text(result) == (
        "Error executing tool drop_branch: the Assistant works only in the Environment named "
        "exactly `dev` or in a Branch Environment; name `dev` or a branch"
    )
    assert_nothing_dropped(branch_warehouse, locks)
    assert locks.timeouts == []


async def test_drop_branch_rejects_a_branch_name_without_letters_or_digits(
    project, branch_warehouse, locks
):
    result = await drop_branch_tool(project, branch="///")

    assert error_text(result) == (
        "Error executing tool drop_branch: --branch: the branch name has no letters or digits "
        "to name a Branch Environment"
    )
    assert branch_warehouse.listed == []
    assert branch_warehouse.deleted_state == []


async def test_a_held_stage_lock_replies_busy_to_drop_branch_at_once(
    project, branch_warehouse, locks
):
    locks.error = WarehouseLocked("stage", 0)

    result = await drop_branch_tool(project, branch="feat")

    assert error_text(result) == (
        "Error executing tool drop_branch: "
        "The stage Warehouse is busy with another run; try again later."
    )
    assert locks.timeouts == [mcp_server.ASSISTANT_LOCK_TIMEOUT]
    assert_nothing_dropped(branch_warehouse, locks)


async def test_drop_branch_without_stages_secret_names_only_the_secret_reference(
    project, branch_warehouse, locks
):
    env = (project / ".env").read_text().splitlines()
    kept = [line for line in env if not line.startswith("ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD=")]
    (project / ".env").write_text("\n".join(kept) + "\n")

    result = await drop_branch_tool(project, branch="feat")

    assert "ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD" in error_text(result)
    assert_nothing_dropped(branch_warehouse, locks)


async def test_drop_branch_on_an_unreachable_warehouse_gives_fixed_text(
    project, branch_warehouse, locks
):
    branch_warehouse.error = WarehouseUnreachable()

    result = await drop_branch_tool(project, branch="feat")

    assert error_text(result) == (
        "Error executing tool drop_branch: "
        "The Warehouse is unreachable or locked by another process."
    )
    assert branch_warehouse.deleted_state == []
    assert locks.events == [("enter", "stage"), ("exit", "stage")]


async def test_drop_branch_without_adops_yaml_names_the_file(tmp_path, branch_warehouse, locks):
    result = await drop_branch_tool(tmp_path, branch="feat")

    assert error_text(result) == "Error executing tool drop_branch: adops.yaml: file not found"
    assert_nothing_dropped(branch_warehouse, locks)


async def test_drop_branch_reduces_an_unexpected_error_to_its_type(
    project, branch_warehouse, locks
):
    secret = secret_values()["ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD"]
    branch_warehouse.error = KeyError(f"postgresql://w:{secret}@localhost/w_stage")

    result = await drop_branch_tool(project, branch="feat")

    assert error_text(result) == "Error executing tool drop_branch: unexpected KeyError"
    assert branch_warehouse.deleted_state == []


@dataclass
class AcceptCalls:
    arguments: list[tuple[object, ...]] = field(default_factory=list)


@pytest.fixture
def accepted(monkeypatch):
    calls = AcceptCalls()
    real = mcp_server.accept_project_schema

    def spy(*arguments):
        calls.arguments.append(arguments)
        return real(*arguments)

    monkeypatch.setattr(mcp_server, "accept_project_schema", spy)
    return calls


async def accept_schema_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "accept_schema", {"pipeline": "shop_sync", **arguments})
    assert_no_secret(result)
    return result


def assert_nothing_accepted(seams, locks, accepted):
    assert_nothing_touched(seams, locks)
    assert accepted.arguments == []
    assert seams.snapshots.shapes == {}


async def test_accept_schema_takes_a_pipeline_a_table_and_dev_or_a_branch(tmp_path):
    schema = (await list_tools(tmp_path))["accept_schema"]

    assert set(schema["properties"]) == {"pipeline", "table", "environment", "branch"}
    assert sorted(schema["required"]) == ["pipeline", "table"]


async def test_accept_schema_in_dev_records_the_shape_without_a_lock(
    project, seams, locks, accepted
):
    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert result.structuredContent == {
        "pipeline": "shop_sync",
        "table": "customers",
        "step": "sync_shop",
        "environment": "dev",
        "column_count": 2,
        "nullable_columns": [],
        "record_path": None,
        "covered_by": [],
    }
    assert accepted.arguments == [(project, "shop_sync", "customers", "dev")]
    assert seams.snapshots.shapes == {("shop_sync", "shop", "customers"): TableShape(CUSTOMERS)}
    assert locks.events == []
    assert locks.timeouts == []


async def test_accept_schema_names_the_columns_that_may_now_be_null(
    project, seams, locks, accepted
):
    seams.snapshots.shapes[("shop_sync", "shop", "customers")] = TableShape(
        {**CUSTOMERS, "phone": "text"}
    )

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert result.structuredContent["nullable_columns"] == ["phone"]
    assert seams.sync.nullable == ["phone"]


def seed_dropped_phone(seams) -> None:
    seams.snapshots.shapes[("shop_sync", "shop", "customers")] = TableShape(
        {**CUSTOMERS, "phone": "text"}
    )


DROPPED_PHONE_RECORD = "pipelines/accepted/20261122T093000Z-shop_sync-customers.yaml"


async def test_accept_schema_writes_a_record_and_returns_its_project_path_without_a_lock(
    project, seams, locks, accepted
):
    seed_dropped_phone(seams)

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert result.structuredContent["record_path"] == DROPPED_PHONE_RECORD
    assert result.structuredContent["covered_by"] == []
    assert (project / DROPPED_PHONE_RECORD).exists()
    [record] = run_command.load_project_records(project)
    assert (record.pipeline, record.table) == ("shop_sync", "customers")
    assert [change.message for change in record.changes] == ["phone dropped"]
    assert locks.events == []
    assert locks.timeouts == []


async def test_accept_schema_names_the_record_that_already_covers_the_change(
    project, seams, locks, accepted
):
    seed_dropped_phone(seams)
    first = await accept_schema_tool(project, table="customers", environment="dev")
    seed_dropped_phone(seams)

    second = await accept_schema_tool(project, table="customers", environment="dev")

    assert second.structuredContent["record_path"] is None
    assert second.structuredContent["covered_by"] == [first.structuredContent["record_path"]]
    assert [path.name for path in (project / "pipelines" / "accepted").iterdir()] == [
        Path(DROPPED_PHONE_RECORD).name
    ]


async def test_accept_schema_refuses_to_overwrite_a_record_with_the_same_name(
    project, seams, locks, accepted
):
    seed_dropped_phone(seams)
    clashing = project / DROPPED_PHONE_RECORD
    clashing.parent.mkdir(parents=True)
    clashing.write_text("pipeline: shop_sync\ntable: orders\nchanges: [id dropped]\n")

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert result.isError
    assert error_text(result) == (
        f"Error executing tool accept_schema: {DROPPED_PHONE_RECORD}: "
        "a record with this name already exists; nothing accepted"
    )
    assert seams.sync.nullable == []


@pytest.mark.parametrize(
    "arguments",
    [
        {"environment": "stage"},
        {"environment": "prod"},
        {"environment": "qa"},
        {"environment": "production"},
        {"environment": "Dev"},
        {"environment": "dev "},
        {"environment": "development"},
        {"environment": "dev", "branch": "feat"},
        {},
    ],
)
async def test_accept_schema_refuses_every_other_environment_before_anything_runs(
    project, seams, locks, accepted, arguments
):
    result = await accept_schema_tool(project, table="customers", **arguments)

    assert "exactly `dev`" in error_text(result)
    assert_nothing_accepted(seams, locks, accepted)


async def test_accept_schema_refuses_a_branch_before_anything_runs(project, seams, locks, accepted):
    result = await accept_schema_tool(project, table="customers", branch="feature")

    assert error_text(result) == (
        "Error executing tool accept_schema: the Assistant accepts a Breaking Change only in "
        "`dev`; to accept one in a Branch Environment, ask the user to run "
        "`adops accept-schema --branch <name>` by hand"
    )
    assert_nothing_accepted(seams, locks, accepted)


async def test_accept_schema_refuses_dev_sharing_stages_warehouse(tmp_path, seams, locks, accepted):
    write_project(tmp_path, DEV_SHARES_STAGE_WAREHOUSE)

    result = await call_tool(
        tmp_path,
        "accept_schema",
        {"pipeline": "shop_sync", "table": "customers", "environment": "dev"},
    )

    assert_no_secret(result, secret_values(DEV_SHARES_STAGE_WAREHOUSE))
    assert "dev uses the same database as stage" in error_text(result)
    assert_nothing_accepted(seams, locks, accepted)


async def test_accept_schema_with_an_unreachable_source_names_only_the_table(project, seams, locks):
    seams.sync.error = SourceUnreachable("customers")

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert error_text(result) == (
        "Error executing tool accept_schema: The Source of table customers is unreachable."
    )
    assert seams.snapshots.shapes == {}


async def test_accept_schema_with_an_unreachable_warehouse_gives_fixed_text(project, seams, locks):
    seams.snapshots.error = WarehouseUnreachable()

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert error_text(result) == (
        "Error executing tool accept_schema: "
        "The Warehouse is unreachable or locked by another process."
    )


async def test_accept_schema_without_devs_source_secret_names_only_the_reference(
    project, seams, locks
):
    env = (project / ".env").read_text().splitlines()
    kept = [line for line in env if not line.startswith("ADOPS_MCP_DEV_SHOP_PASSWORD=")]
    (project / ".env").write_text("\n".join(kept) + "\n")

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert error_text(result) == (
        "Error executing tool accept_schema: these environment variables are not set or are "
        "empty: ADOPS_MCP_DEV_SHOP_PASSWORD"
    )
    assert seams.snapshots.shapes == {}


async def test_accept_schema_for_a_table_missing_from_the_source_says_nothing_was_accepted(
    project, seams, locks
):
    seams.sync.table_exists = False

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert error_text(result) == (
        "Error executing tool accept_schema: "
        "Step sync_shop: customers not found in Source shop; nothing accepted"
    )
    assert seams.snapshots.shapes == {}


async def test_accept_schema_for_a_table_no_step_copies_names_the_problem(project, seams, locks):
    result = await accept_schema_tool(project, table="orders", environment="dev")

    assert error_text(result) == (
        "Error executing tool accept_schema: orders: no Step in Pipeline shop_sync copies it"
    )


async def test_accept_schema_reduces_an_unexpected_error_to_its_type(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_DEV_WAREHOUSE_PASSWORD"]
    seams.snapshots.error = RuntimeError(f"postgresql://w:{secret}@localhost/w_dev")

    result = await accept_schema_tool(project, table="customers", environment="dev")

    assert error_text(result) == "Error executing tool accept_schema: unexpected RuntimeError"


@dataclass
class FileReads:
    calls: list[str] = field(default_factory=list)


@pytest.fixture
def file_reads(monkeypatch):
    reads = FileReads()

    def spy(name):
        def refuse(*_):
            reads.calls.append(name)
            raise AssertionError(f"{name} must not run for a refused Pipeline name")

        return refuse

    monkeypatch.setattr(run_command, "read_settings", spy("read_settings"))
    monkeypatch.setattr(run_command, "read_project_files", spy("read_project_files"))
    monkeypatch.setattr(mcp_server, "validate_project", spy("validate_project"))
    return reads


PIPELINE_TOOL_ARGUMENTS = {
    "run": {"environment": "dev"},
    "preview": {"environment": "dev"},
    "validate": {},
    "accept_schema": {"table": "customers", "environment": "dev"},
}


@pytest.mark.parametrize("tool", sorted(PIPELINE_TOOL_ARGUMENTS))
@pytest.mark.parametrize("pipeline", ["../adops", "a/b", "a\\b", ".."])
async def test_every_pipeline_tool_refuses_a_name_outside_pipelines_before_reading_a_file(
    project, seams, locks, file_reads, tool, pipeline
):
    arguments = {"pipeline": pipeline, **PIPELINE_TOOL_ARGUMENTS[tool]}

    result = await call_tool(project, tool, arguments)

    assert_no_secret(result)
    assert error_text(result) == (
        f"Error executing tool {tool}: the Assistant takes only a plain Pipeline name, the file "
        "name in pipelines/ without .yaml; a name with a slash, a backslash, two dots in a row "
        "or a NUL character is refused"
    )
    assert file_reads.calls == []
    assert_nothing_touched(seams, locks)


VIEW_SETTINGS = (
    SETTINGS.replace("user: w,", "user: wh_login_7,")
    .replace("user: s,", "user: src_login_7,")
    .replace("user: c,", "user: crm_login_7,")
    + """  local:
    warehouse: {type: duckdb, path: hidden_dir/local.duckdb}
    sources:
      shop: {type: postgres, host: src.local.example, port: 5440, database: shop_local,
             user: src_login_7, password_env: ADOPS_MCP_LOCAL_SHOP_PASSWORD}
"""
)

CONNECTION_DETAILS = (
    "localhost",
    "db.internal",
    "src.local.example",
    "w_dev",
    "w_stage",
    "w_prod",
    "crm_dev",
    "shop_local",
    "wh_login_7",
    "src_login_7",
    "crm_login_7",
    "5433",
    "5434",
    "5440",
    "hidden_dir",
    "local.duckdb",
)

FULL_PIPELINE = """
name: a_full
schedule: daily at 02:00
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
  - name: transform_shop
    type: transform
    project: dbt
  - type: reverse_sync
    name: push_stats
    model: customer_stats
    target: crm
    target_table: customer_stats
    key: customer_id
    mapping: {order_count: orders}
"""

DEV_REFERENCES = (
    "ADOPS_MCP_DEV_WAREHOUSE_PASSWORD",
    "ADOPS_MCP_DEV_SHOP_PASSWORD",
    "ADOPS_MCP_DEV_CRM_PASSWORD",
)


@pytest.fixture
def view_project(tmp_path, monkeypatch):
    secrets = write_project(tmp_path, VIEW_SETTINGS)
    (tmp_path / "pipelines" / "z_last.yaml").write_text(FULL_PIPELINE)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)
    return tmp_path


def project_tree(project: Path) -> dict[str, bytes]:
    return {
        path.relative_to(project).as_posix(): path.read_bytes()
        for path in sorted(project.rglob("*"))
        if path.is_file()
    }


async def project_view_tool(project: Path) -> CallToolResult:
    result = await call_tool(project, "project_view", {})
    assert_no_secret(result, secret_values(VIEW_SETTINGS))
    return result


def statuses(reply: dict) -> dict[str, str]:
    found = dict(reply["alert_webhook"])
    for environment in reply["environments"]:
        found.update(environment["secret_references"])
    return found


async def test_project_view_takes_no_arguments(tmp_path):
    schema = (await list_tools(tmp_path))["project_view"]

    assert schema.get("properties", {}) == {}


async def test_project_view_lists_pipelines_steps_schedules_and_environments(
    view_project, seams, locks
):
    before = project_tree(view_project)

    result = await project_view_tool(view_project)

    assert not result.isError
    reply = result.structuredContent
    assert reply["pipelines"] == [
        {
            "name": "a_full",
            "schedule": "daily at 02:00 (cron 0 2 * * *, UTC)",
            "steps": [
                {"name": "sync_shop", "kind": "sync"},
                {"name": "transform_shop", "kind": "transform"},
                {"name": "push_stats", "kind": "reverse_sync"},
            ],
        },
        {"name": "shop_sync", "schedule": None, "steps": [{"name": "sync_shop", "kind": "sync"}]},
    ]
    assert [environment["name"] for environment in reply["environments"]] == [
        "dev",
        "local",
        "prod",
        "stage",
    ]
    assert reply["environments"][1] == {
        "name": "local",
        "warehouse_type": "duckdb",
        "sources": ["shop"],
        "targets": [],
        "secret_references": {"ADOPS_MCP_LOCAL_SHOP_PASSWORD": "set"},
    }
    assert reply["environments"][2] == {
        "name": "prod",
        "warehouse_type": "postgres",
        "sources": ["shop"],
        "targets": ["crm"],
        "secret_references": {
            "ADOPS_MCP_PROD_WAREHOUSE_PASSWORD": "set",
            "ADOPS_MCP_PROD_SHOP_PASSWORD": "set",
            "ADOPS_MCP_PROD_CRM_PASSWORD": "set",
        },
    }
    assert reply["problems"] == []
    assert_nothing_touched(seams, locks)
    assert project_tree(view_project) == before


async def test_project_view_shows_every_secret_reference_set_and_no_connection_detail(
    view_project,
):
    result = await project_view_tool(view_project)

    found = statuses(result.structuredContent)
    assert set(found) == set(secret_values(VIEW_SETTINGS))
    assert set(found.values()) == {"set"}
    assert found["ADOPS_MCP_ALERT_WEBHOOK"] == "set"
    serialized = result.model_dump_json()
    assert [detail for detail in CONNECTION_DETAILS if detail in serialized] == []


def test_the_detail_scan_catches_an_environment_serialized_wholesale():
    settings = load_project_settings(VIEW_SETTINGS)
    serialized = json.dumps(
        {name: env.model_dump(mode="json") for name, env in settings.environments.items()}
    )

    assert [detail for detail in CONNECTION_DETAILS if detail not in serialized] == []


async def test_project_view_schedule_matches_what_validate_prints(view_project):
    (view_project / "adops.yaml").write_text(VIEW_SETTINGS + "timezone: Europe/Berlin\n")

    result = await project_view_tool(view_project)

    pipelines = result.structuredContent["pipelines"]
    lines = [f"{p['name']}: {p['schedule'] or 'no schedule'}" for p in pipelines]
    assert sorted(lines) == sorted(validate_project(view_project, None).schedules)
    assert pipelines[0]["schedule"] == "daily at 02:00 (cron 0 2 * * *, Europe/Berlin)"


async def test_project_view_shows_missing_for_exactly_the_unset_references(
    view_project, monkeypatch
):
    unset = {"ADOPS_MCP_DEV_SHOP_PASSWORD", "ADOPS_MCP_ALERT_WEBHOOK"}
    secrets = secret_values(VIEW_SETTINGS)
    kept = {name: value for name, value in secrets.items() if name not in unset}
    (view_project / ".env").write_text("".join(f"{n}={v}\n" for n, v in kept.items()))
    monkeypatch.setenv("ADOPS_MCP_DEV_CRM_PASSWORD", "")

    result = await project_view_tool(view_project)

    found = statuses(result.structuredContent)
    missing = {name for name, status in found.items() if status == "missing"}
    assert missing == unset | {"ADOPS_MCP_DEV_CRM_PASSWORD"}
    assert found["ADOPS_MCP_DEV_WAREHOUSE_PASSWORD"] == "set"


async def test_project_view_without_an_alert_webhook_shows_none(project):
    (project / "adops.yaml").write_text(
        SETTINGS.replace("alert_webhook_env: ADOPS_MCP_ALERT_WEBHOOK\n", "")
    )

    result = await project_view_tool(project)

    assert result.structuredContent["alert_webhook"] == {}
    dev = result.structuredContent["environments"][0]
    assert list(dev["secret_references"]) == list(DEV_REFERENCES)


async def test_project_view_without_a_pipelines_folder_lists_only_environments(
    view_project, seams, locks
):
    for path in (view_project / "pipelines").iterdir():
        path.unlink()
    (view_project / "pipelines").rmdir()

    result = await project_view_tool(view_project)

    assert not result.isError
    assert result.structuredContent["pipelines"] == []
    assert result.structuredContent["problems"] == []
    assert len(result.structuredContent["environments"]) == 4
    assert_nothing_touched(seams, locks)


async def test_project_view_lists_a_failing_pipelines_problems_and_still_shows_the_rest(
    view_project, seams, locks
):
    (view_project / "pipelines" / "broken.yaml").write_text("name: [broken\n")
    (view_project / "pipelines" / "binary.yaml").write_bytes(b"\xff\xfe\x00")
    before = project_tree(view_project)

    result = await project_view_tool(view_project)

    reply = result.structuredContent
    assert reply["problems"] == [
        "pipelines/binary.yaml: not UTF-8 text",
        "pipelines/broken.yaml: (root): not valid YAML (line 2, column 1)",
    ]
    assert [pipeline["name"] for pipeline in reply["pipelines"]] == ["a_full", "shop_sync"]
    assert_nothing_touched(seams, locks)
    assert project_tree(view_project) == before


async def test_project_view_maps_invalid_adops_yaml_to_its_problem_lines(
    view_project, seams, locks
):
    (view_project / "adops.yaml").write_text("environments: [dev\n")
    before = project_tree(view_project)

    result = await project_view_tool(view_project)

    assert error_text(result) == (
        "Error executing tool project_view: (root): not valid YAML (line 2, column 1)"
    )
    assert_nothing_touched(seams, locks)
    assert project_tree(view_project) == before


async def test_project_view_without_adops_yaml_names_the_file(tmp_path, seams, locks):
    result = await project_view_tool(tmp_path)

    assert error_text(result) == "Error executing tool project_view: adops.yaml: file not found"
    assert_nothing_touched(seams, locks)
    assert project_tree(tmp_path) == {}


async def test_project_view_reduces_an_unexpected_error_to_its_type(view_project, monkeypatch):
    secret = secret_values(VIEW_SETTINGS)["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]

    def explode(_):
        raise RuntimeError(f"password {secret} at db.internal")

    monkeypatch.setattr(mcp_server, "load_pipelines", explode)

    result = await project_view_tool(view_project)

    assert error_text(result) == "Error executing tool project_view: unexpected RuntimeError"


def test_project_view_reply_builds_from_explicit_fields_only():
    settings = load_project_settings(VIEW_SETTINGS)

    reply = mcp_server.project_view_reply(settings, [], ["p: problem"], {})

    assert set(reply) == {"pipelines", "environments", "alert_webhook", "problems"}
    assert set(reply["environments"][0]) == {
        "name",
        "warehouse_type",
        "sources",
        "targets",
        "secret_references",
    }
    assert set(statuses(reply).values()) == {"missing"}
    assert reply["problems"] == ["p: problem"]


async def file_tool(project: Path, tool: str, secrets: dict[str, str], **arguments: object):
    result = await call_tool(project, tool, arguments)
    assert_no_secret(result, secrets)
    return result


async def test_the_file_tools_take_a_path_or_an_optional_directory(tmp_path):
    tools = await list_tools(tmp_path)

    assert set(tools["list_files"]["properties"]) == {"directory"}
    assert tools["list_files"].get("required", []) == []
    assert set(tools["read_file"]["properties"]) == {"path"}
    assert tools["read_file"]["required"] == ["path"]


@pytest.mark.parametrize("relative", ["pipelines/x.yaml", "dbt/models/m.sql", "adops.yaml"])
async def test_read_file_shows_a_secret_value_typed_into_the_file_as_stars(
    project, seams, locks, relative
):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]
    path = project / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    original = path.read_text() if path.exists() else ""
    path.write_text(f"{original}# pasted by mistake: {secret}\n")
    before = project_tree(project)

    result = await file_tool(project, "read_file", secret_values(), path=relative)

    assert not result.isError
    assert result.structuredContent["path"] == relative
    assert result.structuredContent["text"].endswith("# pasted by mistake: ***\n")
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_read_file_redacts_the_process_environment_value_that_wins(
    project, seams, locks, monkeypatch
):
    monkeypatch.setenv("ADOPS_MCP_DEV_SHOP_PASSWORD", "from-the-process-env")
    (project / "pipelines" / "x.yaml").write_text("from-the-process-env\n")
    secrets = {**secret_values(), "process": "from-the-process-env"}

    result = await file_tool(project, "read_file", secrets, path="pipelines/x.yaml")

    assert result.structuredContent["text"] == "***\n"


@pytest.mark.parametrize("relative", [".env", ".ENV", "pipelines/.env", "../.env"])
async def test_read_file_refuses_dotenv_without_its_text(project, seams, locks, relative):
    (project / "pipelines" / ".env").write_text((project / ".env").read_text())
    before = project_tree(project)

    result = await file_tool(project, "read_file", secret_values(), path=relative)

    assert error_text(result) == f"Error executing tool read_file: {OUTSIDE_PROJECT_FILES}"
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_read_file_refuses_a_symlink_to_dotenv(project, seams, locks):
    (project / "pipelines" / "env.yaml").symlink_to(project / ".env")

    result = await file_tool(project, "read_file", secret_values(), path="pipelines/env.yaml")

    assert error_text(result) == f"Error executing tool read_file: {OUTSIDE_PROJECT_FILES}"


async def test_read_file_names_a_missing_file(project, seams, locks):
    result = await file_tool(project, "read_file", secret_values(), path="pipelines/nope.yaml")

    assert error_text(result) == (
        "Error executing tool read_file: pipelines/nope.yaml: file not found"
    )
    assert_nothing_touched(seams, locks)


async def test_read_file_with_broken_adops_yaml_returns_its_problem_lines(project, seams, locks):
    (project / "adops.yaml").write_text("environments: [dev\n")

    result = await file_tool(project, "read_file", secret_values(), path="pipelines/shop_sync.yaml")

    assert error_text(result) == (
        "Error executing tool read_file: (root): not valid YAML (line 2, column 1)"
    )


async def test_read_file_reduces_an_unexpected_error_to_its_type(project, monkeypatch):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]

    def explode(*_):
        raise RuntimeError(f"password {secret}")

    monkeypatch.setattr(mcp_server, "read_project_file", explode)

    result = await file_tool(project, "read_file", secret_values(), path="adops.yaml")

    assert error_text(result) == "Error executing tool read_file: unexpected RuntimeError"


async def test_list_files_lists_only_allowed_paths(project, seams, locks):
    (project / "pipelines" / ".env").write_text("X=1")
    (project / ".adops").mkdir()
    (project / ".adops" / "state.json").write_text("{}")
    (project / "dbt" / "models").mkdir(parents=True)
    (project / "dbt" / "models" / "m.sql").write_text("select 1")
    before = project_tree(project)

    result = await file_tool(project, "list_files", secret_values())

    assert result.structuredContent == {
        "files": ["adops.yaml", "dbt/models/m.sql", "pipelines/shop_sync.yaml"]
    }
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_list_files_in_one_directory(project, seams, locks):
    result = await file_tool(project, "list_files", secret_values(), directory="pipelines")

    assert result.structuredContent == {"files": ["pipelines/shop_sync.yaml"]}


async def test_list_files_refuses_a_directory_outside_the_project_files(project, seams, locks):
    result = await file_tool(project, "list_files", secret_values(), directory=".github")

    assert error_text(result) == f"Error executing tool list_files: {OUTSIDE_PROJECT_FILES}"


async def edit_tool(project: Path, path: str, content: str) -> CallToolResult:
    return await file_tool(project, "edit_file", secret_values(), path=path, content=content)


SECOND_PIPELINE = """
name: second
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: orders
        mode: full_refresh
        primary_key: id
"""


async def test_edit_file_takes_a_path_and_the_whole_content(tmp_path):
    schema = (await list_tools(tmp_path))["edit_file"]

    assert set(schema["properties"]) == {"path", "content"}
    assert sorted(schema["required"]) == ["content", "path"]


async def test_edit_file_creates_a_pipeline_that_validate_then_accepts(project, seams, locks):
    result = await edit_tool(project, "pipelines/second.yaml", SECOND_PIPELINE)

    assert not result.isError
    assert result.structuredContent == {
        "path": "pipelines/second.yaml",
        "bytes_written": len(SECOND_PIPELINE.encode()),
        "created": True,
    }
    assert (project / "pipelines" / "second.yaml").read_text() == SECOND_PIPELINE
    validated = await file_tool(project, "validate", secret_values(), pipeline="second")
    assert validated.structuredContent["valid"] is True
    assert_nothing_touched(seams, locks)


async def test_edit_file_overwrites_and_validate_reports_the_new_problem(project, seams, locks):
    result = await edit_tool(project, "pipelines/shop_sync.yaml", "name: shop_sync\nsteps: []\n")

    assert result.structuredContent["created"] is False
    validated = await file_tool(project, "validate", secret_values(), pipeline="shop_sync")
    assert validated.structuredContent["valid"] is False
    assert validated.structuredContent["problems"] != []
    assert_nothing_touched(seams, locks)


async def test_edit_file_never_echoes_the_content(project):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]
    content = f"# token: {secret}\n{SECOND_PIPELINE}"

    result = await edit_tool(project, "pipelines/second.yaml", content)

    assert not result.isError
    assert set(result.structuredContent) == {"path", "bytes_written", "created"}
    assert "token" not in result.model_dump_json()
    assert (project / "pipelines" / "second.yaml").read_text() == content


@pytest.mark.parametrize(
    ("relative", "message"),
    [
        ("adops.yaml", WRITES_ONLY_UNDER_ROOTS),
        (".env", OUTSIDE_PROJECT_FILES),
        (".ENV", OUTSIDE_PROJECT_FILES),
        ("pipelines/.env", OUTSIDE_PROJECT_FILES),
        ("../adops.yaml", OUTSIDE_PROJECT_FILES),
        (".github/workflows/ci.yml", OUTSIDE_PROJECT_FILES),
    ],
)
async def test_edit_file_refuses_settings_and_dotenv_leaving_every_file_unchanged(
    project, seams, locks, relative, message
):
    before = project_tree(project)

    result = await edit_tool(project, relative, "environments: {}\n")

    assert error_text(result) == f"Error executing tool edit_file: {message}"
    assert project_tree(project) == before
    assert_nothing_touched(seams, locks)


async def test_edit_file_refuses_a_symlink_to_adops_yaml(project):
    (project / "pipelines" / "settings.yaml").symlink_to(project / "adops.yaml")
    before = (project / "adops.yaml").read_bytes()

    result = await edit_tool(project, "pipelines/settings.yaml", "environments: {}\n")

    assert error_text(result) == f"Error executing tool edit_file: {WRITES_ONLY_UNDER_ROOTS}"
    assert (project / "adops.yaml").read_bytes() == before


async def test_edit_file_refuses_oversized_content_before_writing(project):
    before = project_tree(project)

    result = await edit_tool(project, "pipelines/big.yaml", "x" * (MAX_WRITE_BYTES + 1))

    assert error_text(result) == f"Error executing tool edit_file: {NOT_WRITABLE_TEXT}"
    assert project_tree(project) == before


async def test_edit_file_refuses_a_directory(project):
    before = project_tree(project)

    result = await edit_tool(project, "pipelines", "x")

    assert error_text(result) == f"Error executing tool edit_file: {WRITES_ONLY_UNDER_ROOTS}"
    (project / "pipelines" / "folder").mkdir()
    result = await edit_tool(project, "pipelines/folder", "x")
    assert error_text(result) == f"Error executing tool edit_file: {NOT_A_FILE_TARGET}"
    assert project_tree(project) == before


async def test_edit_file_names_only_the_path_when_the_write_fails(project, monkeypatch):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]

    def deny(*_):
        raise PermissionError(f"denied {secret}")

    monkeypatch.setattr(Path, "write_bytes", deny)

    result = await edit_tool(project, "pipelines/shop_sync.yaml", "x")

    assert error_text(result) == (
        "Error executing tool edit_file: pipelines/shop_sync.yaml: could not be written"
    )


async def test_edit_file_reduces_an_unexpected_error_to_its_type(project, monkeypatch):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]

    def explode(*_):
        raise RuntimeError(f"password {secret}")

    monkeypatch.setattr(mcp_server, "write_project_file", explode)

    result = await edit_tool(project, "pipelines/a.yaml", "x")

    assert error_text(result) == "Error executing tool edit_file: unexpected RuntimeError"


def test_project_secret_values_are_the_set_secret_references(view_project, monkeypatch):
    secrets = secret_values(VIEW_SETTINGS)
    monkeypatch.setenv("ADOPS_MCP_LOCAL_SHOP_PASSWORD", "")
    (view_project / ".env").write_text(
        "".join(
            f"{name}={value}\n"
            for name, value in secrets.items()
            if name != "ADOPS_MCP_LOCAL_SHOP_PASSWORD"
        )
    )
    expected = {v for n, v in secrets.items() if n != "ADOPS_MCP_LOCAL_SHOP_PASSWORD"}

    assert set(mcp_server.project_secret_values(view_project)) == expected


def test_project_secret_values_without_an_alert_webhook(tmp_path, monkeypatch):
    settings = SETTINGS.replace("alert_webhook_env: ADOPS_MCP_ALERT_WEBHOOK\n", "")
    secrets = write_project(tmp_path, settings)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)

    assert set(mcp_server.project_secret_values(tmp_path)) == set(secrets.values())
    assert "ADOPS_MCP_ALERT_WEBHOOK" not in secrets


OLDEST_LOG = "20261120T080000Z-shop_sync.log"
MIDDLE_LOG = "20261121T080000Z-a_sync.log"
NEWEST_LOG = "20261122T080000Z-shop_sync.log"


async def logs_tool(project: Path, secrets: dict[str, str] | None = None, **arguments: object):
    result = await call_tool(project, "read_logs", arguments)
    assert_no_secret(result, secrets)
    return result


def plant_run_log(project: Path, environment: str, name: str, text: str = "ok\n") -> Path:
    path = project / ".adops" / "logs" / environment / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def read_logs_refusal(result: CallToolResult, message: str = ONLY_RUN_LOGS) -> None:
    assert error_text(result) == f"Error executing tool read_logs: {message}"


async def test_read_logs_takes_an_environment_or_a_branch_and_an_optional_name(tmp_path):
    tools = await list_tools(tmp_path)

    assert set(tools["read_logs"]["properties"]) == {"environment", "branch", "name"}
    assert tools["read_logs"].get("required", []) == []


async def test_read_logs_lists_only_dev_run_logs_newest_first(project, seams, locks):
    for name in (MIDDLE_LOG, OLDEST_LOG, NEWEST_LOG, "notes.txt", "20261122T080000Z-x.txt"):
        plant_run_log(project, "dev", name)
    (project / ".adops" / "logs" / "dev" / "20261123T080000Z-folder.log").mkdir()
    plant_run_log(project, "stage", "20261124T080000Z-shop_sync.log")
    before = project_tree(project)

    result = await logs_tool(project, environment="dev")

    assert result.structuredContent == {
        "environment": "dev",
        "logs": [NEWEST_LOG, MIDDLE_LOG, OLDEST_LOG],
        "name": None,
        "text": None,
    }
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_read_logs_without_a_log_folder_lists_nothing_and_creates_nothing(
    project, seams, locks
):
    before = project_tree(project)

    result = await logs_tool(project, environment="dev")

    assert not result.isError
    assert result.structuredContent["logs"] == []
    assert not (project / ".adops").exists()
    assert project_tree(project) == before


async def test_read_logs_reads_one_run_log_with_secret_values_masked(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]
    plant_run_log(project, "dev", NEWEST_LOG, f"Pipeline shop_sync failed: {secret}\n")
    before = project_tree(project)

    result = await logs_tool(project, environment="dev", name=NEWEST_LOG)

    assert result.structuredContent == {
        "environment": "dev",
        "logs": [],
        "name": NEWEST_LOG,
        "text": "Pipeline shop_sync failed: ***\n",
    }
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_read_logs_works_when_dev_shares_stages_database(tmp_path, seams, locks, monkeypatch):
    secrets = write_project(tmp_path, DEV_SHARES_STAGE_WAREHOUSE)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)
    plant_run_log(tmp_path, "dev", NEWEST_LOG, "dev log\n")

    listed = await logs_tool(tmp_path, secrets, environment="dev")
    read = await logs_tool(tmp_path, secrets, environment="dev", name=NEWEST_LOG)

    assert listed.structuredContent["logs"] == [NEWEST_LOG]
    assert read.structuredContent["text"] == "dev log\n"
    assert_nothing_touched(seams, locks)


@pytest.mark.parametrize("environment", ["stage", "prod", "qa"])
async def test_read_logs_refuses_every_environment_but_dev(project, seams, locks, environment):
    plant_run_log(project, environment, NEWEST_LOG, "not for the Assistant\n")
    before = project_tree(project)

    listed = await logs_tool(project, environment=environment)
    read = await logs_tool(project, environment=environment, name=NEWEST_LOG)

    for result in (listed, read):
        read_logs_refusal(result, f"{ONLY_DEV_OR_BRANCH}; {environment!r} is refused")
    assert project_tree(project) == before


@pytest.mark.parametrize(
    "name",
    [
        "../x.log",
        f"../stage/{NEWEST_LOG}",
        "/etc/passwd",
        "notes.txt",
        f"{NEWEST_LOG}/../x.log",
        "20261122T080000Z-Shop.log",
    ],
)
async def test_read_logs_refuses_any_name_that_is_not_a_run_log(project, seams, locks, name):
    plant_run_log(project, "dev", "notes.txt", "notes\n")
    plant_run_log(project, "stage", NEWEST_LOG, "stage log\n")
    before = project_tree(project)

    result = await logs_tool(project, environment="dev", name=name)

    read_logs_refusal(result)
    assert project_tree(project) == before


async def test_read_logs_refuses_an_absolute_path_to_a_run_log(project, seams, locks):
    path = plant_run_log(project, "dev", NEWEST_LOG, "dev log\n")

    result = await logs_tool(project, environment="dev", name=str(path))

    read_logs_refusal(result)


async def test_read_logs_refuses_a_symlinked_run_log_and_does_not_list_it(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD"]
    stage_log = plant_run_log(project, "stage", OLDEST_LOG, f"stage {secret}\n")
    plant_run_log(project, "dev", OLDEST_LOG).unlink()
    (project / ".adops" / "logs" / "dev" / OLDEST_LOG).symlink_to(stage_log)
    plant_run_log(project, "dev", NEWEST_LOG)

    read = await logs_tool(project, environment="dev", name=OLDEST_LOG)
    listed = await logs_tool(project, environment="dev")

    read_logs_refusal(read)
    assert listed.structuredContent["logs"] == [NEWEST_LOG]


def link_dev_logs_to_stage(project: Path) -> None:
    plant_run_log(project, "stage", NEWEST_LOG, "stage log\n")
    (project / ".adops" / "logs" / "dev").symlink_to(project / ".adops" / "logs" / "stage")


def link_logs_folder_to_stage(project: Path) -> None:
    plant_run_log(project, "stage", NEWEST_LOG, "stage log\n")
    logs = project / ".adops" / "logs"
    logs.rename(project / "real_logs")
    elsewhere = project / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "dev").symlink_to(project / "real_logs" / "stage")
    logs.symlink_to(elsewhere)
    assert (logs / "dev" / NEWEST_LOG).read_text() == "stage log\n"


def link_adops_folder_elsewhere(project: Path) -> None:
    other = project / "other_project"
    plant_run_log(other, "dev", NEWEST_LOG, "another Project's log\n")
    (project / ".adops").symlink_to(other / ".adops")


@pytest.mark.parametrize(
    "link", [link_dev_logs_to_stage, link_logs_folder_to_stage, link_adops_folder_elsewhere]
)
async def test_read_logs_refuses_a_symlinked_folder_on_the_way(project, seams, locks, link):
    link(project)
    before = project_tree(project)

    listed = await logs_tool(project, environment="dev")
    read = await logs_tool(project, environment="dev", name=NEWEST_LOG)

    read_logs_refusal(listed)
    read_logs_refusal(read)
    assert project_tree(project) == before


async def test_read_logs_names_a_run_log_that_does_not_exist(project, seams, locks):
    result = await logs_tool(project, environment="dev", name=NEWEST_LOG)

    assert error_text(result) == (
        f"Error executing tool read_logs: {NEWEST_LOG}: no such Run Log in dev"
    )
    assert not (project / ".adops").exists()


@pytest.mark.parametrize(
    "content", [b"x" * (MAX_READ_BYTES + 1), b"\xff\xfe not utf-8\n"], ids=["large", "binary"]
)
async def test_read_logs_refuses_a_run_log_it_cannot_read_as_text(project, seams, locks, content):
    plant_run_log(project, "dev", NEWEST_LOG).write_bytes(content)

    result = await logs_tool(project, environment="dev", name=NEWEST_LOG)

    read_logs_refusal(result, NOT_READABLE_TEXT)


async def test_read_logs_reads_back_the_run_log_a_branch_run_wrote(project, seams, locks):
    branch = "feature/" + "very-long-branch-name-" * 3
    environments = load_project_settings(SETTINGS).environments
    expected = stage_branch_name(stage_environment(environments), branch)
    assert expected != "br_" + branch.replace("/", "_").replace("-", "_")
    seams.write_run_log = write_run_log
    await run_tool(project, pipeline="shop_sync", branch=branch)
    seams.calls.clear()
    locks.events.clear()
    locks.timeouts.clear()
    before = project_tree(project)

    listed = await logs_tool(project, branch=branch)
    [name] = listed.structuredContent["logs"]
    read = await logs_tool(project, branch=branch, name=name)

    assert listed.structuredContent["environment"] == expected
    assert (project / ".adops" / "logs" / expected / name).is_file()
    assert name == f"{NOW.strftime('%Y%m%dT%H%M%SZ')}-shop_sync.log"
    summary = f"Pipeline shop_sync passed in Environment {expected}.\n"
    assert summary in read.structuredContent["text"]
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_read_logs_for_a_branch_lists_its_branch_environment_folder(project, seams, locks):
    plant_run_log(project, "br_feature_x", NEWEST_LOG)
    plant_run_log(project, "dev", OLDEST_LOG)

    result = await logs_tool(project, branch="feature/x")

    assert result.structuredContent["environment"] == "br_feature_x"
    assert result.structuredContent["logs"] == [NEWEST_LOG]


async def test_read_logs_for_a_branch_needs_a_stage_environment(tmp_path, seams, locks):
    settings = SETTINGS.split("  stage:")[0]
    secrets = write_project(tmp_path, settings)

    result = await logs_tool(tmp_path, secrets, branch="feat")

    assert "a Branch run needs a stage Environment" in error_text(result)


async def test_read_logs_reduces_an_unexpected_error_to_its_type(project, monkeypatch):
    secret = secret_values()["ADOPS_MCP_PROD_WAREHOUSE_PASSWORD"]
    plant_run_log(project, "dev", NEWEST_LOG)

    def explode(*_):
        raise RuntimeError(f"password {secret}")

    monkeypatch.setattr(mcp_server, "read_capped_text", explode)

    result = await logs_tool(project, environment="dev", name=NEWEST_LOG)

    assert error_text(result) == "Error executing tool read_logs: unexpected RuntimeError"


async def connection_tool(
    project: Path, secrets: dict[str, str] | None = None, **arguments: object
) -> CallToolResult:
    result = await call_tool(project, "test_connection", arguments)
    assert_no_secret(result, secrets)
    return result


def reachable(kind: str, name: str) -> dict[str, object]:
    return {"kind": kind, "name": name, "ok": True, "message": None}


def unreachable(kind: str, name: str, message: str) -> dict[str, object]:
    return {"kind": kind, "name": name, "ok": False, "message": message}


def add_pipeline(project: Path, file: str, source: str) -> None:
    text = PIPELINE.replace("shop_sync", file).replace("source: shop", f"source: {source}")
    (project / "pipelines" / f"{file}.yaml").write_text(text)


async def test_test_connection_takes_an_environment_or_a_branch(tmp_path):
    tools = await list_tools(tmp_path)

    assert set(tools["test_connection"]["properties"]) == {"environment", "branch"}
    assert tools["test_connection"].get("required", []) == []


async def test_test_connection_reports_every_dev_connection_reachable(project, seams, locks):
    before = project_tree(project)

    result = await connection_tool(project, environment="dev")

    assert result.structuredContent == {
        "environment": "dev",
        "results": [
            reachable("warehouse", "warehouse"),
            reachable("source", "shop"),
            reachable("target", "crm"),
        ],
        "problems": [],
    }
    assert seams.probe.databases == ["w_dev", "shop", "crm_dev"]
    passwords = [connection.password for connection in seams.probe.postgres_calls]
    expected = ["DEV_WAREHOUSE", "DEV_SHOP", "DEV_CRM"]
    assert passwords == [secret_values()[f"ADOPS_MCP_{name}_PASSWORD"] for name in expected]
    assert seams.calls == ["build_connection_probe"]
    assert locks.events == [] and locks.timeouts == []
    assert project_tree(project) == before


async def test_test_connection_for_a_branch_checks_stages_warehouse_and_targets(
    project, seams, locks
):
    result = await connection_tool(project, branch="feature/x")

    assert result.structuredContent["environment"] == "br_feature_x"
    assert [r["ok"] for r in result.structuredContent["results"]] == [True, True, True]
    assert seams.probe.databases == ["w_stage", "shop", "crm"]
    assert (
        seams.probe.postgres_calls[0].password
        == secret_values()["ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD"]
    )
    assert locks.events == []


@pytest.mark.parametrize(
    ("database", "expected"),
    [
        ("w_dev", unreachable("warehouse", "warehouse", "Warehouse is unreachable")),
        ("shop", unreachable("source", "shop", "Source shop is unreachable")),
        ("crm_dev", unreachable("target", "crm", "Target crm is unreachable")),
    ],
)
async def test_test_connection_reports_an_unreachable_connection_and_checks_the_rest(
    project, seams, locks, database, expected
):
    seams.probe.unreachable = {database}
    before = project_tree(project)

    result = await connection_tool(project, environment="dev")

    assert not result.isError
    results = result.structuredContent["results"]
    assert expected in results
    assert [r["ok"] for r in results].count(False) == 1
    assert seams.probe.databases == ["w_dev", "shop", "crm_dev"]
    assert locks.events == []
    assert project_tree(project) == before


async def test_test_connection_checks_a_duckdb_warehouse_by_its_path(
    tmp_path, seams, locks, monkeypatch
):
    settings = SETTINGS.replace(
        """warehouse: {type: postgres, host: localhost, port: 5433, database: w_dev, user: w,
                password_env: ADOPS_MCP_DEV_WAREHOUSE_PASSWORD}""",
        "warehouse: {type: duckdb, path: dev.duckdb}",
    )
    secrets = write_project(tmp_path, settings)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)
    seams.probe.unreachable = {"dev.duckdb"}

    result = await connection_tool(tmp_path, secrets, environment="dev")

    results = result.structuredContent["results"]
    assert results[0] == unreachable("warehouse", "warehouse", "Warehouse is unreachable")
    assert seams.probe.duckdb_calls == ["dev.duckdb"]
    assert seams.probe.databases == ["shop", "crm_dev"]


async def test_test_connection_names_a_missing_secret_reference_for_that_source_only(
    project, seams, locks
):
    env_file = project / ".env"
    lines = env_file.read_text().splitlines(keepends=True)
    env_file.write_text("".join(line for line in lines if "DEV_SHOP" not in line))

    result = await connection_tool(project, environment="dev")

    results = result.structuredContent["results"]
    assert results[1] == unreachable(
        "source",
        "shop",
        "these environment variables are not set or are empty: ADOPS_MCP_DEV_SHOP_PASSWORD",
    )
    assert results[0]["ok"] and results[2]["ok"]
    assert seams.probe.databases == ["w_dev", "crm_dev"]


async def test_test_connection_reports_a_source_the_environment_does_not_define(
    project, seams, locks
):
    add_pipeline(project, "billing_sync", "billing")

    result = await connection_tool(project, environment="dev")

    assert result.structuredContent["results"] == [
        reachable("warehouse", "warehouse"),
        unreachable("source", "billing", "Source billing is unreachable"),
        reachable("source", "shop"),
        reachable("target", "crm"),
    ]
    assert seams.probe.databases == ["w_dev", "shop", "crm_dev"]


async def test_test_connection_checks_a_source_shared_by_pipelines_once(project, seams, locks):
    add_pipeline(project, "orders_sync", "shop")

    result = await connection_tool(project, environment="dev")

    names = [r["name"] for r in result.structuredContent["results"]]
    assert names == ["warehouse", "shop", "crm"]
    assert seams.probe.databases.count("shop") == 1


async def test_test_connection_lists_a_broken_pipeline_and_still_checks(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_DEV_WAREHOUSE_PASSWORD"]
    (project / "pipelines" / "broken.yaml").write_text(f"name: broken\n{secret}: [\n")
    before = project_tree(project)

    result = await connection_tool(project, environment="dev")

    reply = result.structuredContent
    assert reply["problems"] and all(
        p.startswith("pipelines/broken.yaml") for p in reply["problems"]
    )
    assert not any(secret in problem for problem in reply["problems"])
    assert reply["results"][0] == reachable("warehouse", "warehouse")
    assert project_tree(project) == before


@pytest.mark.parametrize(
    "arguments",
    [
        {"environment": "stage"},
        {"environment": "prod"},
        {"environment": "qa"},
        {"environment": "dev", "branch": "feat"},
        {},
    ],
)
async def test_test_connection_refuses_every_other_environment_before_probing(
    project, seams, locks, arguments
):
    before = project_tree(project)

    result = await connection_tool(project, **arguments)

    assert "exactly `dev`" in error_text(result)
    assert_nothing_touched(seams, locks)
    assert project_tree(project) == before


async def test_test_connection_refuses_a_dev_that_shares_stages_database(
    tmp_path, seams, locks, monkeypatch
):
    secrets = write_project(tmp_path, DEV_SHARES_STAGE_WAREHOUSE)
    for name in secrets:
        monkeypatch.delenv(name, raising=False)
    before = project_tree(tmp_path)

    result = await connection_tool(tmp_path, secrets, environment="dev")

    assert result.isError
    assert "stage" in error_text(result)
    assert_nothing_touched(seams, locks)
    assert project_tree(tmp_path) == before


async def test_test_connection_reduces_an_unexpected_error_to_its_type(project, seams, locks):
    secret = secret_values()["ADOPS_MCP_DEV_WAREHOUSE_PASSWORD"]
    seams.probe.error = RuntimeError(f"password {secret} rejected")

    result = await connection_tool(project, environment="dev")

    assert error_text(result) == "Error executing tool test_connection: unexpected RuntimeError"


def test_the_connection_probe_seam_builds_the_driver_probe(tmp_path):
    assert isinstance(mcp_server.build_connection_probe(tmp_path), DriverConnectionProbe)
