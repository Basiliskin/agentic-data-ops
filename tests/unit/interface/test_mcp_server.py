import io
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import anyio
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CallToolResult, TextContent
from mcp_harness import (
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
    ReverseSyncPreview,
    RunPlan,
    RunResult,
    StepReport,
    StepStatus,
    Sync,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import WarehouseUnreachableError
from adops.domain.checks import CheckResult, CheckStatus
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_project_settings
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.interface import cli, mcp_server, run_command
from adops.interface.run_command import error_lines

pytestmark = pytest.mark.anyio

runner = CliRunner()

SEAMS = (
    "build_sync",
    "build_snapshot_store",
    "build_clock",
    "build_orchestrator",
    "build_transform",
    "build_reverse_sync",
    "build_fingerprint_store",
    "build_project_files",
)

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


@dataclass
class Seams:
    orchestrator: FakeOrchestrator = field(default_factory=FakeOrchestrator)
    fingerprints: StageFingerprints = field(default_factory=StageFingerprints)
    calls: list[str] = field(default_factory=list)
    errors: dict[str, Exception] = field(default_factory=dict)


@pytest.fixture
def seams(monkeypatch):
    fake = Seams()
    real_project_files = run_command.build_project_files
    results = {
        "build_orchestrator": lambda *_: fake.orchestrator,
        "build_fingerprint_store": lambda *_: fake.fingerprints,
        "build_project_files": real_project_files,
    }

    def spy(name):
        def build(*args, **kwargs):
            fake.calls.append(name)
            if name in fake.errors:
                raise fake.errors[name]
            return results.get(name, lambda *_: object())(*args)

        return build

    for name in SEAMS:
        monkeypatch.setattr(run_command, name, spy(name))
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


async def test_dev_sharing_stages_warehouse_is_refused_before_anything_runs(
    tmp_path, seams, locks
):
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
    assert seams.calls == []


def test_run_project_keeps_the_cli_wait_by_default(project, seams, locks):
    run_command.run_project(project, "shop_sync", "dev")

    assert locks.timeouts == [DEFAULT_LOCK_TIMEOUT]


def test_adops_run_keeps_the_cli_wait(project, seams, locks):
    result = runner.invoke(cli.app, ["run", "shop_sync", "--project", str(project)])

    assert result.exit_code == 0, result.output
    assert locks.timeouts == [DEFAULT_LOCK_TIMEOUT]


async def test_a_branch_run_without_stages_secret_names_the_secret_reference(
    project, seams, locks
):
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

    assert error_text(result) == "Error executing tool run: " + "\n".join(
        error_lines(caught.value)
    )
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
    stdin = io.StringIO("".join(json.dumps(r) + "\n" for r in requests))
    stdout = io.StringIO()

    with anyio.fail_after(10):
        await mcp_server.serve_streams(
            mcp_server.build_server(tmp_path), anyio.wrap_file(stdin), anyio.wrap_file(stdout)
        )

    replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
    tools = next(r for r in replies if r.get("id") == 2)["result"]["tools"]
    assert {tool["name"] for tool in tools} == {"run", "preview", "validate"}


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
    assert set(await list_tools(tmp_path)) == {"run", "preview", "validate"}


async def validate_tool(project: Path, **arguments: object) -> CallToolResult:
    result = await call_tool(project, "validate", arguments)
    assert_no_secret(result)
    return result


async def test_validate_takes_only_an_optional_pipeline(tmp_path):
    schema = (await list_tools(tmp_path))["validate"]

    assert set(schema["properties"]) == {"pipeline"}
    assert schema.get("required", []) == []


@pytest.mark.parametrize("arguments", [{}, {"pipeline": "shop_sync"}])
async def test_validate_passes_a_valid_project_without_connecting(
    project, seams, locks, arguments
):
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
