import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing import Annotated, TypedDict

import anyio
import typer
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.stdio import stdio_server

from adops.application.ports import (
    CheckResult,
    ReverseSyncPreview,
    RunResult,
    StepReport,
    WarehouseLocked,
)
from adops.application.run_pipeline import pick_environment
from adops.domain.assistant_access import (
    DEV,
    AssistantRefusal,
    assistant_target,
    refuse_shared_dev_connection,
)
from adops.domain.loader import load_project_settings
from adops.domain.reverse_sync_diff import PREVIEW_SAMPLE_SIZE
from adops.infrastructure.dlt_sync import duckdb_file
from adops.interface import run_command
from adops.interface.run_command import RUN_ERRORS, error_lines
from adops.interface.validate_command import validate_project

SERVER_NAME = "adops"
# The Assistant gets an immediate "busy" reply instead of waiting on another run's lock.
ASSISTANT_LOCK_TIMEOUT = 0


class CheckReply(TypedDict):
    table: str
    check: str
    status: str
    message: str


class UpdateKeyReply(TypedDict):
    key: str
    columns: list[str]


class PreviewReply(TypedDict):
    would_insert: int
    would_update: int
    would_delete: int
    kept: int
    sample_insert_keys: list[str]
    sample_update_keys: list[UpdateKeyReply]
    sample_delete_keys: list[str]
    sample_kept_keys: list[str]


class StepReply(TypedDict):
    name: str
    status: str
    checks: list[CheckReply]
    rows_loaded: dict[str, int]
    error: str | None
    preview: PreviewReply | None


class RunReply(TypedDict):
    pipeline: str
    environment: str
    succeeded: bool
    steps: list[StepReply]


class StepPreviewReply(TypedDict):
    step: str
    preview: PreviewReply


class StepErrorReply(TypedDict):
    step: str
    error: str


class PreviewRunReply(TypedDict):
    pipeline: str
    environment: str
    succeeded: bool
    previews: list[StepPreviewReply]
    errors: list[StepErrorReply]


class ValidateReply(TypedDict):
    valid: bool
    problems: list[str]
    skipped: list[str]
    schedules: list[str]


@dataclass(frozen=True)
class RunArguments:
    """What `run_project` receives once the Assistant's request has passed every refusal."""

    environment: str
    branch: str | None


def guarded_run_arguments(
    project: Path, environment: str | None, branch: str | None
) -> RunArguments:
    """Refuse anything but dev or a Branch Environment, reading adops.yaml as it is now."""
    target = assistant_target(environment, branch)
    if target.branch is not None:
        return RunArguments(DEV, target.branch)
    environments = load_project_settings(run_command.read_settings(project)).environments
    pick_environment(environments, DEV)
    refuse_shared_dev_connection(
        environments, lambda path: str(duckdb_file(project, path).resolve())
    )
    return RunArguments(DEV, None)


def build_server(project: Path) -> FastMCP:
    server = FastMCP(SERVER_NAME)

    @server.tool()
    def run(pipeline: str, environment: str | None = None, branch: str | None = None) -> RunReply:
        """Run a Pipeline in the dev Environment or in a Git branch's Branch Environment.

        Give `environment: "dev"` or a `branch` name, not both. Every other Environment is
        refused. Reverse Sync Steps only Preview what they would change.
        """
        with _sanitized_errors():
            return run_reply(_guarded_run(project, pipeline, environment, branch))

    @server.tool()
    def preview(
        pipeline: str, environment: str | None = None, branch: str | None = None
    ) -> PreviewRunReply:
        """Show what a Pipeline's Reverse Sync Steps would change in dev or a Branch Environment.

        Runs the Pipeline like `run` and replies with only each Reverse Sync Step's counts, up to
        5 sample keys per kind and the changed column names, plus any failing Step's error.
        """
        with _sanitized_errors():
            return preview_reply(_guarded_run(project, pipeline, environment, branch))

    @server.tool()
    def validate(pipeline: str | None = None) -> ValidateReply:
        """Check adops.yaml and one Pipeline, or every Pipeline, without connecting to anything."""
        with _sanitized_errors():
            report = validate_project(project, pipeline)
            return {
                "valid": not report.problems,
                "problems": report.problems,
                "skipped": report.skipped,
                "schedules": report.schedules,
            }

    return server


def _guarded_run(
    project: Path, pipeline: str, environment: str | None, branch: str | None
) -> RunResult:
    arguments = guarded_run_arguments(project, environment, branch)
    result = run_command.run_project(
        project,
        pipeline,
        arguments.environment,
        arguments.branch,
        lock_timeout=ASSISTANT_LOCK_TIMEOUT,
    )
    _refuse_written(result)
    return result


@contextmanager
def _sanitized_errors() -> Iterator[None]:
    """Turn every failure into a ToolError whose text is already redacted."""
    try:
        yield
    except WarehouseLocked as error:
        raise ToolError(
            f"The {error.environment} Warehouse is busy with another run; try again later."
        ) from None
    except (AssistantRefusal, *RUN_ERRORS) as error:
        raise ToolError("\n".join(error_lines(error))) from None
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        raise ToolError(f"unexpected {type(error).__name__}") from None


def _refuse_written(result: RunResult) -> None:
    if any(step.preview is not None and step.preview.written for step in result.steps):
        raise AssistantRefusal(
            f"a Reverse Sync Step reported a write in {result.environment}; "
            "the Assistant only Previews, so this run is reported as refused"
        )


def run_reply(result: RunResult) -> RunReply:
    return {
        "pipeline": result.pipeline,
        "environment": result.environment,
        "succeeded": result.succeeded,
        "steps": [_step_reply(step) for step in result.steps],
    }


def _step_reply(step: StepReport) -> StepReply:
    return {
        "name": step.name,
        "status": str(step.status),
        "checks": [_check_reply(check) for check in step.checks],
        "rows_loaded": dict(step.rows_loaded),
        "error": step.error,
        "preview": None if step.preview is None else _preview_reply(step.preview),
    }


def preview_reply(result: RunResult) -> PreviewRunReply:
    return {
        "pipeline": result.pipeline,
        "environment": result.environment,
        "succeeded": result.succeeded,
        "previews": [
            {"step": step.name, "preview": _preview_reply(step.preview)}
            for step in result.steps
            if step.preview is not None
        ],
        "errors": [
            {"step": step.name, "error": step.error}
            for step in result.steps
            if step.error is not None
        ],
    }


def _check_reply(check: CheckResult) -> CheckReply:
    return {
        "table": check.table,
        "check": check.check,
        "status": str(check.status),
        "message": check.message,
    }


def _preview_reply(preview: ReverseSyncPreview) -> PreviewReply:
    # Capped here as well as in the domain, so a wider sample from a port never reaches the reply.
    sample = preview.sample
    cap = PREVIEW_SAMPLE_SIZE
    return {
        "would_insert": preview.inserts,
        "would_update": preview.updates,
        "would_delete": preview.deletes,
        "kept": preview.kept,
        "sample_insert_keys": [str(key) for key in sample.inserts[:cap]],
        "sample_update_keys": [
            {"key": str(key), "columns": list(columns)} for key, columns in sample.updates[:cap]
        ],
        "sample_delete_keys": [str(key) for key in sample.deletes[:cap]],
        "sample_kept_keys": [str(key) for key in sample.kept[:cap]],
    }


@contextmanager
def stdout_to_stderr() -> Iterator[int]:
    """Send everything written to stdout to stderr, yielding a descriptor of the real stdout.

    Dagster, dbt and dlt print to stdout, from Python and from subprocesses; on the stdio
    transport stdout carries the MCP protocol, so only the yielded descriptor may write there.
    """
    sys.stdout.flush()
    protocol = os.dup(1)
    saved_stdout = sys.stdout
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    try:
        yield protocol
    finally:
        sys.stdout = saved_stdout
        os.dup2(protocol, 1)
        os.close(protocol)


def serve(server: FastMCP) -> None:
    with stdout_to_stderr() as protocol:
        stdin = TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
        stdout = TextIOWrapper(os.fdopen(protocol, "wb", closefd=False), encoding="utf-8")
        anyio.run(serve_streams, server, anyio.wrap_file(stdin), anyio.wrap_file(stdout))


async def serve_streams(
    server: FastMCP, stdin: anyio.AsyncFile[str], stdout: anyio.AsyncFile[str]
) -> None:
    lowlevel = server._mcp_server
    async with stdio_server(stdin, stdout) as (read_stream, write_stream):
        await lowlevel.run(read_stream, write_stream, lowlevel.create_initialization_options())


def mcp(
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Serve the Assistant's MCP tools over stdio for a Project.

    The tools work only in the dev Environment and in Branch Environments.
    """
    serve(build_server(project.resolve()))
