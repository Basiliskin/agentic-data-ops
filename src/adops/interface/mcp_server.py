import os
import sys
from collections.abc import Iterator, Mapping
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

from adops.application.accept_schema import AcceptedSchema
from adops.application.drop_branch import DroppedBranch
from adops.application.ports import (
    CheckResult,
    ReverseSyncPreview,
    RunResult,
    SourceUnreachable,
    StepReport,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import StepError, pick_environment
from adops.application.secrets import SET, secret_reference_status, secret_references
from adops.domain.assistant_access import (
    DEV,
    AssistantRefusal,
    assistant_target,
    plain_pipeline_name,
    refuse_branch_schema_acceptance,
    refuse_shared_dev_connection,
)
from adops.domain.environment import Environment, ProjectSettings
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline
from adops.domain.reverse_sync_diff import PREVIEW_SAMPLE_SIZE
from adops.infrastructure.dlt_sync import duckdb_file
from adops.interface import run_command
from adops.interface.accept_schema_command import accept_project_schema
from adops.interface.assistant_files import (
    list_project_files,
    read_project_file,
    write_project_file,
)
from adops.interface.drop_branch_command import drop_project_branch
from adops.interface.run_command import RUN_ERRORS, error_lines
from adops.interface.validate_command import schedule_text, validate_project

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


class DroppedBranchReply(TypedDict):
    environment: str
    dropped_schemas: list[str]


class AcceptedSchemaReply(TypedDict):
    pipeline: str
    table: str
    step: str
    environment: str
    column_count: int
    nullable_columns: list[str]


class ValidateReply(TypedDict):
    valid: bool
    problems: list[str]
    skipped: list[str]
    schedules: list[str]


class StepViewReply(TypedDict):
    name: str
    kind: str


class PipelineViewReply(TypedDict):
    name: str
    schedule: str | None
    steps: list[StepViewReply]


class EnvironmentViewReply(TypedDict):
    name: str
    warehouse_type: str
    sources: list[str]
    targets: list[str]
    secret_references: dict[str, str]


class ProjectViewReply(TypedDict):
    pipelines: list[PipelineViewReply]
    environments: list[EnvironmentViewReply]
    alert_webhook: dict[str, str]
    problems: list[str]


class ListFilesReply(TypedDict):
    files: list[str]


class ReadFileReply(TypedDict):
    path: str
    text: str


class EditFileReply(TypedDict):
    path: str
    bytes_written: int
    created: bool


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
            name = plain_pipeline_name(pipeline)
            return run_reply(_guarded_run(project, name, environment, branch))

    @server.tool()
    def preview(
        pipeline: str, environment: str | None = None, branch: str | None = None
    ) -> PreviewRunReply:
        """Show what a Pipeline's Reverse Sync Steps would change in dev or a Branch Environment.

        Runs the Pipeline like `run` and replies with only each Reverse Sync Step's counts, up to
        5 sample keys per kind and the changed column names, plus any failing Step's error.
        """
        with _sanitized_errors():
            name = plain_pipeline_name(pipeline)
            return preview_reply(_guarded_run(project, name, environment, branch))

    @server.tool()
    def validate(pipeline: str | None = None) -> ValidateReply:
        """Check adops.yaml and one Pipeline, or every Pipeline, without connecting to anything."""
        with _sanitized_errors():
            name = None if pipeline is None else plain_pipeline_name(pipeline)
            report = validate_project(project, name)
            return {
                "valid": not report.problems,
                "problems": report.problems,
                "skipped": report.skipped,
                "schedules": report.schedules,
            }

    @server.tool()
    def drop_branch(branch: str) -> DroppedBranchReply:
        """Drop a Git branch's Branch Environment: its schemas in stage's Warehouse and its state.

        Only schemas named br_<branch>__… are dropped; stage's own schemas are never touched.
        Replies busy at once when another run holds stage's Warehouse lock.
        """
        with _sanitized_errors():
            assistant_target(None, branch)
            dropped = drop_project_branch(project, branch, lock_timeout=ASSISTANT_LOCK_TIMEOUT)
            return dropped_branch_reply(dropped)

    @server.tool()
    def accept_schema(
        pipeline: str, table: str, environment: str | None = None, branch: str | None = None
    ) -> AcceptedSchemaReply:
        """Accept an intended Breaking Change in dev: record the Source table's current shape.

        Give `environment: "dev"`; every other Environment and any `branch` is refused. The
        next dev run compares against this shape. No other Environment is changed.
        """
        with _sanitized_errors():
            name = plain_pipeline_name(pipeline)
            arguments = guarded_run_arguments(project, environment, branch)
            refuse_branch_schema_acceptance(arguments.branch)
            return accepted_schema_reply(accept_project_schema(project, name, table, DEV))

    @server.tool()
    def project_view() -> ProjectViewReply:
        """Show the Project: Pipelines with their Steps and Schedule, and every Environment.

        For each Secret Reference only whether it is set or missing is shown, never its value,
        and no connection detail is included. Pipeline files that fail to load are listed in
        `problems`; the other Pipelines are still shown.
        """
        with _sanitized_errors():
            settings = load_project_settings(run_command.read_settings(project))
            pipelines, problems = load_pipelines(project)
            return project_view_reply(
                settings, pipelines, problems, run_command.project_env(project)
            )

    @server.tool()
    def list_files(directory: str = "") -> ListFilesReply:
        """List adops.yaml and the files under pipelines/ and dbt/, or under one directory.

        Paths are relative to the Project. .env files and anything a symlink takes outside
        those places are never listed.
        """
        with _sanitized_errors():
            return {"files": list_project_files(project, directory)}

    @server.tool()
    def read_file(path: str) -> ReadFileReply:
        """Read adops.yaml or a file under pipelines/ or dbt/, by its Project-relative path.

        .env files are refused. Any Secret Reference value found in the text is shown as ***.
        """
        with _sanitized_errors():
            text = read_project_file(project, path, project_secret_values(project))
            return {"path": path, "text": text}

    @server.tool()
    def edit_file(path: str, content: str) -> EditFileReply:
        """Write a whole file under pipelines/ or dbt/, creating it and its folders if missing.

        `content` replaces the file. adops.yaml, .env files and every other path are refused.
        Nothing is validated here: call `validate` afterwards.
        """
        with _sanitized_errors():
            created = write_project_file(project, path, content)
            return edit_file_reply(path, content, created)

    return server


def edit_file_reply(path: str, content: str, created: bool) -> EditFileReply:
    # The content is never echoed: it may hold a value the Assistant should not see again.
    return {"path": path, "bytes_written": len(content.encode("utf-8")), "created": created}


def project_secret_values(project: Path) -> list[str]:
    """The values adops.yaml's set Secret References resolve to, for redaction only."""
    settings = load_project_settings(run_command.read_settings(project))
    references = [settings.alert_webhook_env] if settings.alert_webhook_env else []
    for environment in settings.environments.values():
        references += secret_references(
            environment.warehouse,
            list(environment.sources.values()),
            list(environment.targets.values()),
        )
    env = run_command.project_env(project)
    status = secret_reference_status(env, references)
    return [env[name] for name, state in status.items() if state == SET]


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
    except WarehouseUnreachable:
        raise ToolError("The Warehouse is unreachable or locked by another process.") from None
    except SourceUnreachable as error:
        raise ToolError(f"The Source of table {error.table} is unreachable.") from None
    except (AssistantRefusal, StepError, *RUN_ERRORS) as error:
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


def load_pipelines(project: Path) -> tuple[list[Pipeline], list[str]]:
    """The Pipelines in pipelines/ that load, by name, and the problem lines of the rest."""
    pipelines: list[Pipeline] = []
    problems: list[str] = []
    for path in run_command.pipeline_files(project):
        relative = path.relative_to(project).as_posix()
        try:
            pipelines.append(load_pipeline(path.read_text(encoding="utf-8")))
        except UnicodeDecodeError:
            problems.append(f"{relative}: not UTF-8 text")
        except PipelineValidationError as error:
            problems.extend(f"{relative}: {problem}" for problem in error.problems)
    return sorted(pipelines, key=lambda pipeline: pipeline.name), problems


def project_view_reply(
    settings: ProjectSettings,
    pipelines: list[Pipeline],
    problems: list[str],
    env: Mapping[str, str],
) -> ProjectViewReply:
    alert_webhook = [settings.alert_webhook_env] if settings.alert_webhook_env else []
    return {
        "pipelines": [_pipeline_view(pipeline, settings.timezone) for pipeline in pipelines],
        "environments": [
            _environment_view(name, environment, env)
            for name, environment in sorted(settings.environments.items())
        ],
        "alert_webhook": secret_reference_status(env, alert_webhook),
        "problems": list(problems),
    }


def _pipeline_view(pipeline: Pipeline, timezone: str) -> PipelineViewReply:
    return {
        "name": pipeline.name,
        "schedule": schedule_text(pipeline, timezone),
        "steps": [{"name": step.name, "kind": step.type} for step in pipeline.steps],
    }


def _environment_view(
    name: str, environment: Environment, env: Mapping[str, str]
) -> EnvironmentViewReply:
    # Built field by field: the connection models carry host, user and database.
    references = secret_references(
        environment.warehouse,
        list(environment.sources.values()),
        list(environment.targets.values()),
    )
    return {
        "name": name,
        "warehouse_type": environment.warehouse.type,
        "sources": sorted(environment.sources),
        "targets": sorted(environment.targets),
        "secret_references": secret_reference_status(env, references),
    }


def dropped_branch_reply(dropped: DroppedBranch) -> DroppedBranchReply:
    return {"environment": dropped.environment, "dropped_schemas": list(dropped.schemas)}


def accepted_schema_reply(accepted: AcceptedSchema) -> AcceptedSchemaReply:
    return {
        "pipeline": accepted.pipeline,
        "table": accepted.table,
        "step": accepted.step,
        "environment": accepted.environment,
        "column_count": len(accepted.shape.columns),
        "nullable_columns": list(accepted.nullable_columns),
    }


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
