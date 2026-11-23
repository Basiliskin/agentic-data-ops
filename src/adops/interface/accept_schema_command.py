from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from adops.application.accept_schema import (
    AcceptedSchema,
    AlreadyCovered,
    FoundSchemaChanges,
    NewRecord,
    apply_schema_acceptance,
    find_schema_changes,
    plan_accepted_record,
)
from adops.application.accept_schema import accept_schema as accept_table_schema
from adops.application.ports import WarehouseLocked, WarehouseUnreachable
from adops.application.run_pipeline import StepError, stage_branch_name, stage_environment
from adops.application.secrets import MissingSecretError
from adops.domain.accepted_change_record import AcceptedChangeRecord, dump_accepted_record
from adops.domain.assistant_access import DEV
from adops.domain.branch_environment import STAGE, SchemaLayout
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_project_settings
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.interface import run_command
from adops.interface.assistant_files import resolve_project_file
from adops.interface.run_command import (
    ACCEPTED_RECORDS_DIR,
    BRANCH_WITH_ENV,
    PIPELINES_DIR,
    ProjectFileError,
    fail,
)


class RecordExists(ProjectFileError):
    pass


@dataclass(frozen=True)
class SchemaAcceptance:
    """What accept-schema found, what it accepted (dev only) and which record covers it."""

    found: FoundSchemaChanges
    accepted: AcceptedSchema | None
    record_path: Path | None
    covering_record_ids: tuple[str, ...]


def record_file(record_id: str) -> Path:
    """The Project-relative path of an accepted-change record."""
    return PIPELINES_DIR / ACCEPTED_RECORDS_DIR / f"{record_id}.yaml"


def accept_schema(
    pipeline: Annotated[str, typer.Argument(help="Pipeline name (pipelines/<name>.yaml).")],
    table: Annotated[str, typer.Argument(help="Source table whose change you accept.")],
    env: Annotated[
        str | None, typer.Option("--env", help="Environment from adops.yaml [default: dev].")
    ] = None,
    branch: Annotated[
        str | None,
        typer.Option(
            "--branch",
            help="Accept in this Git branch's Branch Environment, built from stage's settings.",
        ),
    ] = None,
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Accept an intended Breaking Change and write its accepted-change record.

    In dev the table's current Source shape also becomes dev's baseline. Any other Environment
    is only read: commit the record so its next run accepts the change.

    With --branch, the Branch Environment's baseline is accepted directly, under stage's
    Warehouse lock, and no record is written.
    """
    if env is not None and branch is not None:
        raise typer.BadParameter(BRANCH_WITH_ENV, param_hint="'--branch'")
    try:
        if branch is None:
            acceptance = accept_project_schema(project.resolve(), pipeline, table, env or DEV)
        else:
            accepted = accept_branch_schema(project.resolve(), pipeline, table, branch)
    except PipelineValidationError as error:
        fail(*error.problems)
    except (ProjectFileError, MissingSecretError, StepError, WarehouseLocked) as error:
        fail(str(error))
    except WarehouseUnreachable:
        fail(f"Pipeline {pipeline}, table {table}: Warehouse is unreachable")
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while accepting {table}")
    if branch is not None:
        _echo_accepted(accepted)
        return
    if acceptance.accepted is not None:
        _echo_accepted(acceptance.accepted)
    _echo_record(acceptance)


def accept_project_schema(
    project: Path, pipeline: str, table: str, environment: str
) -> SchemaAcceptance:
    """Find the table's Breaking Changes, write a record for them, and in dev accept them.

    The record is created before dev's baseline changes, so a refused record leaves dev as it
    was. Every other Environment is opened read-only and only the record file is written.
    """
    settings_text, pipeline_text = run_command.read_project_files(project, pipeline)
    kind = run_command.warehouse_kind(settings_text, environment)
    sync = run_command.build_sync(project, pipeline, environment, kind)
    store = run_command.build_snapshot_store(
        project, environment, kind, read_only=environment != DEV
    )
    found = find_schema_changes(
        pipeline_text,
        settings_text,
        environment,
        table,
        run_command.project_env(project),
        sync,
        store,
    )
    plan = plan_accepted_record(
        found.pipeline,
        found.table,
        found.changes,
        run_command.load_project_records(project),
        run_command.build_clock().now(),
    )
    record_path = _write_record(project, plan.record) if isinstance(plan, NewRecord) else None
    covering = plan.record_ids if isinstance(plan, AlreadyCovered) else ()
    accepted = apply_schema_acceptance(found, sync, store) if environment == DEV else None
    return SchemaAcceptance(found, accepted, record_path, covering)


def accept_branch_schema(
    project: Path,
    pipeline: str,
    table: str,
    branch: str,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> AcceptedSchema:
    """Accept a table's current shape as a Branch Environment's baseline; no record is written.

    A Branch lives in stage's Warehouse, so this waits up to `lock_timeout` for stage's lock.
    """
    settings_text, pipeline_text = run_command.read_project_files(project, pipeline)
    stage = stage_environment(load_project_settings(settings_text).environments)
    name = stage_branch_name(stage, branch)
    layout = SchemaLayout.for_branch(name)
    kind = stage.warehouse.type
    env = run_command.project_env(project)
    warehouse = run_command.resolve_environment_warehouse(stage, env)
    with run_command.build_warehouse_lock(warehouse, project, STAGE, lock_timeout):
        return accept_table_schema(
            pipeline_text,
            settings_text,
            STAGE,
            table,
            env,
            run_command.build_sync(project, pipeline, name, kind, layout=layout),
            run_command.build_snapshot_store(project, name, kind, layout=layout),
            name,
        )


def _write_record(project: Path, record: AcceptedChangeRecord) -> Path:
    relative = record_file(record.id)
    path = resolve_project_file(project, relative.as_posix(), write=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as file:
            file.write(dump_accepted_record(record))
    except FileExistsError:
        raise RecordExists(
            f"{relative}: a record with this name already exists; nothing accepted"
        ) from None
    return relative


def _echo_accepted(accepted: AcceptedSchema) -> None:
    typer.echo(
        f"Accepted the current shape of {accepted.table} ({len(accepted.shape.columns)} columns, "
        f"Step {accepted.step}) for Pipeline {accepted.pipeline} "
        f"in Environment {accepted.environment}."
    )
    if accepted.nullable_columns:
        typer.echo(
            "These columns are no longer in the Source and may now be NULL in the Warehouse: "
            + ", ".join(accepted.nullable_columns)
        )


def _echo_record(acceptance: SchemaAcceptance) -> None:
    found = acceptance.found
    if acceptance.record_path is not None:
        changes = ", ".join(change.message for change in found.changes)
        typer.echo(
            f"Wrote accepted-change record {acceptance.record_path.as_posix()} ({changes}). "
            "Commit it so stage and prod accept the change on their next run."
        )
    elif acceptance.covering_record_ids:
        paths = ", ".join(record_file(i).as_posix() for i in acceptance.covering_record_ids)
        typer.echo(f"Already accepted by {paths}; no record written.")
    elif acceptance.accepted is None:
        reason = (
            "no Breaking Change found"
            if found.had_previous_snapshot
            else "no previous snapshot to compare with"
        )
        typer.echo(
            f"Pipeline {found.pipeline}, table {found.table} in Environment "
            f"{found.environment}: {reason}; no record written."
        )
