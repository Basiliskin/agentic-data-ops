from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from adops.application.ports import ResolvedWarehouse, SchemaSnapshotStore, Sync
from adops.application.run_pipeline import StepError, pick_environment, relax_dropped_columns
from adops.application.secrets import (
    resolve_connection,
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
from adops.domain.accepted_change_record import AcceptedChangeRecord, cover
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import ensure_sources_defined, load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.run_log import TIMESTAMP_FORMAT, is_plain_name
from adops.domain.schema_snapshot import BreakingChange, TableShape, find_breaking_changes


@dataclass(frozen=True)
class AcceptedSchema:
    pipeline: str
    environment: str
    step: str
    table: str
    shape: TableShape
    nullable_columns: tuple[str, ...] = ()
    changes: tuple[BreakingChange, ...] = ()


@dataclass(frozen=True)
class FoundSchemaChanges:
    pipeline: str
    environment: str
    step: SyncStep
    source: str
    table: str
    shape: TableShape
    warehouse: ResolvedWarehouse
    had_previous_snapshot: bool
    changes: tuple[BreakingChange, ...]


@dataclass(frozen=True)
class NothingToRecord:
    pass


@dataclass(frozen=True)
class AlreadyCovered:
    record_ids: tuple[str, ...]


@dataclass(frozen=True)
class NewRecord:
    record: AcceptedChangeRecord


RecordPlan = NothingToRecord | AlreadyCovered | NewRecord


def find_schema_changes(
    pipeline_text: str,
    settings_text: str,
    settings_environment_name: str,
    table: str,
    env: Mapping[str, str],
    sync: Sync,
    snapshot_store: SchemaSnapshotStore,
    environment_name: str | None = None,
) -> FoundSchemaChanges:
    """Compare a table's current Source shape with its snapshot, changing nothing.

    Sources and secrets come from the settings Environment; the result is reported under
    environment_name, so a Branch can use stage's settings under its own name.
    """
    pipeline = load_pipeline(pipeline_text)
    settings = load_project_settings(settings_text)
    environment = pick_environment(settings.environments, settings_environment_name)
    ensure_sources_defined(pipeline, environment)
    step = _only_step_with(pipeline, table)

    source_connection = environment.sources[step.source]
    secrets = resolve_secret_references(
        env, secret_references(environment.warehouse, [source_connection])
    )
    facts = sync.preflight(step, resolve_connection(source_connection, secrets))[table]
    if not facts.table_exists:
        raise StepError(
            f"Step {step.name}: {table} not found in Source {step.source}; nothing accepted"
        )

    shape = TableShape(facts.column_types)
    warehouse = resolve_warehouse(environment.warehouse, secrets)
    previous = snapshot_store.load(warehouse, pipeline.name, step.source, table)
    changes = () if previous is None else find_breaking_changes(previous, shape)
    return FoundSchemaChanges(
        pipeline.name,
        environment_name or settings_environment_name,
        step,
        step.source,
        table,
        shape,
        warehouse,
        previous is not None,
        changes,
    )


def apply_schema_acceptance(
    found: FoundSchemaChanges, sync: Sync, snapshot_store: SchemaSnapshotStore
) -> AcceptedSchema:
    """Relax dropped columns' NOT NULL and save the found shape as the new snapshot."""
    dropped = relax_dropped_columns(
        sync, found.step, found.step.tables[0], found.warehouse, found.changes
    )
    snapshot_store.save(found.warehouse, found.pipeline, found.source, found.table, found.shape)
    return AcceptedSchema(
        found.pipeline,
        found.environment,
        found.step.name,
        found.table,
        found.shape,
        dropped,
        found.changes,
    )


def accept_schema(
    pipeline_text: str,
    settings_text: str,
    settings_environment_name: str,
    table: str,
    env: Mapping[str, str],
    sync: Sync,
    snapshot_store: SchemaSnapshotStore,
    environment_name: str | None = None,
) -> AcceptedSchema:
    """Record a table's current Source shape as its accepted snapshot in one Environment."""
    found = find_schema_changes(
        pipeline_text,
        settings_text,
        settings_environment_name,
        table,
        env,
        sync,
        snapshot_store,
        environment_name,
    )
    return apply_schema_acceptance(found, sync, snapshot_store)


def plan_accepted_record(
    pipeline_name: str,
    table: str,
    changes: tuple[BreakingChange, ...],
    existing_records: Iterable[AcceptedChangeRecord],
    now: datetime,
) -> RecordPlan:
    """Decide whether found changes need a new accepted-change record."""
    if not changes:
        return NothingToRecord()
    same_table = [
        record
        for record in existing_records
        if record.pipeline == pipeline_name and record.table == table
    ]
    coverage = cover(changes, same_table, applied_ids=())
    if not coverage.uncovered:
        return AlreadyCovered(coverage.used_ids)
    if not is_plain_name(table):
        raise PipelineValidationError(
            [f"{table}: an accepted-change record needs a plain table name (a-z, 0-9, _)"]
        )
    record_id = f"{now.astimezone(UTC).strftime(TIMESTAMP_FORMAT)}-{pipeline_name}-{table}"
    return NewRecord(AcceptedChangeRecord(record_id, pipeline_name, table, changes))


def _only_step_with(pipeline: Pipeline, table: str) -> SyncStep:
    steps = [
        step
        for step in pipeline.steps
        if isinstance(step, SyncStep) and any(t.name == table for t in step.tables)
    ]
    if not steps:
        raise PipelineValidationError([f"{table}: no Step in Pipeline {pipeline.name} copies it"])
    if len(steps) > 1:
        names = ", ".join(step.name for step in steps)
        raise PipelineValidationError([f"{table}: copied by more than one Step ({names})"])
    [step] = steps
    return step.model_copy(update={"tables": [t for t in step.tables if t.name == table]})
