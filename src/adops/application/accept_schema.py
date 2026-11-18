from collections.abc import Mapping
from dataclasses import dataclass

from adops.application.ports import SchemaSnapshotStore, Sync
from adops.application.run_pipeline import StepError, pick_environment, resolve_connection
from adops.application.secrets import resolve_secret_references
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import ensure_sources_defined, load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.schema_snapshot import BreakingChangeKind, TableShape, find_breaking_changes


@dataclass(frozen=True)
class AcceptedSchema:
    pipeline: str
    environment: str
    step: str
    table: str
    shape: TableShape
    nullable_columns: tuple[str, ...] = ()


def accept_schema(
    pipeline_text: str,
    settings_text: str,
    environment_name: str,
    table: str,
    env: Mapping[str, str],
    sync: Sync,
    snapshot_store: SchemaSnapshotStore,
) -> AcceptedSchema:
    """Record a table's current Source shape as its accepted snapshot in one Environment."""
    pipeline = load_pipeline(pipeline_text)
    settings = load_project_settings(settings_text)
    environment = pick_environment(settings.environments, environment_name)
    ensure_sources_defined(pipeline, environment)
    step = _only_step_with(pipeline, table)

    source_connection = environment.sources[step.source]
    secrets = resolve_secret_references(
        env, (environment.warehouse.password_env, source_connection.password_env)
    )
    facts = sync.preflight(step, resolve_connection(source_connection, secrets))[table]
    if not facts.table_exists:
        raise StepError(
            f"Step {step.name}: {table} not found in Source {step.source}; nothing accepted"
        )

    shape = TableShape(facts.column_types)
    warehouse = resolve_connection(environment.warehouse, secrets)
    previous = snapshot_store.load(warehouse, pipeline.name, step.source, table)
    dropped = _dropped_columns(previous, shape)
    if dropped:
        # Loaded rows no longer carry these columns, so a NOT NULL copied from the Source would
        # reject every new row.
        sync.make_columns_nullable(step, step.tables[0], warehouse, dropped)
    snapshot_store.save(warehouse, pipeline.name, step.source, table, shape)
    return AcceptedSchema(pipeline.name, environment_name, step.name, table, shape, dropped)


def _dropped_columns(previous: TableShape | None, current: TableShape) -> tuple[str, ...]:
    if previous is None:
        return ()
    return tuple(
        change.column
        for change in find_breaking_changes(previous, current)
        if change.kind is BreakingChangeKind.DROPPED
    )


def _only_step_with(pipeline: Pipeline, table: str) -> SyncStep:
    steps = [step for step in pipeline.steps if any(t.name == table for t in step.tables)]
    if not steps:
        raise PipelineValidationError([f"{table}: no Step in Pipeline {pipeline.name} copies it"])
    if len(steps) > 1:
        names = ", ".join(step.name for step in steps)
        raise PipelineValidationError([f"{table}: copied by more than one Step ({names})"])
    [step] = steps
    return step.model_copy(update={"tables": [t for t in step.tables if t.name == table]})
