from collections.abc import Mapping
from dataclasses import dataclass

from adops.application.ports import SchemaSnapshotStore, Sync
from adops.application.run_pipeline import StepError, pick_environment, relax_dropped_columns
from adops.application.secrets import (
    resolve_connection,
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import ensure_sources_defined, load_pipeline, load_project_settings
from adops.domain.pipeline import Pipeline, SyncStep
from adops.domain.schema_snapshot import TableShape, find_breaking_changes


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
    dropped = relax_dropped_columns(sync, step, step.tables[0], warehouse, changes)
    snapshot_store.save(warehouse, pipeline.name, step.source, table, shape)
    return AcceptedSchema(pipeline.name, environment_name, step.name, table, shape, dropped)


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
