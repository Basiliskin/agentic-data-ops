import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations.sql_client import SqlClientBase

from adops.application.ports import ResolvedWarehouse
from adops.domain.branch_environment import SchemaLayout
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.dlt_sync import (
    DEFAULT_LAYOUT,
    local_state_dir,
    warehouse_client,
    warehouse_destination,
)

SNAPSHOT_TABLE = "_adops_schema_snapshots"
APPLIED_RECORDS_TABLE = "_adops_applied_records"


@dataclass(frozen=True)
class _Tables:
    snapshots: str
    applied_records: str


class WarehouseSnapshotStore:
    """The SchemaSnapshotStore port backed by a table in the Environment's own Warehouse."""

    def __init__(
        self,
        project: Path,
        warehouse_kind: str,
        *,
        environment: str,
        layout: SchemaLayout = DEFAULT_LAYOUT,
    ) -> None:
        self._project = project
        self._layout = layout
        self._pipelines_dir = local_state_dir(project, environment)
        self._warehouse_kind = warehouse_kind

    def load(
        self, warehouse: ResolvedWarehouse, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        with self._client(warehouse) as (client, tables):
            rows = client.execute_sql(
                f"SELECT shape_json FROM {tables.snapshots} "
                "WHERE pipeline = %s AND source = %s AND table_name = %s",
                pipeline,
                source,
                table,
            )
        if not rows:
            return None
        return TableShape(json.loads(rows[0][0]))

    def applied_record_ids(self, warehouse: ResolvedWarehouse) -> frozenset[str]:
        with self._client(warehouse) as (client, tables):
            rows = client.execute_sql(f"SELECT record_id FROM {tables.applied_records}")
        return frozenset(row[0] for row in rows or ())

    def save(
        self,
        warehouse: ResolvedWarehouse,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
        *,
        applied_record_ids: Sequence[str] = (),
    ) -> None:
        shape_json = json.dumps(dict(shape.columns), sort_keys=True)
        with self._client(warehouse) as (client, tables), client.begin_transaction():
            client.execute_sql(
                f"DELETE FROM {tables.snapshots} "
                "WHERE pipeline = %s AND source = %s AND table_name = %s",
                pipeline,
                source,
                table,
            )
            client.execute_sql(
                f"INSERT INTO {tables.snapshots} (pipeline, source, table_name, shape_json) "
                "VALUES (%s, %s, %s, %s)",
                pipeline,
                source,
                table,
                shape_json,
            )
            for record_id in applied_record_ids:
                client.execute_sql(
                    f"INSERT INTO {tables.applied_records} (record_id, pipeline, table_name) "
                    "VALUES (%s, %s, %s)",
                    record_id,
                    pipeline,
                    table,
                )

    @contextmanager
    def _client(
        self, warehouse: ResolvedWarehouse
    ) -> Iterator[tuple[SqlClientBase[Any], _Tables]]:
        pipeline = dlt.pipeline(
            pipeline_name="adops_schema_snapshots",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(
                self._warehouse_kind,
                warehouse,
                self._project,
                branch_schemas=self._layout != DEFAULT_LAYOUT,
            ),
            dataset_name=self._layout.adops,
        )
        with warehouse_client(pipeline) as client:
            if not client.has_dataset():
                client.create_dataset()
            tables = _Tables(
                snapshots=client.make_qualified_table_name(SNAPSHOT_TABLE),
                applied_records=client.make_qualified_table_name(APPLIED_RECORDS_TABLE),
            )
            client.execute_sql(
                f"CREATE TABLE IF NOT EXISTS {tables.snapshots} ("
                "pipeline TEXT NOT NULL, source TEXT NOT NULL, table_name TEXT NOT NULL, "
                "shape_json TEXT NOT NULL, "
                "recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            client.execute_sql(
                f"CREATE TABLE IF NOT EXISTS {tables.applied_records} ("
                "record_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL, table_name TEXT NOT NULL, "
                "applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            yield client, tables
