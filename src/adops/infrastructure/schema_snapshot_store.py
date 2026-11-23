import json
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import dlt
from dlt.common.destination import Destination
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from dlt.destinations.sql_client import SqlClientBase

from adops.application.ports import ResolvedDuckDb, ResolvedWarehouse
from adops.domain.branch_environment import SchemaLayout
from adops.domain.schema_snapshot import TableShape
from adops.infrastructure.dlt_sync import (
    DEFAULT_LAYOUT,
    duckdb_file,
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


class ReadOnlySnapshotStoreError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("this schema snapshot store is read-only and cannot save")


class WarehouseSnapshotStore:
    """The SchemaSnapshotStore port backed by a table in the Environment's own Warehouse.

    Built read_only, it never writes: no adops schema or table is created, no dlt local state is
    kept, and a missing Warehouse file, schema or table reads as nothing saved yet.
    """

    def __init__(
        self,
        project: Path,
        warehouse_kind: str,
        *,
        environment: str,
        layout: SchemaLayout = DEFAULT_LAYOUT,
        read_only: bool = False,
    ) -> None:
        self._project = project
        self._layout = layout
        self._pipelines_dir = local_state_dir(project, environment)
        self._warehouse_kind = warehouse_kind
        self._read_only = read_only

    def load(
        self, warehouse: ResolvedWarehouse, pipeline: str, source: str, table: str
    ) -> TableShape | None:
        with self._client(warehouse, SNAPSHOT_TABLE) as found:
            if found is None:
                return None
            client, tables = found
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
        with self._client(warehouse, APPLIED_RECORDS_TABLE) as found:
            if found is None:
                return frozenset()
            client, tables = found
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
        if self._read_only:
            raise ReadOnlySnapshotStoreError()
        shape_json = json.dumps(dict(shape.columns), sort_keys=True)
        with self._writable_client(warehouse) as (client, tables), client.begin_transaction():
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
        self, warehouse: ResolvedWarehouse, table: str
    ) -> Iterator[tuple[SqlClientBase[Any], _Tables] | None]:
        if not self._read_only:
            with self._writable_client(warehouse) as found:
                yield found
            return
        if isinstance(warehouse, ResolvedDuckDb) and not self._duckdb_path(warehouse).exists():
            yield None
            return
        with ExitStack() as stack:
            pipelines_dir = Path(stack.enter_context(TemporaryDirectory(prefix="adops-read-")))
            client = stack.enter_context(
                warehouse_client(self._pipeline(pipelines_dir, self._destination(warehouse)))
            )
            yield (client, _tables(client)) if _has_table(client, table) else None

    @contextmanager
    def _writable_client(
        self, warehouse: ResolvedWarehouse
    ) -> Iterator[tuple[SqlClientBase[Any], _Tables]]:
        pipeline = self._pipeline(self._pipelines_dir, self._destination(warehouse))
        with warehouse_client(pipeline) as client:
            if not client.has_dataset():
                client.create_dataset()
            tables = _tables(client)
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

    def _pipeline(self, pipelines_dir: Path, destination: Destination) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name="adops_schema_snapshots",
            pipelines_dir=str(pipelines_dir),
            destination=destination,
            dataset_name=self._layout.adops,
        )

    def _destination(self, warehouse: ResolvedWarehouse) -> Destination:
        branch_schemas = self._layout != DEFAULT_LAYOUT
        if self._read_only and isinstance(warehouse, ResolvedDuckDb):
            options = {"enable_dataset_name_normalization": False} if branch_schemas else {}
            credentials = DuckDbCredentials(str(self._duckdb_path(warehouse)), read_only=True)
            return dlt.destinations.duckdb(credentials=credentials, **options)
        return warehouse_destination(
            self._warehouse_kind, warehouse, self._project, branch_schemas=branch_schemas
        )

    def _duckdb_path(self, warehouse: ResolvedDuckDb) -> Path:
        return duckdb_file(self._project, warehouse.path)


def _tables(client: SqlClientBase[Any]) -> _Tables:
    return _Tables(
        snapshots=client.make_qualified_table_name(SNAPSHOT_TABLE),
        applied_records=client.make_qualified_table_name(APPLIED_RECORDS_TABLE),
    )


def _has_table(client: SqlClientBase[Any], table: str) -> bool:
    rows = client.execute_sql(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        client.dataset_name,
        table,
    )
    return bool(rows)
