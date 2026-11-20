import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations.sql_client import SqlClientBase

from adops.application.ports import ResolvedWarehouse, WarehouseUnreachable
from adops.domain.branch_environment import BRANCH_PREFIX, STAGE
from adops.infrastructure.dlt_sync import (
    DEFAULT_LAYOUT,
    local_state_dir,
    warehouse_client,
    warehouse_destination,
)

BRANCH_ENVIRONMENT = re.compile(re.escape(BRANCH_PREFIX) + r"[a-z0-9_]+")


class WarehouseSchemaAdmin:
    """The WarehouseSchemas port over stage's Warehouse, Postgres or DuckDB (read-write)."""

    def __init__(self, project: Path, warehouse_kind: str) -> None:
        self._project = project
        self._warehouse_kind = warehouse_kind

    def list_schemas(self, warehouse: ResolvedWarehouse) -> list[str]:
        return self._in_warehouse(warehouse, _list_schemas)

    def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
        self._in_warehouse(warehouse, lambda client: _drop_schema(client, schema))

    def _in_warehouse[T](
        self, warehouse: ResolvedWarehouse, work: Callable[[SqlClientBase[Any]], T]
    ) -> T:
        try:
            with warehouse_client(self._pipeline(warehouse)) as client:
                return work(client)
        except Exception:
            pass
        # Driver text can hold the DSN, the file path or a lock holder's PID, so none is kept.
        raise WarehouseUnreachable()

    def _pipeline(self, warehouse: ResolvedWarehouse) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name="adops_warehouse_schemas",
            pipelines_dir=str(local_state_dir(self._project, STAGE)),
            destination=warehouse_destination(self._warehouse_kind, warehouse, self._project),
            dataset_name=DEFAULT_LAYOUT.adops,
        )


def _list_schemas(client: SqlClientBase[Any]) -> list[str]:
    rows = client.execute_sql(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE catalog_name = current_database()"
    )
    return [name for (name,) in rows]


def _drop_schema(client: SqlClientBase[Any], schema: str) -> None:
    quoted = schema.replace('"', '""')
    client.execute_sql(f'DROP SCHEMA IF EXISTS "{quoted}" CASCADE')


class LocalBranchState:
    """The BranchState port: a Branch Environment's dlt state folder under .adops/dlt."""

    def __init__(self, project: Path) -> None:
        self._project = project

    def delete(self, environment: str) -> None:
        if BRANCH_ENVIRONMENT.fullmatch(environment) is None:
            raise ValueError(f"not a Branch Environment name: {environment!r}")
        state = local_state_dir(self._project, environment)
        if state.is_dir():
            shutil.rmtree(state)
