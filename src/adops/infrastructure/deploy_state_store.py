from collections.abc import Callable, Collection
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations.sql_client import SqlClientBase

from adops.application.ports import ResolvedWarehouse, WarehouseUnreachable
from adops.domain.deploy_rerun import DeployState
from adops.infrastructure.dlt_sync import (
    DEFAULT_LAYOUT,
    local_state_dir,
    warehouse_client,
    warehouse_destination,
)

DEPLOY_STATE_TABLE = "_adops_deploy_state"
MARKER = "settings_marker"
SEEN_RECORD = "seen_record"


class WarehouseDeployStateStore:
    """The DeployStateStore port backed by a table in the Environment's own adops schema.

    The Settings Marker is one row; each seen Accepted-Change Record id is one row per Pipeline.
    """

    def __init__(
        self,
        project: Path,
        warehouse_kind: str,
        warehouse: ResolvedWarehouse,
        *,
        environment: str,
    ) -> None:
        self._project = project
        self._pipelines_dir = local_state_dir(project, environment)
        self._warehouse_kind = warehouse_kind
        self._warehouse = warehouse

    def load(self) -> DeployState:
        return self._in_warehouse(_load)

    def save_seen_records(self, pipeline: str, record_ids: Collection[str]) -> None:
        self._in_warehouse(lambda client: _save_seen_records(client, pipeline, record_ids))

    def save_marker(self, marker: str) -> None:
        self._in_warehouse(lambda client: _save_marker(client, marker))

    def _in_warehouse[T](self, work: Callable[[SqlClientBase[Any]], T]) -> T:
        try:
            with warehouse_client(self._pipeline()) as client:
                return work(client)
        except Exception:
            pass
        # Driver text can hold the DSN or the file path, so none of it is kept.
        raise WarehouseUnreachable()

    def _pipeline(self) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name="adops_deploy_state",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(self._warehouse_kind, self._warehouse, self._project),
            dataset_name=DEFAULT_LAYOUT.adops,
        )


def _load(client: SqlClientBase[Any]) -> DeployState:
    exists = client.execute_sql(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        client.dataset_name,
        DEPLOY_STATE_TABLE,
    )
    if not exists[0][0]:
        return DeployState(settings_marker=None)
    rows = client.execute_sql(
        f"SELECT kind, value FROM {client.make_qualified_table_name(DEPLOY_STATE_TABLE)}"
    )
    markers = [value for kind, value in rows if kind == MARKER]
    return DeployState(
        settings_marker=markers[0] if markers else None,
        seen_record_ids=frozenset(value for kind, value in rows if kind == SEEN_RECORD),
    )


def _save_seen_records(
    client: SqlClientBase[Any], pipeline: str, record_ids: Collection[str]
) -> None:
    qualified = _created_table(client)
    with client.begin_transaction():
        for record_id in record_ids:
            client.execute_sql(
                f"INSERT INTO {qualified} (kind, pipeline, value) VALUES (%s, %s, %s) "
                "ON CONFLICT (kind, pipeline, value) DO NOTHING",
                SEEN_RECORD,
                pipeline,
                record_id,
            )


def _save_marker(client: SqlClientBase[Any], marker: str) -> None:
    qualified = _created_table(client)
    with client.begin_transaction():
        client.execute_sql(f"DELETE FROM {qualified} WHERE kind = %s", MARKER)
        client.execute_sql(
            f"INSERT INTO {qualified} (kind, pipeline, value) VALUES (%s, '', %s)",
            MARKER,
            marker,
        )


def _created_table(client: SqlClientBase[Any]) -> str:
    if not client.has_dataset():
        client.create_dataset()
    qualified = client.make_qualified_table_name(DEPLOY_STATE_TABLE)
    client.execute_sql(
        f"CREATE TABLE IF NOT EXISTS {qualified} ("
        "kind TEXT NOT NULL, pipeline TEXT NOT NULL, value TEXT NOT NULL, "
        "PRIMARY KEY (kind, pipeline, value))"
    )
    return qualified
