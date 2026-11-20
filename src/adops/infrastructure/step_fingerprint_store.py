from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations.sql_client import SqlClientBase

from adops.application.ports import ResolvedWarehouse, WarehouseUnreachable
from adops.infrastructure.dlt_sync import (
    DEFAULT_LAYOUT,
    local_state_dir,
    warehouse_client,
    warehouse_destination,
)

FINGERPRINT_TABLE = "_adops_step_fingerprints"
GENERATED_DIRS = frozenset({"target", "logs", "dbt_packages", ".adops"})
GENERATED_SUFFIXES = (".pyc",)


class WarehouseFingerprintStore:
    """The StepFingerprintStore port backed by a table in stage's adops schema.

    Rows come back in the order they were last saved, which is Step order for a stage run.
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

    def load(self, pipeline: str) -> Mapping[str, str]:
        return self._in_warehouse(lambda client: _load(client, pipeline))

    def save(self, pipeline: str, step: str, fingerprint: str) -> None:
        self._in_warehouse(lambda client: _save(client, pipeline, step, fingerprint))

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
            pipeline_name="adops_step_fingerprints",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(self._warehouse_kind, self._warehouse, self._project),
            dataset_name=DEFAULT_LAYOUT.adops,
        )


def _load(client: SqlClientBase[Any], pipeline: str) -> dict[str, str]:
    exists = client.execute_sql(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        client.dataset_name,
        FINGERPRINT_TABLE,
    )
    if not exists[0][0]:
        return {}
    rows = client.execute_sql(
        f"SELECT step, fingerprint FROM {client.make_qualified_table_name(FINGERPRINT_TABLE)} "
        "WHERE pipeline = %s ORDER BY saved_order",
        pipeline,
    )
    return {step: fingerprint for step, fingerprint in rows}


def _save(client: SqlClientBase[Any], pipeline: str, step: str, fingerprint: str) -> None:
    if not client.has_dataset():
        client.create_dataset()
    qualified = client.make_qualified_table_name(FINGERPRINT_TABLE)
    client.execute_sql(
        f"CREATE TABLE IF NOT EXISTS {qualified} ("
        "pipeline TEXT NOT NULL, step TEXT NOT NULL, fingerprint TEXT NOT NULL, "
        "saved_order BIGINT NOT NULL, PRIMARY KEY (pipeline, step))"
    )
    client.execute_sql(
        f"INSERT INTO {qualified} (pipeline, step, fingerprint, saved_order) "
        f"SELECT %s, %s, %s, coalesce(max(saved_order), 0) + 1 FROM {qualified} "
        "WHERE pipeline = %s "
        "ON CONFLICT (pipeline, step) DO UPDATE "
        "SET fingerprint = excluded.fingerprint, saved_order = excluded.saved_order",
        pipeline,
        step,
        fingerprint,
        pipeline,
    )


def read_project_tree(project_root: Path, relative_dir: str) -> Mapping[str, bytes]:
    """Every file under a Project folder except what dbt or adops generate there."""
    root = project_root / relative_dir
    files = {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*")
        if path.is_file() and not _is_generated(path.relative_to(root))
    }
    return {name: files[name].read_bytes() for name in sorted(files)}


def _is_generated(relative: Path) -> bool:
    return bool(GENERATED_DIRS.intersection(relative.parts[:-1])) or relative.name.endswith(
        GENERATED_SUFFIXES
    )


@dataclass(frozen=True)
class ProjectTree:
    """The ProjectFiles port over the Project folder on disk."""

    project_root: Path

    def read_tree(self, relative_dir: str) -> Mapping[str, bytes]:
        return read_project_tree(self.project_root, relative_dir)
