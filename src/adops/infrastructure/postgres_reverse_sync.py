from collections import Counter
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dlt
import psycopg
from dlt.destinations.sql_client import SqlClientBase
from psycopg import sql
from psycopg.rows import dict_row

from adops.application.ports import (
    ResolvedConnection,
    ResolvedWarehouse,
    ReverseSyncRead,
    TargetUnreachable,
    WriteFailed,
)
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff, Row, compute_diff, normalize_value
from adops.infrastructure.dbt_transform import TARGET_SCHEMA
from adops.infrastructure.dlt_sync import (
    local_state_dir,
    warehouse_client,
    warehouse_destination,
)

CONNECT_TIMEOUT_SECONDS = 10


class _DiffChanged(Exception):
    pass


class PostgresReverseSync:
    """The ReverseSync port: reads a dbt model from the Warehouse and writes a Postgres Target.

    `apply` re-reads the Target under lock and refuses a Diff that no longer matches what `read`
    saw, so it must follow `read` for the same Step on the same instance.
    """

    def __init__(self, project_dir: Path, *, environment: str, warehouse_kind: str) -> None:
        self._project_dir = project_dir
        self._pipelines_dir = local_state_dir(project_dir, environment)
        self._warehouse_kind = warehouse_kind
        self._result_rows: dict[str, tuple[Row, ...]] = {}

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead:
        result_rows = self._read_result(step, warehouse)
        self._result_rows[step.name] = result_rows
        columns = target_columns(step)
        with _connected(target) as connection:
            existing = _existing_columns(connection, step.target_table)
            missing = tuple(column for column in columns if column not in existing)
            # Selecting a column the Target lacks would fail; target_columns_exist reports it.
            target_rows = () if missing or not existing else _target_rows(connection, step)
        keys = Counter(normalize_value(row[step.key]) for row in result_rows)
        null_keys = keys.pop(None, 0)
        return ReverseSyncRead(
            result_rows=result_rows,
            target_rows=target_rows,
            target_table_exists=bool(existing),
            missing_target_columns=missing,
            duplicate_keys=sum(keys.values()) - len(keys),
            null_keys=null_keys,
        )

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        result_rows = self._result_rows.get(step.name)
        failed = False
        with _connected(target) as connection:
            try:
                with connection.transaction():
                    locked = _target_rows(connection, step, for_update=True)
                    if result_rows is None or diff != _diff(step, result_rows, locked):
                        raise _DiffChanged()
                    _write(connection, step, diff)
            except (psycopg.Error, _DiffChanged):
                failed = True
        if failed:
            raise WriteFailed()

    def _read_result(self, step: ReverseSyncStep, warehouse: ResolvedWarehouse) -> tuple[Row, ...]:
        pipeline = dlt.pipeline(
            pipeline_name=f"adops_reverse_sync_{step.name}",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(self._warehouse_kind, warehouse, self._project_dir),
            dataset_name=TARGET_SCHEMA,
        )
        columns = result_columns(step)
        with warehouse_client(pipeline) as client:
            rows = client.execute_sql(_result_query(client, step.model, step.key, columns))
        return tuple(dict(zip(columns, row, strict=True)) for row in rows)


def result_columns(step: ReverseSyncStep) -> tuple[str, ...]:
    return tuple(dict.fromkeys((step.key, *step.mapping)))


def target_columns(step: ReverseSyncStep) -> tuple[str, ...]:
    return tuple(dict.fromkeys((step.mapping.get(step.key, step.key), *step.mapping.values())))


def _result_query(
    client: SqlClientBase[Any], model: str, key: str, columns: tuple[str, ...]
) -> str:
    selected = ", ".join(client.escape_column_name(column) for column in columns)
    table = client.make_qualified_table_name(model)
    return f"SELECT {selected} FROM {table} ORDER BY {client.escape_column_name(key)}"


@contextmanager
def _connected(target: ResolvedConnection) -> Iterator[psycopg.Connection[dict[str, Any]]]:
    try:
        connection = psycopg.connect(
            host=target.host,
            port=target.port,
            dbname=target.database,
            user=target.user,
            password=target.password,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            row_factory=dict_row,
        )
    except psycopg.Error:
        connection = None
    if connection is None:
        raise TargetUnreachable()
    with connection:
        yield connection


def _existing_columns(connection: psycopg.Connection[dict[str, Any]], table: str) -> set[str]:
    rows = connection.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    ).fetchall()
    return {row["column_name"] for row in rows}


def _target_rows(
    connection: psycopg.Connection[dict[str, Any]],
    step: ReverseSyncStep,
    for_update: bool = False,
) -> tuple[Row, ...]:
    query = sql.SQL("SELECT {columns} FROM {table}{lock}").format(
        columns=sql.SQL(", ").join(map(sql.Identifier, target_columns(step))),
        table=sql.Identifier(step.target_table),
        lock=sql.SQL(" FOR UPDATE" if for_update else ""),
    )
    return tuple(connection.execute(query).fetchall())


def _diff(
    step: ReverseSyncStep, result_rows: tuple[Row, ...], target_rows: tuple[Row, ...]
) -> Diff:
    return compute_diff(result_rows, target_rows, step.key, step.mapping, step.delete_missing)


def _write(
    connection: psycopg.Connection[dict[str, Any]], step: ReverseSyncStep, diff: Diff
) -> None:
    table = sql.Identifier(step.target_table)
    key = sql.Identifier(diff.key)
    with connection.cursor() as cursor:
        cursor.executemany(
            sql.SQL("DELETE FROM {table} WHERE {key} = %s").format(table=table, key=key),
            [(deleted,) for deleted in diff.deletes],
        )
        for update in diff.updates:
            cursor.execute(
                _update_statement(table, key, update.values), (*update.values.values(), update.key)
            )
        columns = target_columns(step)
        cursor.executemany(
            sql.SQL("INSERT INTO {table} ({columns}) VALUES ({values})").format(
                table=table,
                columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                values=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            ),
            [tuple(row[column] for column in columns) for row in diff.inserts],
        )


def _update_statement(
    table: sql.Identifier, key: sql.Identifier, values: Mapping[str, object]
) -> sql.Composed:
    assignments = sql.SQL(", ").join(
        sql.SQL("{column} = %s").format(column=sql.Identifier(column)) for column in values
    )
    return sql.SQL("UPDATE {table} SET {assignments} WHERE {key} = %s").format(
        table=table, assignments=assignments, key=key
    )
