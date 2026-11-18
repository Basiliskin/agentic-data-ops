import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dlt
import sqlalchemy as sa
from dlt.common.destination import Destination
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.sources.sql_database import sql_table

from adops.application.ports import MASK, ResolvedConnection, SyncResult
from adops.application.run_pipeline import SyncFailedError
from adops.domain.checks import LoadFacts, SourceFacts, SyncMode
from adops.domain.pipeline import SyncStep, SyncTable

URL_PATTERN = re.compile(r"[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
NAMING = NamingConvention()


class UnknownConnectionKindError(ValueError):
    def __init__(self, role: str, kind: str, known: Mapping[str, Any]) -> None:
        super().__init__(f"unknown {role} kind {kind!r} (known: {', '.join(sorted(known))})")


def _postgres_engine(connection: ResolvedConnection) -> sa.Engine:
    return sa.create_engine(
        sa.URL.create(
            "postgresql+psycopg",
            username=connection.user,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            database=connection.database,
        )
    )


def _sqlite_engine(connection: ResolvedConnection) -> sa.Engine:
    return sa.create_engine(sa.URL.create("sqlite", database=connection.database))


def _postgres_destination(connection: ResolvedConnection) -> Destination:
    return dlt.destinations.postgres(
        credentials={
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.user,
            "password": connection.password,
        }
    )


def _duckdb_destination(connection: ResolvedConnection) -> Destination:
    return dlt.destinations.duckdb(credentials=connection.database)


SOURCE_ENGINES: dict[str, Callable[[ResolvedConnection], sa.Engine]] = {
    "postgres": _postgres_engine,
    "sqlite": _sqlite_engine,
}

WAREHOUSE_DESTINATIONS: dict[str, Callable[[ResolvedConnection], Destination]] = {
    "postgres": _postgres_destination,
    "duckdb": _duckdb_destination,
}


def source_engine(kind: str, connection: ResolvedConnection) -> sa.Engine:
    if kind not in SOURCE_ENGINES:
        raise UnknownConnectionKindError("Source", kind, SOURCE_ENGINES)
    return SOURCE_ENGINES[kind](connection)


def warehouse_destination(kind: str, connection: ResolvedConnection) -> Destination:
    if kind not in WAREHOUSE_DESTINATIONS:
        raise UnknownConnectionKindError("Warehouse", kind, WAREHOUSE_DESTINATIONS)
    return WAREHOUSE_DESTINATIONS[kind](connection)


class DltSync:
    """The Sync port backed by dlt; Incremental Sync state lives under the Project's `.adops/`."""

    def __init__(
        self,
        project_dir: Path,
        pipeline: str,
        source_kind: str = "postgres",
        warehouse_kind: str = "postgres",
    ) -> None:
        self._pipelines_dir = project_dir / ".adops" / "dlt"
        self._pipeline = pipeline
        self._source_kind = source_kind
        self._warehouse_kind = warehouse_kind

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        engine = source_engine(self._source_kind, source)
        facts: dict[str, SourceFacts] = {}
        try:
            for table in step.tables:
                facts[table.name] = _guarded(
                    step, table, (source,), _source_facts, engine, table.name
                )
        finally:
            engine.dispose()
        return facts

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
    ) -> SyncResult:
        engine = source_engine(self._source_kind, source)
        pipeline = dlt.pipeline(
            pipeline_name=f"adops_{self._pipeline}_{step.name}",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(self._warehouse_kind, warehouse),
            dataset_name=f"raw_{step.source}",
        )
        rows_loaded: dict[str, int] = {}
        load_facts: dict[str, LoadFacts] = {}
        try:
            for table in step.tables:
                rows_loaded[table.name], load_facts[table.name] = _guarded(
                    step, table, (source, warehouse), _load_table, pipeline, engine, table
                )
        finally:
            engine.dispose()
        return SyncResult(rows_loaded, load_facts)


def _load_table(
    pipeline: dlt.Pipeline, engine: sa.Engine, table: SyncTable
) -> tuple[int, LoadFacts]:
    source_rows = None
    if table.mode is SyncMode.FULL_REFRESH:
        source_rows = _source_row_count(engine, table.name)
    before = _measure_warehouse(pipeline, table)
    pipeline.run(_resource(engine, table))
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    after = _measure_warehouse(pipeline, table)
    facts = LoadFacts(
        warehouse_rows_before=before.rows,
        warehouse_rows_after=after.rows,
        duplicate_keys=after.duplicate_keys,
        null_keys=after.null_keys,
        source_rows=source_rows,
        cursor_max_before=before.cursor_max,
        cursor_max_after=after.cursor_max,
    )
    return row_counts.get(NAMING.normalize_table_identifier(table.name), 0), facts


def _resource(engine: sa.Engine, table: SyncTable) -> Any:
    if table.mode is SyncMode.INCREMENTAL:
        return sql_table(
            credentials=engine,
            table=table.name,
            incremental=dlt.sources.incremental(table.cursor),
            write_disposition="merge",
            primary_key=table.primary_key,
        )
    return sql_table(
        credentials=engine,
        table=table.name,
        write_disposition="replace",
        primary_key=table.primary_key,
    )


def _source_facts(engine: sa.Engine, table: str) -> SourceFacts:
    inspector = sa.inspect(engine)
    if not inspector.has_table(table):
        return SourceFacts(table_exists=False, columns=frozenset())
    return SourceFacts(
        table_exists=True,
        columns=frozenset(column["name"] for column in inspector.get_columns(table)),
    )


def _source_row_count(engine: sa.Engine, table: str) -> int:
    with engine.connect() as connection:
        count = sa.select(sa.func.count()).select_from(sa.table(table))
        return connection.execute(count).scalar_one()


@dataclass(frozen=True)
class _Measurement:
    rows: int
    duplicate_keys: int
    null_keys: int
    cursor_max: Any


NOT_LOADED_YET = _Measurement(rows=0, duplicate_keys=0, null_keys=0, cursor_max=None)


def _measure_warehouse(pipeline: dlt.Pipeline, table: SyncTable) -> _Measurement:
    name = NAMING.normalize_table_identifier(table.name)
    with pipeline.sql_client() as client:
        exists = client.execute_sql(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            client.dataset_name,
            name,
        )
        if not exists[0][0]:
            return NOT_LOADED_YET
        key = client.escape_column_name(NAMING.normalize_identifier(table.primary_key))
        cursor = "NULL"
        if table.cursor is not None:
            cursor = f"max({client.escape_column_name(NAMING.normalize_identifier(table.cursor))})"
        rows, keys, distinct_keys, cursor_max = client.execute_sql(
            f"SELECT count(*), count({key}), count(DISTINCT {key}), {cursor} "
            f"FROM {client.make_qualified_table_name(name)}"
        )[0]
    return _Measurement(rows, keys - distinct_keys, rows - keys, cursor_max)


def _guarded[T](
    step: SyncStep,
    table: SyncTable,
    connections: tuple[ResolvedConnection, ...],
    call: Callable[..., T],
    *args: Any,
) -> T:
    try:
        return call(*args)
    except Exception as error:
        message = _redact(f"{type(error).__name__}: {error}", connections)
    # Raised outside `except` so the original error, which may hold a secret, is not chained.
    raise SyncFailedError(f"Step {step.name}, table {table.name}: {message}")


def _redact(text: str, connections: tuple[ResolvedConnection, ...]) -> str:
    for connection in connections:
        if connection.password:
            text = text.replace(connection.password, MASK)
    return URL_PATTERN.sub(MASK, text)
