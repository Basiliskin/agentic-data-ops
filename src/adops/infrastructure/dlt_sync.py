import logging
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import dlt
import sqlalchemy as sa
from dlt.common.destination import Destination
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.destinations.sql_client import SqlClientBase
from dlt.sources.sql_database import sql_table

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    SourceUnreachable,
    SyncResult,
    WarehouseUnreachable,
)
from adops.application.run_pipeline import SyncFailedError
from adops.domain.branch_environment import SchemaLayout
from adops.domain.checks import LoadFacts, SourceFacts, SyncMode
from adops.domain.pipeline import (
    AcceptedValuesCheck,
    DeclaredCheck,
    FreshnessCheck,
    NotNullCheck,
    SyncStep,
    SyncTable,
)
from adops.domain.schema_snapshot import DLT_COLUMN_PREFIX, normalize_column_type

NAMING = NamingConvention()
DEFAULT_LAYOUT = SchemaLayout.default()


def _silence_dlt_logger() -> None:
    # dlt logs load failures with tracebacks and row values to its own stderr handler
    # (propagate=False). It reuses the first handler it finds, so a lone NullHandler keeps
    # it quiet even after dlt resets the level on every pipeline.
    dlt_logger = logging.getLogger("dlt")
    for handler in dlt_logger.handlers[:]:
        dlt_logger.removeHandler(handler)
    dlt_logger.addHandler(logging.NullHandler())
    dlt_logger.propagate = False
    dlt_logger.setLevel(logging.CRITICAL + 1)


_silence_dlt_logger()


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


def local_state_dir(project_dir: Path, environment: str) -> Path:
    """dlt's local state, the Incremental cursor included, is kept apart per Environment."""
    return project_dir / ".adops" / "dlt" / environment


def duckdb_file(project_dir: Path, path: str) -> Path:
    """A relative DuckDB path belongs to the Project folder, not the process cwd."""
    return project_dir / path


def _postgres_destination(
    connection: ResolvedConnection, _project_dir: Path, **options: Any
) -> Destination:
    return dlt.destinations.postgres(
        credentials={
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.user,
            "password": connection.password,
        },
        **options,
    )


def _duckdb_destination(
    connection: ResolvedDuckDb, project_dir: Path, **options: Any
) -> Destination:
    return dlt.destinations.duckdb(
        credentials=str(duckdb_file(project_dir, connection.path)), **options
    )


SOURCE_ENGINES: dict[str, Callable[[ResolvedConnection], sa.Engine]] = {
    "postgres": _postgres_engine,
    "sqlite": _sqlite_engine,
}

WAREHOUSE_DESTINATIONS: dict[str, Callable[..., Destination]] = {
    "postgres": _postgres_destination,
    "duckdb": _duckdb_destination,
}


def source_engine(kind: str, connection: ResolvedConnection) -> sa.Engine:
    if kind not in SOURCE_ENGINES:
        raise UnknownConnectionKindError("Source", kind, SOURCE_ENGINES)
    return SOURCE_ENGINES[kind](connection)


def warehouse_destination(
    kind: str,
    connection: ResolvedWarehouse,
    project_dir: Path,
    *,
    branch_schemas: bool = False,
) -> Destination:
    if kind not in WAREHOUSE_DESTINATIONS:
        raise UnknownConnectionKindError("Warehouse", kind, WAREHOUSE_DESTINATIONS)
    options: dict[str, Any] = {}
    if branch_schemas:
        # dlt would otherwise collapse the "__" in Branch Schema names to "_".
        options["enable_dataset_name_normalization"] = False
    return WAREHOUSE_DESTINATIONS[kind](connection, project_dir, **options)


class DltSync:
    """The Sync port backed by dlt; Incremental Sync state lives in the Environment's state dir."""

    def __init__(
        self,
        project_dir: Path,
        pipeline: str,
        *,
        environment: str,
        warehouse_kind: str,
        source_kind: str = "postgres",
        layout: SchemaLayout = DEFAULT_LAYOUT,
    ) -> None:
        self._project_dir = project_dir
        self._layout = layout
        self._pipelines_dir = local_state_dir(project_dir, environment)
        self._pipeline = pipeline
        self._source_kind = source_kind
        self._warehouse_kind = warehouse_kind

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        engine = source_engine(self._source_kind, source)
        facts: dict[str, SourceFacts] = {}
        try:
            for table in step.tables:
                facts[table.name] = self._guarded(
                    step, table, "preflight failed", _source_facts, engine, table
                )
        finally:
            engine.dispose()
        return facts

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedWarehouse
    ) -> SyncResult:
        engine = source_engine(self._source_kind, source)
        pipeline = self._warehouse_pipeline(step, warehouse)
        rows_loaded: dict[str, int] = {}
        load_facts: dict[str, LoadFacts] = {}
        try:
            for table in step.tables:
                rows_loaded[table.name], load_facts[table.name] = self._guarded(
                    step, table, "load failed", _load_table, pipeline, engine, table
                )
        finally:
            engine.dispose()
        return SyncResult(rows_loaded, load_facts)

    def make_columns_nullable(
        self,
        step: SyncStep,
        table: SyncTable,
        warehouse: ResolvedWarehouse,
        columns: Collection[str],
    ) -> None:
        pipeline = self._warehouse_pipeline(step, warehouse)
        self._guarded(
            step, table, "making columns nullable failed", _drop_not_null, pipeline, table, columns
        )

    def _guarded[T](
        self, step: SyncStep, table: SyncTable, failed: str, call: Callable[..., T], *args: Any
    ) -> T:
        try:
            return call(*args)
        except (SourceUnreachable, WarehouseUnreachable):
            raise
        except UnsupportedFreshnessColumnError as error:
            reason = error.reason
        except Exception:
            reason = failed
        # Driver text can hold host:port, secrets and row values, so none of it is kept.
        raise SyncFailedError(
            f"Pipeline {self._pipeline}, Step {step.name}, table {table.name}: {reason}"
        )

    def _warehouse_pipeline(self, step: SyncStep, warehouse: ResolvedWarehouse) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name=f"adops_{self._pipeline}_{step.name}",
            pipelines_dir=str(self._pipelines_dir),
            destination=warehouse_destination(
                self._warehouse_kind,
                warehouse,
                self._project_dir,
                branch_schemas=self._layout != DEFAULT_LAYOUT,
            ),
            dataset_name=self._layout.raw(step.source),
        )


def _load_table(
    pipeline: dlt.Pipeline, engine: sa.Engine, table: SyncTable
) -> tuple[int, LoadFacts]:
    _reach_source(engine, table)
    source_rows = None
    if table.mode is SyncMode.FULL_REFRESH:
        source_rows = _source_row_count(engine, table.name)
    before = _measure_warehouse(pipeline, table)
    pipeline.run(_resource(engine, table))
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    after = _measure_warehouse(pipeline, table, table.checks)
    facts = LoadFacts(
        warehouse_rows_before=before.rows,
        warehouse_rows_after=after.rows,
        duplicate_keys=after.duplicate_keys,
        null_keys=after.null_keys,
        source_rows=source_rows,
        cursor_max_before=before.cursor_max,
        cursor_max_after=after.cursor_max,
        null_counts=after.null_counts,
        unexpected_value_counts=after.unexpected_value_counts,
        newest_values=after.newest_values,
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


def _reach_source(engine: sa.Engine, table: SyncTable) -> None:
    try:
        engine.connect().close()
    except Exception:
        pass
    else:
        return
    raise SourceUnreachable(table.name)


@contextmanager
def warehouse_client(pipeline: dlt.Pipeline) -> Iterator[SqlClientBase[Any]]:
    """Opens the Warehouse sql_client, turning a failed connect into WarehouseUnreachable."""
    with ExitStack() as stack:
        try:
            client = stack.enter_context(pipeline.sql_client())
        except Exception:
            client = None
        if client is None:
            raise WarehouseUnreachable()
        yield client


def _source_facts(engine: sa.Engine, sync_table: SyncTable) -> SourceFacts:
    _reach_source(engine, sync_table)
    table = sync_table.name
    inspector = sa.inspect(engine)
    if not inspector.has_table(table):
        return SourceFacts(table_exists=False, columns=frozenset())
    columns = [
        column
        for column in inspector.get_columns(table)
        if not column["name"].startswith(DLT_COLUMN_PREFIX)
    ]
    return SourceFacts(
        table_exists=True,
        columns=frozenset(column["name"] for column in columns),
        column_types={
            column["name"]: normalize_column_type(column["type"].compile(dialect=engine.dialect))
            for column in columns
        },
        row_count=_source_row_count(engine, table),
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
    null_counts: dict[str, int] = field(default_factory=dict)
    unexpected_value_counts: dict[str, int] = field(default_factory=dict)
    newest_values: dict[str, datetime | None] = field(default_factory=dict)


_MEASURED_CHECKS = (NotNullCheck, AcceptedValuesCheck, FreshnessCheck)
type _MeasuredCheck = NotNullCheck | AcceptedValuesCheck | FreshnessCheck


class UnsupportedFreshnessColumnError(TypeError):
    def __init__(self, column: str, value: object) -> None:
        kind = type(value).__name__
        self.reason = f"freshness column {column} holds {kind} values, not timestamps"
        super().__init__(self.reason)


def _measure_warehouse(
    pipeline: dlt.Pipeline, table: SyncTable, declared: Sequence[DeclaredCheck] = ()
) -> _Measurement:
    name = NAMING.normalize_table_identifier(table.name)
    measured = [check for check in declared if isinstance(check, _MEASURED_CHECKS)]
    with warehouse_client(pipeline) as client:
        exists = client.execute_sql(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            client.dataset_name,
            name,
        )
        if not exists[0][0]:
            empty_table = [None if isinstance(c, FreshnessCheck) else 0 for c in measured]
            return _Measurement(0, 0, 0, None, *_declared_facts(measured, empty_table))
        key = _column(client, table.primary_key)
        cursor = "NULL"
        if table.cursor is not None:
            cursor = f"max({_column(client, table.cursor)})"
        expressions = [f"count(*), count({key}), count(DISTINCT {key}), {cursor}"]
        parameters: list[str] = []
        for check in measured:
            expression, values = _declared_expression(client, check)
            expressions.append(expression)
            parameters.extend(values)
        query = f"SELECT {', '.join(expressions)} FROM {client.make_qualified_table_name(name)}"
        rows, keys, distinct_keys, cursor_max, *declared_values = client.execute_sql(
            query, *parameters
        )[0]
    return _Measurement(
        rows,
        keys - distinct_keys,
        rows - keys,
        cursor_max,
        *_declared_facts(measured, declared_values),
    )


def _column(client: SqlClientBase[Any], column: str) -> str:
    return client.escape_column_name(NAMING.normalize_identifier(column))


def _declared_expression(
    client: SqlClientBase[Any], check: _MeasuredCheck
) -> tuple[str, list[str]]:
    column = _column(client, check.column)
    if isinstance(check, NotNullCheck):
        return f"count(*) - count({column})", []
    if isinstance(check, AcceptedValuesCheck):
        # NULL is not "outside the list": CAST keeps it NULL and NOT IN yields NULL, so the
        # CASE skips it. A missing value is not_null's job, as in dbt's accepted_values.
        placeholders = ", ".join(["%s"] * len(check.values))
        expression = f"count(CASE WHEN CAST({column} AS TEXT) NOT IN ({placeholders}) THEN 1 END)"
        return expression, [str(value) for value in check.values]
    return f"max({column})", []


def _declared_facts(
    checks: Sequence[_MeasuredCheck],
    values: Sequence[Any],
) -> tuple[dict[str, int], dict[str, int], dict[str, datetime | None]]:
    null_counts: dict[str, int] = {}
    unexpected_value_counts: dict[str, int] = {}
    newest_values: dict[str, datetime | None] = {}
    for check, value in zip(checks, values, strict=True):
        if isinstance(check, NotNullCheck):
            null_counts[check.column] = value
        elif isinstance(check, AcceptedValuesCheck):
            unexpected_value_counts[check.column] = value
        else:
            newest_values[check.column] = _as_datetime(check.column, value)
    return null_counts, unexpected_value_counts, newest_values


def _as_datetime(column: str, value: object) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time())
    raise UnsupportedFreshnessColumnError(column, value)


def _drop_not_null(pipeline: dlt.Pipeline, table: SyncTable, columns: Collection[str]) -> None:
    name = NAMING.normalize_table_identifier(table.name)
    wanted = {NAMING.normalize_identifier(column) for column in columns}
    with warehouse_client(pipeline) as client:
        _drop_not_null_in_dataset(client, name, wanted)
        # Merge loads land in the staging copy first, which carries the same NOT NULL.
        with client.with_staging_dataset():
            _drop_not_null_in_dataset(client, name, wanted)


def _drop_not_null_in_dataset(client: SqlClientBase[Any], table: str, columns: set[str]) -> None:
    not_null = client.execute_sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND is_nullable = 'NO'",
        client.dataset_name,
        table,
    )
    qualified = client.make_qualified_table_name(table)
    for column in sorted(columns & {row[0] for row in not_null}):
        client.execute_sql(
            f"ALTER TABLE {qualified} ALTER COLUMN {client.escape_column_name(column)} "
            "DROP NOT NULL"
        )
