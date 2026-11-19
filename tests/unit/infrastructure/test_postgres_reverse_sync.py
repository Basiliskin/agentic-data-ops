from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import duckdb
import psycopg
import pytest
from conftest import PostgresTarget
from psycopg import sql
from test_dbt_transform import write_dbt_project
from test_dlt_sync import SENTINEL_PASSWORD, assert_no_secret, unreachable_postgres

from adops.application.ports import (
    ResolvedDuckDb,
    ReverseSync,
    TargetUnreachable,
    WriteFailed,
)
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import compute_diff
from adops.domain.transform import TransformStep
from adops.infrastructure.dbt_transform import TARGET_SCHEMA, DbtTransform
from adops.infrastructure.postgres_reverse_sync import PostgresReverseSync

MODEL = "customer_stats"
TARGET_ROWS = [(1, "a@x", 10), (2, "b@x", 20), (3, "c@x", 30)]
RESULT_ROWS = [(1, "a@x", 10), (2, "b@x", 25), (4, "d@x", 40), (5, "e@x", 50)]


def reverse_sync_step(table: str, *, model: str = MODEL, delete_missing: bool = False):
    return ReverseSyncStep(
        type="reverse_sync",
        name="push_stats",
        model=model,
        target="shop",
        target_table=table,
        key="customer_id",
        mapping={"email": "email", "order_total": "total"},
        delete_missing=delete_missing,
    )


def execute(dsn: str, statement: sql.Composable, params: tuple[object, ...] = ()) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(statement, params)


def seed(target: PostgresTarget, rows: list[tuple[int, str, int]] = TARGET_ROWS) -> None:
    with psycopg.connect(target.dsn, autocommit=True) as connection:
        connection.cursor().executemany(
            sql.SQL("INSERT INTO {} VALUES (%s, %s, %s)").format(sql.Identifier(target.table)),
            rows,
        )


def table_rows(target: PostgresTarget) -> list[tuple[object, ...]]:
    with psycopg.connect(target.dsn) as connection:
        query = sql.SQL("SELECT * FROM {} ORDER BY customer_id").format(
            sql.Identifier(target.table)
        )
        return connection.execute(query).fetchall()


def duckdb_warehouse(
    tmp_path: Path, rows: list[tuple[object, ...]], total_type: str = "INTEGER"
) -> ResolvedDuckDb:
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(f"CREATE SCHEMA {TARGET_SCHEMA}")
        connection.execute(
            f"CREATE TABLE {TARGET_SCHEMA}.{MODEL} "
            f"(customer_id INTEGER, email VARCHAR, order_total {total_type})"
        )
        connection.executemany(f"INSERT INTO {TARGET_SCHEMA}.{MODEL} VALUES (?, ?, ?)", rows)
    return ResolvedDuckDb(str(path))


@pytest.fixture
def adapter(tmp_path: Path) -> PostgresReverseSync:
    return PostgresReverseSync(tmp_path, environment="dev", warehouse_kind="duckdb")


@pytest.fixture
def postgres_model(postgres_target: PostgresTarget) -> Iterator[str]:
    model = f"stats_{uuid4().hex[:12]}"
    qualified = sql.Identifier(TARGET_SCHEMA, model)
    execute(postgres_target.dsn, sql.SQL("CREATE SCHEMA IF NOT EXISTS analytics"))
    execute(
        postgres_target.dsn,
        sql.SQL("CREATE TABLE {} (customer_id integer, email text, order_total integer)").format(
            qualified
        ),
    )
    try:
        yield model
    finally:
        execute(postgres_target.dsn, sql.SQL("DROP TABLE {}").format(qualified))


def test_postgres_reverse_sync_is_a_reverse_sync(adapter: PostgresReverseSync):
    assert isinstance(adapter, ReverseSync)


def test_the_warehouse_kind_must_be_given(tmp_path: Path):
    with pytest.raises(TypeError):
        PostgresReverseSync(tmp_path)  # type: ignore[call-arg]


def test_read_measures_the_dbt_result_and_the_target_on_duckdb(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    rows = [*RESULT_ROWS, (5, "dup@x", 51), (None, "nokey@x", 0)]
    warehouse = duckdb_warehouse(tmp_path, rows)

    read = adapter.read(
        reverse_sync_step(postgres_target.table), warehouse, postgres_target.connection
    )

    assert read.result_rows[0] == {"customer_id": 1, "email": "a@x", "order_total": 10}
    assert len(read.result_rows) == 6
    assert sorted(row["customer_id"] for row in read.target_rows) == [1, 2, 3]
    assert read.target_rows[0].keys() == {"customer_id", "email", "total"}
    assert read.target_table_exists is True
    assert read.missing_target_columns == ()
    assert (read.duplicate_keys, read.null_keys) == (1, 1)


def test_read_lists_target_columns_the_mapping_needs_but_the_table_lacks(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    execute(
        postgres_target.dsn,
        sql.SQL("ALTER TABLE {} DROP COLUMN total").format(sql.Identifier(postgres_target.table)),
    )

    read = adapter.read(
        reverse_sync_step(postgres_target.table),
        duckdb_warehouse(tmp_path, RESULT_ROWS),
        postgres_target.connection,
    )

    assert read.target_table_exists is True
    assert read.missing_target_columns == ("total",)
    assert read.target_rows == ()


def test_read_reports_a_missing_target_table_as_a_fact(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    read = adapter.read(
        reverse_sync_step(f"absent_{uuid4().hex[:8]}"),
        duckdb_warehouse(tmp_path, RESULT_ROWS),
        postgres_target.connection,
    )

    assert read.target_table_exists is False
    assert read.target_rows == ()
    assert len(read.result_rows) == 4


def test_read_takes_the_dbt_result_from_a_postgres_warehouse(
    tmp_path: Path, postgres_target: PostgresTarget, postgres_model: str
):
    seed(postgres_target)
    execute(
        postgres_target.dsn,
        sql.SQL("INSERT INTO {} VALUES (7, 'g@x', 70)").format(
            sql.Identifier(TARGET_SCHEMA, postgres_model)
        ),
    )
    adapter = PostgresReverseSync(tmp_path, environment="dev", warehouse_kind="postgres")

    read = adapter.read(
        reverse_sync_step(postgres_target.table, model=postgres_model),
        postgres_target.connection,
        postgres_target.connection,
    )

    assert read.result_rows == ({"customer_id": 7, "email": "g@x", "order_total": 70},)
    assert len(read.target_rows) == 3


def test_dbt_build_then_read_in_one_process_shares_the_duckdb_file(
    tmp_path: Path, postgres_target: PostgresTarget
):
    root = tmp_path / "project"
    root.mkdir()
    model_sql = "select * from (values (1, 'a@x', 10)) as t(customer_id, email, order_total)"
    write_dbt_project(root, {MODEL: model_sql}, schema="version: 2\n")
    warehouse = ResolvedDuckDb(str(tmp_path / "warehouse.duckdb"))
    transform = DbtTransform(root, warehouse_kind="duckdb")

    built = transform.run(TransformStep(type="transform", name="build", project="dbt"), warehouse)
    read = PostgresReverseSync(root, environment="dev", warehouse_kind="duckdb").read(
        reverse_sync_step(postgres_target.table), warehouse, postgres_target.connection
    )

    assert built.crashed is False
    assert read.result_rows == ({"customer_id": 1, "email": "a@x", "order_total": 10},)


def test_apply_commits_inserts_updates_and_deletes_in_one_transaction(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    step = reverse_sync_step(postgres_target.table, delete_missing=True)
    read = adapter.read(step, duckdb_warehouse(tmp_path, RESULT_ROWS), postgres_target.connection)
    diff = compute_diff(read.result_rows, read.target_rows, step.key, step.mapping, True)

    adapter.apply(step, postgres_target.connection, diff)

    assert (len(diff.inserts), len(diff.updates), diff.deletes) == (2, 1, (3,))
    assert table_rows(postgres_target) == RESULT_ROWS


def test_apply_without_delete_missing_keeps_target_rows_absent_from_the_result(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    step = reverse_sync_step(postgres_target.table)
    read = adapter.read(step, duckdb_warehouse(tmp_path, RESULT_ROWS), postgres_target.connection)
    diff = compute_diff(read.result_rows, read.target_rows, step.key, step.mapping, False)

    adapter.apply(step, postgres_target.connection, diff)

    assert table_rows(postgres_target) == sorted([*RESULT_ROWS, (3, "c@x", 30)])


def test_a_failure_on_the_last_statement_leaves_the_target_exactly_as_it_was(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    before = table_rows(postgres_target)
    rows = [(1, "a@x", "10"), (2, "b@x", "25"), (4, "d@x", "40"), (5, "e@x", "not-a-number")]
    step = reverse_sync_step(postgres_target.table, delete_missing=True)
    read = adapter.read(
        step, duckdb_warehouse(tmp_path, rows, "VARCHAR"), postgres_target.connection
    )
    diff = compute_diff(read.result_rows, read.target_rows, step.key, step.mapping, True)
    assert diff.deletes == (3,)
    assert diff.updates
    assert [row["customer_id"] for row in diff.inserts] == [4, 5]

    with pytest.raises(WriteFailed) as raised:
        adapter.apply(step, postgres_target.connection, diff)

    assert table_rows(postgres_target) == before
    assert_no_secret(raised.value)


def test_apply_refuses_when_the_target_changed_since_the_read(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    step = reverse_sync_step(postgres_target.table, delete_missing=True)
    read = adapter.read(step, duckdb_warehouse(tmp_path, RESULT_ROWS), postgres_target.connection)
    diff = compute_diff(read.result_rows, read.target_rows, step.key, step.mapping, True)
    execute(
        postgres_target.dsn,
        sql.SQL("UPDATE {} SET total = 11 WHERE customer_id = 1").format(
            sql.Identifier(postgres_target.table)
        ),
    )
    changed = table_rows(postgres_target)

    with pytest.raises(WriteFailed):
        adapter.apply(step, postgres_target.connection, diff)

    assert table_rows(postgres_target) == changed
    assert changed == [(1, "a@x", 11), (2, "b@x", 20), (3, "c@x", 30)]


def test_apply_refuses_a_diff_it_never_read(
    tmp_path: Path, adapter: PostgresReverseSync, postgres_target: PostgresTarget
):
    seed(postgres_target)
    step = reverse_sync_step(postgres_target.table)
    diff = compute_diff(
        [{"customer_id": 9, "email": "z@x", "order_total": 90}],
        [],
        "customer_id",
        step.mapping,
        False,
    )

    with pytest.raises(WriteFailed):
        adapter.apply(step, postgres_target.connection, diff)

    assert table_rows(postgres_target) == TARGET_ROWS


def test_unreachable_target_raises_target_unreachable_without_driver_text(
    tmp_path: Path, adapter: PostgresReverseSync
):
    step = reverse_sync_step("customer_stats")
    warehouse = duckdb_warehouse(tmp_path, RESULT_ROWS)
    unreachable = unreachable_postgres()

    with pytest.raises(TargetUnreachable) as on_read:
        adapter.read(step, warehouse, unreachable)
    with pytest.raises(TargetUnreachable) as on_apply:
        adapter.apply(step, unreachable, compute_diff([], [], step.key, step.mapping, False))

    for error in (on_read.value, on_apply.value):
        assert_no_secret(error)
        assert SENTINEL_PASSWORD not in repr(error.args)
