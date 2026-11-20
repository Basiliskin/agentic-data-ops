import os
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from adops.application.ports import ResolvedConnection

TEST_POSTGRES_DSN = "ADOPS_TEST_POSTGRES_DSN"
LOCAL_POSTGRES_HINT = (
    f"{TEST_POSTGRES_DSN} is not set; start Postgres with "
    "`docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=adops postgres:17-alpine` and set "
    f"{TEST_POSTGRES_DSN}=postgresql://postgres:adops@localhost:55432/postgres"
)


@dataclass(frozen=True)
class PostgresTarget:
    dsn: str
    connection: ResolvedConnection
    table: str


@pytest.fixture
def postgres_target() -> Iterator[PostgresTarget]:
    """A freshly created Target table in the test Postgres, dropped after the test."""
    dsn = _test_postgres_dsn()
    params = conninfo_to_dict(dsn)
    connection = _resolved(params, str(params["dbname"]))
    table = f"target_{uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(
            sql.SQL(
                "CREATE TABLE {} (customer_id integer PRIMARY KEY, email text, total integer)"
            ).format(sql.Identifier(table))
        )
    try:
        yield PostgresTarget(dsn, connection, table)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))


def _test_postgres_dsn() -> str:
    dsn = os.environ.get(TEST_POSTGRES_DSN)
    if not dsn:
        pytest.fail(LOCAL_POSTGRES_HINT, pytrace=False)
    return dsn


def _resolved(params: dict[str, object], database: str) -> ResolvedConnection:
    return ResolvedConnection(
        host=str(params.get("host", "localhost")),
        port=int(str(params.get("port", 5432))),
        database=database,
        user=str(params["user"]),
        password=str(params.get("password", "")),
    )


@dataclass(frozen=True)
class PostgresWarehouse:
    connection: ResolvedConnection
    dsn: str

    def rows(self, query: str, *parameters: object) -> list[tuple]:
        with psycopg.connect(self.dsn) as connection:
            return connection.execute(query, parameters).fetchall()

    def execute(self, *statements: str) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as connection:
            for statement in statements:
                connection.execute(statement)

    def schemas(self) -> set[str]:
        return {row[0] for row in self.rows("SELECT schema_name FROM information_schema.schemata")}


@pytest.fixture
def postgres_warehouse() -> Iterator[PostgresWarehouse]:
    """A freshly created, empty database in the test Postgres, dropped after the test."""
    dsn = _test_postgres_dsn()
    params = conninfo_to_dict(dsn)
    database = f"adops_test_{uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        yield PostgresWarehouse(
            _resolved(params, database), make_conninfo(**{**params, "dbname": database})
        )
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database))
            )
