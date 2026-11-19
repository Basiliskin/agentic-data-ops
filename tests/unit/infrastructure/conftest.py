import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from adops.application.ports import ResolvedConnection

HOLD_DUCKDB_FILE = (
    "import sys, duckdb; db = duckdb.connect(sys.argv[1]); print('locked', flush=True);"
    " sys.stdin.read()"
)


@contextmanager
def _duckdb_held_by_another_process(path: Path) -> Iterator[None]:
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLD_DUCKDB_FILE, str(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        yield
    finally:
        holder.kill()
        holder.wait()


@pytest.fixture
def duckdb_held_by_another_process() -> Callable[[Path], AbstractContextManager[None]]:
    """Holds a read-write DuckDB connection on a file from a second process, as a writer would."""
    return _duckdb_held_by_another_process


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
    dsn = os.environ.get(TEST_POSTGRES_DSN)
    if not dsn:
        pytest.fail(LOCAL_POSTGRES_HINT, pytrace=False)
    params = conninfo_to_dict(dsn)
    connection = ResolvedConnection(
        host=str(params.get("host", "localhost")),
        port=int(params.get("port", 5432)),
        database=str(params["dbname"]),
        user=str(params["user"]),
        password=str(params.get("password", "")),
    )
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
