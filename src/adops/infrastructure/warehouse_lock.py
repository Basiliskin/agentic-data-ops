import fcntl
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import IO

import psycopg

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.infrastructure.dlt_sync import duckdb_file

DEFAULT_LOCK_TIMEOUT = 30 * 60
DEFAULT_POLL_SECONDS = 1.0
CONNECT_TIMEOUT_SECONDS = 10
# Advisory locks are scoped to one database, so a fixed key gives one lock per Warehouse.
ADVISORY_LOCK_KEY = 0x6164_6F70_735F_6C6B
APPLICATION_NAME = "adops-warehouse-lock"

type TryAcquire = Callable[[], bool]


@contextmanager
def warehouse_lock(
    warehouse: ResolvedWarehouse,
    project_dir: Path,
    environment: str,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    poll: float = DEFAULT_POLL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> Iterator[None]:
    """Hold the Warehouse's exclusive lock for the block, waiting up to `timeout` seconds for it."""

    def wait_for(try_acquire: TryAcquire) -> None:
        deadline = monotonic() + timeout
        while not try_acquire():
            if monotonic() >= deadline:
                raise WarehouseLocked(environment, timeout)
            sleep(poll)

    if isinstance(warehouse, ResolvedDuckDb):
        lock = _file_lock(_lock_file(duckdb_file(project_dir, warehouse.path)), wait_for)
    else:
        lock = _advisory_lock(warehouse, wait_for)
    with lock:
        yield


def _lock_file(warehouse_file: Path) -> Path:
    return warehouse_file.with_name(warehouse_file.name + ".lock")


@contextmanager
def _advisory_lock(
    warehouse: ResolvedConnection, wait_for: Callable[[TryAcquire], None]
) -> Iterator[None]:
    connection = _connect(warehouse)
    acquired = False

    def try_acquire() -> bool:
        nonlocal acquired
        acquired = _try_advisory_lock(connection)
        return acquired

    try:
        wait_for(try_acquire)
        yield
    finally:
        if acquired:
            # A broken connection already ended the session, and with it the lock.
            with suppress(psycopg.Error):
                connection.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
        connection.close()


def _connect(warehouse: ResolvedConnection) -> psycopg.Connection:
    try:
        connection = psycopg.connect(
            host=warehouse.host,
            port=warehouse.port,
            dbname=warehouse.database,
            user=warehouse.user,
            password=warehouse.password,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            application_name=APPLICATION_NAME,
            autocommit=True,
        )
    except psycopg.Error:
        connection = None
    if connection is None:
        raise WarehouseUnreachable()
    return connection


def _try_advisory_lock(connection: psycopg.Connection) -> bool:
    try:
        row = connection.execute(
            "SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,)
        ).fetchone()
    except psycopg.Error:
        row = None
    if row is None:
        raise WarehouseUnreachable()
    return bool(row[0])


@contextmanager
def _file_lock(path: Path, wait_for: Callable[[TryAcquire], None]) -> Iterator[None]:
    # flock belongs to the open file, so a crashed holder's lock clears when its process exits.
    handle = _open_lock_file(path)
    with handle:
        wait_for(lambda: _try_flock(handle))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _open_lock_file(path: Path) -> IO[bytes]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("ab")
    except OSError:
        handle = None
    if handle is None:
        raise WarehouseUnreachable()
    return handle


def _try_flock(handle: IO[bytes]) -> bool:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True
