from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from pathlib import Path

import psycopg
import pytest

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    WarehouseLocked,
    WarehouseUnreachable,
)
from adops.infrastructure import warehouse_lock as module
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT, warehouse_lock

POLL = 1.0
SENTINEL_PASSWORD = "s3cr3t-warehouse-lock-sentinel"
ADVISORY_LOCKS = (
    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted "
    "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
)


@dataclass
class FakeClock:
    """A monotonic clock that only moves when the code under test sleeps."""

    now: float = 0.0
    sleeps: list[float] = field(default_factory=list)
    on_sleep: Callable[[], None] = lambda: None

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        self.on_sleep()


def locked(warehouse, project: Path, clock: FakeClock, timeout: float = DEFAULT_LOCK_TIMEOUT):
    return warehouse_lock(
        warehouse,
        project,
        "stage",
        timeout=timeout,
        poll=POLL,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


def test_the_default_wait_is_a_fixed_thirty_minutes() -> None:
    assert DEFAULT_LOCK_TIMEOUT == 30 * 60


# DuckDB: an OS file lock on '<warehouse file>.lock'


def test_duckdb_lock_lives_next_to_the_warehouse_file_in_a_created_folder(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb(".adops/warehouse_stage.duckdb")

    with locked(warehouse, tmp_path, FakeClock()):
        assert (tmp_path / ".adops" / "warehouse_stage.duckdb.lock").is_file()

    assert not (tmp_path / ".adops" / "warehouse_stage.duckdb").exists()


def test_duckdb_second_holder_waits_until_the_first_exits(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb("stage.duckdb")
    first = ExitStack()
    first.enter_context(locked(warehouse, tmp_path, FakeClock()))
    clock = FakeClock(on_sleep=first.close)
    entered = False

    with locked(warehouse, tmp_path, clock):
        entered = True

    assert entered
    assert clock.sleeps == [POLL]


def test_duckdb_times_out_with_a_message_naming_no_path(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb(".adops/warehouse_stage.duckdb")
    clock = FakeClock()

    with locked(warehouse, tmp_path, FakeClock()):
        with pytest.raises(WarehouseLocked) as raised:
            with locked(warehouse, tmp_path, clock):
                pytest.fail("entered a held lock")

    error = raised.value
    assert str(error) == "Another run is using the stage Warehouse; gave up after 30 minutes."
    for leak in (str(tmp_path), "warehouse_stage", ".lock", ".adops"):
        assert leak not in str(error)
        assert leak not in repr(error)
    assert error.__context__ is None
    assert clock.sleeps == [POLL] * int(DEFAULT_LOCK_TIMEOUT / POLL)


def test_a_zero_timeout_fails_without_sleeping(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb("stage.duckdb")
    clock = FakeClock()

    with locked(warehouse, tmp_path, FakeClock()):
        with pytest.raises(WarehouseLocked):
            with locked(warehouse, tmp_path, clock, timeout=0):
                pass

    assert clock.sleeps == []


def test_duckdb_lock_is_released_when_the_block_raises(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb("stage.duckdb")

    with pytest.raises(RuntimeError):
        with locked(warehouse, tmp_path, FakeClock()):
            raise RuntimeError("the run failed")

    with locked(warehouse, tmp_path, FakeClock(), timeout=0):
        pass


def test_duckdb_lock_is_released_when_waiting_times_out(tmp_path: Path) -> None:
    warehouse = ResolvedDuckDb("stage.duckdb")

    with locked(warehouse, tmp_path, FakeClock()):
        with pytest.raises(WarehouseLocked):
            with locked(warehouse, tmp_path, FakeClock(), timeout=0):
                pass

    with locked(warehouse, tmp_path, FakeClock(), timeout=0):
        pass


def test_two_duckdb_warehouses_do_not_block_each_other(tmp_path: Path) -> None:
    with locked(ResolvedDuckDb("stage.duckdb"), tmp_path, FakeClock()):
        with locked(ResolvedDuckDb("prod.duckdb"), tmp_path, FakeClock(), timeout=0):
            pass


def test_an_unwritable_lock_folder_is_unreachable_and_names_no_path(tmp_path: Path) -> None:
    (tmp_path / ".adops").write_text("a file where the folder should be")
    warehouse = ResolvedDuckDb(".adops/warehouse_stage.duckdb")

    with pytest.raises(WarehouseUnreachable) as raised:
        with locked(warehouse, tmp_path, FakeClock()):
            pytest.fail("entered without a lock file")

    assert str(tmp_path) not in str(raised.value) + repr(raised.value)
    assert raised.value.__context__ is None


# Postgres: a session-level advisory lock on its own autocommit connection


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[psycopg.Connection]:
    """Every connection warehouse_lock opens, in order."""
    opened: list[psycopg.Connection] = []
    real_connect = psycopg.connect

    def spy(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(psycopg, "connect", spy)
    return opened


def test_postgres_lock_uses_its_own_autocommit_connection_for_the_whole_block(
    tmp_path: Path, postgres_warehouse, connections
) -> None:
    with locked(postgres_warehouse.connection, tmp_path, FakeClock()):
        (connection,) = connections
        assert connection.autocommit
        assert not connection.closed
        assert postgres_warehouse.rows(ADVISORY_LOCKS) == [(1,)]

    assert connection.closed
    assert postgres_warehouse.rows(ADVISORY_LOCKS) == [(0,)]


def test_postgres_second_holder_times_out_then_gets_the_lock_after_the_first_exits(
    tmp_path: Path, postgres_warehouse, connections
) -> None:
    warehouse = postgres_warehouse.connection
    first = ExitStack()
    first.enter_context(locked(warehouse, tmp_path, FakeClock()))

    with pytest.raises(WarehouseLocked) as raised:
        with locked(warehouse, tmp_path, FakeClock(), timeout=POLL):
            pytest.fail("entered a held lock")
    assert connections[-1].closed

    clock = FakeClock(on_sleep=first.close)
    with locked(warehouse, tmp_path, clock):
        pass

    assert clock.sleeps == [POLL]
    assert_no_connection_detail(raised.value, warehouse)


def test_postgres_lock_is_released_and_closed_when_the_block_raises(
    tmp_path: Path, postgres_warehouse, connections
) -> None:
    warehouse = postgres_warehouse.connection

    with pytest.raises(RuntimeError):
        with locked(warehouse, tmp_path, FakeClock()):
            raise RuntimeError("the run failed")

    assert connections[-1].closed
    with locked(warehouse, tmp_path, FakeClock(), timeout=0):
        pass


def test_two_postgres_databases_do_not_block_each_other(
    tmp_path: Path, postgres_warehouse, postgres_target
) -> None:
    with locked(postgres_warehouse.connection, tmp_path, FakeClock()):
        with locked(postgres_target.connection, tmp_path, FakeClock(), timeout=0):
            pass


def test_an_unreachable_postgres_is_reported_without_connection_detail(
    tmp_path: Path, postgres_warehouse
) -> None:
    warehouse = replace(postgres_warehouse.connection, password=SENTINEL_PASSWORD)

    with pytest.raises(WarehouseUnreachable) as raised:
        with locked(warehouse, tmp_path, FakeClock()):
            pytest.fail("entered without a connection")

    assert_no_connection_detail(raised.value, warehouse)


def test_losing_the_connection_while_waiting_is_unreachable_and_closes_it(
    tmp_path: Path, postgres_warehouse, connections
) -> None:
    warehouse = postgres_warehouse.connection
    with locked(warehouse, tmp_path, FakeClock()):
        clock = FakeClock(on_sleep=lambda: connections[-1].close())
        with pytest.raises(WarehouseUnreachable) as raised:
            with locked(warehouse, tmp_path, clock):
                pytest.fail("entered a held lock")

    assert clock.sleeps == [POLL]
    assert_no_connection_detail(raised.value, warehouse)


def test_losing_the_connection_inside_the_block_still_exits_cleanly(
    tmp_path: Path, postgres_warehouse, connections
) -> None:
    warehouse = postgres_warehouse.connection

    with locked(warehouse, tmp_path, FakeClock()):
        connections[-1].close()

    assert connections[-1].closed


def assert_no_connection_detail(error: Exception, warehouse: ResolvedConnection) -> None:
    text = str(error) + repr(error)
    for detail in (warehouse.password, warehouse.user, warehouse.host, warehouse.database):
        assert detail not in text
    assert error.__cause__ is None
    assert error.__context__ is None


def test_lock_key_fits_a_postgres_bigint() -> None:
    assert 0 < module.ADVISORY_LOCK_KEY < 2**63
