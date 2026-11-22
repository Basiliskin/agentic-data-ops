import socket
from dataclasses import replace
from pathlib import Path

import duckdb

from adops.application.connection_check import ConnectionProbe
from adops.application.ports import ResolvedConnection
from adops.infrastructure.connection_probe import DriverConnectionProbe


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def no_side_files(path: Path) -> bool:
    return not path.exists() and not Path(f"{path}.lock").exists()


def test_the_driver_probe_satisfies_the_application_port(tmp_path):
    probe: ConnectionProbe = DriverConnectionProbe(tmp_path)

    assert isinstance(probe, DriverConnectionProbe)


def test_a_reachable_postgres_connects_without_creating_anything(postgres_warehouse):
    schemas = postgres_warehouse.schemas()

    assert DriverConnectionProbe(Path()).postgres(postgres_warehouse.connection) is True
    assert postgres_warehouse.schemas() == schemas


def test_a_wrong_password_is_unreachable(postgres_warehouse):
    connection = replace(postgres_warehouse.connection, password="not-the-password")

    assert DriverConnectionProbe(Path()).postgres(connection) is False


def test_a_closed_port_is_unreachable():
    connection = ResolvedConnection("127.0.0.1", closed_port(), "db", "adops", "secret")

    assert DriverConnectionProbe(Path()).postgres(connection) is False


def test_an_existing_duckdb_file_is_reachable(tmp_path):
    duckdb.connect(str(tmp_path / "warehouse.duckdb")).close()

    assert DriverConnectionProbe(tmp_path).duckdb("warehouse.duckdb") is True
    assert not (tmp_path / "warehouse.duckdb.lock").exists()


def test_an_absent_duckdb_file_in_a_writable_folder_is_reachable_and_not_created(tmp_path):
    assert DriverConnectionProbe(tmp_path).duckdb("warehouse.duckdb") is True
    assert no_side_files(tmp_path / "warehouse.duckdb")


def test_a_missing_folder_is_unreachable_and_not_created(tmp_path):
    assert DriverConnectionProbe(tmp_path).duckdb("data/warehouse.duckdb") is False
    assert not (tmp_path / "data").exists()


def test_an_absent_file_in_a_read_only_folder_is_unreachable(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    folder.chmod(0o500)
    try:
        assert DriverConnectionProbe(tmp_path).duckdb("data/warehouse.duckdb") is False
    finally:
        folder.chmod(0o700)
    assert no_side_files(folder / "warehouse.duckdb")


def test_a_file_held_by_a_running_writer_is_unreachable(tmp_path, duckdb_held_by_another_process):
    path = tmp_path / "warehouse.duckdb"
    with duckdb_held_by_another_process(path):
        assert DriverConnectionProbe(tmp_path).duckdb("warehouse.duckdb") is False
    assert not Path(f"{path}.lock").exists()


def test_a_file_that_is_not_a_duckdb_database_is_unreachable(tmp_path):
    (tmp_path / "warehouse.duckdb").write_text("not a database")

    assert DriverConnectionProbe(tmp_path).duckdb("warehouse.duckdb") is False
