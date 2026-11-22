import os
from pathlib import Path

import duckdb
import psycopg

from adops.application.ports import ResolvedConnection
from adops.infrastructure.dlt_sync import duckdb_file


class DriverConnectionProbe:
    """Opens and closes a connection to answer whether it works; never writes or creates."""

    def __init__(self, project_dir: Path) -> None:
        self._project_dir = project_dir

    def postgres(self, connection: ResolvedConnection) -> bool:
        try:
            psycopg.connect(
                host=connection.host,
                port=connection.port,
                dbname=connection.database,
                user=connection.user,
                password=connection.password,
                connect_timeout=10,
            ).close()
        except psycopg.Error:
            return False
        return True

    def duckdb(self, path: str) -> bool:
        file = duckdb_file(self._project_dir, path)
        if not file.exists():
            # A first run creates the file, so a writable folder is all it needs.
            return file.parent.is_dir() and os.access(file.parent, os.W_OK)
        try:
            duckdb.connect(str(file), read_only=True).close()
        except duckdb.Error:
            return False
        return True
