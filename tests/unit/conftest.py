import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

import pytest

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
