import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


@pytest.fixture(autouse=True)
def _plain_cli_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keeps Rich from styling CLI output as it does on CI, so tests see plain text."""
    for name in ("GITHUB_ACTIONS", "FORCE_COLOR", "TTY_COMPATIBLE", "TTY_INTERACTIVE"):
        monkeypatch.delenv(name, raising=False)


@dataclass
class Webhook:
    """A local HTTP endpoint that records every POST and answers with `status` after `delay`."""

    port: int
    status: int = 200
    delay: float = 0.0
    requests: list[tuple[str, str, bytes]] = field(default_factory=list)

    def url(self, path: str = "/hook") -> str:
        return f"http://127.0.0.1:{self.port}{path}"


@pytest.fixture
def webhook() -> Iterator[Webhook]:
    hook = Webhook(port=0)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            hook.requests.append((self.path, self.headers["Content-Type"], body))
            time.sleep(hook.delay)
            self.send_response(hook.status)
            self.end_headers()

        def log_message(self, *_: object) -> None:
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    hook.port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield hook
    finally:
        httpd.shutdown()
        httpd.server_close()
