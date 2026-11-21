from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from adops.application.ports import ResolvedWarehouse
from adops.interface import run_command


@dataclass
class FakeLocks:
    """Stands in for build_warehouse_lock; `events` interleaves lock entry/exit with test marks."""

    error: Exception | None = None
    events: list[tuple[str, ...]] = field(default_factory=list)
    warehouses: list[ResolvedWarehouse] = field(default_factory=list)

    @contextmanager
    def build(
        self, warehouse: ResolvedWarehouse, project: Path, environment: str
    ) -> Iterator[None]:
        if self.error is not None:
            raise self.error
        self.warehouses.append(warehouse)
        self.events.append(("enter", environment))
        try:
            yield
        finally:
            self.events.append(("exit", environment))

    def mark(self, event: str) -> None:
        self.events.append((event,))


@pytest.fixture
def locks(monkeypatch: pytest.MonkeyPatch) -> FakeLocks:
    fake = FakeLocks()
    monkeypatch.setattr(run_command, "build_warehouse_lock", fake.build)
    return fake
