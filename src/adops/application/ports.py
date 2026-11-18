from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from adops.domain.checks import CheckResult, LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep

MASK = "***"


@dataclass(frozen=True)
class ResolvedConnection:
    """A Postgres connection with its Secret Reference resolved; the password never prints."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)

    def __repr__(self) -> str:
        return (
            f"ResolvedConnection(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r}, password={MASK!r})"
        )


@dataclass(frozen=True)
class SyncResult:
    rows_loaded: Mapping[str, int]
    load_facts: Mapping[str, LoadFacts]


@runtime_checkable
class Sync(Protocol):
    """Copies a Step's allowlisted tables from a Source into the Warehouse and measures them."""

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        ...

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
    ) -> SyncResult:
        ...


class StepStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    CHECK_FAILED = "check_failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class StepReport:
    name: str
    status: StepStatus
    checks: tuple[CheckResult, ...] = ()
    rows_loaded: Mapping[str, int] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class StepOutcome:
    checks: tuple[CheckResult, ...]
    rows_loaded: Mapping[str, int]


class StepTask(Protocol):
    """One Step, ready to run: preflight, sync, then Checks. Raises StepError on failure."""

    @property
    def name(self) -> str: ...

    def __call__(self, sync: Sync) -> StepOutcome: ...


@dataclass(frozen=True)
class RunPlan:
    pipeline: str
    environment: str
    steps: tuple[StepTask, ...]


@dataclass(frozen=True)
class RunResult:
    pipeline: str
    environment: str
    steps: tuple[StepReport, ...]

    @property
    def succeeded(self) -> bool:
        return all(step.status is StepStatus.PASSED for step in self.steps)


@runtime_checkable
class Orchestrator(Protocol):
    def execute(self, plan: RunPlan, sync: Sync) -> RunResult: ...
