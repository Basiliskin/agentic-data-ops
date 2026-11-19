from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from adops.domain.checks import CheckResult, LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep, SyncTable
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.reverse_sync_diff import Diff, PreviewSample, Row
from adops.domain.schema_snapshot import TableShape
from adops.domain.transform import DbtTestFacts, TransformStep

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
class ResolvedDuckDb:
    """A DuckDB Warehouse file, with the path exactly as the Project settings wrote it."""

    path: str


type ResolvedWarehouse = ResolvedConnection | ResolvedDuckDb


class SourceUnreachable(Exception):
    """A Sync could not connect to the Source; it names the table and holds no driver text."""

    def __init__(self, table: str) -> None:
        super().__init__(table)
        self.table = table


class WarehouseUnreachable(Exception):
    """An adapter could not connect to the Warehouse; it holds no driver text."""


class TargetUnreachable(Exception):
    """A Reverse Sync could not connect to its Target; it holds no driver text."""


class WriteFailed(Exception):
    """A Reverse Sync write was rolled back and left the Target unchanged; no driver text."""


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
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedWarehouse
    ) -> SyncResult:
        ...

    def make_columns_nullable(
        self,
        step: SyncStep,
        table: SyncTable,
        warehouse: ResolvedWarehouse,
        columns: Collection[str],
    ) -> None:
        """Let loaded columns that the Source no longer has hold NULL in the Warehouse."""
        ...


@dataclass(frozen=True)
class TransformResult:
    """What one dbt build measured; it carries model and test names only, never dbt's text."""

    tests: tuple[DbtTestFacts, ...]
    crashed: bool
    failed_models: tuple[str, ...]


@runtime_checkable
class Transform(Protocol):
    """Runs a Transform Step's dbt models and tests in the Warehouse and reports what happened."""

    def run(self, step: TransformStep, warehouse: ResolvedWarehouse) -> TransformResult: ...


@dataclass(frozen=True)
class ReverseSyncRead:
    """The dbt result rows (dbt column names), the Target's rows (Target column names), measured.

    `duplicate_keys` and `null_keys` count rows of the dbt result.
    """

    result_rows: tuple[Row, ...]
    target_rows: tuple[Row, ...]
    target_table_exists: bool
    missing_target_columns: tuple[str, ...]
    duplicate_keys: int
    null_keys: int


@runtime_checkable
class ReverseSync(Protocol):
    """Reads a dbt result from the Warehouse and applies a computed Diff to a Target table."""

    def read(
        self, step: ReverseSyncStep, warehouse: ResolvedWarehouse, target: ResolvedConnection
    ) -> ReverseSyncRead: ...

    def apply(self, step: ReverseSyncStep, target: ResolvedConnection, diff: Diff) -> None:
        """Apply exactly `diff` in one transaction."""
        ...


@runtime_checkable
class SchemaSnapshotStore(Protocol):
    """Keeps each Source table's last accepted shape in the Warehouse; it never judges a change."""

    def load(
        self, warehouse: ResolvedWarehouse, pipeline: str, source: str, table: str
    ) -> TableShape | None: ...

    def save(
        self,
        warehouse: ResolvedWarehouse,
        pipeline: str,
        source: str,
        table: str,
        shape: TableShape,
    ) -> None: ...


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """The current moment, timezone-aware."""
        ...


class StepStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    CHECK_FAILED = "check_failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ReverseSyncPreview:
    """How many Target rows a Reverse Sync changes, with a key-only sample; never row values."""

    inserts: int
    updates: int
    deletes: int
    kept: int
    sample: PreviewSample
    written: bool


@dataclass(frozen=True)
class StepReport:
    name: str
    status: StepStatus
    checks: tuple[CheckResult, ...] = ()
    rows_loaded: Mapping[str, int] = field(default_factory=dict)
    error: str | None = None
    preview: ReverseSyncPreview | None = None


@dataclass(frozen=True)
class StepOutcome:
    checks: tuple[CheckResult, ...]
    rows_loaded: Mapping[str, int]
    preview: ReverseSyncPreview | None = None


class StepTask(Protocol):
    """One Step, ready to run, with everything it needs bound in. Raises StepError on failure."""

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
