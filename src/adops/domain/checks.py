from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from adops.domain.pipeline import SyncTable

CursorValue = int | float | Decimal | str | date | datetime


class CheckName(StrEnum):
    SOURCE_TABLES_EXIST = "source_tables_exist"
    PRIMARY_KEY_UNIQUE = "primary_key_unique"
    PRIMARY_KEY_NOT_NULL = "primary_key_not_null"
    ROW_COUNT_MATCHES = "row_count_matches"
    NO_ROW_LOSS = "no_row_loss"
    CURSOR_NOT_BACKWARDS = "cursor_not_backwards"


class SyncMode(StrEnum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


PREFLIGHT_CHECKS = (CheckName.SOURCE_TABLES_EXIST,)

CHECKS_BY_MODE: dict[SyncMode, tuple[CheckName, ...]] = {
    SyncMode.FULL_REFRESH: (
        CheckName.SOURCE_TABLES_EXIST,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.PRIMARY_KEY_NOT_NULL,
        CheckName.ROW_COUNT_MATCHES,
    ),
    SyncMode.INCREMENTAL: (
        CheckName.SOURCE_TABLES_EXIST,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.PRIMARY_KEY_NOT_NULL,
        CheckName.NO_ROW_LOSS,
        CheckName.CURSOR_NOT_BACKWARDS,
    ),
}


class CheckStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class CheckResult:
    check: CheckName
    table: str
    status: CheckStatus
    message: str


@dataclass(frozen=True)
class SourceFacts:
    """What the Source has for one table before anything is copied."""

    table_exists: bool
    columns: frozenset[str]


@dataclass(frozen=True)
class LoadFacts:
    """What one table looks like in the Warehouse before and after a load."""

    warehouse_rows_before: int
    warehouse_rows_after: int
    duplicate_keys: int
    null_keys: int
    source_rows: int | None = None
    cursor_max_before: CursorValue | None = None
    cursor_max_after: CursorValue | None = None


def evaluate_preflight_checks(table: SyncTable, source_facts: SourceFacts) -> list[CheckResult]:
    return _evaluate(table, PREFLIGHT_CHECKS, lambda check: _missing_in_source(table, source_facts))


def evaluate_load_checks(table: SyncTable, load_facts: LoadFacts) -> list[CheckResult]:
    load_checks = tuple(c for c in table.applicable_checks() if c not in PREFLIGHT_CHECKS)
    return _evaluate(table, load_checks, lambda check: _LOAD_RULES[check](load_facts))


def _evaluate(
    table: SyncTable,
    checks: tuple[CheckName, ...],
    failure_of: Callable[[CheckName], str | None],
) -> list[CheckResult]:
    results = []
    for check in checks:
        reason = table.skip_reason(check)
        if reason is not None:
            results.append(
                CheckResult(
                    check,
                    table.name,
                    CheckStatus.SKIPPED,
                    f"{table.name}: {check} skipped — {reason}",
                )
            )
            continue
        failure = failure_of(check)
        if failure is None:
            results.append(
                CheckResult(check, table.name, CheckStatus.PASS, f"{table.name}: {check} passed")
            )
        else:
            results.append(
                CheckResult(
                    check, table.name, CheckStatus.FAIL, f"{table.name}: {check} failed — {failure}"
                )
            )
    return results


def _missing_in_source(table: SyncTable, facts: SourceFacts) -> str | None:
    if not facts.table_exists:
        return "table not found in the Source"
    missing = [column for column in table.required_columns() if column not in facts.columns]
    if missing:
        return "missing columns in the Source: " + ", ".join(missing)
    return None


def _primary_key_unique(facts: LoadFacts) -> str | None:
    if facts.duplicate_keys:
        return f"{facts.duplicate_keys} duplicate primary key values in the Warehouse"
    return None


def _primary_key_not_null(facts: LoadFacts) -> str | None:
    if facts.null_keys:
        return f"{facts.null_keys} rows with an empty primary key in the Warehouse"
    return None


def _row_count_matches(facts: LoadFacts) -> str | None:
    if facts.source_rows is None:
        return "the Source row count was not measured"
    if facts.warehouse_rows_after != facts.source_rows:
        return f"Warehouse has {facts.warehouse_rows_after} rows, Source has {facts.source_rows}"
    return None


def _no_row_loss(facts: LoadFacts) -> str | None:
    if facts.warehouse_rows_after < facts.warehouse_rows_before:
        return (
            f"Warehouse went from {facts.warehouse_rows_before} to "
            f"{facts.warehouse_rows_after} rows"
        )
    return None


def _cursor_not_backwards(facts: LoadFacts) -> str | None:
    if facts.cursor_max_before is None:
        return None
    if facts.cursor_max_after is None:
        return "the cursor column has no values after the load"
    if facts.cursor_max_after < facts.cursor_max_before:
        return "the highest cursor value is lower than before the load"
    return None


_LOAD_RULES: dict[CheckName, Callable[[LoadFacts], str | None]] = {
    CheckName.PRIMARY_KEY_UNIQUE: _primary_key_unique,
    CheckName.PRIMARY_KEY_NOT_NULL: _primary_key_not_null,
    CheckName.ROW_COUNT_MATCHES: _row_count_matches,
    CheckName.NO_ROW_LOSS: _no_row_loss,
    CheckName.CURSOR_NOT_BACKWARDS: _cursor_not_backwards,
}
