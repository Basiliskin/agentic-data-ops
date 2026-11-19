from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from adops.domain.errors import RuleViolation

if TYPE_CHECKING:
    from adops.domain.pipeline import SyncTable
    from adops.domain.reverse_sync import ReverseSyncStep

CursorValue = int | float | Decimal | str | date | datetime


class CheckName(StrEnum):
    SOURCE_TABLES_EXIST = "source_tables_exist"
    SOURCE_NOT_EMPTY = "source_not_empty"
    PRIMARY_KEY_UNIQUE = "primary_key_unique"
    PRIMARY_KEY_NOT_NULL = "primary_key_not_null"
    ROW_COUNT_MATCHES = "row_count_matches"
    NO_ROW_LOSS = "no_row_loss"
    CURSOR_NOT_BACKWARDS = "cursor_not_backwards"
    TARGET_COLUMNS_EXIST = "target_columns_exist"
    KEY_UNIQUE = "key_unique"
    KEY_NOT_NULL = "key_not_null"
    MAX_CHANGE = "max_change"
    RESULT_NOT_EMPTY = "result_not_empty"


class SyncMode(StrEnum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


PREFLIGHT_CHECKS = (CheckName.SOURCE_TABLES_EXIST, CheckName.SOURCE_NOT_EMPTY)

CHECKS_BY_MODE: dict[SyncMode, tuple[CheckName, ...]] = {
    SyncMode.FULL_REFRESH: (
        CheckName.SOURCE_TABLES_EXIST,
        CheckName.SOURCE_NOT_EMPTY,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.PRIMARY_KEY_NOT_NULL,
        CheckName.ROW_COUNT_MATCHES,
    ),
    SyncMode.INCREMENTAL: (
        CheckName.SOURCE_TABLES_EXIST,
        CheckName.SOURCE_NOT_EMPTY,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.PRIMARY_KEY_NOT_NULL,
        CheckName.NO_ROW_LOSS,
        CheckName.CURSOR_NOT_BACKWARDS,
    ),
}


REVERSE_SYNC_CHECKS = (
    CheckName.TARGET_COLUMNS_EXIST,
    CheckName.KEY_UNIQUE,
    CheckName.KEY_NOT_NULL,
    CheckName.MAX_CHANGE,
    CheckName.RESULT_NOT_EMPTY,
)


class SkipChecks(BaseModel):
    """An opt-out from Checks, allowed only with a written reason."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checks: list[str] = Field(min_length=1)
    reason: str

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, reason: str) -> str:
        if not reason.strip():
            raise RuleViolation([((), "must explain why these Checks are skipped")])
        return reason


class CheckStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    status: CheckStatus
    message: str


@dataclass(frozen=True)
class SourceFacts:
    """What the Source has for one table before anything is copied."""

    table_exists: bool
    columns: frozenset[str]
    column_types: Mapping[str, str] = field(default_factory=dict)
    row_count: int | None = None


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
    null_counts: Mapping[str, int] = field(default_factory=dict)
    unexpected_value_counts: Mapping[str, int] = field(default_factory=dict)
    newest_values: Mapping[str, datetime | None] = field(default_factory=dict)


@dataclass(frozen=True)
class ReverseSyncFacts:
    """What the dbt result, the Reverse Sync target and their Diff look like before a write.

    `diff_deletes` counts target rows absent from the dbt result, whether or not they are deleted.
    """

    target_table_exists: bool
    missing_target_columns: tuple[str, ...]
    result_rows: int
    duplicate_keys: int
    null_keys: int
    target_rows: int
    diff_updates: int
    diff_deletes: int


def evaluate_preflight_checks(table: SyncTable, source_facts: SourceFacts) -> list[CheckResult]:
    return _evaluate(
        table.name,
        table.skip_reason,
        PREFLIGHT_CHECKS,
        lambda check: _PREFLIGHT_RULES[check](table, source_facts),
    )


def evaluate_load_checks(table: SyncTable, load_facts: LoadFacts) -> list[CheckResult]:
    load_checks = tuple(c for c in table.applicable_checks() if c not in PREFLIGHT_CHECKS)
    return _evaluate(
        table.name, table.skip_reason, load_checks, lambda check: _LOAD_RULES[check](load_facts)
    )


def evaluate_declared_checks(
    table: SyncTable, load_facts: LoadFacts, now: datetime
) -> list[CheckResult]:
    declared = {check.name: check for check in table.checks}
    return _evaluate(
        table.name,
        table.skip_reason,
        tuple(declared),
        lambda name: _DECLARED_RULES[declared[name].type](declared[name], load_facts, now),
    )


def evaluate_reverse_sync(
    step: ReverseSyncStep, facts: ReverseSyncFacts
) -> tuple[CheckResult, ...]:
    def skip_reason(check: str) -> str | None:
        if step.skip_checks is not None and check in step.skip_checks.checks:
            return step.skip_checks.reason
        return None

    return tuple(
        _evaluate(
            step.target_table,
            skip_reason,
            REVERSE_SYNC_CHECKS,
            lambda check: _REVERSE_SYNC_RULES[check](step, facts),
        )
    )


def _evaluate(
    subject: str,
    skip_reason: Callable[[str], str | None],
    checks: Sequence[str],
    failure_of: Callable[[str], str | None],
) -> list[CheckResult]:
    results = []
    for check in checks:
        reason = skip_reason(check)
        if reason is not None:
            results.append(
                CheckResult(
                    check, subject, CheckStatus.SKIPPED, f"{subject}: {check} skipped — {reason}"
                )
            )
            continue
        failure = failure_of(check)
        if failure is None:
            results.append(
                CheckResult(check, subject, CheckStatus.PASS, f"{subject}: {check} passed")
            )
        else:
            results.append(
                CheckResult(
                    check, subject, CheckStatus.FAIL, f"{subject}: {check} failed — {failure}"
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


def _empty_in_source(table: SyncTable, facts: SourceFacts) -> str | None:
    if facts.row_count == 0:
        return f"table {table.name} is empty in the Source"
    return None


_PREFLIGHT_RULES: dict[CheckName, Callable[[SyncTable, SourceFacts], str | None]] = {
    CheckName.SOURCE_TABLES_EXIST: _missing_in_source,
    CheckName.SOURCE_NOT_EMPTY: _empty_in_source,
}


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


def _not_null(facts: LoadFacts, column: str) -> str | None:
    if column not in facts.null_counts:
        return "the null count was not measured"
    nulls = facts.null_counts[column]
    if nulls:
        return f"{nulls} rows with no value"
    return None


def _accepted_values(facts: LoadFacts, column: str) -> str | None:
    if column not in facts.unexpected_value_counts:
        return "the count of values outside the accepted list was not measured"
    unexpected = facts.unexpected_value_counts[column]
    if unexpected:
        return f"{unexpected} rows with a value outside the accepted list"
    return None


def _min_rows(facts: LoadFacts, count: int) -> str | None:
    if facts.warehouse_rows_after < count:
        return f"Warehouse has {facts.warehouse_rows_after} rows, at least {count} required"
    return None


def _freshness(facts: LoadFacts, column: str, max_age_hours: int, now: datetime) -> str | None:
    if column not in facts.newest_values:
        return "the newest value was not measured"
    newest = facts.newest_values[column]
    if newest is None:
        return "the column has no values"
    age = _as_utc(now) - _as_utc(newest)
    if age > timedelta(hours=max_age_hours):
        hours = age / timedelta(hours=1)
        return f"the newest value is {hours:.1f} hours old, more than {max_age_hours} hours"
    return None


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


_DECLARED_RULES: dict[str, Callable[[Any, LoadFacts, datetime], str | None]] = {
    "not_null": lambda check, facts, _now: _not_null(facts, check.column),
    "accepted_values": lambda check, facts, _now: _accepted_values(facts, check.column),
    "min_rows": lambda check, facts, _now: _min_rows(facts, check.count),
    "freshness": lambda check, facts, now: _freshness(
        facts, check.column, check.max_age_hours, now
    ),
}


def _target_columns_exist(step: ReverseSyncStep, facts: ReverseSyncFacts) -> str | None:
    if not facts.target_table_exists:
        return f"table {step.target_table} not found in the target {step.target}"
    if facts.missing_target_columns:
        return "missing columns in the target: " + ", ".join(facts.missing_target_columns)
    return None


def _key_unique(step: ReverseSyncStep, facts: ReverseSyncFacts) -> str | None:
    if facts.duplicate_keys:
        return f"{facts.duplicate_keys} duplicate {step.key} values in the dbt result"
    return None


def _key_not_null(step: ReverseSyncStep, facts: ReverseSyncFacts) -> str | None:
    if facts.null_keys:
        return f"{facts.null_keys} rows with an empty {step.key} in the dbt result"
    return None


def _max_change(step: ReverseSyncStep, facts: ReverseSyncFacts) -> str | None:
    if facts.target_rows == 0:
        return None
    changes = facts.diff_updates + (facts.diff_deletes if step.delete_missing else 0)
    if changes / facts.target_rows > step.max_change_share:
        return (
            f"{changes} of {facts.target_rows} target rows would change, "
            f"more than the allowed share {step.max_change_share}"
        )
    return None


def _result_not_empty(step: ReverseSyncStep, facts: ReverseSyncFacts) -> str | None:
    if facts.result_rows == 0:
        return f"the dbt model {step.model} has no rows"
    return None


_REVERSE_SYNC_RULES: dict[str, Callable[[ReverseSyncStep, ReverseSyncFacts], str | None]] = {
    CheckName.TARGET_COLUMNS_EXIST: _target_columns_exist,
    CheckName.KEY_UNIQUE: _key_unique,
    CheckName.KEY_NOT_NULL: _key_not_null,
    CheckName.MAX_CHANGE: _max_change,
    CheckName.RESULT_NOT_EMPTY: _result_not_empty,
}
