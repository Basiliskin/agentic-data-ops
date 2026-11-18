from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

DLT_COLUMN_PREFIX = "_dlt_"

_TYPE_ALIASES = {
    "character varying": "varchar",
    "char varying": "varchar",
    "character": "char",
    "bpchar": "char",
    "int": "integer",
    "int4": "integer",
    "int2": "smallint",
    "int8": "bigint",
    "serial4": "integer",
    "serial": "integer",
    "serial8": "bigint",
    "bigserial": "bigint",
    "float4": "real",
    "float8": "double precision",
    "float": "double precision",
    "decimal": "numeric",
    "bool": "boolean",
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "time with time zone": "timetz",
    "time without time zone": "time",
}

_PARAMETERS = re.compile(r"\(([^)]*)\)")


def normalize_column_type(raw: str) -> str:
    """One spelling per Postgres type, keeping parameters so a precision change still differs."""
    text = " ".join(raw.lower().split())
    array_depth = 0
    while text.endswith("[]"):
        text = text[:-2].rstrip()
        array_depth += 1
    parameters = ""
    found = _PARAMETERS.search(text)
    if found is not None:
        values = [value.strip() for value in found.group(1).split(",")]
        parameters = "(" + ", ".join(values) + ")"
        text = " ".join((text[: found.start()] + " " + text[found.end() :]).split())
    return _TYPE_ALIASES.get(text, text) + parameters + "[]" * array_depth


@dataclass(frozen=True)
class TableShape:
    """A table's columns, each mapped to its normalized type."""

    columns: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized = {name: normalize_column_type(kind) for name, kind in self.columns.items()}
        object.__setattr__(self, "columns", MappingProxyType(normalized))


class BreakingChangeKind(StrEnum):
    DROPPED = "dropped"
    RETYPED = "retyped"


@dataclass(frozen=True)
class BreakingChange:
    column: str
    kind: BreakingChangeKind
    previous_type: str
    current_type: str | None = None

    @property
    def message(self) -> str:
        if self.kind is BreakingChangeKind.DROPPED:
            return f"{self.column} dropped"
        return f"{self.column} retyped ({self.previous_type} -> {self.current_type})"


def find_breaking_changes(previous: TableShape, current: TableShape) -> tuple[BreakingChange, ...]:
    """Dropped and retyped columns; a rename is its old name dropped; added columns are fine."""
    changes = []
    for column, previous_type in previous.columns.items():
        if column.startswith(DLT_COLUMN_PREFIX):
            continue
        current_type = current.columns.get(column)
        if current_type is None:
            changes.append(BreakingChange(column, BreakingChangeKind.DROPPED, previous_type))
        elif current_type != previous_type:
            changes.append(
                BreakingChange(column, BreakingChangeKind.RETYPED, previous_type, current_type)
            )
    return tuple(sorted(changes, key=lambda change: (change.column, change.kind)))
