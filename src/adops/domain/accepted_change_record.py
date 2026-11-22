import re
from collections.abc import Collection, Iterable
from dataclasses import dataclass

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from adops.domain.errors import PipelineValidationError, RuleViolation
from adops.domain.loader import _parse, format_path
from adops.domain.schema_snapshot import BreakingChange, BreakingChangeKind, normalize_column_type

_DROPPED_SUFFIX = " dropped"
_RETYPED = re.compile(r"(?P<column>.+) retyped \((?P<previous>.+?) -> (?P<current>.+)\)")
_CHANGE_WORDING = "must read '<column> dropped' or '<column> retyped (<from> -> <to>)'"


@dataclass(frozen=True)
class AcceptedChangeRecord:
    id: str
    pipeline: str
    table: str
    changes: tuple[BreakingChange, ...]


@dataclass(frozen=True)
class Coverage:
    uncovered: tuple[BreakingChange, ...]
    used_ids: tuple[str, ...]


class _RecordDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    pipeline: str = Field(min_length=1)
    table: str = Field(min_length=1)
    changes: list[str] = Field(min_length=1)


def parse_change(text: str) -> BreakingChange:
    """Read one change written in BreakingChange.message wording."""
    if text.endswith(_DROPPED_SUFFIX) and len(text) > len(_DROPPED_SUFFIX):
        # A record does not name a dropped column's old type; matching ignores it.
        return BreakingChange(text[: -len(_DROPPED_SUFFIX)], BreakingChangeKind.DROPPED, "")
    found = _RETYPED.fullmatch(text)
    if found is not None:
        previous = normalize_column_type(found["previous"])
        current = normalize_column_type(found["current"])
        if previous and current:
            return BreakingChange(found["column"], BreakingChangeKind.RETYPED, previous, current)
    raise RuleViolation([((), _CHANGE_WORDING)])


def load_accepted_record(record_id: str, text: str) -> AcceptedChangeRecord:
    document = _parse(text)
    try:
        validated = _RecordDocument.model_validate(document)
    except ValidationError as error:
        problems = [
            f"{format_path(tuple(detail['loc']))}: {detail['msg']}"
            for detail in error.errors(include_input=False, include_url=False)
        ]
    else:
        return AcceptedChangeRecord(
            record_id, validated.pipeline, validated.table, _parse_changes(validated.changes)
        )
    raise PipelineValidationError(problems)


def dump_accepted_record(record: AcceptedChangeRecord) -> str:
    document = {
        "pipeline": record.pipeline,
        "table": record.table,
        "changes": [change.message for change in record.changes],
    }
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def cover(
    found: Iterable[BreakingChange],
    records: Iterable[AcceptedChangeRecord],
    applied_ids: Collection[str],
) -> Coverage:
    """Split found changes into those unapplied records accept and those still breaking."""
    unapplied = [record for record in records if record.id not in applied_ids]
    accepted_by = {
        record.id: {_match_key(change) for change in record.changes} for record in unapplied
    }
    uncovered = []
    used = set()
    for change in found:
        key = _match_key(change)
        covering = {record_id for record_id, keys in accepted_by.items() if key in keys}
        if not covering:
            uncovered.append(change)
        used |= covering
    return Coverage(
        tuple(uncovered), tuple(record_id for record_id in accepted_by if record_id in used)
    )


def _parse_changes(texts: list[str]) -> tuple[BreakingChange, ...]:
    changes = []
    problems = []
    for index, text in enumerate(texts):
        try:
            changes.append(parse_change(text))
        except RuleViolation:
            problems.append(f"{format_path(('changes', index))}: {_CHANGE_WORDING}")
    if problems:
        raise PipelineValidationError(problems)
    return tuple(changes)


def _match_key(change: BreakingChange) -> tuple[str, ...]:
    if change.kind is BreakingChangeKind.DROPPED:
        return (change.column, change.kind)
    return (
        change.column,
        change.kind,
        normalize_column_type(change.previous_type),
        normalize_column_type(change.current_type or ""),
    )
