from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

Row = Mapping[str, object]

WRITING_ENVIRONMENTS = frozenset({"stage", "prod"})
PREVIEW_SAMPLE_SIZE = 5


@dataclass(frozen=True)
class RowUpdate:
    key: object
    changed_columns: tuple[str, ...]
    values: Row


@dataclass(frozen=True)
class Diff:
    """The changes that make a Reverse Sync target match the dbt result, in target column names.

    `deletes` holds target keys absent from the dbt result when `delete_missing` is set;
    without it those keys land in `kept` and the target rows stay.
    """

    key: str
    inserts: tuple[Row, ...]
    updates: tuple[RowUpdate, ...]
    deletes: tuple[object, ...]
    kept: tuple[object, ...]
    delete_missing: bool


@dataclass(frozen=True)
class PreviewSample:
    inserts: tuple[object, ...]
    updates: tuple[tuple[object, tuple[str, ...]], ...]
    deletes: tuple[object, ...]
    kept: tuple[object, ...]


def writes_allowed(environment_name: str) -> bool:
    return environment_name in WRITING_ENVIRONMENTS


def normalize_value(value: object) -> object:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | Decimal):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return value


def compute_diff(
    result_rows: Sequence[Row],
    target_rows: Sequence[Row],
    key: str,
    mapping: Mapping[str, str],
    delete_missing: bool,
) -> Diff:
    target_key = mapping.get(key, key)
    target_by_key = {normalize_value(row[target_key]): row for row in target_rows}
    inserts: list[Row] = []
    updates: list[RowUpdate] = []
    seen: set[object] = set()
    for result_row in result_rows:
        row = _renamed(result_row, key, mapping)
        normalized_key = normalize_value(row[target_key])
        seen.add(normalized_key)
        current = target_by_key.get(normalized_key)
        if current is None:
            inserts.append(row)
            continue
        changed = tuple(
            column
            for column, value in row.items()
            if column != target_key and not _same(value, current.get(column))
        )
        if changed:
            updates.append(
                RowUpdate(row[target_key], changed, {column: row[column] for column in changed})
            )
    absent = tuple(row[target_key] for norm, row in target_by_key.items() if norm not in seen)
    return Diff(
        key=target_key,
        inserts=tuple(inserts),
        updates=tuple(updates),
        deletes=absent if delete_missing else (),
        kept=() if delete_missing else absent,
        delete_missing=delete_missing,
    )


def preview_sample(diff: Diff, limit: int = PREVIEW_SAMPLE_SIZE) -> PreviewSample:
    return PreviewSample(
        inserts=tuple(row[diff.key] for row in diff.inserts[:limit]),
        updates=tuple((update.key, update.changed_columns) for update in diff.updates[:limit]),
        deletes=diff.deletes[:limit],
        kept=diff.kept[:limit],
    )


def _renamed(result_row: Row, key: str, mapping: Mapping[str, str]) -> dict[str, object]:
    row = {mapping.get(key, key): result_row[key]}
    row.update({target: result_row[source] for source, target in mapping.items()})
    return row


def _same(left: object, right: object) -> bool:
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return normalize_value(left) == normalize_value(right)
