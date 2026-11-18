import pytest

from adops.domain.schema_snapshot import (
    BreakingChange,
    BreakingChangeKind,
    TableShape,
    find_breaking_changes,
    normalize_column_type,
)

DROPPED = BreakingChangeKind.DROPPED
RETYPED = BreakingChangeKind.RETYPED


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("character varying", "varchar"),
        ("CHARACTER VARYING(255)", "varchar(255)"),
        ("int4", "integer"),
        ("int8", "bigint"),
        ("timestamp with time zone", "timestamptz"),
        ("timestamp(6) with time zone", "timestamptz(6)"),
        ("timestamp without time zone", "timestamp"),
        ("bool", "boolean"),
        ("float8", "double precision"),
        ("decimal(10,2)", "numeric(10, 2)"),
        ("int4[]", "integer[]"),
        ("  Text  ", "text"),
    ],
)
def test_aliases_normalize_to_one_spelling(alias, canonical):
    assert normalize_column_type(alias) == canonical


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        ("character varying", "varchar"),
        ("int4", "integer"),
        ("int8", "bigint"),
        ("timestamp with time zone", "timestamptz"),
        ("bool", "boolean"),
        ("NUMERIC(10,2)", "numeric(10, 2)"),
        ("numeric( 10 , 2 )", "numeric(10,2)"),
    ],
)
def test_alias_spellings_are_not_a_breaking_change(previous, current):
    assert find_breaking_changes(TableShape({"c": previous}), TableShape({"c": current})) == ()


def test_no_change_gives_nothing():
    shape = TableShape({"id": "integer", "email": "text"})

    assert find_breaking_changes(shape, shape) == ()


def test_added_column_is_not_a_breaking_change():
    previous = TableShape({"id": "integer"})
    current = TableShape({"id": "integer", "email": "text"})

    assert find_breaking_changes(previous, current) == ()


def test_dropped_column_is_reported():
    previous = TableShape({"id": "integer", "email": "text"})

    assert find_breaking_changes(previous, TableShape({"id": "integer"})) == (
        BreakingChange("email", DROPPED, "text"),
    )


def test_rename_is_exactly_one_drop_of_the_old_name():
    previous = TableShape({"id": "integer", "email": "text"})
    current = TableShape({"id": "integer", "email_address": "text"})

    assert find_breaking_changes(previous, current) == (BreakingChange("email", DROPPED, "text"),)


def test_retyped_column_is_reported_with_both_normalized_types():
    previous = TableShape({"id": "int4"})
    current = TableShape({"id": "int8"})

    assert find_breaking_changes(previous, current) == (
        BreakingChange("id", RETYPED, "integer", "bigint"),
    )


def test_precision_change_is_a_retype():
    previous = TableShape({"total": "numeric(10, 2)"})
    current = TableShape({"total": "numeric(12, 2)"})

    (change,) = find_breaking_changes(previous, current)

    assert change.kind is RETYPED
    assert change.message == "total retyped (numeric(10, 2) -> numeric(12, 2))"


def test_dlt_columns_are_ignored():
    previous = TableShape({"id": "integer", "_dlt_load_id": "text", "_dlt_id": "text"})
    current = TableShape({"id": "integer"})

    assert find_breaking_changes(previous, current) == ()


def test_several_changes_come_back_in_one_sorted_order():
    previous = TableShape({"zip": "text", "amount": "integer", "city": "text", "name": "text"})
    current = TableShape({"name": "varchar", "amount": "bigint"})

    assert find_breaking_changes(previous, current) == (
        BreakingChange("amount", RETYPED, "integer", "bigint"),
        BreakingChange("city", DROPPED, "text"),
        BreakingChange("name", RETYPED, "text", "varchar"),
        BreakingChange("zip", DROPPED, "text"),
    )


def test_messages_name_column_and_types_only():
    dropped = BreakingChange("email", DROPPED, "text")
    retyped = BreakingChange("id", RETYPED, "integer", "bigint")

    assert (dropped.message, retyped.message) == (
        "email dropped",
        "id retyped (integer -> bigint)",
    )


def test_table_shape_is_immutable_and_compares_by_columns():
    shape = TableShape({"id": "INT4"})

    assert shape == TableShape({"id": "integer"})
    assert dict(shape.columns) == {"id": "integer"}
    assert TableShape() == TableShape({})
    with pytest.raises(TypeError):
        shape.columns["id"] = "text"  # type: ignore[index]
    with pytest.raises(AttributeError):
        shape.columns = {}  # type: ignore[misc]
