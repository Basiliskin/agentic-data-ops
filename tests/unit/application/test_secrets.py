import pytest

from adops.application.secrets import MissingSecretError, resolve_secret_references
from adops.domain.secret_reference import SecretReference


def refs(*names: str) -> list[SecretReference]:
    return [SecretReference(name=name) for name in names]


def test_returns_value_of_every_reference():
    env = {"A_PASSWORD": "a-value", "B_PASSWORD": "b-value", "UNRELATED": "x"}

    assert resolve_secret_references(env, refs("A_PASSWORD", "B_PASSWORD")) == {
        "A_PASSWORD": "a-value",
        "B_PASSWORD": "b-value",
    }


def test_reports_every_missing_name_sorted_in_one_error():
    with pytest.raises(MissingSecretError) as raised:
        resolve_secret_references({}, refs("Z_PASSWORD", "A_PASSWORD", "Z_PASSWORD"))

    assert raised.value.names == ("A_PASSWORD", "Z_PASSWORD")
    assert str(raised.value) == (
        "these environment variables are not set or are empty: A_PASSWORD, Z_PASSWORD"
    )


def test_error_names_missing_variable_but_never_shows_a_set_value():
    env = {"SET_PASSWORD": "visible-sentinel"}

    with pytest.raises(MissingSecretError) as raised:
        resolve_secret_references(env, refs("SET_PASSWORD", "UNSET_PASSWORD"))

    assert raised.value.names == ("UNSET_PASSWORD",)
    assert "visible-sentinel" not in str(raised.value)
    assert "visible-sentinel" not in repr(raised.value)
    assert raised.value.__cause__ is None and raised.value.__context__ is None


def test_variable_set_to_empty_string_counts_as_missing():
    with pytest.raises(MissingSecretError) as raised:
        resolve_secret_references({"EMPTY_PASSWORD": ""}, refs("EMPTY_PASSWORD"))

    assert raised.value.names == ("EMPTY_PASSWORD",)


def test_no_references_resolves_to_nothing():
    assert resolve_secret_references({"A": "1"}, []) == {}
