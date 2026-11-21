import pytest

from adops.application.secrets import (
    MissingSecretError,
    redact_secret_values,
    resolve_secret_references,
    secret_reference_status,
    secret_references,
)
from adops.domain.environment import DuckDbConnection, PostgresConnection
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


def postgres(password_env: str) -> PostgresConnection:
    return PostgresConnection.model_validate(
        {
            "type": "postgres",
            "host": "h",
            "port": 5432,
            "database": "d",
            "user": "u",
            "password_env": password_env,
        }
    )


def test_target_passwords_are_secret_references_beside_the_warehouse_and_sources():
    found = secret_references(
        postgres("WH_PASSWORD"), [postgres("SRC_PASSWORD")], [postgres("TGT_PASSWORD")]
    )

    assert [ref.name for ref in found] == ["WH_PASSWORD", "SRC_PASSWORD", "TGT_PASSWORD"]


def test_a_duckdb_warehouse_needs_only_the_target_password():
    duckdb = DuckDbConnection(type="duckdb", path="w.duckdb")

    found = secret_references(duckdb, [], [postgres("TGT_PASSWORD")])

    assert [ref.name for ref in found] == ["TGT_PASSWORD"]


def test_a_missing_target_password_is_named_and_its_value_never_shown():
    refs_needed = secret_references(postgres("WH_PASSWORD"), [], [postgres("TGT_PASSWORD")])

    with pytest.raises(MissingSecretError) as raised:
        resolve_secret_references({"WH_PASSWORD": "wh-secret-value"}, refs_needed)

    assert raised.value.names == ("TGT_PASSWORD",)
    assert "wh-secret-value" not in str(raised.value)


def test_status_reports_set_for_a_reference_with_a_value():
    env = {"A_PASSWORD": "a-value", "UNRELATED": "x"}

    assert secret_reference_status(env, refs("A_PASSWORD")) == {"A_PASSWORD": "set"}


def test_status_reports_missing_for_an_unset_or_empty_reference():
    env = {"EMPTY_PASSWORD": ""}

    assert secret_reference_status(env, refs("UNSET_PASSWORD", "EMPTY_PASSWORD")) == {
        "UNSET_PASSWORD": "missing",
        "EMPTY_PASSWORD": "missing",
    }


def test_status_agrees_with_resolution_on_an_empty_value():
    env = {"EMPTY_PASSWORD": ""}

    with pytest.raises(MissingSecretError):
        resolve_secret_references(env, refs("EMPTY_PASSWORD"))
    assert secret_reference_status(env, refs("EMPTY_PASSWORD")) == {"EMPTY_PASSWORD": "missing"}


def test_status_lists_a_name_referenced_twice_once():
    env = {"SHARED_PASSWORD": "shared-value"}

    status = secret_reference_status(env, refs("SHARED_PASSWORD", "SHARED_PASSWORD"))

    assert status == {"SHARED_PASSWORD": "set"}


def test_status_never_carries_a_value():
    env = {"A_PASSWORD": "a-distinctive-value"}

    status = secret_reference_status(env, refs("A_PASSWORD", "B_PASSWORD"))

    assert set(status.values()) <= {"set", "missing"}
    assert "a-distinctive-value" not in repr(status)


def test_redaction_replaces_one_value():
    assert redact_secret_values("password: hunter2\n", ["hunter2"]) == "password: ***\n"


def test_redaction_replaces_two_values():
    assert redact_secret_values("a=one b=two", ["one", "two"]) == "a=*** b=***"


def test_redaction_of_overlapping_values_leaves_neither():
    text = "long=prefix-secret-suffix short=secret"

    redacted = redact_secret_values(text, ["secret", "prefix-secret-suffix"])

    assert redacted == "long=*** short=***"
    assert "secret" not in redacted


def test_redaction_replaces_a_repeated_value_everywhere():
    assert redact_secret_values("x x x", ["x"]) == "*** *** ***"


def test_redaction_skips_an_empty_value():
    assert redact_secret_values("unchanged text", ["", ""]) == "unchanged text"


def test_redaction_without_values_leaves_the_text():
    assert redact_secret_values("unchanged text", []) == "unchanged text"
