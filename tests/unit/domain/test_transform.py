import dataclasses

import pytest

from adops.domain.checks import CheckResult, CheckStatus
from adops.domain.transform import DbtTestFacts, DbtTestStatus, evaluate_dbt_tests


def fact(
    status: str,
    failures: int | None = 0,
    *,
    name: str = "unique_customers_id",
    table: str = "customers",
    unique_id: str = "test.shop.unique_customers_id.a1",
) -> DbtTestFacts:
    return DbtTestFacts(unique_id, name, table, DbtTestStatus(status), failures)


def only_result(facts: DbtTestFacts) -> CheckResult:
    (result,) = evaluate_dbt_tests([facts])
    return result


def test_a_passing_dbt_test_is_a_passing_check():
    assert only_result(fact("pass")) == CheckResult(
        "unique_customers_id",
        "customers",
        CheckStatus.PASS,
        "customers: dbt test unique_customers_id passed",
    )


def test_a_warned_dbt_test_is_a_passing_check_that_says_it_warned():
    assert only_result(fact("warn", 2)) == CheckResult(
        "unique_customers_id",
        "customers",
        CheckStatus.PASS,
        "customers: dbt test unique_customers_id warned — 2 failing rows; counted as passed",
    )


def test_a_failed_dbt_test_is_a_failing_check_naming_test_table_and_count():
    assert only_result(fact("fail", 3)) == CheckResult(
        "unique_customers_id",
        "customers",
        CheckStatus.FAIL,
        "customers: dbt test unique_customers_id failed — 3 failing rows",
    )


@pytest.mark.parametrize(
    ("status", "verdict", "message"),
    [
        (
            "fail",
            CheckStatus.FAIL,
            "customers: dbt test unique_customers_id failed"
            " — dbt did not report how many rows failed",
        ),
        (
            "warn",
            CheckStatus.PASS,
            "customers: dbt test unique_customers_id warned"
            " — dbt did not report how many rows failed; counted as passed",
        ),
    ],
)
def test_a_missing_failure_count_is_said_plainly(status, verdict, message):
    result = only_result(fact(status, None))

    assert (result.status, result.message) == (verdict, message)
    assert "None" not in result.message
    assert "0 failures" not in result.message


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ("error", "customers: dbt test unique_customers_id failed — dbt could not run the test"),
        ("skipped", "customers: dbt test unique_customers_id failed — dbt skipped the test"),
    ],
)
def test_an_errored_or_skipped_dbt_test_is_a_failing_check_with_a_fixed_message(status, message):
    assert only_result(fact(status, 7)) == CheckResult(
        "unique_customers_id", "customers", CheckStatus.FAIL, message
    )


def test_every_documented_dbt_status_has_a_verdict():
    verdicts = {status: only_result(fact(status)).status for status in DbtTestStatus}

    assert verdicts == {
        DbtTestStatus.PASS: CheckStatus.PASS,
        DbtTestStatus.WARN: CheckStatus.PASS,
        DbtTestStatus.FAIL: CheckStatus.FAIL,
        DbtTestStatus.ERROR: CheckStatus.FAIL,
        DbtTestStatus.SKIPPED: CheckStatus.FAIL,
    }


def test_tests_with_the_same_name_on_one_table_get_distinct_checks():
    results = evaluate_dbt_tests(
        [
            fact("pass", name="unique", unique_id="test.shop.unique.a1"),
            fact("fail", 1, name="unique", unique_id="test.shop.unique.b2"),
        ]
    )

    pairs = [(result.table, result.check) for result in results]
    assert pairs == [("customers", "test.shop.unique.a1"), ("customers", "test.shop.unique.b2")]
    assert [result.message for result in results] == [
        "customers: dbt test unique passed",
        "customers: dbt test unique failed — 1 failing rows",
    ]


def test_tests_with_the_same_name_on_different_tables_keep_their_names():
    results = evaluate_dbt_tests(
        [
            fact("pass", name="unique", table="customers", unique_id="test.shop.unique.a1"),
            fact("pass", name="unique", table="orders", unique_id="test.shop.unique.b2"),
        ]
    )

    assert [(result.table, result.check) for result in results] == [
        ("customers", "unique"),
        ("orders", "unique"),
    ]


def test_check_keys_are_unique_across_a_mixed_set_of_tests():
    results = evaluate_dbt_tests(
        [
            fact("pass", name="unique", table="customers", unique_id="u1"),
            fact("pass", name="unique", table="customers", unique_id="u2"),
            fact("pass", name="not_null", table="customers", unique_id="n1"),
            fact("pass", name="unique", table="orders", unique_id="u3"),
        ]
    )

    pairs = [(result.table, result.check) for result in results]
    assert len(set(pairs)) == len(pairs) == 4


def test_no_dbt_tests_give_no_checks():
    assert evaluate_dbt_tests(()) == ()


def test_dbt_test_facts_are_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        fact("pass").status = DbtTestStatus.FAIL  # type: ignore[misc]
