from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from adops.domain.checks import CheckResult, CheckStatus
from adops.domain.names import IDENTIFIER


class TransformStep(BaseModel):
    """A Step that runs the dbt models and tests of one dbt project in the Warehouse."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["transform"]
    name: str = Field(pattern=IDENTIFIER)
    project: str = Field(min_length=1)


class DbtTestStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class DbtTestFacts:
    """What dbt reported for one test it was asked to run."""

    unique_id: str
    name: str
    table: str
    status: DbtTestStatus
    failures: int | None


def evaluate_dbt_tests(facts: Iterable[DbtTestFacts]) -> tuple[CheckResult, ...]:
    facts = tuple(facts)
    names_per_table = Counter((fact.table, fact.name) for fact in facts)
    return tuple(
        CheckResult(
            fact.name if names_per_table[(fact.table, fact.name)] == 1 else fact.unique_id,
            fact.table,
            _verdict(fact.status),
            f"{fact.table}: dbt test {fact.name} {_outcome(fact)}",
        )
        for fact in facts
    )


def _verdict(status: DbtTestStatus) -> CheckStatus:
    if status in (DbtTestStatus.PASS, DbtTestStatus.WARN):
        return CheckStatus.PASS
    return CheckStatus.FAIL


_FIXED_OUTCOMES = {
    DbtTestStatus.PASS: "passed",
    DbtTestStatus.ERROR: "failed — dbt could not run the test",
    DbtTestStatus.SKIPPED: "failed — dbt skipped the test",
}


def _outcome(fact: DbtTestFacts) -> str:
    if fact.status == DbtTestStatus.WARN:
        return f"warned{_failing_rows(fact.failures)}; counted as passed"
    if fact.status == DbtTestStatus.FAIL:
        return f"failed{_failing_rows(fact.failures)}"
    return _FIXED_OUTCOMES[fact.status]


def _failing_rows(failures: int | None) -> str:
    if failures is None:
        return " — dbt did not report how many rows failed"
    return f" — {failures} failing rows"
