from collections import Counter
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)

from adops.domain.checks import CHECKS_BY_MODE, CheckName, SyncMode
from adops.domain.errors import RuleViolation

IDENTIFIER = r"^[a-z][a-z0-9_]*$"


class _DeclaredCheckBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class NotNullCheck(_DeclaredCheckBase):
    type: Literal["not_null"]
    column: str = Field(min_length=1)

    @property
    def name(self) -> str:
        return f"not_null({self.column})"


class AcceptedValuesCheck(_DeclaredCheckBase):
    type: Literal["accepted_values"]
    column: str = Field(min_length=1)
    values: list[str | int] = Field(min_length=1)

    @field_validator("values")
    @classmethod
    def _values_unique(cls, values: list[str | int]) -> list[str | int]:
        seen: set[str | int] = set()
        problems = []
        for index, value in enumerate(values):
            if value in seen:
                problems.append(((index,), "repeats an earlier value"))
            seen.add(value)
        if problems:
            raise RuleViolation(problems)
        return values

    @property
    def name(self) -> str:
        return f"accepted_values({self.column})"


class MinRowsCheck(_DeclaredCheckBase):
    type: Literal["min_rows"]
    count: int = Field(ge=1)

    @property
    def name(self) -> str:
        return "min_rows"


class FreshnessCheck(_DeclaredCheckBase):
    type: Literal["freshness"]
    column: str = Field(min_length=1)
    max_age_hours: int = Field(ge=1)

    @property
    def name(self) -> str:
        return f"freshness({self.column})"


DECLARED_CHECK_TYPES = ("not_null", "accepted_values", "min_rows", "freshness")


def _declared_check_type(value: Any) -> str | None:
    tag = value.get("type") if isinstance(value, dict) else getattr(value, "type", None)
    return tag if isinstance(tag, str) else None


DeclaredCheck = Annotated[
    Annotated[NotNullCheck, Tag("not_null")]
    | Annotated[AcceptedValuesCheck, Tag("accepted_values")]
    | Annotated[MinRowsCheck, Tag("min_rows")]
    | Annotated[FreshnessCheck, Tag("freshness")],
    Discriminator(
        _declared_check_type,
        custom_error_type="declared_check_type",
        custom_error_message="type must be one of: " + ", ".join(DECLARED_CHECK_TYPES),
    ),
]


class SkipChecks(BaseModel):
    """A per-table opt-out from Checks, allowed only with a written reason."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checks: list[str] = Field(min_length=1)
    reason: str

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, reason: str) -> str:
        if not reason.strip():
            raise RuleViolation([((), "must explain why these Checks are skipped")])
        return reason


class SyncTable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    mode: SyncMode
    primary_key: str = Field(min_length=1)
    cursor: str | None = Field(default=None, min_length=1)
    checks: list[DeclaredCheck] = Field(default_factory=list)
    skip_checks: SkipChecks | None = None

    @model_validator(mode="after")
    def _consistent_with_mode(self) -> "SyncTable":
        problems = []
        if self.mode is SyncMode.INCREMENTAL and self.cursor is None:
            problems.append((("cursor",), "required for an incremental table"))
        problems.extend(self._duplicate_declared_checks())
        problems.extend(self._unknown_skipped_checks())
        if problems:
            raise RuleViolation(problems)
        return self

    def _duplicate_declared_checks(self) -> list[tuple[tuple[str | int, ...], str]]:
        first_index: dict[str, int] = {}
        problems = []
        for index, check in enumerate(self.checks):
            if check.name in first_index:
                problems.append(
                    (("checks", index), f"the same Check as checks[{first_index[check.name]}]")
                )
            else:
                first_index[check.name] = index
        return problems

    def _unknown_skipped_checks(self) -> list[tuple[tuple[str | int, ...], str]]:
        if self.skip_checks is None:
            return []
        applicable = self.applicable_checks()
        declared = self.declared_check_names()
        problems = []
        for index, check in enumerate(self.skip_checks.checks):
            if check in applicable or check in declared:
                continue
            location = ("skip_checks", "checks", index)
            if check in set(CheckName):
                problems.append((location, f"does not apply to a {self.mode} table"))
            else:
                problems.append(
                    (
                        location,
                        "must be a Check declared on this table or one of: "
                        + ", ".join(applicable),
                    )
                )
        return problems

    def applicable_checks(self) -> tuple[CheckName, ...]:
        return CHECKS_BY_MODE[self.mode]

    def declared_check_names(self) -> tuple[str, ...]:
        return tuple(check.name for check in self.checks)

    def required_columns(self) -> tuple[str, ...]:
        if self.cursor is None:
            return (self.primary_key,)
        return (self.primary_key, self.cursor)

    def skip_reason(self, check: str) -> str | None:
        if self.skip_checks is not None and check in self.skip_checks.checks:
            return self.skip_checks.reason
        return None


class SyncStep(BaseModel):
    """A Step that Syncs an explicit allowlist of tables from one Source."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=IDENTIFIER)
    source: str = Field(min_length=1)
    tables: list[SyncTable] = Field(min_length=1)


class Pipeline(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=IDENTIFIER)
    steps: list[SyncStep] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_step_names(self) -> "Pipeline":
        counts = Counter(step.name for step in self.steps)
        problems = [
            (("steps", index, "name"), "another Step already has this name")
            for index, step in enumerate(self.steps)
            if counts[step.name] > 1
        ]
        if problems:
            raise RuleViolation(problems)
        return self
