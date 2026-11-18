from collections import Counter

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from adops.domain.checks import CHECKS_BY_MODE, CheckName, SyncMode
from adops.domain.errors import RuleViolation

IDENTIFIER = r"^[a-z][a-z0-9_]*$"


class SkipChecks(BaseModel):
    """A per-table opt-out from built-in Checks, allowed only with a written reason."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checks: list[CheckName] = Field(min_length=1)
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
    skip_checks: SkipChecks | None = None

    @model_validator(mode="after")
    def _consistent_with_mode(self) -> "SyncTable":
        problems = []
        if self.mode is SyncMode.INCREMENTAL and self.cursor is None:
            problems.append((("cursor",), "required for an incremental table"))
        if self.skip_checks is not None:
            applicable = CHECKS_BY_MODE[self.mode]
            problems.extend(
                (("skip_checks", "checks", index), f"does not apply to a {self.mode} table")
                for index, check in enumerate(self.skip_checks.checks)
                if check not in applicable
            )
        if problems:
            raise RuleViolation(problems)
        return self

    def applicable_checks(self) -> tuple[CheckName, ...]:
        return CHECKS_BY_MODE[self.mode]

    def required_columns(self) -> tuple[str, ...]:
        if self.cursor is None:
            return (self.primary_key,)
        return (self.primary_key, self.cursor)

    def skip_reason(self, check: CheckName) -> str | None:
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
