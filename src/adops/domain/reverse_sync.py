import re
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from adops.domain.checks import REVERSE_SYNC_CHECKS, CheckName, SkipChecks
from adops.domain.errors import RuleViolation
from adops.domain.names import IDENTIFIER

Identifier = Annotated[str, StringConstraints(pattern=IDENTIFIER)]

DEFAULT_MAX_CHANGE_SHARE = 0.5


class ReverseSyncStep(BaseModel):
    """A Step that copies a dbt model's rows from the Warehouse into a table of a Target.

    `max_change_share` is the largest share of the Target's rows one run may update
    or delete before the max_change Check fails; it defaults to DEFAULT_MAX_CHANGE_SHARE.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["reverse_sync"]
    name: Identifier
    model: Identifier
    target: Identifier
    target_table: Identifier
    key: Identifier
    mapping: dict[Identifier, Identifier] = Field(min_length=1)
    delete_missing: bool = False
    max_change_share: float = Field(default=DEFAULT_MAX_CHANGE_SHARE, gt=0, le=1)
    skip_checks: SkipChecks | None = None

    @field_validator("mapping", mode="before")
    @classmethod
    def _column_names_are_identifiers(cls, mapping: Any) -> Any:
        if not isinstance(mapping, dict):
            return mapping
        problems = [
            ((), f"entry {position}: the dbt column name must match {IDENTIFIER}")
            for position, column in enumerate(mapping, start=1)
            if not isinstance(column, str) or re.fullmatch(IDENTIFIER, column) is None
        ]
        if problems:
            raise RuleViolation(problems)
        return mapping

    @model_validator(mode="after")
    def _only_reverse_sync_checks_skipped(self) -> "ReverseSyncStep":
        if self.skip_checks is None:
            return self
        problems = []
        for index, check in enumerate(self.skip_checks.checks):
            if check in REVERSE_SYNC_CHECKS:
                continue
            location = ("skip_checks", "checks", index)
            if check in set(CheckName):
                problems.append((location, "does not apply to a Reverse Sync Step"))
            else:
                problems.append((location, "must be one of: " + ", ".join(REVERSE_SYNC_CHECKS)))
        if problems:
            raise RuleViolation(problems)
        return self
