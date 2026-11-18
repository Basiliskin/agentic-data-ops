import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from adops.domain.errors import RuleViolation

ENV_VAR_NAME = r"^[A-Z][A-Z0-9_]*$"


class SecretReference(BaseModel):
    """The name of an environment variable holding a secret; never the secret itself."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=ENV_VAR_NAME)

    @model_validator(mode="before")
    @classmethod
    def _from_name(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return data
        if not isinstance(data, str) or not re.fullmatch(ENV_VAR_NAME, data):
            raise RuleViolation([((), "must be an environment variable name in UPPER_SNAKE_CASE")])
        return {"name": data}
