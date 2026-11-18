from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from adops.domain.errors import RuleViolation
from adops.domain.secret_reference import SecretReference

REQUIRED_ENVIRONMENTS = ("dev",)


class PostgresConnection(BaseModel):
    """Where a Postgres Source or Warehouse lives; the password is only a Secret Reference."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["postgres"]
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    database: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password_env: SecretReference

    @model_validator(mode="before")
    @classmethod
    def _reject_literal_password(cls, data: Any) -> Any:
        if isinstance(data, dict) and "password" in data:
            raise RuleViolation(
                [
                    (
                        ("password",),
                        "literal passwords are not allowed; set password_env to the name "
                        "of the environment variable that holds it",
                    )
                ]
            )
        return data


class Environment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    warehouse: PostgresConnection
    sources: dict[str, PostgresConnection]


class ProjectSettings(BaseModel):
    """The Project settings file `adops.yaml`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    environments: dict[str, Environment]

    @model_validator(mode="after")
    def _required_environments_present(self) -> "ProjectSettings":
        missing = [name for name in REQUIRED_ENVIRONMENTS if name not in self.environments]
        if missing:
            raise RuleViolation(
                [(("environments", name), "this Environment is required") for name in missing]
            )
        return self
