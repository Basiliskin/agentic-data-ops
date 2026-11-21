from typing import Annotated, Any, Literal
from zoneinfo import available_timezones

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    StringConstraints,
    Tag,
    field_validator,
    model_validator,
)

from adops.domain.errors import RuleViolation
from adops.domain.names import IDENTIFIER
from adops.domain.secret_reference import SecretReference

REQUIRED_ENVIRONMENTS = ("dev",)
DEFAULT_TIMEZONE = "UTC"


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


class DuckDbConnection(BaseModel):
    """Where a DuckDB Warehouse file lives; DuckDB has no password, so no Secret Reference."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["duckdb"]
    path: str = Field(min_length=1)


WAREHOUSE_TYPES = ("postgres", "duckdb")


def _connection_type(value: Any) -> str | None:
    tag = value.get("type") if isinstance(value, dict) else getattr(value, "type", None)
    return tag if isinstance(tag, str) else None


WarehouseConnection = Annotated[
    Annotated[PostgresConnection, Tag("postgres")] | Annotated[DuckDbConnection, Tag("duckdb")],
    Discriminator(
        _connection_type,
        custom_error_type="warehouse_type",
        custom_error_message="type must be one of: " + ", ".join(WAREHOUSE_TYPES),
    ),
]


class Environment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    warehouse: WarehouseConnection
    sources: dict[str, PostgresConnection]
    targets: dict[str, PostgresConnection] = Field(default_factory=dict)


EnvironmentName = Annotated[str, StringConstraints(pattern=IDENTIFIER)]


class ProjectSettings(BaseModel):
    """The Project settings file `adops.yaml`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    environments: dict[EnvironmentName, Environment]
    timezone: str = DEFAULT_TIMEZONE
    alert_webhook_env: SecretReference | None = None

    @field_validator("timezone")
    @classmethod
    def _known_timezone(cls, name: str) -> str:
        # An exact match: ZoneInfo itself accepts `utc` on a case-insensitive disk like macOS's,
        # which the Linux Server would then reject.
        if name not in available_timezones():
            raise RuleViolation(
                [((), "not a known time zone; use an IANA name such as Europe/Berlin or UTC")]
            )
        return name

    @model_validator(mode="after")
    def _required_environments_present(self) -> "ProjectSettings":
        missing = [name for name in REQUIRED_ENVIRONMENTS if name not in self.environments]
        if missing:
            raise RuleViolation(
                [(("environments", name), "this Environment is required") for name in missing]
            )
        return self
