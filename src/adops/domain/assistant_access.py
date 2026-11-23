from collections.abc import Callable, Mapping
from dataclasses import dataclass

from adops.domain.environment import DuckDbConnection, Environment, PostgresConnection

DEV = "dev"
PROTECTED_ENVIRONMENTS = ("stage", "prod")
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

ONLY_DEV_OR_BRANCH = (
    "the Assistant works only in the Environment named exactly `dev` or in a Branch Environment"
)

NOT_A_PLAIN_PIPELINE_NAME = (
    "the Assistant takes only a plain Pipeline name, the file name in pipelines/ without .yaml; "
    "a name with a slash, a backslash, two dots in a row or a NUL character is refused"
)
NAME_UNSAFE_PARTS = ("/", "\\", "..", "\0")

SETTINGS_FILE_NAME = "adops.yaml"
PROJECT_FILE_ROOTS = ("pipelines", "dbt")
DOTENV_NAME = ".env"
OUTSIDE_PROJECT_FILES = (
    "the Assistant works only with adops.yaml and the files under pipelines/ and dbt/, "
    "given as a path relative to the Project; an absolute path, a '..' part "
    "and any .env file are refused"
)
WRITES_ONLY_UNDER_ROOTS = (
    "the Assistant writes only files under pipelines/ and dbt/; "
    "adops.yaml is read-only for the Assistant"
)

ACCEPT_SCHEMA_DEV_ONLY = (
    "the Assistant accepts a Breaking Change only in `dev`; "
    "to accept one in a Branch Environment, ask the user to run "
    "`adops accept-schema --branch <name>` by hand"
)


class AssistantRefusal(Exception):
    """A refused Assistant request; its fixed message never carries connection values."""


@dataclass(frozen=True)
class AssistantTarget:
    """Where an Assistant request acts: dev when `branch` is None, else that Branch Environment."""

    branch: str | None = None


def assistant_target(environment: str | None, branch: str | None) -> AssistantTarget:
    if environment is not None and branch is not None:
        raise AssistantRefusal(f"{ONLY_DEV_OR_BRANCH}; give an Environment or a branch, not both")
    if environment is None and not branch:
        raise AssistantRefusal(f"{ONLY_DEV_OR_BRANCH}; name `dev` or a branch")
    if environment is None:
        return AssistantTarget(branch=branch)
    if environment != DEV:
        raise AssistantRefusal(f"{ONLY_DEV_OR_BRANCH}; {environment!r} is refused")
    return AssistantTarget()


def plain_pipeline_name(name: str) -> str:
    """The name, or a refusal that does not repeat it, so it cannot reach outside pipelines/."""
    if not name or any(part in name for part in NAME_UNSAFE_PARTS):
        raise AssistantRefusal(NOT_A_PLAIN_PIPELINE_NAME)
    return name


def allowed_project_path(relative: str, *, write: bool = False) -> str:
    """The Project-relative path with `/` separators, or a refusal that does not repeat it.

    With `write`, only a file under pipelines/ or dbt/ is allowed.
    """
    normalized = relative.replace("\\", "/")
    if "\0" in normalized or normalized.startswith("/"):
        raise AssistantRefusal(OUTSIDE_PROJECT_FILES)
    parts = [part for part in normalized.split("/") if part not in ("", ".")]
    if not parts or ".." in parts or any(_is_dotenv(part) for part in parts):
        raise AssistantRefusal(OUTSIDE_PROJECT_FILES)
    path = "/".join(parts)
    if path != SETTINGS_FILE_NAME and parts[0] not in PROJECT_FILE_ROOTS:
        raise AssistantRefusal(OUTSIDE_PROJECT_FILES)
    if write and len(parts) < 2:
        raise AssistantRefusal(WRITES_ONLY_UNDER_ROOTS)
    return path


def _is_dotenv(part: str) -> bool:
    lowered = part.lower()
    return lowered == DOTENV_NAME or lowered.startswith(DOTENV_NAME + ".")


def refuse_branch_schema_acceptance(branch: str | None) -> None:
    if branch is not None:
        raise AssistantRefusal(ACCEPT_SCHEMA_DEV_ONLY)


ConnectionKey = tuple[str, ...]


def refuse_shared_dev_connection(
    environments: Mapping[str, Environment], duckdb_identity: Callable[[str], str]
) -> None:
    """Refuse dev when its Warehouse or a Reverse Sync target is a stage or prod database.

    `duckdb_identity` maps a DuckDB path to a comparable identity, such as its absolute path;
    the caller supplies it because resolving paths needs the filesystem.
    """

    def keys(environment: Environment) -> set[ConnectionKey]:
        connections = [environment.warehouse, *environment.targets.values()]
        return {_connection_key(connection, duckdb_identity) for connection in connections}

    dev = keys(environments[DEV])
    for name in PROTECTED_ENVIRONMENTS:
        protected = environments.get(name)
        if protected is not None and dev & keys(protected):
            raise AssistantRefusal(
                f"dev uses the same database as {name}, so the Assistant refuses to work in dev; "
                "give dev its own Warehouse and Reverse Sync target databases"
            )


def _connection_key(
    connection: PostgresConnection | DuckDbConnection, duckdb_identity: Callable[[str], str]
) -> ConnectionKey:
    if isinstance(connection, DuckDbConnection):
        return ("duckdb", duckdb_identity(connection.path))
    host = connection.host.lower()
    if host in LOOPBACK_HOSTS:
        host = "localhost"
    return ("postgres", host, str(connection.port), connection.database)
