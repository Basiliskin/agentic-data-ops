from collections.abc import Callable, Mapping
from dataclasses import dataclass

from adops.domain.environment import DuckDbConnection, Environment, PostgresConnection

DEV = "dev"
PROTECTED_ENVIRONMENTS = ("stage", "prod")
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

ONLY_DEV_OR_BRANCH = (
    "the Assistant works only in the Environment named exactly `dev` or in a Branch Environment"
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
