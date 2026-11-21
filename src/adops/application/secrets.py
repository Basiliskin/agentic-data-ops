from collections.abc import Iterable, Mapping, Sequence

from adops.application.ports import ResolvedConnection, ResolvedDuckDb, ResolvedWarehouse
from adops.domain.environment import DuckDbConnection, PostgresConnection
from adops.domain.secret_reference import SecretReference


class MissingSecretError(Exception):
    """Secret References whose environment variables are unset or empty; names only."""

    def __init__(self, names: Iterable[str]) -> None:
        self.names = tuple(sorted(set(names)))
        super().__init__(
            "these environment variables are not set or are empty: " + ", ".join(self.names)
        )


def resolve_secret_references(
    env: Mapping[str, str], refs: Iterable[SecretReference]
) -> dict[str, str]:
    """Resolve every reference at once; an empty value counts as missing."""
    names = {ref.name for ref in refs}
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise MissingSecretError(missing)
    return {name: env[name] for name in names}


SET = "set"
MISSING = "missing"


def secret_reference_status(
    env: Mapping[str, str], refs: Iterable[SecretReference]
) -> dict[str, str]:
    """Whether each reference is set, by the rule resolve_secret_references applies; no values."""
    return {ref.name: SET if env.get(ref.name) else MISSING for ref in refs}


REDACTED = "***"


def redact_secret_values(text: str, values: Iterable[str]) -> str:
    """Replace every non-empty value with ***, longest first so no part of a longer one stays."""
    for value in sorted({value for value in values if value}, key=len, reverse=True):
        text = text.replace(value, REDACTED)
    return text


def secret_references(
    warehouse: PostgresConnection | DuckDbConnection,
    sources: Sequence[PostgresConnection],
    targets: Sequence[PostgresConnection] = (),
) -> list[SecretReference]:
    connections = [*sources, *targets]
    if isinstance(warehouse, PostgresConnection):
        connections.insert(0, warehouse)
    return [connection.password_env for connection in connections]


def resolve_warehouse(
    connection: PostgresConnection | DuckDbConnection, secrets: Mapping[str, str]
) -> ResolvedWarehouse:
    if isinstance(connection, DuckDbConnection):
        return ResolvedDuckDb(connection.path)
    return resolve_connection(connection, secrets)


def resolve_connection(
    connection: PostgresConnection, secrets: Mapping[str, str]
) -> ResolvedConnection:
    return ResolvedConnection(
        host=connection.host,
        port=connection.port,
        database=connection.database,
        user=connection.user,
        password=secrets[connection.password_env.name],
    )
