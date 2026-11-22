from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from adops.application.ports import ResolvedConnection
from adops.application.secrets import (
    MissingSecretError,
    resolve_connection,
    resolve_secret_references,
)
from adops.domain.environment import DuckDbConnection, Environment, PostgresConnection

WAREHOUSE = "warehouse"
WAREHOUSE_UNREACHABLE = "Warehouse is unreachable"


def source_unreachable(name: str) -> str:
    return f"Source {name} is unreachable"


def target_unreachable(name: str) -> str:
    return f"Target {name} is unreachable"


class ConnectionKind(StrEnum):
    WAREHOUSE = "warehouse"
    SOURCE = "source"
    TARGET = "target"


@dataclass(frozen=True)
class ConnectionResult:
    kind: ConnectionKind
    name: str
    ok: bool
    message: str | None = None


class ConnectionProbe(Protocol):
    """Connects and disconnects only; answers whether it could, never with driver text."""

    def postgres(self, connection: ResolvedConnection) -> bool: ...

    def duckdb(self, path: str) -> bool: ...


def check_connections(
    environment: Environment,
    source_names: Iterable[str],
    env_values: Mapping[str, str],
    probe: ConnectionProbe,
) -> list[ConnectionResult]:
    """One result per Warehouse, Source used and Reverse Sync target, in that order."""
    results = [_check_warehouse(environment.warehouse, env_values, probe)]
    for name in sorted(set(source_names)):
        source = environment.sources.get(name)
        unreachable = source_unreachable(name)
        if source is None:
            results.append(ConnectionResult(ConnectionKind.SOURCE, name, False, unreachable))
        else:
            results.append(
                _check_postgres(ConnectionKind.SOURCE, name, source, unreachable, env_values, probe)
            )
    for name, target in environment.targets.items():
        results.append(
            _check_postgres(
                ConnectionKind.TARGET, name, target, target_unreachable(name), env_values, probe
            )
        )
    return results


def _check_warehouse(
    warehouse: PostgresConnection | DuckDbConnection,
    env_values: Mapping[str, str],
    probe: ConnectionProbe,
) -> ConnectionResult:
    if isinstance(warehouse, DuckDbConnection):
        ok = probe.duckdb(warehouse.path)
        return _result(ConnectionKind.WAREHOUSE, WAREHOUSE, ok, WAREHOUSE_UNREACHABLE)
    return _check_postgres(
        ConnectionKind.WAREHOUSE, WAREHOUSE, warehouse, WAREHOUSE_UNREACHABLE, env_values, probe
    )


def _check_postgres(
    kind: ConnectionKind,
    name: str,
    connection: PostgresConnection,
    unreachable: str,
    env_values: Mapping[str, str],
    probe: ConnectionProbe,
) -> ConnectionResult:
    try:
        secrets = resolve_secret_references(env_values, [connection.password_env])
    except MissingSecretError as missing:
        return ConnectionResult(kind, name, False, str(missing))
    return _result(kind, name, probe.postgres(resolve_connection(connection, secrets)), unreachable)


def _result(kind: ConnectionKind, name: str, ok: bool, unreachable: str) -> ConnectionResult:
    return ConnectionResult(kind, name, ok, None if ok else unreachable)
