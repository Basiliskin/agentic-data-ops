from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from adops.application.ports import ResolvedWarehouse
from adops.application.run_pipeline import stage_branch_name, stage_environment
from adops.application.secrets import (
    resolve_secret_references,
    resolve_warehouse,
    secret_references,
)
from adops.domain.branch_environment import drop_prefix
from adops.domain.loader import load_project_settings


@runtime_checkable
class WarehouseSchemas(Protocol):
    """Lists and drops whole schemas in a Warehouse."""

    def list_schemas(self, warehouse: ResolvedWarehouse) -> Sequence[str]: ...

    def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
        """Drop the schema and everything in it; a schema that is already gone is fine."""
        ...


@runtime_checkable
class BranchState(Protocol):
    """The local state a Branch Environment's runs keep outside the Warehouse."""

    def delete(self, environment: str) -> None: ...


@dataclass(frozen=True)
class DroppedBranch:
    environment: str
    schemas: tuple[str, ...]


def drop_branch(
    settings_text: str,
    branch: str,
    env: Mapping[str, str],
    schemas: WarehouseSchemas,
    state: BranchState,
) -> DroppedBranch:
    """Drop a Branch Environment's schemas from stage's Warehouse, then its local state.

    Only schemas whose names start with the Branch's exact prefix are dropped, so stage's
    schemas and other Branches' schemas are never touched. The local state goes last, so a
    Warehouse failure leaves it for a re-run.
    """
    stage = stage_environment(load_project_settings(settings_text).environments)
    environment = stage_branch_name(stage, branch)
    secrets = resolve_secret_references(env, secret_references(stage.warehouse, ()))
    warehouse = resolve_warehouse(stage.warehouse, secrets)
    prefix = drop_prefix(environment)
    dropped = tuple(
        sorted(name for name in schemas.list_schemas(warehouse) if name.startswith(prefix))
    )
    for name in dropped:
        schemas.drop(warehouse, name)
    state.delete(environment)
    return DroppedBranch(environment, dropped)
