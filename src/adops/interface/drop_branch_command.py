from pathlib import Path
from typing import Annotated

import typer

from adops.application.drop_branch import BranchState, DroppedBranch, WarehouseSchemas
from adops.application.drop_branch import drop_branch as drop_branch_environment
from adops.application.ports import WarehouseLocked, WarehouseUnreachable
from adops.application.run_pipeline import stage_environment
from adops.application.secrets import MissingSecretError
from adops.domain.branch_environment import STAGE
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_project_settings
from adops.infrastructure.warehouse_lock import DEFAULT_LOCK_TIMEOUT
from adops.infrastructure.warehouse_schemas import LocalBranchState, WarehouseSchemaAdmin
from adops.interface import run_command
from adops.interface.run_command import ProjectFileError, fail


def drop_branch(
    branch: Annotated[str, typer.Argument(help="Git branch whose Branch Environment to drop.")],
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Drop a Branch Environment: its schemas in stage's Warehouse and its local dlt state.

    Only schemas named br_<branch>__… are dropped; stage's own schemas are never touched.
    Dropping a Branch that has nothing left succeeds. Re-run `adops run --branch` to rebuild it.
    """
    try:
        dropped = drop_project_branch(project.resolve(), branch)
    except PipelineValidationError as error:
        fail(*error.problems)
    except (ProjectFileError, MissingSecretError, WarehouseLocked) as error:
        fail(str(error))
    except WarehouseUnreachable:
        fail(f"Branch {branch}: stage's Warehouse is unreachable or locked by another process")
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while dropping Branch {branch}")
    if dropped.schemas:
        typer.echo(
            f"Dropped Branch Environment {dropped.environment}: " + ", ".join(dropped.schemas)
        )
    else:
        typer.echo(f"Branch Environment {dropped.environment} has no schemas left to drop.")


def drop_project_branch(
    project: Path, branch: str, lock_timeout: float = DEFAULT_LOCK_TIMEOUT
) -> DroppedBranch:
    """Drop a Branch Environment, waiting up to `lock_timeout` seconds for stage's lock."""
    settings_text = run_command.read_settings(project)
    stage = stage_environment(load_project_settings(settings_text).environments)
    env = run_command.project_env(project)
    warehouse = run_command.resolve_environment_warehouse(stage, env)
    with run_command.build_warehouse_lock(warehouse, project, STAGE, lock_timeout):
        return drop_branch_environment(
            settings_text,
            branch,
            env,
            build_warehouse_schemas(project, stage.warehouse.type),
            build_branch_state(project),
        )


def build_warehouse_schemas(project: Path, warehouse_kind: str) -> WarehouseSchemas:
    return WarehouseSchemaAdmin(project, warehouse_kind)


def build_branch_state(project: Path) -> BranchState:
    return LocalBranchState(project)
