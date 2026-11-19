from pathlib import Path
from typing import Annotated

import typer

from adops.application.accept_schema import AcceptedSchema
from adops.application.accept_schema import accept_schema as accept_table_schema
from adops.application.ports import WarehouseUnreachable
from adops.application.run_pipeline import StepError
from adops.application.secrets import MissingSecretError
from adops.domain.errors import PipelineValidationError
from adops.interface import run_command
from adops.interface.run_command import ProjectFileError, fail


def accept_schema(
    pipeline: Annotated[str, typer.Argument(help="Pipeline name (pipelines/<name>.yaml).")],
    table: Annotated[str, typer.Argument(help="Source table whose change you accept.")],
    env: Annotated[str, typer.Option("--env", help="Environment from adops.yaml.")] = "dev",
    project: Annotated[Path, typer.Option(help="Project folder.")] = Path("."),
) -> None:
    """Accept an intended Breaking Change: record the table's current Source shape.

    The next run compares against this shape. Only the chosen Environment is changed.
    """
    try:
        accepted = accept_project_schema(project.resolve(), pipeline, table, env)
    except PipelineValidationError as error:
        fail(*error.problems)
    except (ProjectFileError, MissingSecretError, StepError) as error:
        fail(str(error))
    except WarehouseUnreachable:
        fail(f"Pipeline {pipeline}, table {table}: Warehouse is unreachable")
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        fail(f"unexpected {type(error).__name__} while accepting {table}")
    typer.echo(
        f"Accepted the current shape of {accepted.table} ({len(accepted.shape.columns)} columns, "
        f"Step {accepted.step}) for Pipeline {accepted.pipeline} "
        f"in Environment {accepted.environment}."
    )
    if accepted.nullable_columns:
        typer.echo(
            "These columns are no longer in the Source and may now be NULL in the Warehouse: "
            + ", ".join(accepted.nullable_columns)
        )


def accept_project_schema(
    project: Path, pipeline: str, table: str, environment: str
) -> AcceptedSchema:
    settings_text, pipeline_text = run_command.read_project_files(project, pipeline)
    kind = run_command.warehouse_kind(settings_text, environment)
    return accept_table_schema(
        pipeline_text,
        settings_text,
        environment,
        table,
        run_command.project_env(project),
        run_command.build_sync(project, pipeline, kind),
        run_command.build_snapshot_store(project, kind),
    )
