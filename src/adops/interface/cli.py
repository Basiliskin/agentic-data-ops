from typing import Annotated

import typer

from adops import __version__
from adops.interface.accept_schema_command import accept_schema
from adops.interface.drop_branch_command import drop_branch
from adops.interface.init_command import init
from adops.interface.mcp_server import mcp
from adops.interface.run_command import run
from adops.interface.validate_command import validate

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)
app.command()(init)
app.command()(run)
app.command()(validate)
app.command()(accept_schema)
app.command()(drop_branch)
app.command()(mcp)


def _print_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_print_version,
            is_eager=True,
            help="Show the adops version and exit.",
        ),
    ] = False,
) -> None:
    """adops: sync, transform and check data into your Postgres or DuckDB warehouse."""


def main() -> None:
    app()
