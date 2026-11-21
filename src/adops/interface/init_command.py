from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Annotated

import typer

from adops import __version__

TEMPLATES = files("adops.templates")
RENAMED = {"gitignore": ".gitignore"}
VERSION_MARKER = "{{ADOPS_VERSION}}"
SERVER_CONFIG_FILES = ("workspace.yaml", "dagster.yaml")


def init(
    path: Annotated[
        Path, typer.Argument(help="Folder to create the Project in; it must be empty or missing.")
    ] = Path("."),
    demo: Annotated[
        bool, typer.Option("--demo", help="Create the demo shop Project with sample data.")
    ] = False,
) -> None:
    """Create a new Project: your settings and Pipelines, no tool code."""
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        typer.echo(f"{path} is not an empty folder; choose a new or empty one.", err=True)
        raise typer.Exit(1)

    if demo:
        _copy_tree(TEMPLATES / "demo", path)
        _write_server_files(path)
        typer.echo(f"Created the demo Project in {path}.")
        typer.echo("Next: copy .env.example to .env, then run `docker compose up --wait` there.")
    else:
        _copy_tree(TEMPLATES / "plain", path)
        (path / "pipelines").mkdir()
        _write_server_files(path)
        typer.echo(f"Created a Project in {path}.")


def _write_server_files(project: Path) -> None:
    server = TEMPLATES / "server"
    dockerfile = (server / "Dockerfile").read_text().replace(VERSION_MARKER, __version__)
    (project / "Dockerfile").write_text(dockerfile)
    (project / "server").mkdir()
    for name in SERVER_CONFIG_FILES:
        (project / "server" / name).write_bytes((server / name).read_bytes())


def _copy_tree(source: Traversable, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for entry in source.iterdir():
        destination = target / RENAMED.get(entry.name, entry.name)
        if entry.is_dir():
            _copy_tree(entry, destination)
        else:
            destination.write_bytes(entry.read_bytes())
