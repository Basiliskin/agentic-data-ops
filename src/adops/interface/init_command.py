from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Annotated

import typer

from adops import __version__
from adops.infrastructure.git_start import STARTING_COMMIT, GitStart, start_repository

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
    no_git: Annotated[
        bool, typer.Option("--no-git", help="Do not create a Git repository for the Project.")
    ] = False,
) -> None:
    """Create a new Project: your settings and Pipelines, no tool code."""
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        typer.echo(f"{path} is not an empty folder; choose a new or empty one.", err=True)
        raise typer.Exit(1)

    if demo:
        _copy_tree(TEMPLATES / "demo", path)
        _write_server_files(path)
        _write_workflow_files(path)
    else:
        _copy_tree(TEMPLATES / "plain", path)
        (path / "pipelines").mkdir()
        _write_server_files(path)
        _write_workflow_files(path)
    typer.echo(f"Created {'the demo Project' if demo else 'a Project'} in {path}.")
    if not no_git:
        typer.echo(GIT_START_MESSAGES[start_repository(path)])
    if demo:
        typer.echo("Next: copy .env.example to .env, then run `docker compose up --wait` there.")


GIT_START_MESSAGES = {
    GitStart.COMMITTED: "Saved these starting files in a new Git repository, on branch main.",
    GitStart.NO_GIT: "Git is not installed, so no Git repository was created.",
    GitStart.INSIDE_REPOSITORY: "This folder is already inside a Git repository, left as it is.",
    GitStart.COMMIT_FAILED: (
        "Created a Git repository, but Git could not save the starting files, most often because"
        " no Git name and email are set. Set them with `git config --global user.name` and"
        f' `git config --global user.email`, then run `git commit -m "{STARTING_COMMIT}"` there.'
    ),
}


def _write_server_files(project: Path) -> None:
    server = TEMPLATES / "server"
    dockerfile = (server / "Dockerfile").read_text().replace(VERSION_MARKER, __version__)
    (project / "Dockerfile").write_text(dockerfile)
    (project / "server").mkdir()
    for name in SERVER_CONFIG_FILES:
        (project / "server" / name).write_bytes((server / name).read_bytes())


def _write_workflow_files(project: Path) -> None:
    workflows = project / ".github" / "workflows"
    workflows.mkdir(parents=True)
    for template in (TEMPLATES / "github" / "workflows").iterdir():
        text = template.read_text().replace(VERSION_MARKER, __version__)
        (workflows / template.name).write_text(text)


def _copy_tree(source: Traversable, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for entry in source.iterdir():
        destination = target / RENAMED.get(entry.name, entry.name)
        if entry.is_dir():
            _copy_tree(entry, destination)
        else:
            destination.write_bytes(entry.read_bytes())
