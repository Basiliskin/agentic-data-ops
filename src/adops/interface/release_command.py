"""`adops server-release`: run by a stage or prod deploy on the Server, in place of a checkout."""

from pathlib import Path, PurePosixPath
from typing import Annotated

import typer

from adops.infrastructure.dagster_server import DagsterInstance, in_use_release_folders
from adops.infrastructure.release_folders import (
    RELEASES,
    Layout,
    delete_release,
    layout,
    prepare_release,
    releases_to_delete,
    switch_current,
)

PLAYBOOK = "docs/playbooks/promotion.md"
MOVE_SECTION = "One-time move to release folders"
CONFIG_SECTION = "When a deploy changes the Server config"
CLEANUP_PAUSED = (
    "Old release folders were kept: a run in progress has no recorded release folder, "
    "so it may be reading any of them."
)


def server_release(
    server_dir: Annotated[
        Path,
        typer.Option(
            exists=True, file_okay=False, help="The Server folder that holds releases/ and current."
        ),
    ],
    checkout: Annotated[
        Path,
        typer.Option(
            "--from", exists=True, file_okay=False, help="The validated checkout to deploy."
        ),
    ],
    commit: Annotated[str, typer.Option(help="The commit, used as the release folder name.")],
) -> None:
    """Copy a checkout into releases/<commit>, point current at it and delete old releases.

    Changes no file on the old Server folder layout or when the commit changes the Server config.
    """
    refusal = _refusal(server_dir, checkout)
    if refusal:
        typer.echo(refusal, err=True)
        raise typer.Exit(1)
    try:
        prepare_release(server_dir, checkout, commit)
    except ValueError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from None
    switch_current(server_dir, commit)
    typer.echo(f"current: {RELEASES}/{commit}")
    with DagsterInstance.get() as instance:
        in_use = in_use_release_folders(instance, PurePosixPath(server_dir.resolve() / RELEASES))
    if in_use.untagged_run_in_progress:
        typer.echo(CLEANUP_PAUSED)
    for name in releases_to_delete(
        server_dir, in_use.names, untagged_run_in_progress=in_use.untagged_run_in_progress
    ):
        delete_release(server_dir, name)
        typer.echo(f"deleted: {RELEASES}/{name}")


def _refusal(server_dir: Path, checkout: Path) -> str | None:
    found = layout(server_dir)
    if found is Layout.OLD:
        return (
            f"{server_dir} has the old layout (no releases/ folder and no 'current' link). "
            f"Move it once by hand: {_see(MOVE_SECTION)} Nothing was changed."
        )
    if found is Layout.HALF_MOVED:
        return (
            f"{server_dir} is half moved: releases/ or 'current' exists, but 'current' is not a "
            f"relative link to a folder in releases/. Finish the move: {_see(MOVE_SECTION)} "
            "Nothing was changed."
        )
    changed = _changed_server_config(server_dir, checkout)
    if changed:
        return (
            f"This commit changes the Server config: {', '.join(changed)}. Update it by hand: "
            f"{_see(CONFIG_SECTION)} Nothing was changed."
        )
    return None


def _changed_server_config(server_dir: Path, checkout: Path) -> list[str]:
    names = {"docker-compose.yml"} | {
        f"server/{path.name}"
        for root in (server_dir, checkout)
        for path in (root / "server").glob("*.yaml")
    }
    return sorted(
        name for name in names if _content(server_dir / name) != _content(checkout / name)
    )


def _content(path: Path) -> bytes | None:
    return path.read_bytes() if path.is_file() else None


def _see(section: str) -> str:
    return f'see "{section}" in {PLAYBOOK}.'
