"""Release folders inside a Server folder: releases/<commit> plus a relative 'current' link.

Each deploy copies the validated checkout into its own folder and swaps 'current' in one rename,
so a run never reads a mix of old and new Project files. .env and .adops/ stay one shared copy at
the Server folder root. Links are relative because the Server folder is bind-mounted at another
path inside the containers.
"""

import os
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath

RELEASES = "releases"
CURRENT = "current"
SHARED = (".env", ".adops")
NOT_COPIED = frozenset({".git", *SHARED})
TMP_SUFFIX = ".tmp"
KEEP_NEWEST = 3


class Layout(StrEnum):
    NEW = "new"
    OLD = "old"
    HALF_MOVED = "half-moved"


@dataclass(frozen=True)
class ReleaseEntry:
    name: str
    created: float
    is_dir: bool


def layout(server_dir: Path) -> Layout:
    if current_release(server_dir) is not None:
        return Layout.NEW
    if os.path.lexists(server_dir / RELEASES) or os.path.lexists(server_dir / CURRENT):
        return Layout.HALF_MOVED
    return Layout.OLD


def current_release(server_dir: Path) -> str | None:
    """The commit 'current' names, or None unless it is a relative link to a real release folder."""
    current = server_dir / CURRENT
    if not current.is_symlink():
        return None
    parts = PurePosixPath(os.readlink(current)).parts
    if len(parts) != 2 or parts[0] != RELEASES or not _is_release_name(parts[1]):
        return None
    releases = server_dir / RELEASES
    if not _is_real_dir(releases) or not _is_real_dir(releases / parts[1]):
        return None
    return parts[1]


def prepare_release(server_dir: Path, checkout: Path, commit: str) -> Path:
    """Copy the checkout into releases/<commit>; a commit already prepared is left as it is."""
    _check_release_name(commit)
    (server_dir / ".adops").mkdir(exist_ok=True)
    releases = server_dir / RELEASES
    release = releases / commit
    if os.path.lexists(release):
        return release
    releases.mkdir(exist_ok=True)
    staging = releases / f"{commit}{TMP_SUFFIX}"
    _remove(staging)
    shutil.copytree(checkout, staging, symlinks=True, ignore=_top_level(checkout, NOT_COPIED))
    for name in SHARED:
        (staging / name).symlink_to(Path("..", "..", name))
    os.replace(staging, release)
    return release


def switch_current(server_dir: Path, commit: str) -> None:
    _check_release_name(commit)
    if not _is_real_dir(server_dir / RELEASES / commit):
        raise FileNotFoundError(f"No release folder {RELEASES}/{commit} in {server_dir}")
    staging = server_dir / f"{CURRENT}{TMP_SUFFIX}"
    _remove(staging)
    staging.symlink_to(f"{RELEASES}/{commit}")
    os.replace(staging, server_dir / CURRENT)


def delete_release(server_dir: Path, commit: str) -> None:
    """Removes links inside the folder without following them, so shared .env and .adops stay."""
    _check_release_name(commit)
    _remove(server_dir / RELEASES / commit)


def releases_to_delete(
    server_dir: Path, in_use: Iterable[str], *, untagged_run_in_progress: bool
) -> list[str]:
    """`in_use` holds release folder names (not paths) that an in-progress run is reading."""
    releases = server_dir / RELEASES
    entries = [_entry(path) for path in releases.iterdir()] if _is_real_dir(releases) else []
    return select_releases_to_delete(
        entries,
        current=current_release(server_dir),
        in_use=in_use,
        untagged_run_in_progress=untagged_run_in_progress,
    )


def select_releases_to_delete(
    entries: Iterable[ReleaseEntry],
    *,
    current: str | None,
    in_use: Iterable[str],
    untagged_run_in_progress: bool,
) -> list[str]:
    """Keep the newest KEEP_NEWEST, the current one and every one in use; delete nothing when an
    in-progress run has no recorded folder, since it may be reading any of them."""
    if untagged_run_in_progress:
        return []
    releases = sorted(
        (entry for entry in entries if entry.is_dir and _is_release_name(entry.name)),
        key=lambda entry: entry.created,
        reverse=True,
    )
    kept = {current, *in_use}
    return [entry.name for entry in releases[KEEP_NEWEST:] if entry.name not in kept]


def _entry(path: Path) -> ReleaseEntry:
    # A release folder's own mtime moves whenever a run writes a file at its root, so its age comes
    # from the .adops link prepare_release made; a link is never modified after it is created.
    link = path / ".adops"
    stamp = link if link.is_symlink() else path
    return ReleaseEntry(path.name, stamp.lstat().st_mtime, _is_real_dir(path))


def _top_level(root: Path, names: frozenset[str]):
    def ignore(directory: str, entries: list[str]) -> set[str]:
        return names.intersection(entries) if Path(directory) == root else set()

    return ignore


def _is_release_name(name: str) -> bool:
    return name not in ("", ".", "..") and "/" not in name and not name.endswith(TMP_SUFFIX)


def _check_release_name(commit: str) -> None:
    if not _is_release_name(commit):
        raise ValueError(f"Not a release folder name: {commit!r}")


def _is_real_dir(path: Path) -> bool:
    return path.is_dir() and not path.is_symlink()


def _remove(path: Path) -> None:
    if _is_real_dir(path):
        shutil.rmtree(path)
    elif os.path.lexists(path):
        path.unlink()
