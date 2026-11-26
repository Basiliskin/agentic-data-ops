"""A new Project's first Git save, so its first pull request holds only the person's change."""

import shutil
import subprocess
from enum import StrEnum
from pathlib import Path

STARTING_COMMIT = "Start the adops Project"


class GitStart(StrEnum):
    COMMITTED = "committed"
    NO_GIT = "no_git"
    INSIDE_REPOSITORY = "inside_repository"
    COMMIT_FAILED = "commit_failed"


def start_repository(project: Path) -> GitStart:
    """Make `project` a Git repository on main whose one commit holds every file in it.

    A folder already inside a repository is left alone. The commit can fail, most often because
    no Git name and email are set; the repository then exists with its files staged.
    """
    if shutil.which("git") is None:
        return GitStart.NO_GIT
    if _git(project, "rev-parse", "--is-inside-work-tree").returncode == 0:
        return GitStart.INSIDE_REPOSITORY
    for args in (("init", "--quiet", "-b", "main"), ("add", "--all")):
        if _git(project, *args).returncode != 0:
            return GitStart.COMMIT_FAILED
    if _git(project, "commit", "--quiet", "-m", STARTING_COMMIT).returncode != 0:
        return GitStart.COMMIT_FAILED
    return GitStart.COMMITTED


def _git(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=project, capture_output=True, text=True)
