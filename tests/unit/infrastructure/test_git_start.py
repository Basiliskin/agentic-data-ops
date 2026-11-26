import os
import subprocess

import pytest

from adops.infrastructure import git_start
from adops.infrastructure.git_start import STARTING_COMMIT, GitStart, start_repository

IDENTITY = {
    "GIT_AUTHOR_NAME": "adops",
    "GIT_AUTHOR_EMAIL": "adops@example.com",
    "GIT_COMMITTER_NAME": "adops",
    "GIT_COMMITTER_EMAIL": "adops@example.com",
}


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch):
    """No user or system Git settings, so results never depend on this machine."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for name, value in IDENTITY.items():
        monkeypatch.setenv(name, value)


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def project(tmp_path):
    folder = tmp_path / "project"
    (folder / "dbt").mkdir(parents=True)
    (folder / "adops.yaml").write_text("environments: {}\n")
    (folder / "dbt" / "model.sql").write_text("select 1\n")
    (folder / ".gitignore").write_text(".env\n")
    (folder / ".env").write_text("SECRET=do-not-save\n")
    return folder


def test_a_new_folder_gets_one_starting_commit_on_main_with_every_file(project):
    assert start_repository(project) is GitStart.COMMITTED

    assert git("branch", "--show-current", cwd=project) == "main"
    assert git("log", "--format=%s", cwd=project) == STARTING_COMMIT
    saved = git("ls-files", cwd=project).splitlines()
    assert saved == [".gitignore", "adops.yaml", "dbt/model.sql"]
    assert git("status", "--porcelain", cwd=project) == ""


def test_a_folder_inside_a_repository_is_left_alone(tmp_path, project):
    git("init", "--quiet", "-b", "main", str(tmp_path), cwd=tmp_path)

    assert start_repository(project) is GitStart.INSIDE_REPOSITORY
    assert not (project / ".git").exists()


def test_without_git_installed_nothing_is_created(project, monkeypatch):
    monkeypatch.setattr(git_start.shutil, "which", lambda name: None)

    assert start_repository(project) is GitStart.NO_GIT
    assert not (project / ".git").exists()


def test_a_commit_git_refuses_leaves_the_repository_with_no_history(project, monkeypatch):
    monkeypatch.setenv("GIT_AUTHOR_NAME", "")

    assert start_repository(project) is GitStart.COMMIT_FAILED
    assert (project / ".git").is_dir()
    unborn = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=project)
    assert unborn.returncode != 0


def test_a_failing_init_stops_before_staging(project, monkeypatch):
    calls = []

    def failing_init(folder, *args):
        calls.append(args[0])
        # Not inside a repository yet, and `git init` fails.
        return subprocess.CompletedProcess(args, returncode=0 if args[0] == "add" else 1)

    monkeypatch.setattr(git_start, "_git", failing_init)

    assert start_repository(project) is GitStart.COMMIT_FAILED
    assert calls == ["rev-parse", "init"]
