import hashlib
import os
import re
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

import pytest
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.remote_origin import RegisteredCodeLocationOrigin, RemoteRepositoryOrigin
from dagster._core.test_utils import create_run_for_test
from typer.testing import CliRunner

from adops.infrastructure.dagster_server import RELEASE_TAG, in_use_release_folders
from adops.infrastructure.release_folders import current_release, prepare_release, switch_current
from adops.interface import cli
from adops.interface.release_command import (
    CLEANUP_PAUSED,
    CONFIG_SECTION,
    MOVE_SECTION,
    PLAYBOOK,
)

runner = CliRunner()

REPO = Path(__file__).parents[3]
COMPOSE = "services: {}\n"
SERVER_CONFIG = "storage: {}\n"
# Dagster stores a queued run only with the code location it will launch from.
ORIGIN = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("server"), "repo").get_job_origin(
    "shop_sync"
)


@pytest.fixture(autouse=True)
def dagster_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    yield home


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    path = tmp_path / "checkout"
    (path / "server").mkdir(parents=True)
    (path / ".git").mkdir()
    (path / "docker-compose.yml").write_text(COMPOSE)
    (path / "server" / "dagster.yaml").write_text(SERVER_CONFIG)
    (path / "adops.yaml").write_text("version: 1\n")
    return path


@pytest.fixture
def old_server(tmp_path: Path) -> Path:
    path = tmp_path / "server_folder"
    (path / "server").mkdir(parents=True)
    (path / ".adops").mkdir()
    (path / ".adops" / "state").write_text("cursor\n")
    (path / ".env").write_text("SECRET=1\n")
    (path / "docker-compose.yml").write_text(COMPOSE)
    (path / "server" / "dagster.yaml").write_text(SERVER_CONFIG)
    return path


@pytest.fixture
def server(old_server: Path, checkout: Path) -> Path:
    add_release(old_server, checkout, "first", age=100)
    switch_current(old_server, "first")
    return old_server


def add_release(server: Path, checkout: Path, name: str, *, age: int) -> None:
    prepare_release(server, checkout, name)
    stamp = 1_700_000_000 - age
    os.utime(server / "releases" / name / ".adops", (stamp, stamp), follow_symlinks=False)


def add_run(status: DagsterRunStatus, tag: str | None = None) -> None:
    tags = {} if tag is None else {RELEASE_TAG: tag}
    with DagsterInstance.get() as instance:
        create_run_for_test(
            instance, job_name="shop_sync", status=status, tags=tags, remote_job_origin=ORIGIN
        )


def tag_for(server: Path, name: str) -> str:
    return str(server.resolve() / "releases" / name)


def release(server: Path, checkout: Path, commit: str) -> tuple[int, str, str]:
    result = runner.invoke(
        cli.app,
        [
            "server-release",
            "--server-dir",
            str(server),
            "--from",
            str(checkout),
            "--commit",
            commit,
        ],
    )
    return result.exit_code, result.stdout, result.stderr


def releases(server: Path) -> set[str]:
    return {path.name for path in (server / "releases").iterdir()}


def snapshot(root: Path) -> dict[str, tuple[str, str]]:
    tree: dict[str, tuple[str, str]] = {}
    for directory, dirs, files in os.walk(root):
        for name in dirs + files:
            path = Path(directory) / name
            key = str(path.relative_to(root))
            if path.is_symlink():
                tree[key] = ("link", os.readlink(path))
            elif path.is_dir():
                tree[key] = ("dir", "")
            else:
                tree[key] = ("file", hashlib.sha256(path.read_bytes()).hexdigest())
    return tree


def assert_refused_unchanged(server: Path, checkout: Path, *expected: str) -> None:
    before = snapshot(server)

    code, out, err = release(server, checkout, "next")

    assert code != 0
    assert out == ""
    for text in (*expected, PLAYBOOK, "Nothing was changed."):
        assert text in err
    assert snapshot(server) == before


def test_the_old_layout_is_refused_without_changing_a_file(old_server, checkout):
    assert_refused_unchanged(old_server, checkout, "old layout", MOVE_SECTION)


def test_a_releases_folder_without_current_is_half_moved(old_server, checkout):
    (old_server / "releases" / "abc").mkdir(parents=True)

    assert_refused_unchanged(old_server, checkout, "half moved", MOVE_SECTION)


def test_an_absolute_current_link_is_half_moved(old_server, checkout):
    (old_server / "releases" / "abc").mkdir(parents=True)
    (old_server / "current").symlink_to(old_server / "releases" / "abc")

    assert_refused_unchanged(old_server, checkout, "half moved", MOVE_SECTION)


def test_a_changed_docker_compose_is_refused_without_changing_a_file(server, checkout):
    (checkout / "docker-compose.yml").write_text("services: {daemon: {}}\n")

    assert_refused_unchanged(server, checkout, "docker-compose.yml", CONFIG_SECTION)


def test_a_changed_server_yaml_is_refused(server, checkout):
    (checkout / "server" / "dagster.yaml").write_text("storage: {postgres: {}}\n")

    assert_refused_unchanged(server, checkout, "server/dagster.yaml", CONFIG_SECTION)


def test_an_added_server_yaml_is_refused(server, checkout):
    (checkout / "server" / "workspace.yaml").write_text("load_from: []\n")

    assert_refused_unchanged(server, checkout, "server/workspace.yaml", CONFIG_SECTION)


def test_a_removed_server_yaml_is_refused(server, checkout):
    (server / "server" / "workspace.yaml").write_text("load_from: []\n")

    assert_refused_unchanged(server, checkout, "server/workspace.yaml", CONFIG_SECTION)


def test_a_removed_docker_compose_is_refused(server, checkout):
    (checkout / "docker-compose.yml").unlink()

    assert_refused_unchanged(server, checkout, "docker-compose.yml", CONFIG_SECTION)


def test_a_commit_that_is_not_a_folder_name_is_refused(server, checkout):
    before = snapshot(server)

    code, _, err = release(server, checkout, "..")

    assert code != 0
    assert "Not a release folder name" in err
    assert snapshot(server) == before


def test_a_deploy_switches_current_and_keeps_newest_three_current_and_in_use(server, checkout):
    for age, name in enumerate(["r4", "r3", "r2", "r1"], start=1):
        add_release(server, checkout, name, age=age * 10)
    add_run(DagsterRunStatus.STARTED, tag_for(server, "r1"))
    add_run(DagsterRunStatus.QUEUED, tag_for(server, "r2"))
    add_run(DagsterRunStatus.SUCCESS, tag_for(server, "first"))

    code, out, err = release(server, checkout, "new")

    assert (code, err) == (0, "")
    assert current_release(server) == "new"
    assert out.splitlines() == ["current: releases/new", "deleted: releases/first"]
    assert releases(server) == {"new", "r4", "r3", "r2", "r1"}


def test_deleting_a_release_keeps_the_shared_state_it_links_to(server, checkout):
    for age, name in enumerate(["r3", "r2", "r1"], start=1):
        add_release(server, checkout, name, age=age * 10)

    code, out, _ = release(server, checkout, "new")

    assert code == 0
    assert "deleted: releases/first" in out
    assert (server / ".adops" / "state").read_text() == "cursor\n"
    assert (server / ".env").read_text() == "SECRET=1\n"


def test_the_folder_current_named_before_the_switch_is_kept_while_a_run_uses_it(server, checkout):
    for age, name in enumerate(["r3", "r2", "r1"], start=1):
        add_release(server, checkout, name, age=age * 10)
    add_run(DagsterRunStatus.STARTED, tag_for(server, "first"))

    code, out, _ = release(server, checkout, "new")

    assert code == 0
    assert out.splitlines() == ["current: releases/new", "deleted: releases/r1"]
    assert releases(server) == {"new", "r3", "r2", "first"}


def test_an_untagged_run_in_progress_pauses_cleanup(server, checkout):
    for age, name in enumerate(["r3", "r2", "r1"], start=1):
        add_release(server, checkout, name, age=age * 10)
    add_run(DagsterRunStatus.STARTED)

    code, out, _ = release(server, checkout, "new")

    assert code == 0
    assert out.splitlines() == ["current: releases/new", CLEANUP_PAUSED]
    assert releases(server) == {"new", "r3", "r2", "r1", "first"}


def test_a_branch_run_outside_releases_protects_nothing(server, checkout, tmp_path):
    for age, name in enumerate(["r3", "r2", "r1"], start=1):
        add_release(server, checkout, name, age=age * 10)
    add_run(DagsterRunStatus.STARTED, str(tmp_path / "pr_checkout" / "first"))

    code, out, _ = release(server, checkout, "new")

    assert code == 0
    assert "deleted: releases/first" in out


def test_deploying_the_same_commit_twice_succeeds_and_keeps_current(server, checkout):
    assert release(server, checkout, "new")[0] == 0
    before = snapshot(server)

    code, out, _ = release(server, checkout, "new")

    assert code == 0
    assert out.splitlines() == ["current: releases/new"]
    assert current_release(server) == "new"
    assert snapshot(server) == before


def test_in_use_release_folders_reads_every_unfinished_run(server, tmp_path):
    for status, name in [
        (DagsterRunStatus.STARTED, "a"),
        (DagsterRunStatus.STARTING, "b"),
        (DagsterRunStatus.QUEUED, "c"),
        (DagsterRunStatus.SUCCESS, "d"),
        (DagsterRunStatus.FAILURE, "e"),
    ]:
        add_run(status, tag_for(server, name))
    add_run(DagsterRunStatus.STARTED, str(tmp_path / "elsewhere" / "f"))

    with DagsterInstance.get() as instance:
        in_use = in_use_release_folders(instance, PurePosixPath(server.resolve() / "releases"))

    assert in_use.names == {"a", "b", "c"}
    assert not in_use.untagged_run_in_progress


def test_in_use_release_folders_flags_an_untagged_run_in_progress(server):
    add_run(DagsterRunStatus.SUCCESS)
    with DagsterInstance.get() as instance:
        assert not in_use_release_folders(instance, PurePosixPath("/r")).untagged_run_in_progress

    add_run(DagsterRunStatus.STARTED)

    with DagsterInstance.get() as instance:
        assert in_use_release_folders(instance, PurePosixPath("/r")).untagged_run_in_progress


def test_stop_message_anchors_are_headings_in_the_promotion_playbook():
    headings = re.findall(r"^## (.+)$", (REPO / PLAYBOOK).read_text(), flags=re.MULTILINE)

    assert MOVE_SECTION in headings
    assert CONFIG_SECTION in headings


def test_help_lists_the_options():
    result = runner.invoke(cli.app, ["server-release", "--help"])

    for option in ("--server-dir", "--from", "--commit"):
        assert option in result.stdout
