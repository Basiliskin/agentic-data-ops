import os
from pathlib import Path

import pytest

from adops.infrastructure.release_folders import (
    KEEP_NEWEST,
    Layout,
    ReleaseEntry,
    current_release,
    layout,
    prepare_release,
    releases_to_delete,
    select_releases_to_delete,
    switch_current,
)


@pytest.fixture
def server(tmp_path):
    folder = tmp_path / "server"
    folder.mkdir()
    (folder / ".env").write_text("SECRET=shared\n")
    (folder / ".adops").mkdir()
    return folder


@pytest.fixture
def checkout(tmp_path):
    folder = tmp_path / "checkout"
    (folder / "pipelines").mkdir(parents=True)
    (folder / "dbt" / ".adops").mkdir(parents=True)
    (folder / ".git").mkdir()
    (folder / ".adops").mkdir()
    (folder / "adops.yaml").write_text("environments: {}\n")
    (folder / "pipelines" / "orders.yaml").write_text("steps: []\n")
    (folder / "dbt" / ".adops" / "nested.txt").write_text("kept\n")
    (folder / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (folder / ".env").write_text("SECRET=from-checkout\n")
    (folder / ".adops" / "state").write_text("checkout state\n")
    (folder / "big").mkdir()
    (folder / "big" / "data.bin").write_text("not followed\n")
    (folder / "link-to-big").symlink_to("big")
    return folder


def make_release(server, commit):
    (server / "releases" / commit).mkdir(parents=True)


def link_current(server, target):
    (server / "current").symlink_to(target)


def test_layout_old_when_project_files_sit_at_the_root(server):
    (server / "adops.yaml").write_text("environments: {}\n")

    assert layout(server) is Layout.OLD


def test_layout_half_moved_when_releases_exist_without_current(server):
    make_release(server, "abc")

    assert layout(server) is Layout.HALF_MOVED


def test_layout_half_moved_when_current_exists_without_releases(server):
    link_current(server, "releases/abc")

    assert layout(server) is Layout.HALF_MOVED


def test_layout_new_for_a_relative_link_into_releases(server):
    make_release(server, "abc")
    link_current(server, "releases/abc")

    assert layout(server) is Layout.NEW
    assert current_release(server) == "abc"


def test_layout_not_new_for_an_absolute_current_link(server):
    make_release(server, "abc")
    link_current(server, server / "releases" / "abc")

    assert (server / "current").resolve() == (server / "releases" / "abc").resolve()
    assert layout(server) is Layout.HALF_MOVED


def test_layout_not_new_for_a_dangling_current_link(server):
    make_release(server, "abc")
    link_current(server, "releases/missing")

    assert layout(server) is Layout.HALF_MOVED


def test_layout_not_new_when_current_is_a_real_directory(server):
    make_release(server, "abc")
    (server / "current").mkdir()

    assert layout(server) is Layout.HALF_MOVED


@pytest.mark.parametrize("target", ["../elsewhere/abc", "elsewhere/abc", "releases/abc/dbt", "."])
def test_layout_not_new_when_current_points_outside_releases(server, target):
    make_release(server, "abc")
    (server / "releases" / "abc" / "dbt").mkdir()
    (server.parent / "elsewhere" / "abc").mkdir(parents=True)
    (server / "elsewhere" / "abc").mkdir(parents=True)
    link_current(server, target)

    assert layout(server) is Layout.HALF_MOVED


@pytest.mark.parametrize("target", ["releases/..", "releases/abc.tmp"])
def test_layout_not_new_when_current_names_no_release(server, target):
    make_release(server, "abc.tmp")
    link_current(server, target)

    assert layout(server) is Layout.HALF_MOVED


def test_layout_not_new_when_the_release_is_a_link(server, tmp_path):
    (tmp_path / "outside").mkdir()
    (server / "releases").mkdir()
    (server / "releases" / "abc").symlink_to(tmp_path / "outside")
    link_current(server, "releases/abc")

    assert layout(server) is Layout.HALF_MOVED


def test_layout_not_new_when_releases_is_a_link(server, tmp_path):
    (tmp_path / "elsewhere" / "abc").mkdir(parents=True)
    (server / "releases").symlink_to(tmp_path / "elsewhere")
    link_current(server, "releases/abc")

    assert layout(server) is Layout.HALF_MOVED


def test_prepare_copies_the_checkout_without_git_or_shared_state(server, checkout):
    release = prepare_release(server, checkout, "abc")

    assert release == server / "releases" / "abc"
    assert (release / "adops.yaml").read_text() == "environments: {}\n"
    assert (release / "pipelines" / "orders.yaml").read_text() == "steps: []\n"
    assert (release / "dbt" / ".adops" / "nested.txt").read_text() == "kept\n"
    assert not (release / ".git").exists()
    assert not (server / "releases" / "abc.tmp").exists()


def test_prepare_links_shared_state_relatively(server, checkout):
    release = prepare_release(server, checkout, "abc")

    for name in (".env", ".adops"):
        assert (release / name).is_symlink()
        assert os.readlink(release / name) == f"../../{name}"
    assert (release / ".env").read_text() == "SECRET=shared\n"
    assert (release / ".adops").resolve() == (server / ".adops").resolve()


def test_prepare_keeps_symlinks_in_the_checkout_as_links(server, checkout):
    release = prepare_release(server, checkout, "abc")

    assert (release / "link-to-big").is_symlink()
    assert os.readlink(release / "link-to-big") == "big"


def test_prepare_twice_leaves_the_release_unchanged(server, checkout):
    release = prepare_release(server, checkout, "abc")
    before = release.stat()
    (checkout / "adops.yaml").write_text("changed\n")

    assert prepare_release(server, checkout, "abc") == release

    after = release.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert (release / "adops.yaml").read_text() == "environments: {}\n"


def test_prepare_replaces_a_staging_folder_left_by_a_crash(server, checkout):
    leftover = server / "releases" / "abc.tmp"
    leftover.mkdir(parents=True)
    (leftover / "half-copied").write_text("partial\n")

    release = prepare_release(server, checkout, "abc")

    assert not (release / "half-copied").exists()
    assert (release / "adops.yaml").exists()
    assert not leftover.exists()


def test_prepare_replaces_a_staging_link_left_by_a_crash(server, checkout, tmp_path):
    (server / "releases").mkdir()
    (server / "releases" / "abc.tmp").symlink_to(tmp_path / "missing")

    release = prepare_release(server, checkout, "abc")

    assert (release / "adops.yaml").exists()


@pytest.mark.parametrize("commit", ["", ".", "..", "a/b", "abc.tmp"])
def test_prepare_rejects_a_name_that_is_not_a_release(server, checkout, commit):
    with pytest.raises(ValueError, match="Not a release folder name"):
        prepare_release(server, checkout, commit)

    assert not (server / "releases").exists()


def test_switch_points_current_at_the_release_relatively(server):
    make_release(server, "abc")

    switch_current(server, "abc")

    assert os.readlink(server / "current") == "releases/abc"
    assert layout(server) is Layout.NEW


def test_switch_replaces_an_existing_current_link(server):
    make_release(server, "abc")
    make_release(server, "def")
    switch_current(server, "abc")

    switch_current(server, "def")

    assert os.readlink(server / "current") == "releases/def"
    assert not os.path.lexists(server / "current.tmp")


def test_switch_replaces_a_leftover_staging_link(server):
    make_release(server, "abc")
    (server / "current.tmp").symlink_to("releases/gone")

    switch_current(server, "abc")

    assert os.readlink(server / "current") == "releases/abc"
    assert not os.path.lexists(server / "current.tmp")


def test_switch_to_a_missing_release_leaves_current_alone(server):
    make_release(server, "abc")
    switch_current(server, "abc")

    with pytest.raises(FileNotFoundError, match="releases/missing"):
        switch_current(server, "missing")

    assert os.readlink(server / "current") == "releases/abc"


def test_switch_rejects_a_name_that_is_not_a_release(server):
    with pytest.raises(ValueError, match="Not a release folder name"):
        switch_current(server, "../outside")


def entry(name, created, is_dir=True):
    return ReleaseEntry(name, created, is_dir)


def select(entries, *, current=None, in_use=(), untagged=False):
    return select_releases_to_delete(
        entries, current=current, in_use=in_use, untagged_run_in_progress=untagged
    )


def test_cleanup_orders_by_creation_time_not_by_name():
    entries = [entry("fff", 1), entry("eee", 2), entry("ccc", 3), entry("bbb", 4), entry("aaa", 5)]

    assert sorted(select(entries)) == ["eee", "fff"]


def test_cleanup_keeps_everything_when_there_are_at_most_three():
    assert select([entry("a", 1), entry("b", 2), entry("c", 3)]) == []
    assert KEEP_NEWEST == 3


def test_cleanup_keeps_an_in_use_folder_outside_the_newest_three():
    entries = [entry(name, created) for created, name in enumerate("abcdef")]

    assert sorted(select(entries, in_use={"b"})) == ["a", "c"]


def test_cleanup_keeps_the_current_folder_even_when_old():
    entries = [entry(name, created) for created, name in enumerate("abcde")]

    assert select(entries, current="a") == ["b"]


def test_cleanup_deletes_nothing_while_an_untagged_run_is_in_progress():
    entries = [entry(name, created) for created, name in enumerate("abcdef")]

    assert select(entries, in_use={"b"}, untagged=True) == []


def test_cleanup_never_returns_staging_folders_or_files():
    entries = [
        entry("old.tmp", 0),
        entry("note", 0, is_dir=False),
        *(entry(name, created) for created, name in enumerate("abcd", start=1)),
    ]

    assert select(entries) == ["a"]


def test_releases_to_delete_reads_the_server_folder(server):
    for created, commit in enumerate(["fff", "eee", "ddd", "ccc", "bbb", "aaa"], start=1):
        make_release(server, commit)
        (server / "releases" / commit / ".adops").symlink_to("../../.adops")
        os.utime(server / "releases" / commit / ".adops", (created, created), follow_symlinks=False)
    (server / "releases" / "zzz.tmp").mkdir()
    (server / "releases" / "stray-file").write_text("x\n")
    link_current(server, "releases/fff")

    chosen = releases_to_delete(server, {"eee"}, untagged_run_in_progress=False)

    assert chosen == ["ddd"]


def test_releases_to_delete_falls_back_to_the_folder_time_without_a_link(server):
    for created, commit in enumerate(["old", "b", "c", "d"], start=1):
        make_release(server, commit)
        os.utime(server / "releases" / commit, (created, created))

    assert releases_to_delete(server, set(), untagged_run_in_progress=False) == ["old"]


def test_releases_to_delete_with_no_releases_folder(server):
    assert releases_to_delete(server, set(), untagged_run_in_progress=False) == []


def test_released_folders_age_by_their_link_not_by_later_writes(server, checkout):
    first = prepare_release(server, checkout, "first")
    for commit in ("second", "third", "fourth"):
        prepare_release(server, checkout, commit)
    for created, commit in enumerate(["first", "second", "third", "fourth"], start=1):
        link = server / "releases" / commit / ".adops"
        os.utime(link, (created, created), follow_symlinks=False)
    (first / "logs").mkdir()
    os.utime(first, (100, 100))

    assert releases_to_delete(server, set(), untagged_run_in_progress=False) == ["first"]
    assert Path(os.readlink(first / ".adops")) == Path("../../.adops")
