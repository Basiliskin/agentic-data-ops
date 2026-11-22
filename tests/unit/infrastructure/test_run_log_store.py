from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from adops.infrastructure import run_log_store
from adops.infrastructure.run_log_store import KEPT_RUN_LOGS, run_log_dir, write_run_log

STARTED = datetime(2026, 11, 21, 14, 30, 5, tzinfo=UTC)


def names(directory: Path) -> list[str]:
    return sorted(entry.name for entry in directory.iterdir())


def test_run_log_dir_is_per_environment(tmp_path):
    assert run_log_dir(tmp_path, "dev") == tmp_path / ".adops" / "logs" / "dev"


def test_write_creates_the_run_log_with_its_text(tmp_path):
    written = write_run_log(tmp_path, "dev", "shop_sync", STARTED, "Pipeline shop_sync: ok\n")

    assert written == tmp_path / ".adops/logs/dev/20261121T143005Z-shop_sync.log"
    assert written.read_text(encoding="utf-8") == "Pipeline shop_sync: ok\n"


def test_same_second_runs_get_the_next_free_second_and_never_overwrite(tmp_path):
    first = write_run_log(tmp_path, "dev", "shop_sync", STARTED, "first")
    second = write_run_log(tmp_path, "dev", "shop_sync", STARTED, "second")
    third = write_run_log(tmp_path, "dev", "shop_sync", STARTED, "third")

    assert [first.name, second.name, third.name] == [
        "20261121T143005Z-shop_sync.log",
        "20261121T143006Z-shop_sync.log",
        "20261121T143007Z-shop_sync.log",
    ]
    assert first.read_text(encoding="utf-8") == "first"
    assert second.read_text(encoding="utf-8") == "second"


def test_the_21st_run_log_deletes_the_oldest(tmp_path):
    for minute in range(KEPT_RUN_LOGS + 1):
        write_run_log(tmp_path, "dev", "shop_sync", STARTED + timedelta(minutes=minute), "x")

    kept = names(run_log_dir(tmp_path, "dev"))
    assert len(kept) == KEPT_RUN_LOGS
    assert "20261121T143005Z-shop_sync.log" not in kept
    assert kept[0] == "20261121T143105Z-shop_sync.log"


def test_pruning_orders_by_name_not_by_write_order(tmp_path):
    for minute in range(1, KEPT_RUN_LOGS + 1):
        write_run_log(tmp_path, "dev", "a", STARTED + timedelta(minutes=minute), "x")
    write_run_log(tmp_path, "dev", "a", STARTED, "older run written last")

    kept = names(run_log_dir(tmp_path, "dev"))
    assert len(kept) == KEPT_RUN_LOGS
    assert "20261121T143005Z-a.log" not in kept


def test_pruning_leaves_other_files_and_other_environments_alone(tmp_path):
    directory = run_log_dir(tmp_path, "dev")
    directory.mkdir(parents=True)
    (directory / "notes.txt").write_text("keep")
    (directory / "debug.log").write_text("keep")
    (directory / "20200101T000000Z-zzz.log").mkdir()
    write_run_log(tmp_path, "stage", "a", STARTED - timedelta(days=1), "other environment")

    for minute in range(KEPT_RUN_LOGS + 5):
        write_run_log(tmp_path, "dev", "a", STARTED + timedelta(minutes=minute), "x")

    kept = names(directory)
    assert {"notes.txt", "debug.log", "20200101T000000Z-zzz.log"} <= set(kept)
    assert len(kept) == KEPT_RUN_LOGS + 3
    assert names(run_log_dir(tmp_path, "stage")) == ["20261120T143005Z-a.log"]


def test_a_run_log_vanishing_during_pruning_is_tolerated(tmp_path, monkeypatch):
    for minute in range(KEPT_RUN_LOGS):
        write_run_log(tmp_path, "dev", "a", STARTED + timedelta(minutes=minute), "x")

    def vanished(self):
        raise FileNotFoundError(self)

    monkeypatch.setattr(Path, "unlink", vanished)
    written = write_run_log(tmp_path, "dev", "a", STARTED + timedelta(hours=1), "x")

    assert written == run_log_dir(tmp_path, "dev") / "20261121T153005Z-a.log"


def test_a_prune_permission_error_returns_none(tmp_path, monkeypatch):
    for minute in range(KEPT_RUN_LOGS):
        write_run_log(tmp_path, "dev", "a", STARTED + timedelta(minutes=minute), "x")

    def root_owned(self):
        raise PermissionError(self)

    monkeypatch.setattr(Path, "unlink", root_owned)

    assert write_run_log(tmp_path, "dev", "a", STARTED + timedelta(hours=1), "x") is None


def test_a_mkdir_failure_returns_none(tmp_path, monkeypatch):
    def refused(self, *args, **kwargs):
        raise PermissionError(self)

    monkeypatch.setattr(Path, "mkdir", refused)

    assert write_run_log(tmp_path, "dev", "a", STARTED, "x") is None


def test_an_unwritable_environment_directory_returns_none(tmp_path):
    directory = run_log_dir(tmp_path, "dev")
    directory.mkdir(parents=True)
    directory.chmod(0o500)
    try:
        assert write_run_log(tmp_path, "dev", "a", STARTED, "x") is None
    finally:
        directory.chmod(0o700)


def test_a_write_failure_returns_none(tmp_path, monkeypatch):
    def disk_full(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(run_log_store.Path, "open", disk_full)

    assert write_run_log(tmp_path, "dev", "a", STARTED, "x") is None


@pytest.mark.parametrize(
    ("environment", "pipeline"),
    [("../x", "a"), ("Dev", "a"), ("", "a"), ("dev", "../a"), ("dev", "Shop"), ("dev", "a/b")],
)
def test_unsafe_names_write_nothing(tmp_path, environment, pipeline):
    assert write_run_log(tmp_path, environment, pipeline, STARTED, "x") is None
    assert list(tmp_path.iterdir()) == []
