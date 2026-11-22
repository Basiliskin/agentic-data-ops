from datetime import datetime, timedelta
from pathlib import Path

from adops.domain.run_log import is_plain_name, is_run_log_name, run_log_file_name

KEPT_RUN_LOGS = 20


def run_log_dir(project: Path, environment: str) -> Path:
    return project / ".adops" / "logs" / environment


def write_run_log(
    project: Path, environment: str, pipeline: str, timestamp: datetime, text: str
) -> Path | None:
    """Write one Run Log and keep the newest ones; a Run Log problem never fails the run."""
    if not (is_plain_name(environment) and is_plain_name(pipeline)):
        return None
    directory = run_log_dir(project, environment)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        written = _write_new(directory, pipeline, timestamp, text)
        _prune(directory)
    except OSError:
        return None
    return written


def _write_new(directory: Path, pipeline: str, timestamp: datetime, text: str) -> Path:
    while True:
        path = directory / run_log_file_name(timestamp, pipeline)
        try:
            with path.open("x", encoding="utf-8") as file:
                file.write(text)
        except FileExistsError:
            timestamp += timedelta(seconds=1)
            continue
        return path


def _prune(directory: Path) -> None:
    run_logs = sorted(
        (entry for entry in directory.iterdir() if is_run_log_name(entry.name) and entry.is_file()),
        key=lambda entry: entry.name,
        reverse=True,
    )
    for stale in run_logs[KEPT_RUN_LOGS:]:
        try:
            stale.unlink()
        except FileNotFoundError:
            continue
