import os
from collections.abc import Iterable
from pathlib import Path

from adops.application.secrets import redact_secret_values
from adops.domain.assistant_access import (
    OUTSIDE_PROJECT_FILES,
    PROJECT_FILE_ROOTS,
    SETTINGS_FILE_NAME,
    AssistantRefusal,
    allowed_project_path,
)
from adops.interface.run_command import ProjectFileError

MAX_READ_BYTES = 256 * 1024
NOT_READABLE_TEXT = (
    f"the Assistant reads only UTF-8 text files of at most {MAX_READ_BYTES // 1024} KiB"
)
# Equal to the read cap, so the Assistant can always read back what it wrote.
MAX_WRITE_BYTES = MAX_READ_BYTES
NOT_WRITABLE_TEXT = f"the Assistant writes only UTF-8 text of at most {MAX_WRITE_BYTES // 1024} KiB"
NOT_A_FILE_TARGET = (
    "the Assistant writes only files; the path or one of its folders is not a file or folder "
    "it can write"
)


def resolve_project_file(project: Path, relative: str, *, write: bool = False) -> Path:
    """The resolved path, refused unless it and whatever it resolves to are Project files.

    Checking the resolved path again is what stops a symlink inside pipelines/ or dbt/ from
    reaching .env, .adops/ or anything outside the Project. Every part of the path is resolved,
    including symlinked folders and a dangling symlink at the end.
    """
    root = project.resolve()
    try:
        resolved = (root / allowed_project_path(relative, write=write)).resolve()
    except (RuntimeError, OSError):
        raise AssistantRefusal(OUTSIDE_PROJECT_FILES) from None
    if not resolved.is_relative_to(root):
        raise AssistantRefusal(OUTSIDE_PROJECT_FILES)
    allowed_project_path(resolved.relative_to(root).as_posix(), write=write)
    return resolved


def list_project_files(project: Path, directory: str = "") -> list[str]:
    """Project-relative paths of the regular files the Assistant may read, sorted."""
    if directory:
        start = allowed_project_path(directory)
        if not resolve_project_file(project, start).exists():
            raise ProjectFileError(f"{directory}: file not found")
        starts = [start]
    else:
        starts = [SETTINGS_FILE_NAME, *PROJECT_FILE_ROOTS]
    found = {path for start in starts for path in _candidates(project, start)}
    return sorted(path for path in found if _is_readable_file(project, path))


def read_project_file(project: Path, relative: str, secret_values: Iterable[str]) -> str:
    path = resolve_project_file(project, relative)
    if not path.is_file():
        raise ProjectFileError(f"{relative}: file not found")
    with path.open("rb") as file:
        data = file.read(MAX_READ_BYTES + 1)
    if len(data) > MAX_READ_BYTES:
        raise AssistantRefusal(NOT_READABLE_TEXT)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise AssistantRefusal(NOT_READABLE_TEXT) from None
    return redact_secret_values(text, secret_values)


def write_project_file(project: Path, relative: str, content: str) -> bool:
    """Write the whole file as UTF-8 text, creating missing folders; True when it is new.

    Every refusal happens before anything on disk changes.
    """
    try:
        data = content.encode("utf-8")
    except UnicodeEncodeError:
        raise AssistantRefusal(NOT_WRITABLE_TEXT) from None
    if len(data) > MAX_WRITE_BYTES:
        raise AssistantRefusal(NOT_WRITABLE_TEXT)
    target = resolve_project_file(project, relative, write=True)
    if target.exists() and not target.is_file():
        raise AssistantRefusal(NOT_A_FILE_TARGET)
    if not _deepest_existing(target.parent).is_dir():
        raise AssistantRefusal(NOT_A_FILE_TARGET)
    created = not target.exists()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    except OSError:
        raise ProjectFileError(
            f"{allowed_project_path(relative, write=True)}: could not be written"
        ) from None
    return created


def _deepest_existing(path: Path) -> Path:
    while not path.exists():
        path = path.parent
    return path


def _candidates(project: Path, start: str) -> Iterable[str]:
    yield start
    # Symlinked directories are not followed; a symlinked file is checked like any other.
    for folder, _, files in os.walk(project / start):
        relative = Path(folder).relative_to(project).as_posix()
        yield from (f"{relative}/{name}" for name in files)


def _is_readable_file(project: Path, relative: str) -> bool:
    try:
        return resolve_project_file(project, relative).is_file()
    except AssistantRefusal:
        return False
