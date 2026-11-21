import os
from pathlib import Path

import pytest

from adops.domain.assistant_access import (
    OUTSIDE_PROJECT_FILES,
    WRITES_ONLY_UNDER_ROOTS,
    AssistantRefusal,
)
from adops.interface.assistant_files import (
    MAX_READ_BYTES,
    MAX_WRITE_BYTES,
    NOT_A_FILE_TARGET,
    NOT_READABLE_TEXT,
    NOT_WRITABLE_TEXT,
    list_project_files,
    read_project_file,
    resolve_project_file,
    write_project_file,
)
from adops.interface.run_command import ProjectFileError


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    (root / "pipelines").mkdir(parents=True)
    (root / "dbt" / "models").mkdir(parents=True)
    (root / ".adops" / "dlt").mkdir(parents=True)
    (root / "adops.yaml").write_text("environments: {}\n")
    (root / "pipelines" / "a.yaml").write_text("name: a\n")
    (root / "dbt" / "models" / "m.sql").write_text("select 1\n")
    (root / ".env").write_text("DOTENV_ONLY=dotenv-secret\n")
    (root / ".adops" / "dlt" / "state.json").write_text("{}")
    (tmp_path / "outside.txt").write_text("outside text")
    (tmp_path / "outside_dir").mkdir()
    (tmp_path / "outside_dir" / "x.yaml").write_text("outside dir")
    return root


def link(project: Path, relative: str, target: Path) -> None:
    (project / relative).symlink_to(target)


def refused(project: Path, relative: str) -> None:
    with pytest.raises(AssistantRefusal) as caught:
        resolve_project_file(project, relative)
    assert str(caught.value) == OUTSIDE_PROJECT_FILES


def test_an_allowed_file_resolves_inside_the_project(project):
    assert resolve_project_file(project, "pipelines/a.yaml") == (
        project / "pipelines" / "a.yaml"
    ).resolve()


def test_a_symlink_to_dotenv_is_refused(project):
    link(project, "pipelines/env.yaml", project / ".env")

    refused(project, "pipelines/env.yaml")


def test_a_symlink_to_a_file_outside_the_project_is_refused(project):
    link(project, "pipelines/out.yaml", project.parent / "outside.txt")

    refused(project, "pipelines/out.yaml")


def test_a_symlink_to_a_directory_outside_the_project_is_refused(project):
    link(project, "pipelines/out", project.parent / "outside_dir")

    refused(project, "pipelines/out/x.yaml")
    refused(project, "pipelines/out")


def test_a_symlink_landing_in_adops_state_is_refused(project):
    link(project, "dbt/state", project / ".adops" / "dlt")

    refused(project, "dbt/state/state.json")


def test_a_symlink_to_an_allowed_file_inside_the_project_is_read(project):
    link(project, "pipelines/alias.yaml", project / "dbt" / "models" / "m.sql")

    assert read_project_file(project, "pipelines/alias.yaml", []) == "select 1\n"


def test_dotenv_and_a_parent_path_are_refused(project):
    refused(project, ".env")
    refused(project, "../outside.txt")


def test_reading_redacts_every_secret_value(project):
    (project / "pipelines" / "a.yaml").write_text("token: tok-123 and pw-456\n")

    text = read_project_file(project, "pipelines/a.yaml", ["tok-123", "pw-456"])

    assert text == "token: *** and ***\n"


def test_reading_a_missing_file_names_the_supplied_path(project):
    with pytest.raises(ProjectFileError) as caught:
        read_project_file(project, "pipelines/nope.yaml", [])
    assert str(caught.value) == "pipelines/nope.yaml: file not found"


def test_reading_a_directory_is_a_missing_file(project):
    with pytest.raises(ProjectFileError):
        read_project_file(project, "dbt/models", [])


def test_reading_a_binary_file_is_refused(project):
    (project / "pipelines" / "b.yaml").write_bytes(b"\xff\xfe\x00")

    with pytest.raises(AssistantRefusal) as caught:
        read_project_file(project, "pipelines/b.yaml", [])
    assert str(caught.value) == NOT_READABLE_TEXT


def test_reading_an_oversized_file_is_refused(project):
    (project / "dbt" / "big.sql").write_text("x" * (MAX_READ_BYTES + 1))

    with pytest.raises(AssistantRefusal) as caught:
        read_project_file(project, "dbt/big.sql", [])
    assert str(caught.value) == NOT_READABLE_TEXT


def test_a_file_of_exactly_the_cap_is_read(project):
    (project / "dbt" / "cap.sql").write_text("x" * MAX_READ_BYTES)

    assert len(read_project_file(project, "dbt/cap.sql", [])) == MAX_READ_BYTES


def test_listing_shows_only_allowed_files(project):
    (project / "pipelines" / ".env").write_text("X=1")
    (project / "pipelines" / "nested").mkdir()
    (project / "pipelines" / "nested" / "b.yaml").write_text("name: b\n")
    link(project, "pipelines/env.yaml", project / ".env")
    link(project, "pipelines/out", project.parent / "outside_dir")
    link(project, "pipelines/alias.yaml", project / "dbt" / "models" / "m.sql")

    assert list_project_files(project) == [
        "adops.yaml",
        "dbt/models/m.sql",
        "pipelines/a.yaml",
        "pipelines/alias.yaml",
        "pipelines/nested/b.yaml",
    ]


def test_listing_one_directory(project):
    assert list_project_files(project, "dbt") == ["dbt/models/m.sql"]


def test_listing_without_pipelines_or_dbt_shows_what_exists(tmp_path):
    (tmp_path / "adops.yaml").write_text("x")

    assert list_project_files(tmp_path) == ["adops.yaml"]


def test_listing_a_missing_directory_names_it(project):
    with pytest.raises(ProjectFileError) as caught:
        list_project_files(project, "dbt/nope")
    assert str(caught.value) == "dbt/nope: file not found"


def test_listing_a_refused_directory_is_refused(project):
    with pytest.raises(AssistantRefusal):
        list_project_files(project, ".adops")


def test_a_symlink_loop_is_refused_and_skipped_by_listing(project):
    link(project, "pipelines/loop", project / "pipelines" / "loop")

    refused(project, "pipelines/loop")
    assert list_project_files(project, "pipelines") == ["pipelines/a.yaml"]


def tree(root: Path) -> dict[str, object]:
    """Every folder, file (with its bytes) and symlink (with its target), links not followed."""
    found: dict[str, object] = {}
    for folder, folders, files in os.walk(root):
        for name in folders + files:
            path = Path(folder) / name
            key = path.relative_to(root).as_posix()
            if path.is_symlink():
                found[key] = ("link", os.readlink(path))
            elif path.is_dir():
                found[key] = "dir"
            else:
                found[key] = path.read_bytes()
    return found


def write_refused(project: Path, relative: str, content: str = "x: 1\n") -> str:
    before = tree(project.parent)
    with pytest.raises(AssistantRefusal) as caught:
        write_project_file(project, relative, content)
    assert tree(project.parent) == before
    return str(caught.value)


def test_writing_a_new_file_creates_it_and_its_folders(project):
    created = write_project_file(project, "dbt/models/new/x.sql", "select 2\n")

    assert created is True
    assert (project / "dbt" / "models" / "new" / "x.sql").read_text() == "select 2\n"


def test_writing_creates_a_missing_root_folder(tmp_path):
    (tmp_path / "adops.yaml").write_text("x")

    assert write_project_file(tmp_path, "pipelines/a.yaml", "name: a\n") is True
    assert (tmp_path / "pipelines" / "a.yaml").read_text() == "name: a\n"


def test_writing_an_existing_file_overwrites_it(project):
    created = write_project_file(project, "pipelines/a.yaml", "name: b\n")

    assert created is False
    assert (project / "pipelines" / "a.yaml").read_text() == "name: b\n"


def test_writing_keeps_the_text_exactly_as_utf8(project):
    write_project_file(project, "pipelines/a.yaml", "name: \u00e4\r\n")

    assert (project / "pipelines" / "a.yaml").read_bytes() == "name: \u00e4\r\n".encode()


def test_writing_through_a_symlink_to_an_allowed_file_writes_the_real_file(project):
    link(project, "pipelines/alias.sql", project / "dbt" / "models" / "m.sql")

    assert write_project_file(project, "pipelines/alias.sql", "select 3\n") is False
    assert (project / "dbt" / "models" / "m.sql").read_text() == "select 3\n"
    assert (project / "pipelines" / "alias.sql").is_symlink()


@pytest.mark.parametrize("relative", ["adops.yaml", "pipelines", "dbt"])
def test_writing_adops_yaml_or_a_root_folder_is_refused_as_read_only(project, relative):
    assert write_refused(project, relative) == WRITES_ONLY_UNDER_ROOTS


@pytest.mark.parametrize(
    "relative",
    [".env", ".ENV", "pipelines/.env", ".adops/dlt/state.json", "server/x", "../outside.txt"],
)
def test_writing_outside_pipelines_and_dbt_is_refused(project, relative):
    assert write_refused(project, relative) == OUTSIDE_PROJECT_FILES


def test_a_symlinked_file_pointing_outside_is_not_written_through(project):
    link(project, "pipelines/out.yaml", project.parent / "outside.txt")

    assert write_refused(project, "pipelines/out.yaml") == OUTSIDE_PROJECT_FILES
    assert (project.parent / "outside.txt").read_text() == "outside text"


def test_a_dangling_symlink_pointing_outside_is_not_written_through(project):
    link(project, "pipelines/dangling.yaml", project.parent / "created_outside.txt")

    assert write_refused(project, "pipelines/dangling.yaml") == OUTSIDE_PROJECT_FILES
    assert not (project.parent / "created_outside.txt").exists()


@pytest.mark.parametrize("relative", ["pipelines/linkdir/x.yaml", "pipelines/linkdir/new/y.yaml"])
def test_a_symlinked_parent_folder_pointing_outside_is_refused(project, relative):
    link(project, "pipelines/linkdir", project.parent / "outside_dir")

    assert write_refused(project, relative) == OUTSIDE_PROJECT_FILES
    assert (project.parent / "outside_dir" / "x.yaml").read_text() == "outside dir"


def test_a_symlink_to_adops_yaml_is_refused(project):
    link(project, "dbt/settings.yaml", project / "adops.yaml")

    assert write_refused(project, "dbt/settings.yaml") == WRITES_ONLY_UNDER_ROOTS


def test_a_symlink_to_dotenv_is_refused_for_writing(project):
    link(project, "pipelines/env.yaml", project / ".env")

    assert write_refused(project, "pipelines/env.yaml") == OUTSIDE_PROJECT_FILES


def test_a_symlinked_folder_landing_in_adops_state_is_refused(project):
    link(project, "dbt/state", project / ".adops" / "dlt")

    assert write_refused(project, "dbt/state/new.json") == OUTSIDE_PROJECT_FILES


def test_a_symlink_loop_is_refused_for_writing(project):
    link(project, "pipelines/loop", project / "pipelines" / "loop")

    assert write_refused(project, "pipelines/loop/x.yaml") == OUTSIDE_PROJECT_FILES


def test_a_directory_target_is_refused(project):
    assert write_refused(project, "dbt/models") == NOT_A_FILE_TARGET


def test_a_file_in_place_of_a_parent_folder_is_refused(project):
    assert write_refused(project, "pipelines/a.yaml/new/x.yaml") == NOT_A_FILE_TARGET


def test_content_above_the_cap_is_refused_and_nothing_is_created(project):
    content = "x" * (MAX_WRITE_BYTES + 1)

    assert write_refused(project, "pipelines/new/big.yaml", content) == NOT_WRITABLE_TEXT
    assert not (project / "pipelines" / "new").exists()


def test_content_above_the_cap_does_not_truncate_an_existing_file(project):
    assert write_refused(project, "pipelines/a.yaml", "\u00e4" * MAX_WRITE_BYTES) == (
        NOT_WRITABLE_TEXT
    )
    assert (project / "pipelines" / "a.yaml").read_text() == "name: a\n"


def test_content_of_exactly_the_cap_is_written_and_reads_back(project):
    write_project_file(project, "dbt/cap.sql", "x" * MAX_WRITE_BYTES)

    assert len(read_project_file(project, "dbt/cap.sql", [])) == MAX_WRITE_BYTES


def test_content_that_is_not_encodable_text_is_refused(project):
    assert write_refused(project, "pipelines/a.yaml", "bad \ud800") == NOT_WRITABLE_TEXT


def test_a_failing_write_names_the_normalized_path_only(project, monkeypatch):
    def deny(*_):
        raise PermissionError("denied: /very/private/path")

    monkeypatch.setattr(Path, "write_bytes", deny)

    with pytest.raises(ProjectFileError) as caught:
        write_project_file(project, "pipelines\\a.yaml", "name: b\n")
    assert str(caught.value) == "pipelines/a.yaml: could not be written"
