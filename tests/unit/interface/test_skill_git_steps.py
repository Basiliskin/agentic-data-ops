from collections.abc import Callable, Sequence
from pathlib import Path

import pytest
from git_harness import git
from typer.testing import CliRunner

from adops.interface import cli
from adops.interface.validate_command import validate_project

runner = CliRunner()

TEMPLATES = Path(__file__).parents[3] / "src" / "adops" / "templates"
REPLAYED_SKILLS = ("adops-share", "adops-breaking-change")
SKILL_GIT_SEQUENCE = (
    "git checkout -b",
    "git add",
    "git commit",
    "git push -u origin",
    "gh pr create",
)

SHARED_PATHS = ("pipelines/", "dbt/", "adops.yaml")
RECORD = "pipelines/accepted/20261123T000000Z-shop_sync-orders.yaml"
RECORD_TEXT = "pipeline: shop_sync\ntable: orders\nchanges:\n  - customer_id dropped\n"
LOCAL_ONLY = (".env", ".adops", "notes.txt")


def skill_text(template: str, skill: str) -> str:
    return (TEMPLATES / template / ".claude" / "skills" / skill / "SKILL.md").read_text()


def out_of_order_command(text: str, sequence: Sequence[str]) -> str | None:
    """Return the first command not found after the one before it, starting at the branch."""
    position = text.find(sequence[0])
    if position < 0:
        return sequence[0]
    for command in sequence[1:]:
        position = text.find(command, position)
        if position < 0:
            return command
    return None


def replay(
    work: Path,
    branch: str,
    paths: Sequence[str],
    message: str,
    after_branch: Callable[[], None],
) -> None:
    steps = {
        "git checkout -b": lambda: [("checkout", "--quiet", "-b", branch)],
        "git add": lambda: [("add", "--", path) for path in paths if (work / path).exists()],
        "git commit": lambda: [("commit", "--quiet", "-m", message)],
        "git push -u origin": lambda: [("push", "--quiet", "-u", "origin", branch)],
        # Opening the pull request needs GitHub; only the Skill text is checked for it.
        "gh pr create": lambda: [],
    }
    for command in SKILL_GIT_SEQUENCE:
        for args in steps[command]():
            git(*args, cwd=work)
        if command == "git checkout -b":
            after_branch()


def pushed_files(origin: Path, branch: str) -> set[str]:
    listing = git("diff-tree", "--no-commit-id", "--name-only", "-r", branch, cwd=origin)
    return set(listing.splitlines())


def tracked_files(origin: Path, branch: str) -> set[str]:
    return set(git("ls-tree", "-r", "--name-only", branch, cwd=origin).splitlines())


def under_shared_paths(path: str) -> bool:
    return any(path == shared or path.startswith(shared) for shared in SHARED_PATHS)


@pytest.fixture
def pushed(tmp_path):
    work = tmp_path / "work"
    result = runner.invoke(cli.app, ["init", str(work), "--demo"])
    assert result.exit_code == 0, result.output
    git("init", "--quiet", "-b", "main", cwd=work)
    git("add", "-A", cwd=work)
    git("commit", "--quiet", "-m", "Start the adops Project", cwd=work)
    origin = tmp_path / "origin.git"
    git("init", "--quiet", "--bare", str(origin), cwd=tmp_path)
    git("remote", "add", "origin", str(origin), cwd=work)
    git("push", "--quiet", "-u", "origin", "main", cwd=work)

    (work / ".env").write_text("WAREHOUSE_PASSWORD=do-not-share\n")
    (work / ".adops" / "logs" / "dev").mkdir(parents=True)
    (work / ".adops" / "logs" / "dev" / "run.log").write_text("local run\n")
    (work / "notes.txt").write_text("not part of any change\n")
    assert all((work / name).exists() for name in LOCAL_ONLY)

    pipeline = work / "pipelines" / "shop_sync.yaml"
    pipeline.write_text(pipeline.read_text().replace("count: 1", "count: 2"))
    replay(work, "share-change", SHARED_PATHS, "Expect two orders", after_branch=lambda: None)

    git("checkout", "--quiet", "main", cwd=work)

    def accept_schema_in_dev():
        record = work / RECORD
        record.parent.mkdir()
        record.write_text(RECORD_TEXT)

    replay(
        work,
        "accept-orders",
        [RECORD],
        "Accept the Breaking Change in orders",
        after_branch=accept_schema_in_dev,
    )

    clone = tmp_path / "clone"
    git("clone", "--quiet", "--branch", "accept-orders", str(origin), str(clone), cwd=tmp_path)
    return origin, clone


def test_share_pushes_only_the_named_paths(pushed):
    origin, _ = pushed

    assert pushed_files(origin, "share-change") == {"pipelines/shop_sync.yaml"}
    assert all(under_shared_paths(path) for path in pushed_files(origin, "share-change"))
    assert not {path.split("/")[0] for path in tracked_files(origin, "share-change")} & set(
        LOCAL_ONLY
    )


def test_breaking_change_pushes_only_the_record(pushed):
    origin, clone = pushed

    assert pushed_files(origin, "accept-orders") == {RECORD}
    assert (clone / RECORD).read_bytes() == RECORD_TEXT.encode()
    assert not any((clone / name).exists() for name in LOCAL_ONLY)


def test_pushed_record_passes_validate(pushed):
    _, clone = pushed

    assert validate_project(clone, None).problems == []
    result = runner.invoke(cli.app, ["validate", "--project", str(clone)])
    assert result.exit_code == 0, result.output


def test_validate_rejects_a_record_for_an_unknown_pipeline(pushed):
    _, clone = pushed
    (clone / RECORD).write_text(RECORD_TEXT.replace("shop_sync", "no_such_pipeline"))

    assert validate_project(clone, None).problems == [
        f"{RECORD}: pipeline: no Pipeline with this name"
    ]


@pytest.mark.parametrize("template", ["demo", "plain"])
@pytest.mark.parametrize("skill", REPLAYED_SKILLS)
def test_skill_runs_the_git_steps_in_the_replayed_order(template, skill):
    assert out_of_order_command(skill_text(template, skill), SKILL_GIT_SEQUENCE) is None


def test_order_check_catches_a_commit_before_the_branch():
    text = skill_text("demo", "adops-share")
    reordered = (
        text.replace("git checkout -b", "\0")
        .replace("git commit", "git checkout -b")
        .replace("\0", "git commit")
    )

    assert out_of_order_command(text, SKILL_GIT_SEQUENCE) is None
    assert out_of_order_command(reordered, SKILL_GIT_SEQUENCE) is not None


def test_order_check_catches_a_dropped_step():
    text = skill_text("demo", "adops-breaking-change").replace("gh pr create", "gh pr view")

    assert out_of_order_command(text, SKILL_GIT_SEQUENCE) == "gh pr create"
