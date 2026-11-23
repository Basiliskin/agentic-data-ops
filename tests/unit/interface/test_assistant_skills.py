import re
from pathlib import Path

import anyio
import pytest
import yaml
from mcp.shared.memory import create_connected_server_and_client_session
from mcp_harness import write_project

from adops.interface.mcp_server import build_server

TEMPLATES = Path(__file__).parents[3] / "src" / "adops" / "templates"
TEMPLATE_NAMES = ("demo", "plain")
SKILL_FILES = sorted(
    path for name in TEMPLATE_NAMES for path in TEMPLATES.glob(f"{name}/.claude/skills/*/SKILL.md")
)

EXPECTED_SKILLS = {
    "adops-setup",
    "adops-build",
    "adops-test-in-dev",
    "adops-share",
    "adops-promote",
    "adops-breaking-change",
}
REQUIRED_TOOLS = {
    "adops-setup": {"test_connection", "validate"},
    "adops-build": {"list_files", "read_file", "project_view", "edit_file", "validate"},
    "adops-test-in-dev": {"run", "preview", "read_logs", "test_connection"},
    "adops-share": {"run", "preview"},
    "adops-promote": {"drop_branch"},
    "adops-breaking-change": {"accept_schema"},
}

ADOPS_TOOL = re.compile(r"mcp__adops__([A-Za-z0-9_-]+)")
FOREIGN_TOOL = re.compile(r"mcp__(?!adops__)[A-Za-z0-9_-]+__[A-Za-z0-9_-]*")
HYPHENATED_NAME = re.compile(r"`([a-z]+(?:-[a-z]+)+)`")
FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)

STAGE_OR_PROD_COMMANDS = (
    re.compile(r"""\benvironment\b["'`]?\s*[:=]\s*["'`]?(stage|prod)\b""", re.IGNORECASE),
    re.compile(r"""--env(ironment)?[\s=]+["'`]?(stage|prod)\b""", re.IGNORECASE),
    re.compile(r"\brun\s+--all\b"),
    re.compile(r"\bgh\s+release\b"),
    re.compile(r"\bgh\s+workflow\s+run\b"),
)

STARTING_COMMIT = "Start the adops Project"
UNSAFE_STAGING = (
    re.compile(r"\bgit\s+add\s+(-A|--all|\.)(?=[\s`]|$)"),
    re.compile(r"\bgit\s+add\b[^`\n]*(?<![\w.])\.(env|adops)(?![\w.])"),
)


def template_of(path: Path) -> str:
    return path.relative_to(TEMPLATES).parts[0]


def skill_id(path: Path) -> str:
    return f"{template_of(path)}/{path.parent.name}"


skills = pytest.mark.parametrize("skill", SKILL_FILES, ids=skill_id)


def unknown_tool_names(text: str, registered: set[str]) -> list[str]:
    """Every tool reference in a Skill that the adops MCP server would not recognise."""
    unknown = [f"mcp__adops__{name}" for name in ADOPS_TOOL.findall(text) if name not in registered]
    unknown += FOREIGN_TOOL.findall(text)
    unknown += [
        f"`{name}`"
        for name in HYPHENATED_NAME.findall(text)
        if name.replace("-", "_") in registered
    ]
    return unknown


def mentioned_tools(text: str) -> set[str]:
    return set(ADOPS_TOOL.findall(text))


def stage_or_prod_lines(text: str) -> list[str]:
    """Lines that would make the Assistant act on stage or prod, rather than merely name them."""
    return [
        line
        for line in text.splitlines()
        if any(pattern.search(line) for pattern in STAGE_OR_PROD_COMMANDS)
    ]


def unsafe_git_lines(text: str) -> list[str]:
    """Lines that commit before any branch exists, or stage everything or a private file.

    The one commit allowed before `git checkout -b` is a new repository's starting save,
    recognised by its fixed message, which holds only the files `adops init` wrote.
    """
    unsafe = []
    branched = False
    for line in text.splitlines():
        if "git checkout -b" in line:
            branched = True
        commits_on_main = "git commit" in line and STARTING_COMMIT not in line and not branched
        if commits_on_main or any(pattern.search(line) for pattern in UNSAFE_STAGING):
            unsafe.append(line)
    return unsafe


def front_matter(text: str) -> dict:
    match = FRONT_MATTER.match(text)
    assert match, "a Skill must start with a --- front matter block"
    return yaml.safe_load(match.group(1))


async def list_tool_names(project: Path) -> set[str]:
    async with create_connected_server_and_client_session(build_server(project)) as session:
        listed = await session.list_tools()
    return {tool.name for tool in listed.tools}


@pytest.fixture(scope="module")
def registered_tools(tmp_path_factory) -> set[str]:
    project = tmp_path_factory.mktemp("project")
    write_project(project)
    return anyio.run(list_tool_names, project)


@pytest.mark.parametrize("template", TEMPLATE_NAMES)
def test_each_template_ships_exactly_the_expected_skills(template):
    folders = {path.parent.name for path in SKILL_FILES if template_of(path) == template}

    assert folders == EXPECTED_SKILLS


def test_demo_and_plain_ship_byte_identical_skills():
    for name in EXPECTED_SKILLS:
        demo, plain = (
            TEMPLATES / template / ".claude" / "skills" / name / "SKILL.md"
            for template in TEMPLATE_NAMES
        )
        assert demo.read_bytes() == plain.read_bytes(), name


def test_every_expected_skill_declares_its_required_tools():
    assert set(REQUIRED_TOOLS) == EXPECTED_SKILLS


@skills
def test_front_matter_names_the_skill_folder_and_describes_it(skill):
    fields = front_matter(skill.read_text())

    assert fields["name"] == skill.parent.name
    assert isinstance(fields["description"], str)
    assert fields["description"].strip()


@skills
def test_skill_names_only_registered_adops_tools(skill, registered_tools):
    assert unknown_tool_names(skill.read_text(), registered_tools) == []


@skills
def test_skill_mentions_the_tools_its_workflow_needs(skill):
    assert REQUIRED_TOOLS[skill.parent.name] <= mentioned_tools(skill.read_text())


@skills
def test_skill_never_acts_on_stage_or_prod(skill):
    assert stage_or_prod_lines(skill.read_text()) == []


@skills
def test_skill_branches_before_committing_and_stages_only_named_paths(skill):
    assert unsafe_git_lines(skill.read_text()) == []


@pytest.mark.parametrize(
    "text",
    [
        'Run `git commit -m "Add refunds"`.\nThen run `git checkout -b add-refunds`.',
        "Run `git checkout -b add-refunds`, then `git add -A`.",
        "Run `git checkout -b add-refunds`, then `git add --all`.",
        "Run `git checkout -b add-refunds`, then `git add .`",
        "Run `git checkout -b add-refunds`, then `git add . && git commit`.",
        "Run `git checkout -b add-refunds`, then `git add pipelines/ .env`.",
        "Run `git checkout -b add-refunds`, then `git add .adops`.",
    ],
)
def test_git_rule_catches_unsafe_commits_and_staging(text):
    assert unsafe_git_lines(text) != []


@pytest.mark.parametrize(
    "text",
    [
        'Run `git add .gitignore`, then `git commit -m "Start the adops Project"`.',
        "Run `git checkout -b add-refunds`.\nRun `git add pipelines/`, then `git commit`.",
        "Run `git checkout -b add-refunds`, then `git add .env.example` and `git add adops.yaml`.",
        "Never stage everything at once, and never stage the password file.",
    ],
)
def test_git_rule_lets_safe_steps_through(text):
    assert unsafe_git_lines(text) == []


@pytest.mark.parametrize(
    "line",
    [
        'Call `mcp__adops__run` with `environment="stage"`.',
        "Call it with environment: prod",
        'Pass {"environment": "prod"} to the tool.',
        "Run `adops run shop_sync --env prod`.",
        "Run `adops run --environment=stage`.",
        "Then run `adops run --all` to load everything.",
        "Publish it with `gh release create v1.0.0`.",
        "Start it with `gh workflow run adops-stage.yml`.",
    ],
)
def test_stage_or_prod_rule_catches_commands_that_target_them(line):
    assert stage_or_prod_lines(line) == [line]


@pytest.mark.parametrize(
    "line",
    [
        "Merging sends the change to stage; you never run anything there yourself.",
        "Stage only the named folders, never everything at once.",
        "The stage and prod Environments are reached only through a pull request or a release.",
        'Call `mcp__adops__test_connection` with `environment` set to `"dev"`.',
        "The Server runs Pipelines in the Environment named by ADOPS_ENVIRONMENT.",
    ],
)
def test_stage_or_prod_rule_lets_prose_through(line):
    assert stage_or_prod_lines(line) == []


SETUP_SKILL = TEMPLATES / "demo" / ".claude" / "skills" / "adops-setup" / "SKILL.md"


@pytest.mark.parametrize(
    ("reference", "replacement"),
    [
        ("`mcp__adops__validate`", "`mcp__adops__read-logs`"),
        ("`mcp__adops__validate`", "`mcp__other__validate`"),
        ("`mcp__adops__test_connection`", "`mcp__adops__test_conection`"),
        ("`mcp__adops__validate`", "`read-logs`"),
    ],
)
def test_tool_name_rule_catches_a_mistyped_reference(reference, replacement, registered_tools):
    text = SETUP_SKILL.read_text()
    assert reference in text

    mutated = text.replace(reference, replacement)

    assert unknown_tool_names(mutated, registered_tools) != []


BREAKING_CHANGE_SKILL = TEMPLATES / "demo" / ".claude" / "skills" / "adops-breaking-change"


def test_breaking_change_skill_handles_a_written_and_an_already_covered_record():
    text = (BREAKING_CHANGE_SKILL / "SKILL.md").read_text()

    assert "`record_path`" in text
    assert "`covered_by`" in text


def test_tool_name_rule_lets_cli_commands_with_hyphens_through(registered_tools):
    text = "Run `adops accept-schema --branch fix-orders`, then call `mcp__adops__validate`."

    assert unknown_tool_names(text, registered_tools) == []
