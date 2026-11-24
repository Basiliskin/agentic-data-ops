import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml
from dagster import DagsterInstance, QueuedRunCoordinator
from dotenv import dotenv_values
from git_harness import GIT_ENV, git
from typer.testing import CliRunner

import adops
from adops.domain.assistant_access import AssistantRefusal, refuse_shared_dev_connection
from adops.domain.loader import (
    ensure_sources_defined,
    ensure_targets_defined,
    load_pipeline,
    load_project_settings,
)
from adops.domain.pipeline import SyncStep
from adops.domain.reverse_sync import ReverseSyncStep
from adops.domain.transform import TransformStep
from adops.interface import cli
from adops.interface.run_command import DAGSTER_FILE

runner = CliRunner()

SERVER_FILES = {"Dockerfile", "server/dagster.yaml", "server/workspace.yaml"}
SERVER_SERVICES = {"dagster_daemon", "dagster_webserver"}
WORKSPACE = "/project/server/workspace.yaml"
PULL_REQUEST_WORKFLOW = ".github/workflows/adops-pull-request.yml"
STAGE_WORKFLOW = ".github/workflows/adops-stage.yml"
PROD_WORKFLOW = ".github/workflows/adops-prod.yml"
DEPLOY_WORKFLOWS = {"stage": STAGE_WORKFLOW, "prod": PROD_WORKFLOW}
WORKFLOW_FILES = {PULL_REQUEST_WORKFLOW, STAGE_WORKFLOW, PROD_WORKFLOW}
CI_WORKFLOW = Path(__file__).parents[3] / ".github" / "workflows" / "ci.yml"
SERVER_COMPOSE_RUN = (
    'docker compose --project-directory "$ADOPS_SERVER_DIR" --profile server run --rm --no-deps'
)

DEMO_FILES = SERVER_FILES | WORKFLOW_FILES | {
    ".claude/skills/adops-breaking-change/SKILL.md",
    ".claude/skills/adops-build/SKILL.md",
    ".claude/skills/adops-promote/SKILL.md",
    ".claude/skills/adops-setup/SKILL.md",
    ".claude/skills/adops-share/SKILL.md",
    ".claude/skills/adops-test-in-dev/SKILL.md",
    ".env.example",
    ".gitignore",
    ".mcp.json",
    "adops.yaml",
    "dagster.yaml",
    "dbt/dbt_project.yml",
    "dbt/models/customer_orders.sql",
    "dbt/models/customer_stats.sql",
    "dbt/models/customers.sql",
    "dbt/models/orders.sql",
    "dbt/models/schema.yml",
    "dbt/models/sources.yml",
    "docker-compose.yml",
    "pipelines/shop_sync.yaml",
    "seed/01_shop.sql",
    "seed/02_shop_dev.sql",
    "seed/03_shop_prod.sql",
    "warehouse_init/01_stage.sql",
    "warehouse_init/02_prod.sql",
}


def listing(root):
    return {path.relative_to(root).as_posix() for path in root.rglob("*")}


def snapshot(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


@pytest.fixture
def demo(tmp_path):
    target = tmp_path / "shop"
    result = runner.invoke(cli.app, ["init", str(target), "--demo"])
    assert result.exit_code == 0, result.output
    return target


@pytest.fixture
def plain(tmp_path):
    target = tmp_path / "project"
    result = runner.invoke(cli.app, ["init", str(target)])
    assert result.exit_code == 0, result.output
    return target


@pytest.fixture(params=["demo", "plain"])
def project(request):
    return request.getfixturevalue(request.param)


def compose_services(project):
    return yaml.safe_load((project / "docker-compose.yml").read_text())["services"]


def compose_variables(text):
    return set(re.findall(r"(?<!\$)\$\{([A-Z][A-Z0-9_]*)", text))


def settings_variables(node):
    if isinstance(node, dict):
        found = set()
        for key, value in node.items():
            if key.endswith("_env"):
                found.add(value)
            else:
                found |= settings_variables(value)
        return found
    return set()


def dagster_variables(node):
    if isinstance(node, dict):
        if set(node) == {"env"}:
            return {node["env"]}
        return set().union(*(dagster_variables(value) for value in node.values()))
    return set()


def test_demo_writes_the_demo_project_files(demo):
    files = {name for name in listing(demo) if (demo / name).is_file()}

    assert files == DEMO_FILES


def test_demo_pipeline_and_settings_pass_the_domain_loaders(demo):
    settings = load_project_settings((demo / "adops.yaml").read_text())
    pipeline = load_pipeline((demo / "pipelines" / "shop_sync.yaml").read_text())

    assert set(settings.environments) == {"dev", "stage", "prod"}
    for environment in settings.environments.values():
        ensure_sources_defined(pipeline, environment)
        ensure_targets_defined(pipeline, environment)
    sync, transform, reverse_sync = pipeline.steps
    assert isinstance(sync, SyncStep)
    assert [table.name for table in sync.tables] == ["customers", "orders"]
    assert transform == TransformStep(type="transform", name="transform_shop", project="dbt")
    assert (demo / transform.project / "dbt_project.yml").is_file()
    columns = ("customer_id", "order_count", "total_amount", "last_order_at")
    assert reverse_sync == ReverseSyncStep(
        type="reverse_sync",
        name="reverse_sync_customer_stats",
        model="customer_stats",
        target="shop",
        target_table="customer_stats",
        key="customer_id",
        mapping={column: column for column in columns},
    )
    assert (demo / "dbt" / "models" / f"{reverse_sync.model}.sql").is_file()


def test_demo_customer_stats_carries_no_personal_data(demo):
    model = (demo / "dbt" / "models" / "customer_stats.sql").read_text()
    seed = (demo / "seed" / "01_shop.sql").read_text()
    [table] = re.findall(r"CREATE TABLE customer_stats \((.*?)\);", seed, re.DOTALL)

    assert "email" not in model.lower()
    assert re.findall(r"as (\w+)", model) == ["order_count", "total_amount", "last_order_at"]
    assert [line.split()[0] for line in table.strip().splitlines()] == [
        "customer_id",
        "order_count",
        "total_amount",
        "last_order_at",
    ]
    assert "customer_id integer PRIMARY KEY" in table


def test_demo_stage_and_prod_keep_their_own_warehouses(demo):
    settings_text = (demo / "adops.yaml").read_text()
    environments = yaml.safe_load(settings_text)["environments"]
    compose = yaml.safe_load((demo / "docker-compose.yml").read_text())

    assert environments["dev"]["warehouse"]["database"] == "warehouse"
    assert environments["stage"]["warehouse"]["database"] == "warehouse_stage"
    assert environments["prod"]["warehouse"]["database"] == "warehouse_prod"
    for name in ("warehouse", "warehouse_stage", "warehouse_prod"):
        assert f"    # warehouse:\n    #   type: duckdb\n    #   path: .adops/{name}.duckdb" in (
            settings_text
        )
    volumes = compose["services"]["warehouse"]["volumes"]
    assert "./warehouse_init:/docker-entrypoint-initdb.d:ro" in volumes
    init = (demo / "warehouse_init" / "01_stage.sql").read_text()
    assert "CREATE DATABASE warehouse_stage;" in init
    prod_init = (demo / "warehouse_init" / "02_prod.sql").read_text()
    statements = [line for line in prod_init.splitlines() if not line.startswith("--")]
    assert statements == ["CREATE DATABASE warehouse_prod;"]


def test_demo_prod_reads_shop_and_writes_only_its_own_databases(demo):
    text = (demo / "adops.yaml").read_text()
    environments = yaml.safe_load(text)["environments"]
    prod = environments["prod"]

    assert prod.keys() == environments["stage"].keys()
    assert prod["sources"] == environments["stage"]["sources"]
    assert prod["warehouse"] == {**environments["stage"]["warehouse"], "database": "warehouse_prod"}
    assert prod["targets"] == {
        "shop": {**environments["stage"]["targets"]["shop"], "database": "shop_prod"}
    }

    def written(environment):
        connections = [environment["warehouse"], *environment["targets"].values()]
        return {(c["host"], c["port"], c["database"]) for c in connections}

    for name in ("dev", "stage"):
        assert written(prod).isdisjoint(written(environments[name])), name
    assert "uncomment" not in text.lower()
    assert "edit host, port and database" in text


def test_demo_dev_previews_against_its_own_target_database(demo):
    settings = load_project_settings((demo / "adops.yaml").read_text())
    dev, stage = settings.environments["dev"], settings.environments["stage"]
    prod = settings.environments["prod"]

    assert dev.targets["shop"].database == "shop_dev"
    assert dev.sources["shop"].database == "shop"
    assert stage.targets["shop"].database == "shop"
    assert stage.sources["shop"].database == "shop"
    assert prod.targets["shop"].database == "shop_prod"
    assert prod.sources["shop"].database == "shop"
    refuse_shared_dev_connection(settings.environments, str)


def test_demo_dev_would_be_refused_if_prod_wrote_into_its_target(demo):
    text = (demo / "adops.yaml").read_text()
    environments = yaml.safe_load(text)["environments"]
    environments["prod"]["targets"]["shop"]["database"] = "shop_dev"
    settings = load_project_settings(yaml.safe_dump({"environments": environments}))

    with pytest.raises(AssistantRefusal, match="same database as prod"):
        refuse_shared_dev_connection(settings.environments, str)


@pytest.mark.parametrize(
    ("seed", "database"), [("02_shop_dev.sql", "shop_dev"), ("03_shop_prod.sql", "shop_prod")]
)
def test_demo_seeds_each_target_database_with_the_same_target_table(demo, seed, database):
    table = re.compile(r"CREATE TABLE customer_stats \(.*?\);", re.DOTALL)
    shop = (demo / "seed" / "01_shop.sql").read_text()
    target = (demo / "seed" / seed).read_text()

    assert sorted(path.name for path in (demo / "seed").iterdir()) == [
        "01_shop.sql",
        "02_shop_dev.sql",
        "03_shop_prod.sql",
    ]
    assert target.index(f"CREATE DATABASE {database} OWNER shop;") < target.index(
        f"\\connect {database}"
    )
    assert target.index(f"\\connect {database}") < target.index("CREATE TABLE")
    assert table.findall(target) == table.findall(shop)


def test_demo_dbt_project_reads_raw_shop_and_ships_no_profile(demo):
    sources = yaml.safe_load((demo / "dbt" / "models" / "sources.yml").read_text())
    [source] = sources["sources"]

    assert (source["name"], source["schema"]) == (
        "raw_shop",
        "{{ var('adops_raw_schema_shop', 'raw_shop') }}",
    )
    assert [table["name"] for table in source["tables"]] == ["customers", "orders"]
    assert not (demo / "dbt" / "profiles.yml").exists()
    ignored = (demo / ".gitignore").read_text().splitlines()
    assert {"dbt/target/", "dbt/logs/"} <= set(ignored)


def test_demo_dbt_tests_check_customer_ids_and_order_customers_only(demo):
    schema = yaml.safe_load((demo / "dbt" / "models" / "schema.yml").read_text())
    tests = {
        (model["name"], column["name"]): column["data_tests"]
        for model in schema["models"]
        for column in model.get("columns", [])
    }

    assert tests == {
        ("customers", "id"): ["unique", "not_null"],
        ("orders", "customer_id"): [
            {"relationships": {"arguments": {"to": "ref('customers')", "field": "id"}}}
        ],
    }


def test_every_variable_the_demo_reads_is_in_env_example(project):
    defined = set(dotenv_values(project / ".env.example"))
    dagster_files = [project / "server" / "dagster.yaml", project / "dagster.yaml"]
    used = (
        compose_variables((project / "docker-compose.yml").read_text())
        | settings_variables(yaml.safe_load((project / "adops.yaml").read_text()))
        | set().union(
            *(
                dagster_variables(yaml.safe_load(path.read_text()))
                for path in dagster_files
                if path.exists()
            )
        )
    )

    assert used - defined == set()
    assert {
        "WAREHOUSE_PASSWORD",
        "DAGSTER_PG_PASSWORD",
        "DAGSTER_WEBSERVER_HOST",
        "DAGSTER_WEBSERVER_PORT",
        "ALERT_WEBHOOK_URL",
    } <= used
    assert {"ADOPS_ENVIRONMENT"} <= defined


def test_settings_ports_match_the_ports_compose_publishes(demo):
    env = dotenv_values(demo / ".env.example")
    environments = yaml.safe_load((demo / "adops.yaml").read_text())["environments"]

    for environment in environments.values():
        assert environment["sources"]["shop"]["port"] == int(env["SHOP_PORT"])
        assert environment["targets"]["shop"]["port"] == int(env["SHOP_PORT"])
        assert environment["warehouse"]["port"] == int(env["WAREHOUSE_PORT"])


def test_demo_settings_hold_no_literal_password(demo):
    for name in ("adops.yaml", "pipelines/shop_sync.yaml"):
        assert not re.search(r"password:\s*[^ $]", (demo / name).read_text()), name


def test_demo_gitignore_keeps_secrets_and_local_state_out(demo):
    ignored = (demo / ".gitignore").read_text().splitlines()

    assert {".env", ".adops/"} <= set(ignored)


def test_plain_gitignore_keeps_secrets_and_run_logs_out(plain):
    assert (plain / ".gitignore").read_text().splitlines() == [".env", ".adops/"]
    subprocess.run(["git", "init", "--quiet", str(plain)], check=True)
    ignored = subprocess.run(
        ["git", "-C", str(plain), "check-ignore", ".env", ".adops/logs/dev/x.log"],
        capture_output=True,
        text=True,
        check=True,
    )

    assert ignored.stdout.splitlines() == [".env", ".adops/logs/dev/x.log"]


@pytest.mark.parametrize(
    ("mode", "databases"),
    [("demo", {"shop", "warehouse", "dagster_meta"}), ("plain", {"dagster_meta"})],
)
def test_compose_without_a_profile_starts_only_pinned_ready_checked_postgres(
    request, mode, databases
):
    services = compose_services(request.getfixturevalue(mode))
    default = {name: service for name, service in services.items() if "profiles" not in service}

    assert set(default) == databases
    assert set(services) == databases | SERVER_SERVICES
    for service in default.values():
        assert service["image"] == "postgres:17-alpine"
        assert "pg_isready" in " ".join(service["healthcheck"]["test"])


def test_server_services_run_from_the_bind_mounted_project(project):
    services = compose_services(project)

    for name in SERVER_SERVICES:
        service = services[name]
        assert service["profiles"] == ["server"]
        assert service["build"] == "."
        assert service["network_mode"] == "host"
        assert "ports" not in service
        assert service["env_file"] == ".env"
        assert service["volumes"] == ["./:/project"]
        assert service["environment"] == {
            "DAGSTER_HOME": "/project/server",
            "ADOPS_PROJECT": "/project",
        }
        assert service["depends_on"] == {"dagster_meta": {"condition": "service_healthy"}}
    assert services["dagster_daemon"]["command"] == [
        "sh",
        "-c",
        f"adops server-recover && exec dagster-daemon run -w {WORKSPACE}",
    ]
    assert services["dagster_webserver"]["command"] == [
        "dagster-webserver",
        "-h",
        "${DAGSTER_WEBSERVER_HOST:?set DAGSTER_WEBSERVER_HOST in .env}",
        "-p",
        "${DAGSTER_WEBSERVER_PORT:?set DAGSTER_WEBSERVER_PORT in .env}",
        "-w",
        WORKSPACE,
    ]


def test_webserver_listens_only_on_localhost_by_default(project):
    text = (project / ".env.example").read_text()
    env = dotenv_values(project / ".env.example")

    assert env["DAGSTER_WEBSERVER_HOST"] == "127.0.0.1"
    assert "SSH tunnel" in text
    assert env["ADOPS_ENVIRONMENT"] == "stage"
    assert env["ALERT_WEBHOOK_URL"] == ""


def test_demo_env_example_says_the_server_runs_stage_or_prod_one_at_a_time(demo):
    text = (demo / ".env.example").read_text()

    assert "stage or prod. One Server runs per machine at a time" in text


def test_prod_workflow_says_how_to_point_prod_at_its_machine_in_either_project(project):
    header = (project / PROD_WORKFLOW).read_text().split("\nname:")[0]

    assert "in a demo Project, edit" in header
    assert "in a plain Project, uncomment the prod block" in header


def test_settings_name_the_alert_webhook_variable(project):
    settings = load_project_settings((project / "adops.yaml").read_text())

    assert settings.alert_webhook_env.name == "ALERT_WEBHOOK_URL"


def test_workspace_loads_the_server_code_location_from_the_package(project):
    workspace = yaml.safe_load((project / "server" / "workspace.yaml").read_text())

    assert workspace == {
        "load_from": [
            {
                "python_module": {
                    "module_name": "adops.interface.server",
                    "attribute": "defs",
                    "working_directory": "/project",
                }
            }
        ]
    }
    assert importlib.util.find_spec("adops.interface.server") is not None


def test_dockerfile_installs_the_current_adops_version_without_copying_files(project):
    dockerfile = (project / "Dockerfile").read_text()

    assert f"ARG ADOPS_PACKAGE=adops=={adops.__version__}\n" in dockerfile
    assert "{{ADOPS_VERSION}}" not in dockerfile
    assert (
        'RUN --mount=type=bind,source=.,target=/build pip install --no-cache-dir "$ADOPS_PACKAGE"'
        in dockerfile
    )
    assert not re.search(r"^\s*(COPY|ADD)\b", dockerfile, re.MULTILINE | re.IGNORECASE)


def test_server_dagster_instance_runs_one_run_at_a_time_on_postgres(project):
    config = yaml.safe_load((project / "server" / "dagster.yaml").read_text())
    coordinator = config["run_coordinator"]
    module = importlib.import_module(coordinator["module"])

    assert getattr(module, coordinator["class"]) is QueuedRunCoordinator
    assert coordinator["config"] == {"max_concurrent_runs": 1}
    assert set(config) == {"storage", "run_coordinator", "run_monitoring"}
    assert config["run_monitoring"] == {
        "enabled": True,
        "start_timeout_seconds": 180,
        "poll_interval_seconds": 120,
        "max_resume_run_attempts": 0,
    }
    connection = config["storage"]["postgres"]["postgres_db"]
    assert all(set(value) == {"env"} for value in connection.values())
    assert connection["password"] == {"env": "DAGSTER_PG_PASSWORD"}


def test_dagster_reads_back_the_server_run_monitoring_settings(project, tmp_path):
    config = yaml.safe_load((project / "server" / "dagster.yaml").read_text())
    del config["storage"]
    home = tmp_path / "dagster_home"
    home.mkdir()
    (home / "dagster.yaml").write_text(yaml.safe_dump(config))

    with DagsterInstance.from_config(str(home)) as instance:
        assert instance.run_monitoring_enabled
        assert instance.run_monitoring_start_timeout_seconds == 180
        assert instance.run_monitoring_poll_interval_seconds == 120
        assert instance.run_monitoring_max_resume_run_attempts == 0
        assert instance.run_monitoring_max_runtime_seconds == 0


def test_demo_keeps_its_host_dagster_storage_unchanged(demo):
    host = yaml.safe_load((demo / "dagster.yaml").read_text())
    server = yaml.safe_load((demo / "server" / "dagster.yaml").read_text())

    assert host == {"storage": server["storage"]}


def test_plain_project_has_no_root_dagster_file_so_host_runs_stay_ephemeral(plain):
    assert not (plain / DAGSTER_FILE).exists()
    assert (plain / "server" / DAGSTER_FILE).is_file()


def test_demo_seed_has_no_time_or_randomness_dependent_values(demo):
    seed = (demo / "seed" / "01_shop.sql").read_text()

    assert not re.search(r"random\(|now\(|current_timestamp", seed, re.IGNORECASE)


def test_plain_init_writes_settings_server_files_and_an_empty_pipelines_folder(plain):
    assert listing(plain) == SERVER_FILES | WORKFLOW_FILES | {
        ".claude",
        ".claude/skills",
        ".claude/skills/adops-breaking-change",
        ".claude/skills/adops-breaking-change/SKILL.md",
        ".claude/skills/adops-build",
        ".claude/skills/adops-build/SKILL.md",
        ".claude/skills/adops-promote",
        ".claude/skills/adops-promote/SKILL.md",
        ".claude/skills/adops-setup",
        ".claude/skills/adops-setup/SKILL.md",
        ".claude/skills/adops-share",
        ".claude/skills/adops-share/SKILL.md",
        ".claude/skills/adops-test-in-dev",
        ".claude/skills/adops-test-in-dev/SKILL.md",
        ".env.example",
        ".github",
        ".github/workflows",
        ".gitignore",
        ".mcp.json",
        "adops.yaml",
        "docker-compose.yml",
        "pipelines",
        "server",
    }
    assert list((plain / "pipelines").iterdir()) == []
    load_project_settings((plain / "adops.yaml").read_text())


def test_mcp_json_registers_the_adops_mcp_server_launched_from_the_project(project):
    assert json.loads((project / ".mcp.json").read_text()) == {
        "mcpServers": {"adops": {"command": "adops", "args": ["mcp"]}}
    }


def test_init_creates_missing_parent_folders(tmp_path):
    target = tmp_path / "a" / "b"

    result = runner.invoke(cli.app, ["init", str(target), "--demo"])

    assert result.exit_code == 0, result.output
    assert (target / "adops.yaml").is_file()


def test_init_accepts_an_existing_empty_folder(tmp_path):
    result = runner.invoke(cli.app, ["init", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "adops.yaml").is_file()


@pytest.mark.parametrize("demo_flag", [["--demo"], []])
def test_init_refuses_a_non_empty_folder_and_leaves_it_untouched(tmp_path, demo_flag):
    (tmp_path / "notes.txt").write_text("mine")
    before = snapshot(tmp_path)

    result = runner.invoke(cli.app, ["init", str(tmp_path), *demo_flag])

    assert result.exit_code == 1
    assert "not an empty folder" in result.output
    assert listing(tmp_path) == {"notes.txt"}
    assert snapshot(tmp_path) == before


def test_init_refuses_a_path_that_is_a_file(tmp_path):
    target = tmp_path / "taken"
    target.write_text("mine")

    result = runner.invoke(cli.app, ["init", str(target)])

    assert result.exit_code == 1
    assert target.read_text() == "mine"


@pytest.mark.parametrize("demo_flag", [["--demo"], []])
def test_init_writes_no_tool_code(tmp_path, demo_flag):
    runner.invoke(cli.app, ["init", str(tmp_path), *demo_flag])

    assert list(tmp_path.rglob("*.py")) == []


def workflow(project, name=PULL_REQUEST_WORKFLOW):
    return yaml.safe_load((project / name).read_text())


def triggers(parsed):
    # PyYAML reads the bare key `on` as the boolean True.
    return parsed[True]


def commands(job):
    return [step["run"] for step in job["steps"] if "run" in step]


def pull_request_context(head_repo, action):
    head = {"repo": {"full_name": head_repo}}
    event = {"action": action, "pull_request": {"head": head}}
    return {"github": {"repository": "acme/shop", "event": event}}


def evaluate(condition, context):
    def operand(text):
        if text.startswith("'") and text.endswith("'"):
            return text[1:-1]
        value = context
        for key in text.split("."):
            value = value[key]
        return value

    results = []
    for clause in condition.split(" && "):
        match = re.fullmatch(r"(\S+) (==|!=) (\S+)", clause)
        assert match, f"unsupported condition: {clause}"
        left, operator, right = match.groups()
        equal = operand(left) == operand(right)
        results.append(equal if operator == "==" else not equal)
    return all(results)


def test_pull_request_workflow_triggers_on_open_update_and_close(project):
    assert triggers(workflow(project)) == {
        "pull_request": {"types": ["opened", "synchronize", "reopened", "closed"]}
    }


@pytest.mark.parametrize(
    ("head_repo", "action", "expected"),
    [
        ("someone/shop", "opened", {"fork-validate"}),
        ("someone/shop", "synchronize", {"fork-validate"}),
        ("someone/shop", "closed", set()),
        ("acme/shop", "opened", {"branch-run"}),
        ("acme/shop", "reopened", {"branch-run"}),
        ("acme/shop", "closed", {"drop-branch"}),
        (None, "opened", {"fork-validate"}),
        (None, "closed", set()),
    ],
)
def test_pull_request_workflow_selects_at_most_one_job_per_event(
    project, head_repo, action, expected
):
    jobs = workflow(project)["jobs"]
    context = pull_request_context(head_repo, action)

    selected = {name for name, job in jobs.items() if evaluate(job["if"], context)}

    assert selected == expected
    assert set(jobs) == {"fork-validate", "branch-run", "drop-branch"}


def test_fork_job_only_validates_on_a_github_hosted_runner(project):
    job = workflow(project)["jobs"]["fork-validate"]

    assert job["runs-on"] == "ubuntu-latest"
    assert job["env"] == {
        "ADOPS_PACKAGE": f"${{{{ vars.ADOPS_PACKAGE || 'adops=={adops.__version__}' }}}}"
    }
    install, validate, skip = commands(job)
    assert '"$RUNNER_TEMP/adops/bin/pip" install "$ADOPS_PACKAGE"' in install
    assert validate == "adops validate"
    assert skip == (
        "echo 'Fork PR: Branch run skipped; a maintainer must run it from a branch in this"
        " repository.'"
    )
    assert "concurrency" not in job


def test_branch_jobs_run_on_the_stage_runner_one_at_a_time_per_branch(project):
    jobs = workflow(project)["jobs"]

    for name in ("branch-run", "drop-branch"):
        job = jobs[name]
        assert job["runs-on"] == ["self-hosted", "adops-stage"]
        assert job["concurrency"] == {
            "group": "adops-branch-${{ github.event.pull_request.head.ref }}",
            "cancel-in-progress": False,
        }
        assert job["env"] == {
            "BRANCH": "${{ github.event.pull_request.head.ref }}",
            "ADOPS_SERVER_DIR": "${{ vars.ADOPS_SERVER_DIR }}",
        }


def test_branch_run_validates_then_runs_every_pipeline_in_the_server_image(project):
    validate, run, cleanup = commands(workflow(project)["jobs"]["branch-run"])

    assert validate.endswith("dagster_daemon adops validate")
    assert run.endswith('dagster_daemon adops run --all --branch "$BRANCH"')
    for command in (validate, run):
        assert command.startswith(SERVER_COMPOSE_RUN)
        assert '-v "$GITHUB_WORKSPACE:/project"' in command
        assert '-v "$ADOPS_SERVER_DIR/.adops:/project/.adops"' in command
    assert cleanup.endswith('dagster_daemon chown -R "$(id -u):$(id -g)" /project')


def test_drop_branch_drops_the_closed_branch_in_the_server_image(project):
    job = workflow(project)["jobs"]["drop-branch"]
    drop, cleanup = commands(job)

    assert job["steps"][0]["with"]["ref"] == "${{ github.event.pull_request.head.sha }}"
    assert drop.startswith(SERVER_COMPOSE_RUN)
    assert '-v "$GITHUB_WORKSPACE:/project"' in drop
    assert '-v "$ADOPS_SERVER_DIR/.adops:/project/.adops"' in drop
    assert drop.endswith('dagster_daemon adops drop-branch "$BRANCH"')
    assert cleanup.endswith('dagster_daemon chown -R "$(id -u):$(id -g)" /project')


def test_branch_jobs_always_give_the_checkout_back_and_never_chown_server_state(project):
    jobs = workflow(project)["jobs"]

    for name in ("branch-run", "drop-branch"):
        last = jobs[name]["steps"][-1]
        assert last["if"] == "always()"
        assert last["run"].startswith(SERVER_COMPOSE_RUN)
        assert '-v "$GITHUB_WORKSPACE:/project"' in last["run"]
        assert ".adops" not in last["run"]


def test_branch_jobs_use_the_server_service_of_the_project_compose_file(project):
    jobs = workflow(project)["jobs"]
    service = "dagster_daemon"

    assert service in compose_services(project)
    assert compose_services(project)[service]["env_file"] == ".env"
    for name in ("branch-run", "drop-branch"):
        for command in commands(jobs[name]):
            assert f" {service} " in command


def every_workflow(project):
    return sorted((project / ".github" / "workflows").glob("*.yml"))


def test_workflows_never_expand_expressions_inside_scripts(project):
    for path in every_workflow(project):
        for job in yaml.safe_load(path.read_text())["jobs"].values():
            for command in commands(job):
                assert "${{" not in command, path.name


def test_workflows_use_no_secrets_and_read_only_permissions(project):
    for path in every_workflow(project):
        text = path.read_text()
        assert "secrets." not in text, path.name
        assert yaml.safe_load(text)["permissions"] == {"contents": "read"}, path.name


def test_workflows_quote_every_shell_variable_from_the_event(project):
    for path in every_workflow(project):
        text = path.read_text()
        assert not re.search(r'(?<!")\$(BRANCH|ADOPS_SERVER_DIR)\b', text), path.name


def test_workflows_pin_actions_exactly_as_ci_does(project):
    pins = set(re.findall(r"uses: (\S+ # v\S+)", CI_WORKFLOW.read_text()))

    for path in every_workflow(project):
        text = path.read_text()
        used = re.findall(r"uses: (.+)", text)
        assert used, path.name
        assert set(used) <= pins, path.name


def test_workflows_carry_the_installed_adops_version(project):
    for path in every_workflow(project):
        text = path.read_text()
        assert "{{ADOPS_VERSION}}" not in text, path.name
    assert f"adops=={adops.__version__}" in (project / PULL_REQUEST_WORKFLOW).read_text()


FOLDER_UPDATE = 'git -C "$ADOPS_SERVER_DIR" checkout --detach FETCH_HEAD'


def deploy_job(project, environment):
    return workflow(project, DEPLOY_WORKFLOWS[environment])["jobs"]["deploy"]


def step_index(job, text):
    matches = [index for index, step in enumerate(job["steps"]) if text in step.get("run", "")]
    assert len(matches) == 1, text
    return matches[0]


def step_script(job, text):
    return job["steps"][step_index(job, text)]["run"]


@pytest.mark.parametrize(
    ("environment", "trigger"),
    [
        ("stage", {"push": {"branches": ["main"]}}),
        ("prod", {"release": {"types": ["published"]}}),
    ],
)
def test_deploy_workflows_trigger_on_merge_to_main_and_on_a_published_release(
    project, environment, trigger
):
    parsed = workflow(project, DEPLOY_WORKFLOWS[environment])

    assert triggers(parsed) == trigger
    assert set(parsed["jobs"]) == {"deploy"}


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_deploy_runs_on_its_server_runner_one_deploy_at_a_time(project, environment):
    job = deploy_job(project, environment)

    assert job["runs-on"] == ["self-hosted", f"adops-{environment}"]
    assert job["concurrency"] == {"group": f"adops-{environment}", "cancel-in-progress": False}
    assert job["env"]["ADOPS_SERVER_DIR"] == "${{ vars.ADOPS_SERVER_DIR }}"


def test_only_the_prod_deploy_waits_for_reviewer_approval(project):
    paths = every_workflow(project)

    assert {path.name for path in paths} == {Path(name).name for name in WORKFLOW_FILES}
    for path in paths:
        parsed = yaml.safe_load(path.read_text())
        assert "environment" not in parsed, path.name
        expected = "prod" if path.name == Path(PROD_WORKFLOW).name else None
        for name, job in parsed["jobs"].items():
            assert job.get("environment") == expected, (path.name, name)


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_deploy_validates_the_checkout_before_moving_the_server_folder(project, environment):
    job = deploy_job(project, environment)
    steps = job["steps"]

    validate = step_index(job, "dagster_daemon adops validate")
    update = step_index(job, FOLDER_UPDATE)
    reload = step_index(job, "reloadRepositoryLocation")
    run = step_index(job, f"dagster_daemon adops run --all --env {environment}")
    assert 0 < validate < update < reload < run == len(steps) - 2
    assert steps[0]["with"]["fetch-depth"] == 0
    assert steps[validate]["run"].startswith(SERVER_COMPOSE_RUN)
    assert '-v "$GITHUB_WORKSPACE:/project"' in steps[validate]["run"]
    assert steps[run]["run"] == (
        f"{SERVER_COMPOSE_RUN} -T dagster_daemon adops run --all --env {environment}"
    )
    assert "if" not in job and "continue-on-error" not in job
    assert all("continue-on-error" not in step for step in steps)
    assert all("if" not in step for step in steps[: run + 1])


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_deploy_always_gives_the_checkout_back_and_never_chowns_server_state(project, environment):
    last = deploy_job(project, environment)["steps"][-1]

    assert last["if"] == "always()"
    assert last["run"] == (
        f'{SERVER_COMPOSE_RUN} -T -v "$GITHUB_WORKSPACE:/project"'
        ' dagster_daemon chown -R "$(id -u):$(id -g)" /project'
    )


@pytest.mark.parametrize(
    ("environment", "version"), [("stage", '"$GITHUB_SHA"'), ("prod", '"refs/tags/$RELEASE_TAG"')]
)
def test_deploy_moves_the_server_folder_by_a_local_fetch_only(project, environment, version):
    text = (project / DEPLOY_WORKFLOWS[environment]).read_text()
    update = step_script(deploy_job(project, environment), FOLDER_UPDATE)

    assert update.splitlines() == [
        f'git -C "$ADOPS_SERVER_DIR" fetch "$GITHUB_WORKSPACE" {version}',
        FOLDER_UPDATE,
    ]
    for forbidden in ("reset --hard", "git clean", "persist-credentials: true"):
        assert forbidden not in text


def test_prod_deploys_the_published_release_tag(project):
    prod = deploy_job(project, "prod")
    stage = deploy_job(project, "stage")

    assert prod["env"]["RELEASE_TAG"] == "${{ github.event.release.tag_name }}"
    assert prod["steps"][0]["with"]["ref"] == "refs/tags/${{ env.RELEASE_TAG }}"
    assert "ref" not in stage["steps"][0]["with"]
    assert "RELEASE_TAG" not in stage["env"]


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_folder_update_moves_the_server_to_the_new_version_and_keeps_its_local_files(
    tmp_path, demo, environment
):
    workspace = tmp_path / "workspace"
    server = tmp_path / "server"
    git("init", "-q", "-b", "main", str(workspace), cwd=tmp_path)
    (workspace / ".gitignore").write_text(".env\n.adops/\n")
    (workspace / "version").write_text("old\n")
    git("add", ".", cwd=workspace)
    git("commit", "-qm", "old", cwd=workspace)
    git("clone", "-q", str(workspace), str(server), cwd=tmp_path)
    (server / ".env").write_text("WAREHOUSE_PASSWORD=secret\n")
    (server / ".adops").mkdir()
    (server / ".adops" / "warehouse.duckdb").write_text("state")
    (workspace / "version").write_text("new\n")
    git("commit", "-qam", "new", cwd=workspace)
    git("tag", "-a", "v1", "-m", "v1", cwd=workspace)
    new = git("rev-parse", "HEAD", cwd=workspace)
    env = {
        **GIT_ENV,
        "ADOPS_SERVER_DIR": str(server),
        "GITHUB_WORKSPACE": str(workspace),
        "GITHUB_SHA": new,
        "RELEASE_TAG": "v1",
    }

    script = step_script(deploy_job(demo, environment), FOLDER_UPDATE)
    subprocess.run(["bash", "-e", "-c", script], env=env, check=True, capture_output=True)

    assert git("rev-parse", "HEAD", cwd=server) == new
    assert (server / "version").read_text() == "new\n"
    assert (server / ".env").read_text() == "WAREHOUSE_PASSWORD=secret\n"
    assert (server / ".adops" / "warehouse.duckdb").read_text() == "state"


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_deploy_reloads_the_code_location_the_server_workspace_names(project, environment):
    script = step_script(deploy_job(project, environment), "reloadRepositoryLocation")
    workspace = yaml.safe_load((project / "server" / "workspace.yaml").read_text())
    location = workspace["load_from"][0]["python_module"]["module_name"]

    assert f'reloadRepositoryLocation(repositoryLocationName: \\"{location}\\")' in script
    assert "{ __typename ... on WorkspaceLocationEntry" in script
    assert "{ loadStatus locationOrLoadError { __typename } }" in script
    assert 'curl --fail --silent --show-error -X POST "http://127.0.0.1:$port/graphql"' in script
    assert "port=$(grep -E '^DAGSTER_WEBSERVER_PORT=' \"$ADOPS_SERVER_DIR/.env\" | cut" in script
    assert not re.search(r'(^|[\s;])(source|\.) +"?\$\{?ADOPS_SERVER_DIR', script)


def test_deploys_reload_the_server_without_restarting_its_containers(project):
    for name in DEPLOY_WORKFLOWS.values():
        text = (project / name).read_text()
        assert not re.search(r"docker compose\b.* (up|down|restart|stop|start)\b", text), name


def reload_result(typename, status="LOADED", location="RepositoryLocation"):
    entry = {"__typename": typename}
    if typename == "WorkspaceLocationEntry":
        entry |= {"loadStatus": status, "locationOrLoadError": {"__typename": location}}
    return {"data": {"reloadRepositoryLocation": entry}}


LOADED = reload_result("WorkspaceLocationEntry")


@pytest.mark.parametrize("environment", ["stage", "prod"])
@pytest.mark.parametrize(
    ("response", "curl_exit", "reloaded"),
    [
        (json.dumps(LOADED), 0, True),
        (json.dumps(LOADED, separators=(",", ":")), 0, True),
        (json.dumps(reload_result("WorkspaceLocationEntry", location="PythonError")), 0, False),
        (json.dumps(reload_result("WorkspaceLocationEntry", status="LOADING")), 0, False),
        (json.dumps(reload_result("RepositoryLocationNotFound")), 0, False),
        (json.dumps(reload_result("UnauthorizedError")), 0, False),
        (json.dumps(reload_result("PythonError")), 0, False),
        (json.dumps({"errors": [{"message": "Cannot query field"}]}), 0, False),
        ("", 22, False),
    ],
)
def test_reload_step_passes_only_when_the_server_loaded_the_new_code(
    tmp_path, demo, environment, response, curl_exit, reloaded
):
    server = tmp_path / "server"
    server.mkdir()
    (server / ".env").write_text('WAREHOUSE_PASSWORD=secret\nDAGSTER_WEBSERVER_PORT="3456"\n')
    fake = tmp_path / "bin"
    fake.mkdir()
    (fake / "curl").write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$CURL_ARGS"\nprintf "%s" "$CURL_RESPONSE"\n'
        'exit "$CURL_EXIT"\n'
    )
    (fake / "curl").chmod(0o755)
    env = {
        "PATH": f"{fake}{os.pathsep}{os.environ['PATH']}",
        "ADOPS_SERVER_DIR": str(server),
        "CURL_ARGS": str(tmp_path / "curl-args"),
        "CURL_RESPONSE": response,
        "CURL_EXIT": str(curl_exit),
    }

    script = step_script(deploy_job(demo, environment), "reloadRepositoryLocation")
    result = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True)

    assert (result.returncode == 0) is reloaded, result.stderr
    assert "http://127.0.0.1:3456/graphql" in (tmp_path / "curl-args").read_text().splitlines()
    assert "secret" not in result.stdout + result.stderr


def test_reload_step_fails_when_the_server_env_has_no_webserver_port(tmp_path, demo):
    server = tmp_path / "server"
    server.mkdir()
    (server / ".env").write_text("WAREHOUSE_PASSWORD=secret\n")
    env = {"PATH": os.environ["PATH"], "ADOPS_SERVER_DIR": str(server)}

    script = step_script(deploy_job(demo, "stage"), "reloadRepositoryLocation")
    result = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True)

    assert result.returncode == 1
    assert "DAGSTER_WEBSERVER_PORT is not set" in result.stderr


def uncomment_prod(text):
    lines = text.splitlines()
    start = lines.index("  # prod:")
    end = start + 1
    while end < len(lines) and lines[end].startswith("  #   "):
        end += 1
    block = [line.replace("  # ", "  ", 1) for line in lines[start:end]]
    return "\n".join([*lines[:start], *block, *lines[end:]])


def test_plain_settings_carry_the_prod_environment_the_release_workflow_needs_commented_out(
    plain,
):
    project = plain
    text = (project / "adops.yaml").read_text()
    environments = yaml.safe_load(text)["environments"]
    reference = "dev"

    assert set(environments) == {"dev"}
    assert "adops-prod.yml" in text
    assert "uncomment this block" in text
    uncommented = uncomment_prod(text)
    settings = load_project_settings(uncommented)
    assert set(settings.environments) == {*environments, "prod"}
    parsed = yaml.safe_load(uncommented)["environments"]
    assert parsed["prod"].keys() == parsed[reference].keys()
    assert parsed["prod"]["warehouse"].keys() == parsed[reference]["warehouse"].keys()
    assert settings_variables(parsed["prod"]) <= set(dotenv_values(project / ".env.example"))
