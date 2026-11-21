import importlib.util
import re

import pytest
import yaml
from dagster import QueuedRunCoordinator
from dotenv import dotenv_values
from typer.testing import CliRunner

import adops
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

DEMO_FILES = SERVER_FILES | {
    ".env.example",
    ".gitignore",
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
    "warehouse_init/01_stage.sql",
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

    assert set(settings.environments) == {"dev", "stage"}
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


def test_demo_stage_keeps_its_own_warehouse(demo):
    settings_text = (demo / "adops.yaml").read_text()
    environments = yaml.safe_load(settings_text)["environments"]
    compose = yaml.safe_load((demo / "docker-compose.yml").read_text())

    assert environments["dev"]["warehouse"]["database"] == "warehouse"
    assert environments["stage"]["warehouse"]["database"] == "warehouse_stage"
    assert "#   path: .adops/warehouse.duckdb" in settings_text
    assert "#   path: .adops/warehouse_stage.duckdb" in settings_text
    volumes = compose["services"]["warehouse"]["volumes"]
    assert "./warehouse_init:/docker-entrypoint-initdb.d:ro" in volumes
    init = (demo / "warehouse_init" / "01_stage.sql").read_text()
    assert "CREATE DATABASE warehouse_stage;" in init


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
    assert services["dagster_daemon"]["command"] == ["dagster-daemon", "run", "-w", WORKSPACE]
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
    assert set(config) == {"storage", "run_coordinator"}
    connection = config["storage"]["postgres"]["postgres_db"]
    assert all(set(value) == {"env"} for value in connection.values())
    assert connection["password"] == {"env": "DAGSTER_PG_PASSWORD"}


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
    assert listing(plain) == SERVER_FILES | {
        ".env.example",
        "adops.yaml",
        "docker-compose.yml",
        "pipelines",
        "server",
    }
    assert list((plain / "pipelines").iterdir()) == []
    load_project_settings((plain / "adops.yaml").read_text())


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
