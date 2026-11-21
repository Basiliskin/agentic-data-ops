import socket
import subprocess
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg
import pytest
import yaml
from demo_project import Demo, connect, dagster_meta, init_demo, warehouse

import adops

pytestmark = pytest.mark.e2e

REPO = Path(__file__).resolve().parents[2]
EVERY_MINUTE = "* * * * *"
SCHEDULE = "shop_sync_schedule"
SCHEDULED_RUN_DEADLINE_SECONDS = 240
POLL_SECONDS = 5
FINISHED_UNSUCCESSFULLY = ("FAILURE", "CANCELED")
SERVER_SERVICES = ("dagster_daemon", "dagster_webserver")
OVERRIDE = "server-image.override.yml"


def write_image_override(project, image):
    services = {service: {"image": image} for service in SERVER_SERVICES}
    (project / OVERRIDE).write_text(yaml.safe_dump({"services": services}))


def server_compose(project, name):
    return ["docker", "compose", "-p", name, "-f", "docker-compose.yml", "-f", OVERRIDE]


def installed_adops(image):
    script = (
        "import adops, importlib.metadata as m;"
        "print(adops.__version__);"
        "print(m.distribution('adops').read_text('direct_url.json'))"
    )
    result = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "python", image, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


@pytest.fixture(scope="session")
def server_image(tmp_path_factory):
    """Built once from this checkout's wheel, so the containers run the code under test."""
    dist = tmp_path_factory.mktemp("dist")
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(dist)], cwd=REPO, check=True)
    [wheel] = dist.glob("adops-*.whl")
    project = tmp_path_factory.mktemp("image") / "shop"
    init_demo(project)
    (project / wheel.name).write_bytes(wheel.read_bytes())
    image = f"adops-e2e-server:{uuid.uuid4().hex[:8]}"
    write_image_override(project, image)
    build = [*server_compose(project, "adops-e2e-image"), "--profile", "server", "build"]
    build += ["--build-arg", f"ADOPS_PACKAGE=/build/{wheel.name}", SERVER_SERVICES[0]]
    subprocess.run(build, cwd=project, check=True)
    try:
        version, direct_url = installed_adops(image)
        assert version == adops.__version__
        assert f"/build/{wheel.name}" in direct_url
        yield image
    finally:
        subprocess.run(["docker", "image", "rm", "-f", image], capture_output=True)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def set_env(project, **values):
    env_file = project / ".env"
    lines = [
        line for line in env_file.read_text().splitlines() if line.split("=", 1)[0] not in values
    ]
    lines += [f"{name}={value}" for name, value in values.items()]
    env_file.write_text("\n".join(lines) + "\n")


def add_schedule(pipeline, cron):
    pipeline.write_text(f'schedule: "{cron}"\n' + pipeline.read_text())


def create_dagster_database(env, name):
    with connect(env, "DAGSTER_PG", env["DAGSTER_PG_USER"], env["DAGSTER_PG_DB"]) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')


@dataclass(frozen=True)
class Server:
    demo: Demo
    compose: list

    def stop(self):
        subprocess.run([*self.compose, "down", "-v"], cwd=self.demo.project, check=True)

    def daemon_log_tail(self):
        logs = [*self.compose, "logs", "--no-color", "--tail", "60", SERVER_SERVICES[0]]
        result = subprocess.run(logs, cwd=self.demo.project, capture_output=True, text=True)
        return result.stdout + result.stderr


@pytest.fixture
def server(demo, server_image):
    """The demo's Server on stage, with its own Dagster database so no earlier run interferes."""
    project = demo.project
    add_schedule(project / "pipelines" / "shop_sync.yaml", EVERY_MINUTE)
    dagster_db = f"dagster_{uuid.uuid4().hex[:8]}"
    create_dagster_database(demo.env, dagster_db)
    set_env(
        project,
        ADOPS_ENVIRONMENT="stage",
        DAGSTER_PG_DB=dagster_db,
        DAGSTER_WEBSERVER_PORT=free_port(),
    )
    write_image_override(project, server_image)
    demo = replace(demo, env={**demo.env, "DAGSTER_PG_DB": dagster_db})
    compose = [*server_compose(project, f"adops-e2e-server-{uuid.uuid4().hex[:8]}")]
    compose += ["--profile", "server"]
    up = [*compose, "up", "-d", "--wait", "--no-build", "--no-deps", *SERVER_SERVICES]
    try:
        subprocess.run(up, cwd=project, check=True)
        yield Server(demo, compose)
    finally:
        subprocess.run([*compose, "down", "-v"], cwd=project, check=True)


def scheduled_runs(env):
    sql = (
        "SELECT r.run_id, r.pipeline_name, r.status FROM runs r"
        " JOIN run_tags t ON t.run_id = r.run_id"
        " WHERE t.key = 'dagster/schedule_name' AND t.value = %s"
    )
    try:
        return dagster_meta(env, sql, SCHEDULE)
    except psycopg.errors.UndefinedTable:
        return []


def wait_for_scheduled_success(server):
    deadline = time.monotonic() + SCHEDULED_RUN_DEADLINE_SECONDS
    while True:
        runs = scheduled_runs(server.demo.env)
        statuses = {status for _, job, status in runs if job == "shop_sync"}
        if "SUCCESS" in statuses:
            return
        if statuses & set(FINISHED_UNSUCCESSFULLY) or time.monotonic() > deadline:
            pytest.fail(
                f"no scheduled shop_sync run succeeded; runs seen: {runs}\n"
                f"dagster_daemon log tail:\n{server.daemon_log_tail()}"
            )
        time.sleep(POLL_SECONDS)


def test_schedule_launches_a_successful_stage_run(server):
    wait_for_scheduled_success(server)
    server.stop()

    loaded = {
        table: warehouse(server.demo, f"SELECT count(*) FROM raw_shop.{table}", "stage")[0][0]
        for table in ("customers", "orders")
    }
    assert loaded == {"customers": 20, "orders": 100}
