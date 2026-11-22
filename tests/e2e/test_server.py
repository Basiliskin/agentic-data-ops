import json
import socket
import subprocess
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import pytest
import yaml
from demo_project import WAREHOUSE_DATABASES, Demo, connect, dagster_meta, init_demo, warehouse

import adops
from adops.infrastructure.warehouse_lock import ADVISORY_LOCK_KEY

pytestmark = pytest.mark.e2e

REPO = Path(__file__).resolve().parents[2]
EVERY_MINUTE = "* * * * *"
SCHEDULE = "shop_sync_schedule"
SCHEDULED_RUN_DEADLINE_SECONDS = 240
POLL_SECONDS = 5
FINISHED_UNSUCCESSFULLY = ("FAILURE", "CANCELED")
SERVER_SERVICES = ("dagster_daemon", "dagster_webserver")
PIPELINE = "shop_sync"
LOCK_HELD_DEADLINE_SECONDS = 30
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


def wait_for_scheduled_success(server, ignoring=()):
    """Returns the id of the first successful scheduled run not in `ignoring`."""
    deadline = time.monotonic() + SCHEDULED_RUN_DEADLINE_SECONDS
    while True:
        runs = [run for run in scheduled_runs(server.demo.env) if run[0] not in ignoring]
        succeeded = [run for run, job, status in runs if (job, status) == (PIPELINE, "SUCCESS")]
        if succeeded:
            return succeeded[0]
        statuses = {status for _, job, status in runs if job == PIPELINE}
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


# Run in a container: on Docker Desktop a host flock is invisible to containers on the bind mount.
FLOCK_SCRIPT = """
import fcntl, os, sys, time
path, mode = sys.argv[1:]
os.makedirs(os.path.dirname(path), exist_ok=True)
handle = open(path, "ab")
if mode == "probe":
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("held")
    else:
        print("free")
    sys.exit()
fcntl.flock(handle, fcntl.LOCK_EX)
print("held", flush=True)
time.sleep(3600)
"""


def duckdb_lock_path(demo):
    settings = yaml.safe_load((demo.project / "adops.yaml").read_text())
    return f"/project/{settings['environments']['stage']['warehouse']['path']}.lock"


def flock_container(demo, image, mode):
    return [
        "docker", "run", "--rm", "-v", f"{demo.project}:/project", "--entrypoint", "python",
        image, "-c", FLOCK_SCRIPT, duckdb_lock_path(demo), mode,
    ]  # fmt: skip


def connect_stage_warehouse(env):
    return connect(env, "WAREHOUSE", "warehouse", WAREHOUSE_DATABASES["stage"])


@contextmanager
def held_postgres_lock(demo):
    with connect_stage_warehouse(demo.env) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        yield


@contextmanager
def held_duckdb_lock(demo, image):
    name = f"adops-e2e-lock-{uuid.uuid4().hex[:8]}"
    hold = flock_container(demo, image, "hold")
    try:
        subprocess.run([*hold[:3], "-d", "--name", name, *hold[3:]], check=True)
        deadline = time.monotonic() + LOCK_HELD_DEADLINE_SECONDS
        while "held" not in container_logs(name):
            if time.monotonic() > deadline:
                pytest.fail(f"lock holder never took the DuckDB lock:\n{container_logs(name)}")
            time.sleep(1)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def container_logs(name):
    result = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    return result.stdout + result.stderr


def stage_lock_is_free(demo, image):
    if demo.warehouse_kind == "duckdb":
        probe = flock_container(demo, image, "probe")
        return subprocess.run(probe, capture_output=True, text=True).stdout.strip() == "free"
    with connect_stage_warehouse(demo.env) as conn:
        [(acquired,)] = conn.execute(
            "SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,)
        ).fetchall()
    return acquired


@pytest.fixture
def stage_warehouse_lock(demo, server_image):
    """Held before the Server starts, so its first scheduled run waits on it in STARTED."""
    with ExitStack() as held:
        if demo.warehouse_kind == "duckdb":
            held.enter_context(held_duckdb_lock(demo, server_image))
        else:
            held.enter_context(held_postgres_lock(demo))
        yield held


def wait_for_started_run(server):
    deadline = time.monotonic() + SCHEDULED_RUN_DEADLINE_SECONDS
    while True:
        runs = scheduled_runs(server.demo.env)
        started = [run for run, job, status in runs if (job, status) == (PIPELINE, "STARTED")]
        if started:
            return started[0]
        if time.monotonic() > deadline:
            pytest.fail(
                f"no scheduled {PIPELINE} run reached STARTED; runs seen: {runs}\n"
                f"dagster_daemon log tail:\n{server.daemon_log_tail()}"
            )
        time.sleep(POLL_SECONDS)


def running_services(server):
    ps = [*server.compose, "ps", "--status", "running", "--services"]
    result = subprocess.run(ps, cwd=server.demo.project, capture_output=True, text=True, check=True)
    return set(result.stdout.split())


def crash_and_restart(server, held_lock, image):
    """SIGKILL both Server services, free the Warehouse lock, start them again; return when."""
    subprocess.run([*server.compose, "kill", *SERVER_SERVICES], cwd=server.demo.project, check=True)
    assert running_services(server).isdisjoint(SERVER_SERVICES)
    held_lock.close()
    assert stage_lock_is_free(server.demo, image), "the killed run worker still holds the lock"
    restarted_at = time.time()
    up = [*server.compose, "up", "-d", "--wait", "--no-build", "--no-deps", *SERVER_SERVICES]
    subprocess.run(up, cwd=server.demo.project, check=True)
    return restarted_at


@dataclass
class AlertReceiver:
    url: str
    bodies: list[bytes] = field(default_factory=list)


@pytest.fixture
def alert_receiver(demo):
    """A webhook on the host; the Server reaches it because it runs with host networking."""
    receiver = AlertReceiver("")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            receiver.bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    receiver.url = f"http://127.0.0.1:{httpd.server_address[1]}/hook"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    set_env(demo.project, ALERT_WEBHOOK_URL=receiver.url)
    try:
        yield receiver
    finally:
        httpd.shutdown()
        httpd.server_close()


def run_status_and_created(env, run_id):
    [(status, created)] = dagster_meta(
        env, "SELECT status, create_timestamp FROM runs WHERE run_id = %s", run_id
    )
    return status, created.replace(tzinfo=UTC)


def test_server_recovers_from_crash_and_next_tick_succeeds(
    alert_receiver, stage_warehouse_lock, server, server_image
):
    began = time.monotonic()
    try:
        stale_run = wait_for_started_run(server)
        assert alert_receiver.bodies == []
        restarted_at = crash_and_restart(server, stage_warehouse_lock, server_image)

        next_run = wait_for_scheduled_success(server, ignoring={stale_run})

        env = server.demo.env
        assert run_status_and_created(env, stale_run)[0] == "FAILURE"
        status, created = run_status_and_created(env, next_run)
        assert next_run != stale_run
        assert status == "SUCCESS"
        assert created > datetime.fromtimestamp(restarted_at, UTC)
        [body] = alert_receiver.bodies
        alert = json.loads(body)
        assert (alert["pipeline"], alert["environment"]) == (PIPELINE, "stage")
        assert "127.0.0.1" not in body.decode()
        assert "Traceback" not in body.decode()
    finally:
        took = time.monotonic() - began
        print(f"\nrecovery case [{server.demo.warehouse_kind}] took {took:.0f}s")
