import os
import shutil
import subprocess
import uuid

import pytest
from demo_project import (
    Demo,
    docker_available,
    init_demo,
    reseed_shop,
    reset_postgres_warehouses,
    use_shipped_duckdb_warehouse,
)
from dotenv import dotenv_values

RELEASE = "e2e"
SERVER_FOLDER_FILES = ("docker-compose.yml", "Dockerfile", "server")
SHARED = (".env", ".adops")


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    if not docker_available():
        if os.environ.get("CI"):
            pytest.fail("Docker is required for the e2e tests in CI")
        pytest.skip("Docker is not available")
    project = tmp_path_factory.mktemp("stack") / "shop"
    init_demo(project)
    compose = ["docker", "compose", "-p", f"adops-e2e-{uuid.uuid4().hex[:8]}"]
    try:
        subprocess.run([*compose, "up", "--wait"], cwd=project, check=True)
        yield project
    finally:
        subprocess.run([*compose, "down", "-v"], cwd=project, check=True)


@pytest.fixture(params=["postgres", "duckdb"])
def demo(request, stack, tmp_path):
    project = tmp_path / "shop"
    init_demo(project)
    env = dotenv_values(project / ".env")
    if request.param == "duckdb":
        use_shipped_duckdb_warehouse(project / "adops.yaml")
    else:
        reset_postgres_warehouses(env)
    reseed_shop(stack, env)
    return Demo(project, env, request.param)


@pytest.fixture
def server_demo(demo):
    """The demo moved to a Server folder's layout: releases/<commit> plus a relative 'current'."""
    root = demo.project
    release = root / "releases" / RELEASE
    release.mkdir(parents=True)
    (root / ".adops").mkdir(exist_ok=True)
    for path in list(root.iterdir()):
        if path.name in SERVER_FOLDER_FILES:
            copy = shutil.copytree if path.is_dir() else shutil.copy2
            copy(path, release / path.name)
        elif path.name not in {*SHARED, "releases"}:
            shutil.move(path, release / path.name)
    for name in SHARED:
        os.symlink(os.path.join("..", "..", name), release / name)
    os.symlink(os.path.join("releases", RELEASE), root / "current")
    return demo
