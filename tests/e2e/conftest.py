import os
import subprocess
import uuid

import pytest
from demo_project import (
    Demo,
    docker_available,
    init_demo,
    reseed_shop,
    use_shipped_duckdb_warehouse,
)
from dotenv import dotenv_values


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
    if request.param == "duckdb":
        use_shipped_duckdb_warehouse(project / "adops.yaml")
    env = dotenv_values(project / ".env")
    reseed_shop(stack, env)
    return Demo(project, env, request.param)
