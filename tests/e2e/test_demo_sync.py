import os
import shutil
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from dotenv import dotenv_values

pytestmark = pytest.mark.e2e

ADOPS = str(Path(sys.executable).parent / "adops")
SEED_EMAIL = "customer7@example.com"
CUSTOMER_CHECKS = (
    "source_tables_exist",
    "primary_key_unique",
    "primary_key_not_null",
    "row_count_matches",
)
ORDER_CHECKS = (
    "source_tables_exist",
    "primary_key_unique",
    "primary_key_not_null",
    "no_row_loss",
    "cursor_not_backwards",
)


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    if not docker_available():
        if os.environ.get("CI"):
            pytest.fail("Docker is required for the e2e tests in CI")
        pytest.skip("Docker is not available")
    project = tmp_path_factory.mktemp("demo") / "shop"
    subprocess.run([ADOPS, "init", str(project), "--demo"], check=True)
    shutil.copy(project / ".env.example", project / ".env")
    compose = ["docker", "compose", "-p", f"adops-e2e-{uuid.uuid4().hex[:8]}"]
    try:
        subprocess.run([*compose, "up", "--wait"], cwd=project, check=True)
        yield project, dotenv_values(project / ".env")
    finally:
        subprocess.run([*compose, "down", "-v"], cwd=project, check=True)


def connect(env, prefix, user, database):
    return psycopg.connect(
        host="localhost",
        port=int(env[f"{prefix}_PORT"]),
        user=user,
        dbname=database,
        password=env[f"{prefix}_PASSWORD"],
        autocommit=True,
    )


def shop(env, sql):
    with connect(env, "SHOP", "shop", "shop") as conn:
        conn.execute(sql)


def warehouse(env, sql):
    with connect(env, "WAREHOUSE", "warehouse", "warehouse") as conn:
        return conn.execute(sql).fetchall()


def dagster_meta(env, sql, *params):
    with connect(env, "DAGSTER_PG", env["DAGSTER_PG_USER"], env["DAGSTER_PG_DB"]) as conn:
        return conn.execute(sql, params).fetchall()


def counts(env):
    return {
        table: warehouse(env, f"SELECT count(*) FROM raw_shop.{table}")[0][0]
        for table in ("customers", "orders")
    }


def adops_run(project):
    result = subprocess.run(
        [ADOPS, "run", "shop_sync"],
        cwd=project,
        capture_output=True,
        text=True,
        env={k: v for k, v in os.environ.items() if k != "DAGSTER_HOME"},
    )
    return result.returncode, result.stdout + result.stderr


def assert_no_secret_or_row_value(output, env):
    for name in ("SHOP_PASSWORD", "WAREHOUSE_PASSWORD", "DAGSTER_PG_PASSWORD"):
        assert env[name] not in output
    assert SEED_EMAIL not in output
    assert "Traceback" not in output


def assert_all_checks_pass(output):
    for check in CUSTOMER_CHECKS:
        assert f"  check customers {check}: pass" in output
    for check in ORDER_CHECKS:
        assert f"  check orders {check}: pass" in output
    assert "check orders row_count_matches" not in output


def test_demo_full_refresh_incremental_and_missing_table(demo):
    project, env = demo

    code, output = adops_run(project)

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 100 rows" in output
    assert_all_checks_pass(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(env) == {"customers": 20, "orders": 100}
    assert dagster_meta(env, "SELECT count(*) FROM runs WHERE status = 'SUCCESS'")[0][0] >= 1

    shop(
        env,
        "INSERT INTO orders VALUES (101, 1, 12.34, TIMESTAMPTZ '2026-03-01 00:00:00+00');"
        "UPDATE orders SET amount = 999.99, updated_at = TIMESTAMPTZ '2026-03-01 00:00:01+00'"
        " WHERE id = 5",
    )

    code, output = adops_run(project)

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 2 rows" in output
    assert_all_checks_pass(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(env) == {"customers": 20, "orders": 101}
    [(amount,)] = warehouse(env, "SELECT amount FROM raw_shop.orders WHERE id = 5")
    assert amount == Decimal("999.99")
    duplicates = "SELECT id FROM raw_shop.orders GROUP BY id HAVING count(*) > 1"
    assert warehouse(env, duplicates) == []

    shop(
        env,
        "INSERT INTO orders VALUES (102, 2, 5.00, TIMESTAMPTZ '2026-04-01 00:00:00+00');"
        "DROP TABLE customers",
    )

    code, output = adops_run(project)

    assert code == 1, output
    assert "  check customers source_tables_exist: fail" in output
    assert "customers: source_tables_exist failed — table not found in the Source" in output
    assert_no_secret_or_row_value(output, env)
    assert counts(env) == {"customers": 20, "orders": 101}

    for name in ("SHOP_PASSWORD", "WAREHOUSE_PASSWORD"):
        leaks = (
            "SELECT (SELECT count(*) FROM event_logs WHERE event LIKE '%%' || %s || '%%')"
            " + (SELECT count(*) FROM runs WHERE run_body LIKE '%%' || %s || '%%')"
        )
        assert dagster_meta(env, leaks, env[name], env[name])[0][0] == 0
