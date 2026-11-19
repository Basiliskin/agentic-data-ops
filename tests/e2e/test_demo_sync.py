import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import psycopg
import pytest
import yaml
from dotenv import dotenv_values

pytestmark = pytest.mark.e2e

ADOPS = str(Path(sys.executable).parent / "adops")
RUN = ("run", "shop_sync")
SEED_EMAIL = "customer7@example.com"
CUSTOMER_CHECKS = (
    "source_tables_exist",
    "source_not_empty",
    "primary_key_unique",
    "primary_key_not_null",
    "row_count_matches",
    "not_null(email)",
)
ORDER_CHECKS = (
    "source_tables_exist",
    "source_not_empty",
    "primary_key_unique",
    "primary_key_not_null",
    "no_row_loss",
    "cursor_not_backwards",
    "min_rows",
)
RELATIONSHIPS = "relationships_orders_customer_id__id__ref_customers_"
DBT_CHECKS = (
    ("customers", "unique_customers_id"),
    ("customers", "not_null_customers_id"),
    ("orders", RELATIONSHIPS),
)
ORPHAN_CUSTOMER_ID = "999"
REVERSE_SYNC = "reverse_sync_customer_stats"
REVERSE_SYNC_CHECKS = (
    "target_columns_exist",
    "key_unique",
    "key_not_null",
    "max_change",
    "result_not_empty",
)
WAREHOUSE_DATABASES = {"dev": "warehouse", "stage": "warehouse_stage"}


def stats_row(customer_id, total_amount, last_order_at):
    hour, minute = last_order_at
    when = datetime(2026, 2, 1, hour, minute, tzinfo=UTC)
    return (customer_id, 5, Decimal(total_amount), when)


SEED_CUSTOMER_STATS = [
    stats_row(1, "1112.05", (13, 30)),
    stats_row(2, "797.10", (13, 40)),
    stats_row(3, "982.15", (13, 50)),
    stats_row(4, "1167.20", (14, 0)),
    stats_row(5, "1352.25", (14, 10)),
    stats_row(6, "1537.30", (14, 20)),
    stats_row(7, "1722.35", (14, 30)),
    stats_row(8, "907.40", (14, 40)),
    stats_row(9, "1092.45", (14, 50)),
    stats_row(10, "1277.50", (15, 0)),
    stats_row(11, "1462.55", (15, 10)),
    stats_row(12, "1647.60", (15, 20)),
    stats_row(13, "1832.65", (15, 30)),
    stats_row(14, "1517.70", (15, 40)),
    stats_row(15, "702.75", (15, 50)),
    stats_row(16, "887.80", (16, 0)),
    stats_row(17, "1072.85", (16, 10)),
    stats_row(18, "1257.90", (16, 20)),
    stats_row(19, "1442.95", (16, 30)),
    stats_row(20, "1627.00", (16, 40)),
]


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@dataclass(frozen=True)
class Demo:
    project: Path
    env: dict
    warehouse_kind: str


def init_demo(project):
    subprocess.run([ADOPS, "init", str(project), "--demo"], check=True)
    shutil.copy(project / ".env.example", project / ".env")


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


def use_shipped_duckdb_warehouse(settings):
    """In every Environment, uncomment the shipped DuckDB block and drop the Postgres one."""
    lines = settings.read_text().splitlines()
    while "    # warehouse:" in lines:
        start = lines.index("    # warehouse:")
        end = start + 1
        while lines[end].startswith("    #   "):
            end += 1
        duckdb_block = [line.replace("# ", "", 1) for line in lines[start:end]]
        postgres_start = max(i for i in range(start) if lines[i] == "    warehouse:")
        lines[postgres_start:end] = duckdb_block
    settings.write_text("\n".join(lines) + "\n")


def reseed_shop(stack, env):
    seed = (stack / "seed" / "01_shop.sql").read_text()
    shop(env, f"DROP SCHEMA public CASCADE; CREATE SCHEMA public; {seed}")


@pytest.fixture(params=["postgres", "duckdb"])
def demo(request, stack, tmp_path):
    project = tmp_path / "shop"
    init_demo(project)
    if request.param == "duckdb":
        use_shipped_duckdb_warehouse(project / "adops.yaml")
    env = dotenv_values(project / ".env")
    reseed_shop(stack, env)
    return Demo(project, env, request.param)


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


def customer_stats(env):
    with connect(env, "SHOP", "shop", "shop") as conn:
        return conn.execute(
            "SELECT customer_id, order_count, total_amount, last_order_at"
            " FROM public.customer_stats ORDER BY customer_id"
        ).fetchall()


def warehouse(demo, sql, environment="dev"):
    if demo.warehouse_kind == "duckdb":
        settings = yaml.safe_load((demo.project / "adops.yaml").read_text())
        path = demo.project / settings["environments"][environment]["warehouse"]["path"]
        with duckdb.connect(str(path), read_only=True) as conn:
            return conn.execute(sql).fetchall()
    database = WAREHOUSE_DATABASES[environment]
    with connect(demo.env, "WAREHOUSE", "warehouse", database) as conn:
        return conn.execute(sql).fetchall()


def dagster_meta(env, sql, *params):
    with connect(env, "DAGSTER_PG", env["DAGSTER_PG_USER"], env["DAGSTER_PG_DB"]) as conn:
        return conn.execute(sql, params).fetchall()


def counts(demo):
    return {
        table: warehouse(demo, f"SELECT count(*) FROM raw_shop.{table}")[0][0]
        for table in ("customers", "orders")
    }


def adops_run(project, *args):
    result = subprocess.run(
        [ADOPS, *args],
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
    assert "@example.com" not in output
    assert "Traceback" not in output


def assert_all_checks_pass(output):
    for check in CUSTOMER_CHECKS:
        assert f"  check customers {check}: pass" in output
    for check in ORDER_CHECKS:
        assert f"  check orders {check}: pass" in output
    assert "check orders row_count_matches" not in output


def assert_transform_passed(output):
    assert "step transform_shop: passed" in output
    for table, check in DBT_CHECKS:
        assert f"  check {table} {check}: pass" in output


def assert_reverse_sync_passed(output):
    assert f"step {REVERSE_SYNC}: passed" in output.splitlines()
    for check in REVERSE_SYNC_CHECKS:
        assert f"  check customer_stats {check}: pass" in output.splitlines()


def analytics_rows(demo, environment="dev"):
    sql = "SELECT count(*) FROM analytics.customer_orders"
    return warehouse(demo, sql, environment)[0][0]


def successful_runs(env):
    try:
        return dagster_meta(env, "SELECT count(*) FROM runs WHERE status = 'SUCCESS'")[0][0]
    except psycopg.errors.UndefinedTable:
        return 0


def test_demo_incremental_breaking_change_and_missing_table(demo):
    project, env = demo.project, demo.env
    successful_runs_before = successful_runs(env)

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 100 rows" in output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 100}
    assert analytics_rows(demo) == 100
    assert successful_runs(env) > successful_runs_before
    assert_reverse_sync_passed(output)
    lines = output.splitlines()
    assert "  Preview: would insert 20, would update 0, would delete 0" in lines
    assert "  sample insert keys: 1, 2, 3, 4, 5" in lines
    assert customer_stats(env) == []

    code, output = adops_run(project, *RUN, "--env", "stage")

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 100 rows" in output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_reverse_sync_passed(output)
    assert "  wrote: inserted 20, updated 0, deleted 0" in output.splitlines()
    assert_no_secret_or_row_value(output, env)
    assert customer_stats(env) == SEED_CUSTOMER_STATS
    assert analytics_rows(demo, "stage") == 100
    stage_stats = "SELECT count(*) FROM analytics.customer_stats"
    assert warehouse(demo, stage_stats, "stage") == [(20,)]
    assert counts(demo) == {"customers": 20, "orders": 100}
    assert analytics_rows(demo) == 100

    shop(
        env,
        "INSERT INTO orders VALUES (101, 1, 12.34, TIMESTAMPTZ '2026-03-01 00:00:00+00');"
        "UPDATE orders SET amount = 999.99, updated_at = TIMESTAMPTZ '2026-03-01 00:00:01+00'"
        " WHERE id = 5",
    )

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 2 rows" in output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 101}
    assert analytics_rows(demo) == 101
    assert_reverse_sync_passed(output)
    assert "  Preview: would insert 0, would update 2, would delete 0" in output.splitlines()
    assert customer_stats(env) == SEED_CUSTOMER_STATS
    [(amount,)] = warehouse(demo, "SELECT amount FROM raw_shop.orders WHERE id = 5")
    assert amount == Decimal("999.99")
    duplicates = "SELECT id FROM raw_shop.orders GROUP BY id HAVING count(*) > 1"
    assert warehouse(demo, duplicates) == []

    shop(
        env,
        f"INSERT INTO orders VALUES (150, {ORPHAN_CUSTOMER_ID}, 7.00,"
        " TIMESTAMPTZ '2026-03-02 00:00:00+00')",
    )

    code, output = adops_run(project, *RUN)

    assert code == 1, output
    assert "step sync_shop: passed — customers 20 rows, orders 1 rows" in output
    assert_all_checks_pass(output)
    assert "step transform_shop: check_failed" in output
    assert f"step {REVERSE_SYNC}: skipped" in output.splitlines()
    assert "  check customers unique_customers_id: pass" in output
    assert f"  check orders {RELATIONSHIPS}: fail" in output
    assert f"orders: dbt test {RELATIONSHIPS} failed — 1 failing rows" in output
    assert "dbt run failed" not in output
    assert_no_secret_or_row_value(output, env)
    assert ORPHAN_CUSTOMER_ID not in output
    assert counts(demo) == {"customers": 20, "orders": 102}
    orphans = (
        "SELECT count(*) FROM raw_shop.orders o"
        " WHERE NOT EXISTS (SELECT 1 FROM raw_shop.customers c WHERE c.id = o.customer_id)"
    )
    assert warehouse(demo, orphans) == [(1,)]

    shop(
        env,
        "UPDATE orders SET customer_id = 1, updated_at = TIMESTAMPTZ '2026-03-03 00:00:00+00'"
        " WHERE id = 150",
    )

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert "step sync_shop: passed — customers 20 rows, orders 1 rows" in output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_reverse_sync_passed(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 102}
    assert warehouse(demo, orphans) == [(0,)]
    assert analytics_rows(demo) == 102

    shop(
        env,
        "ALTER TABLE orders DROP COLUMN customer_id;"
        "INSERT INTO orders VALUES (102, 5.00, TIMESTAMPTZ '2026-03-15 00:00:00+00')",
    )
    before_halt = counts(demo)

    code, output = adops_run(project, *RUN)

    assert code == 1, output
    assert "Breaking Change in orders: customer_id dropped" in output
    assert "step transform_shop: skipped" in output
    assert f"step {REVERSE_SYNC}: skipped" in output.splitlines()
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == before_halt

    code, output = adops_run(project, "accept-schema", "shop_sync", "orders")

    assert code == 0, output
    assert "Accepted the current shape of orders" in output
    assert "in Environment dev." in output
    assert "may now be NULL in the Warehouse: customer_id" in output
    assert_no_secret_or_row_value(output, env)

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 103}

    shop(env, "UPDATE customers SET email = NULL WHERE id = 7")

    code, output = adops_run(project, *RUN)

    assert code == 1, output
    assert "  check customers not_null(email): fail" in output
    assert "customers: not_null(email) failed — 1 rows with no value" in output
    assert "Breaking Change" not in output
    assert "step transform_shop: skipped" in output
    assert f"step {REVERSE_SYNC}: skipped" in output.splitlines()
    assert_no_secret_or_row_value(output, env)
    for nulled_row_value in ("(7,", "2026-01-01 07:00", "2026, 1, 1, 7"):
        assert nulled_row_value not in output

    shop(env, f"UPDATE customers SET email = '{SEED_EMAIL}' WHERE id = 7")

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_no_secret_or_row_value(output, env)
    nulls = "SELECT count(*) FROM raw_shop.customers WHERE email IS NULL"
    assert warehouse(demo, nulls) == [(0,)]

    shop(
        env,
        "INSERT INTO orders VALUES (103, 5.00, TIMESTAMPTZ '2026-04-01 00:00:00+00');"
        "DROP TABLE customers",
    )

    code, output = adops_run(project, *RUN)

    assert code == 1, output
    assert "  check customers source_tables_exist: fail" in output
    assert "customers: source_tables_exist failed — table not found in the Source" in output
    assert "step transform_shop: skipped" in output
    assert f"step {REVERSE_SYNC}: skipped" in output.splitlines()
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 103}

    for name in ("SHOP_PASSWORD", "WAREHOUSE_PASSWORD"):
        leaks = (
            "SELECT (SELECT count(*) FROM event_logs WHERE event LIKE '%%' || %s || '%%')"
            " + (SELECT count(*) FROM runs WHERE run_body LIKE '%%' || %s || '%%')"
        )
        assert dagster_meta(env, leaks, env[name], env[name])[0][0] == 0
