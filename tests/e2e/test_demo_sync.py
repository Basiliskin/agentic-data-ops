import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from demo_project import ADOPS, connect, dagster_meta, shop, warehouse

pytestmark = pytest.mark.e2e

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


def customer_stats(env, database="shop"):
    with connect(env, "SHOP", "shop", database) as conn:
        return conn.execute(
            "SELECT customer_id, order_count, total_amount, last_order_at"
            " FROM public.customer_stats ORDER BY customer_id"
        ).fetchall()


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
    assert customer_stats(env, "shop_dev") == []

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
    assert "  Preview: would insert 20, would update 0, would delete 0" in output.splitlines()
    assert customer_stats(env) == SEED_CUSTOMER_STATS
    assert customer_stats(env, "shop_dev") == []
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
    assert not [
        line
        for line in output.splitlines()
        if line.startswith(("step ", "  ")) and ORPHAN_CUSTOMER_ID in line
    ]
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

    code, output = adops_run(project, *RUN, "--env", "stage")

    assert code == 1, output
    assert "Breaking Change in orders: customer_id dropped" in output
    assert "add an accepted-change record for orders to the PR" in output
    assert_no_secret_or_row_value(output, env)

    code, output = adops_run(project, "accept-schema", "shop_sync", "orders")

    assert code == 0, output
    assert "Accepted the current shape of orders" in output
    assert "in Environment dev." in output
    assert "may now be NULL in the Warehouse: customer_id" in output
    assert_no_secret_or_row_value(output, env)
    record = Path(re.search(r"Wrote accepted-change record (\S+\.yaml) ", output)[1])
    assert record.parent == Path("pipelines", "accepted")
    assert sorted((project / record.parent).iterdir()) == [project / record]

    code, output = adops_run(project, "validate")

    assert code == 0, output

    code, output = adops_run(project, *RUN)

    assert code == 0, output
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_no_secret_or_row_value(output, env)
    assert counts(demo) == {"customers": 20, "orders": 103}

    code, output = adops_run(project, *RUN, "--env", "stage")

    assert code == 0, output
    assert f"  Applied accepted-change record {record.stem} in stage" in output.splitlines()
    assert_no_secret_or_row_value(output, env)

    code, output = adops_run(project, *RUN, "--env", "stage")

    assert code == 0, output
    assert "Applied accepted-change record" not in output

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
    run_logs = [path.read_text() for path in (project / ".adops" / "logs" / "dev").iterdir()]
    assert run_logs
    for name in ("SHOP_PASSWORD", "WAREHOUSE_PASSWORD"):
        assert all(env[name] not in text for text in run_logs)


def stage_schemas(demo):
    sql = (
        "SELECT schema_name FROM information_schema.schemata"
        " WHERE catalog_name = current_database()"
    )
    return sorted(name for (name,) in warehouse(demo, sql, "stage"))


def branch_schemas(demo, branch):
    return [schema for schema in stage_schemas(demo) if schema.startswith(f"br_{branch}__")]


def stage_contents(demo):
    """Every row of every table and view stage owns, so any write by a Branch shows up."""
    tables = warehouse(
        demo,
        "SELECT table_schema, table_name FROM information_schema.tables"
        " WHERE table_catalog = current_database()"
        " AND table_schema NOT IN ('pg_catalog', 'information_schema')",
        "stage",
    )
    contents = {"schemas": [s for s in stage_schemas(demo) if not s.startswith("br_")]}
    for schema, table in sorted(tables):
        if not schema.startswith("br_"):
            rows = warehouse(demo, f'SELECT * FROM "{schema}"."{table}"', "stage")
            contents[f"{schema}.{table}"] = sorted(rows, key=repr)
    return contents


def assert_branch_run_passed(code, output, env):
    assert code == 0, output
    assert_no_secret_or_row_value(output, env)
    assert "wrote:" not in output


def test_branch_environment_defers_rebuilds_and_drops(demo):
    project, env = demo.project, demo.env
    model = project / "dbt" / "models" / "customer_stats.sql"
    pipeline = project / "pipelines" / "shop_sync.yaml"
    original_model, original_pipeline = model.read_text(), pipeline.read_text()

    code, output = adops_run(project, *RUN, "--env", "stage")

    assert code == 0, output
    stage_before = stage_contents(demo)
    target_before = customer_stats(env)
    assert stage_before["adops._adops_step_fingerprints"]
    assert target_before == SEED_CUSTOMER_STATS

    model.write_text("-- changed on a branch\n" + original_model)

    code, output = adops_run(project, *RUN, "--branch", "feat")

    assert_branch_run_passed(code, output, env)
    lines = output.splitlines()
    assert "step sync_shop: deferred to stage" in lines
    assert_transform_passed(output)
    assert_reverse_sync_passed(output)
    assert "  Preview: would insert 0, would update 0, would delete 0" in lines
    assert branch_schemas(demo, "feat") == ["br_feat__analytics"]
    branch_orders = "SELECT count(*) FROM br_feat__analytics.customer_orders"
    assert warehouse(demo, branch_orders, "stage")[0][0] == analytics_rows(demo, "stage")

    code, output = adops_run(project, *RUN, "--branch", "feat2")

    assert_branch_run_passed(code, output, env)
    assert "step sync_shop: deferred to stage" in output.splitlines()
    assert_transform_passed(output)
    assert branch_schemas(demo, "feat2") == ["br_feat2__analytics"]

    sync_edit = original_pipeline.replace("min_rows, count: 1}", "min_rows, count: 2}")
    assert sync_edit != original_pipeline
    pipeline.write_text(sync_edit)

    code, output = adops_run(project, *RUN, "--branch", "feat")

    assert_branch_run_passed(code, output, env)
    lines = output.splitlines()
    assert "step sync_shop: passed — customers 20 rows, orders 100 rows" in lines
    assert_all_checks_pass(output)
    assert_transform_passed(output)
    assert_reverse_sync_passed(output)
    assert "  Preview: would insert 0, would update 0, would delete 0" in lines
    for table in ("customers", "orders"):
        rows = warehouse(demo, f"SELECT count(*) FROM br_feat__raw_shop.{table}", "stage")
        assert rows[0][0] > 0
    assert "br_feat__raw_shop" in branch_schemas(demo, "feat")
    assert stage_contents(demo) == stage_before
    assert customer_stats(env) == target_before

    model.write_text(original_model)
    pipeline.write_text(original_pipeline)

    code, output = adops_run(project, *RUN, "--branch", "feat2")

    assert_branch_run_passed(code, output, env)
    lines = output.splitlines()
    for step in ("sync_shop", "transform_shop", REVERSE_SYNC):
        assert f"step {step}: deferred to stage" in lines
    assert "Nothing changed since stage's last run; br_feat2 uses stage." in lines

    code, output = adops_run(project, "drop-branch", "feat")

    assert code == 0, output
    assert "Dropped Branch Environment br_feat: " in output
    assert_no_secret_or_row_value(output, env)
    assert branch_schemas(demo, "feat") == []
    assert branch_schemas(demo, "feat2") == ["br_feat2__analytics"]
    assert not (project / ".adops" / "dlt" / "br_feat").exists()
    assert stage_contents(demo) == stage_before
    assert customer_stats(env) == target_before

    code, output = adops_run(project, "drop-branch", "feat")

    assert code == 0, output
    assert "Branch Environment br_feat has no schemas left to drop." in output

    code, output = adops_run(project, "drop-branch", "feat2")

    assert code == 0, output
    assert_no_secret_or_row_value(output, env)
    assert [s for s in stage_schemas(demo) if s.startswith("br_")] == []
    assert stage_contents(demo) == stage_before


# Runs `adops` with its Warehouse lock wrapped, so the test sees when each process held it.
LOCK_PROBE = """
import sys, time
from contextlib import contextmanager
from adops.interface import cli, run_command

real_lock = run_command.build_warehouse_lock

@contextmanager
def probed_lock(*args):
    with real_lock(*args):
        print(f"LOCK_HELD {time.time()}", flush=True)
        try:
            yield
        finally:
            print(f"LOCK_RELEASED {time.time()}", flush=True)

run_command.build_warehouse_lock = probed_lock
cli.app(sys.argv[1:], prog_name="adops")
"""


def held_interval(output):
    stamps = dict(
        line.split()
        for line in output.splitlines()
        if line.startswith(("LOCK_HELD ", "LOCK_RELEASED "))
    )
    return float(stamps["LOCK_HELD"]), float(stamps["LOCK_RELEASED"])


def test_two_stage_runs_at_once_do_not_overlap_on_the_warehouse(demo):
    env = {k: v for k, v in os.environ.items() if k != "DAGSTER_HOME"}
    command = [sys.executable, "-c", LOCK_PROBE, *RUN, "--env", "stage"]
    runs = [
        subprocess.Popen(
            command,
            cwd=demo.project,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for _ in range(2)
    ]
    outputs = [run.communicate()[0] for run in runs]

    for run, output in zip(runs, outputs, strict=True):
        assert run.returncode == 0, output
        assert "Pipeline shop_sync passed in Environment stage." in output
        assert_no_secret_or_row_value(output, demo.env)
    first, second = sorted(held_interval(output) for output in outputs)
    overlap = f"both runs held the {demo.warehouse_kind} Warehouse at the same time"
    assert first[1] <= second[0], overlap
