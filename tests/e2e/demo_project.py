import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import psycopg
import yaml

ADOPS = str(Path(sys.executable).parent / "adops")
WAREHOUSE_DATABASES = {"dev": "warehouse", "stage": "warehouse_stage"}


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
