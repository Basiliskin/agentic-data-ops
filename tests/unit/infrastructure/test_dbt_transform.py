import ast
import os
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
import yaml
from conftest import PostgresWarehouse
from dagster import DagsterInstance
from dbt.cli.main import dbtRunner, dbtRunnerResult

import adops
from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    StepStatus,
    Transform,
    TransformResult,
)
from adops.application.run_pipeline import run_pipeline
from adops.domain.pipeline import SyncStep
from adops.domain.schema_snapshot import TableShape
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator
from adops.infrastructure.dbt_transform import (
    PASSWORD_ENV,
    TARGET_SCHEMA,
    DbtTransform,
    profile_renderer,
    render_duckdb_profile,
    render_postgres_profile,
)
from adops.infrastructure.dlt_sync import DltSync, UnknownConnectionKindError
from adops.infrastructure.schema_snapshot_store import WarehouseSnapshotStore

PASSWORD = "s3cr3t-dbt-warehouse-sentinel"
ERROR_SENTINEL = "dbt_error_text_sentinel_7731"
WAREHOUSE = ResolvedConnection("db.internal", 5433, "warehouse", "adops", PASSWORD)
STEP = TransformStep(type="transform", name="transform_shop", project="dbt")
ARTEFACTS = ("target", "logs", "profiles.yml", "dbt_packages")

ORDERS_TESTS = """
version: 2
models:
  - name: orders
    columns:
      - name: id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - not_null
          - accepted_values:
              arguments: {values: [paid, shipped]}
              config: {severity: warn}
"""

SETTINGS = """
environments:
  dev:
    warehouse:
      type: postgres
      host: db.internal
      port: 5433
      database: warehouse
      user: adops
      password_env: WAREHOUSE_PASSWORD
    sources: {}
"""

TRANSFORM_PIPELINE = """
name: shop
steps:
  - {name: transform_shop, type: transform, project: dbt}
"""


def orders_sql(*rows: tuple[int, str | None]) -> str:
    values = ", ".join(f"({id_}, {'null' if s is None else repr(s)})" for id_, s in rows)
    return f"select * from (values {values}) as t(id, status)"


HEALTHY_ORDERS = orders_sql((1, "paid"), (2, "shipped"))


def write_dbt_project(
    root: Path, models: Mapping[str, str], schema: str = ORDERS_TESTS, **extra: str
) -> Path:
    project = root / "dbt"
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "dbt_project.yml").write_text("name: shop\nversion: '1.0'\nprofile: shop\n")
    for name, sql in models.items():
        (project / "models" / f"{name}.sql").write_text(sql)
    (project / "models" / "schema.yml").write_text(schema)
    for relative, text in extra.items():
        path = project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return project


def postgres_password_on_duckdb(database: Path):
    """Runs the Postgres password path on a DuckDB file, reading the password through env_var().

    external_root is used only by external materializations, which these projects lack, so the
    field only proves the variable is set while dbt runs: dbt refuses an env_var() with no value.
    """

    def render(
        profile_name: str, warehouse: ResolvedConnection, _project_root: Path, _schema: str
    ) -> str:
        target = {
            "type": "duckdb",
            "path": str(database),
            "external_root": f"{{{{ env_var('{PASSWORD_ENV}') }}}}",
        }
        return yaml.safe_dump(
            {profile_name: {"target": "warehouse", "outputs": {"warehouse": target}}}
        )

    return render


@pytest.fixture
def root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    project_root.mkdir()
    return project_root


@pytest.fixture
def transform(root: Path) -> DbtTransform:
    return DbtTransform(root, warehouse_kind="duckdb")


@pytest.fixture
def duck(tmp_path: Path) -> ResolvedDuckDb:
    return ResolvedDuckDb(str(tmp_path / "warehouse.duckdb"))


def password_transform(tmp_path: Path, root: Path, **kwargs) -> DbtTransform:
    return DbtTransform(
        root,
        warehouse_kind="postgres",
        render_profile=postgres_password_on_duckdb(tmp_path / "warehouse.duckdb"),
        **kwargs,
    )


def facts_by_name(result: TransformResult) -> dict[str, DbtTestFacts]:
    return {fact.name: fact for fact in result.tests}


class ScanningRunner:
    """A real dbtRunner that also keeps every file dbt wrote to its working folder."""

    written: dict[str, bytes] = {}

    def invoke(self, args: list[str]) -> dbtRunnerResult:
        result = dbtRunner().invoke(args)
        work = Path(args[args.index("--profiles-dir") + 1])
        ScanningRunner.written = {
            path.relative_to(work).as_posix(): path.read_bytes()
            for path in work.rglob("*")
            if path.is_file()
        }
        return result


class EnvironRecordingRunner:
    """A real dbtRunner that remembers the environment dbt saw while it ran."""

    seen: dict[str, str] = {}

    def invoke(self, args: list[str]) -> dbtRunnerResult:
        EnvironRecordingRunner.seen = dict(os.environ)
        return dbtRunner().invoke(args)


class ExplodingRunner:
    def invoke(self, args: list[str]) -> dbtRunnerResult:
        assert os.environ[PASSWORD_ENV] == PASSWORD
        raise RuntimeError("dbt blew up")


def test_dbt_transform_is_a_transform(transform: DbtTransform):
    assert isinstance(transform, Transform)


def test_passing_build_reports_every_test_with_its_model_and_zero_failures(
    root: Path, transform: DbtTransform, duck: ResolvedDuckDb
):
    write_dbt_project(
        root,
        {"orders": HEALTHY_ORDERS},
        **{"tests/orders_have_ids.sql": "select * from {{ ref('orders') }} where id is null"},
    )

    result = transform.run(STEP, duck)

    assert result.crashed is False
    assert result.failed_models == ()
    assert sorted((f.name, f.table, f.status, f.failures) for f in result.tests) == [
        ("accepted_values_orders_status__paid__shipped", "orders", DbtTestStatus.PASS, 0),
        ("not_null_orders_id", "orders", DbtTestStatus.PASS, 0),
        ("not_null_orders_status", "orders", DbtTestStatus.PASS, 0),
        ("orders_have_ids", "orders", DbtTestStatus.PASS, 0),
        ("unique_orders_id", "orders", DbtTestStatus.PASS, 0),
    ]
    assert all(f.unique_id.startswith(f"test.shop.{f.name}") for f in result.tests)


def test_failing_test_reports_the_exact_number_of_failing_rows(
    root: Path, transform: DbtTransform, duck: ResolvedDuckDb
):
    write_dbt_project(
        root, {"orders": orders_sql((1, "paid"), (2, None), (3, None), (4, None))}
    )

    facts = facts_by_name(transform.run(STEP, duck))

    assert facts["not_null_orders_status"].status is DbtTestStatus.FAIL
    assert facts["not_null_orders_status"].failures == 3
    assert facts["not_null_orders_id"].status is DbtTestStatus.PASS


def test_severity_warn_test_reports_warn_with_its_failing_rows(
    root: Path, transform: DbtTransform, duck: ResolvedDuckDb
):
    write_dbt_project(root, {"orders": orders_sql((1, "paid"), (2, "lost"), (3, "stolen"))})

    result = transform.run(STEP, duck)

    warned = facts_by_name(result)["accepted_values_orders_status__paid__shipped"]
    assert warned.status is DbtTestStatus.WARN
    assert warned.failures == 2
    assert result.crashed is False
    assert result.failed_models == ()


def test_broken_model_is_a_failed_model_and_its_tests_are_skipped(
    root: Path, transform: DbtTransform, duck: ResolvedDuckDb
):
    schema = ORDERS_TESTS + "  - name: broken\n    columns:\n      - name: id\n"
    schema += "        data_tests: [not_null]\n"
    write_dbt_project(
        root,
        {"orders": HEALTHY_ORDERS, "broken": f"select * from {ERROR_SENTINEL}"},
        schema=schema,
    )

    result = transform.run(STEP, duck)

    assert result.crashed is False
    assert result.failed_models == ("broken",)
    skipped = facts_by_name(result)["not_null_broken_id"]
    assert (skipped.table, skipped.status, skipped.failures) == (
        "broken",
        DbtTestStatus.SKIPPED,
        None,
    )
    assert ERROR_SENTINEL not in repr(result)


def test_failing_dbt_unit_test_stops_the_build_as_a_failed_model(
    root: Path, transform: DbtTransform, duck: ResolvedDuckDb
):
    schema = ORDERS_TESTS + (
        "unit_tests:\n"
        "  - name: orders_keep_their_ids\n"
        "    model: orders\n"
        "    given: []\n"
        "    expect:\n"
        "      rows:\n"
        "        - {id: 99}\n"
    )
    write_dbt_project(root, {"orders": HEALTHY_ORDERS}, schema=schema)

    result = transform.run(STEP, duck)

    assert result.crashed is False
    assert result.failed_models == ("orders_keep_their_ids",)


def test_missing_project_folder_is_a_crash(transform: DbtTransform, duck: ResolvedDuckDb):
    result = transform.run(STEP, duck)

    assert result == TransformResult(tests=(), crashed=True, failed_models=())


def test_same_adapter_twice_in_one_process_gives_independent_results(
    tmp_path: Path, root: Path, monkeypatch: pytest.MonkeyPatch
):
    transform = password_transform(tmp_path, root)
    monkeypatch.setenv(PASSWORD_ENV, "value-from-before")
    before = dict(os.environ)
    write_dbt_project(root, {"orders": orders_sql((1, "paid"), (2, None))})

    failed = transform.run(STEP, WAREHOUSE)
    after_first = dict(os.environ)
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    passed = transform.run(STEP, WAREHOUSE)

    assert facts_by_name(failed)["not_null_orders_status"].failures == 1
    assert {f.status for f in passed.tests} == {DbtTestStatus.PASS}
    assert after_first == before
    assert dict(os.environ) == before


def test_password_leaves_the_environment_when_dbt_raises(root: Path, monkeypatch):
    monkeypatch.delenv(PASSWORD_ENV, raising=False)
    before = dict(os.environ)
    transform = DbtTransform(root, warehouse_kind="postgres", runner=ExplodingRunner)

    with pytest.raises(RuntimeError, match="dbt blew up"):
        transform.run(STEP, WAREHOUSE)

    assert PASSWORD_ENV not in os.environ
    assert dict(os.environ) == before


def test_dbt_writes_nothing_into_the_project_and_no_file_holds_the_password(
    tmp_path: Path, root: Path
):
    project = write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = password_transform(tmp_path, root, runner=ScanningRunner)

    result = transform.run(STEP, WAREHOUSE)

    assert result.tests
    assert "profiles.yml" in ScanningRunner.written
    assert any(name.startswith("target/") for name in ScanningRunner.written)
    for name, content in ScanningRunner.written.items():
        assert PASSWORD.encode() not in content, name
    for folder in (root, project):
        for artefact in ARTEFACTS:
            assert not (folder / artefact).exists(), folder / artefact
    assert sorted(p.name for p in root.iterdir()) == ["dbt"]


def test_postgres_profile_reads_the_password_only_through_env_var():
    text = render_postgres_profile("adops", WAREHOUSE, Path("unused"))

    assert PASSWORD not in text
    assert f"env_var('{PASSWORD_ENV}')" in text
    target = yaml.safe_load(text)["adops"]["outputs"]["warehouse"]
    assert target == {
        "type": "postgres",
        "host": "db.internal",
        "port": 5433,
        "dbname": "warehouse",
        "user": "adops",
        "password": f"{{{{ env_var('{PASSWORD_ENV}') }}}}",
        "schema": "analytics",
        "threads": 1,
    }


def test_duckdb_profile_points_at_the_absolute_project_file_with_one_thread(tmp_path: Path):
    text = render_duckdb_profile("adops", ResolvedDuckDb("data/wh.duckdb"), tmp_path)

    target = yaml.safe_load(text)["adops"]["outputs"]["warehouse"]
    assert target == {
        "type": "duckdb",
        "path": str(tmp_path / "data" / "wh.duckdb"),
        "schema": "analytics",
        "threads": 1,
    }
    assert Path(target["path"]).is_absolute()
    assert "env_var" not in text


def test_duckdb_profile_resolves_a_relative_project_folder_against_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.chdir(tmp_path)

    text = render_duckdb_profile("adops", ResolvedDuckDb("wh.duckdb"), Path("project"))

    path = yaml.safe_load(text)["adops"]["outputs"]["warehouse"]["path"]
    assert path == str(tmp_path / "project" / "wh.duckdb")


def test_duckdb_profile_keeps_an_absolute_path_as_given(tmp_path: Path):
    elsewhere = tmp_path / "elsewhere" / "wh.duckdb"

    text = render_duckdb_profile("adops", ResolvedDuckDb(str(elsewhere)), tmp_path / "project")

    assert yaml.safe_load(text)["adops"]["outputs"]["warehouse"]["path"] == str(elsewhere)


def test_relative_duckdb_path_builds_into_the_project_file_whatever_the_cwd(
    tmp_path: Path, root: Path, transform: DbtTransform, monkeypatch: pytest.MonkeyPatch
):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    project = write_dbt_project(root, {"orders": HEALTHY_ORDERS})

    result = transform.run(STEP, ResolvedDuckDb("wh.duckdb"))

    assert {f.status for f in result.tests} == {DbtTestStatus.PASS}
    assert not (elsewhere / "wh.duckdb").exists()
    assert not (project / "wh.duckdb").exists()
    with duckdb.connect(str(root / "wh.duckdb")) as connection:
        rows = connection.execute("select id, status from analytics.orders order by id").fetchall()
    assert rows == [(1, "paid"), (2, "shipped")]


def test_duckdb_run_leaves_the_environment_untouched_while_dbt_runs(
    root: Path, duck: ResolvedDuckDb, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv(PASSWORD_ENV, raising=False)
    before = dict(os.environ)
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = DbtTransform(root, warehouse_kind="duckdb", runner=EnvironRecordingRunner)

    result = transform.run(STEP, duck)

    assert result.tests
    assert PASSWORD_ENV not in EnvironRecordingRunner.seen
    assert EnvironRecordingRunner.seen == before
    assert dict(os.environ) == before


def test_duckdb_run_keeps_a_password_variable_the_caller_already_had(
    root: Path, duck: ResolvedDuckDb, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(PASSWORD_ENV, "callers-own-value")
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = DbtTransform(root, warehouse_kind="duckdb", runner=EnvironRecordingRunner)

    transform.run(STEP, duck)

    assert EnvironRecordingRunner.seen[PASSWORD_ENV] == "callers-own-value"
    assert os.environ[PASSWORD_ENV] == "callers-own-value"


def test_postgres_run_sets_the_password_only_while_dbt_runs(
    tmp_path: Path, root: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv(PASSWORD_ENV, raising=False)
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = password_transform(tmp_path, root, runner=EnvironRecordingRunner)

    result = transform.run(STEP, WAREHOUSE)

    assert result.tests
    assert EnvironRecordingRunner.seen[PASSWORD_ENV] == PASSWORD
    assert PASSWORD_ENV not in os.environ


def test_the_warehouse_kind_must_be_given(root: Path):
    with pytest.raises(TypeError):
        DbtTransform(root)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("kind", "renderer"),
    [("postgres", render_postgres_profile), ("duckdb", render_duckdb_profile)],
)
def test_warehouse_kind_picks_its_profile_renderer(root: Path, kind: str, renderer):
    assert profile_renderer(kind) is renderer
    assert DbtTransform(root, warehouse_kind=kind).render_profile is renderer


def test_unknown_warehouse_kind_is_rejected_clearly(root: Path):
    with pytest.raises(UnknownConnectionKindError, match="unknown Warehouse kind 'oracle'"):
        profile_renderer("oracle")
    with pytest.raises(UnknownConnectionKindError, match="known: duckdb, postgres"):
        DbtTransform(root, warehouse_kind="oracle")


SHOP_SOURCES = """
version: 2
sources:
  - name: raw_shop
    tables:
      - name: orders
"""


def test_sync_then_snapshot_then_dbt_share_one_duckdb_file(tmp_path: Path, root: Path):
    source_path = tmp_path / "shop.db"
    with sqlite3.connect(source_path) as connection:
        connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        connection.execute("INSERT INTO orders VALUES (1, 'paid'), (2, 'shipped'), (3, 'paid')")
    source = ResolvedConnection("", 0, str(source_path), "", "")
    warehouse = ResolvedDuckDb("warehouse.duckdb")
    sync_step = SyncStep.model_validate(
        {
            "name": "orders",
            "source": "shop",
            "tables": [{"name": "orders", "mode": "full_refresh", "primary_key": "id"}],
        }
    )
    write_dbt_project(
        root,
        {"orders": "select id, status from {{ source('raw_shop', 'orders') }}"},
        **{"models/sources.yml": SHOP_SOURCES},
    )

    adapter = DltSync(
        root, "shop", environment="dev", source_kind="sqlite", warehouse_kind="duckdb"
    )
    synced = adapter.sync(sync_step, source, warehouse)
    WarehouseSnapshotStore(root, "duckdb", environment="dev").save(
        warehouse, "shop", "shop", "orders", TableShape({"id": "bigint", "status": "text"})
    )
    result = DbtTransform(root, warehouse_kind="duckdb").run(STEP, warehouse)

    assert synced.rows_loaded == {"orders": 3}
    assert result.crashed is False
    assert result.failed_models == ()
    assert {f.status for f in result.tests} == {DbtTestStatus.PASS}
    with duckdb.connect(str(root / "warehouse.duckdb")) as connection:
        (count,) = connection.execute("select count(*) from analytics.orders").fetchone()
    assert count == 3


class Unused:
    """Stands in for the Sync, snapshot store and clock a Transform-only Pipeline never calls."""

    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC)


def test_transform_step_runs_as_a_dagster_job_with_no_password_in_storage(
    tmp_path: Path, root: Path, monkeypatch: pytest.MonkeyPatch
):
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    write_dbt_project(root, {"orders": orders_sql((1, "paid"), (2, None))})

    result = run_pipeline(
        TRANSFORM_PIPELINE,
        SETTINGS,
        "dev",
        {"WAREHOUSE_PASSWORD": PASSWORD},
        Unused(),  # type: ignore[arg-type]
        Unused(),  # type: ignore[arg-type]
        Unused(),
        orchestrator=DagsterOrchestrator(),
        transform=password_transform(tmp_path, root),
    )

    (step,) = result.steps
    assert step.status is StepStatus.CHECK_FAILED
    assert {(c.table, c.check, c.status) for c in step.checks} == {
        ("orders", "unique_orders_id", "pass"),
        ("orders", "not_null_orders_id", "pass"),
        ("orders", "not_null_orders_status", "fail"),
        ("orders", "accepted_values_orders_status__paid__shipped", "pass"),
    }
    with DagsterInstance.get() as instance:
        (run,) = instance.get_runs()
        stored = [str(run.run_config), str(run.tags)]
        stored += [str(record) for record in instance.all_logs(run.run_id)]
    assert any("not_null_orders_status" in text for text in stored)
    for text in stored:
        assert PASSWORD not in text
    for path in home.rglob("*"):
        if path.is_file():
            assert PASSWORD.encode() not in path.read_bytes(), path


def test_only_the_dbt_adapter_imports_dbt():
    package = Path(adops.__file__).parent
    importers = set()
    for source in package.rglob("*.py"):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            if any(name.split(".")[0] == "dbt" for name in names):
                importers.add(source.relative_to(package).as_posix())

    assert importers == {"infrastructure/dbt_transform.py"}


BRANCH_ANALYTICS = "br_x__analytics"
BRANCH_RAW = "br_x__raw_shop"
DEMO_SOURCES = (
    Path(adops.__file__).parent / "templates" / "demo" / "dbt" / "models" / "sources.yml"
).read_text()


class ArgvRecordingRunner:
    """A real dbtRunner that keeps the argv and profiles.yml dbt was given."""

    argv: list[str] = []
    profile: dict = {}

    def invoke(self, args: list[str]) -> dbtRunnerResult:
        ArgvRecordingRunner.argv = list(args)
        work = Path(args[args.index("--profiles-dir") + 1])
        ArgvRecordingRunner.profile = yaml.safe_load((work / "profiles.yml").read_text())
        return dbtRunner().invoke(args)


def expected_argv(root: Path, work: str) -> list[str]:
    return [
        "build",
        "--project-dir",
        str(root / "dbt"),
        "--profiles-dir",
        work,
        "--profile",
        "adops",
        "--target",
        "warehouse",
        "--target-path",
        f"{work}/target",
        "--log-path",
        f"{work}/logs",
        "--quiet",
        "--no-send-anonymous-usage-stats",
    ]


def test_the_default_run_builds_into_analytics_with_the_argv_it_always_had(
    root: Path, duck: ResolvedDuckDb
):
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = DbtTransform(root, warehouse_kind="duckdb", runner=ArgvRecordingRunner)

    result = transform.run(STEP, duck)

    argv = ArgvRecordingRunner.argv
    work = argv[argv.index("--profiles-dir") + 1]
    assert argv == expected_argv(root, work)
    assert "--vars" not in argv
    assert ArgvRecordingRunner.profile["adops"]["outputs"]["warehouse"]["schema"] == "analytics"
    assert (transform.target_schema, dict(transform.raw_schemas)) == (TARGET_SCHEMA, {})
    assert {f.status for f in result.tests} == {DbtTestStatus.PASS}


def test_the_default_postgres_profile_text_is_unchanged():
    text = render_postgres_profile("adops", WAREHOUSE, Path("unused"))

    assert "      schema: analytics\n" in text


@pytest.mark.parametrize(
    ("render", "warehouse"),
    [(render_postgres_profile, WAREHOUSE), (render_duckdb_profile, ResolvedDuckDb("wh.duckdb"))],
)
def test_both_profiles_render_the_target_schema_with_its_double_underscore(
    tmp_path: Path, render, warehouse
):
    text = render("adops", warehouse, tmp_path, BRANCH_ANALYTICS)

    assert yaml.safe_load(text)["adops"]["outputs"]["warehouse"]["schema"] == BRANCH_ANALYTICS


def test_raw_schemas_reach_dbt_as_vars_serialized_as_json(root: Path, duck: ResolvedDuckDb):
    write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = DbtTransform(
        root,
        warehouse_kind="duckdb",
        runner=ArgvRecordingRunner,
        raw_schemas={"shop": "raw_shop", "crm": 'odd"name'},
    )

    transform.run(STEP, duck)

    argv = ArgvRecordingRunner.argv
    payload = argv[argv.index("--vars") + 1]
    assert yaml.safe_load(payload) == {
        "adops_raw_schema_shop": "raw_shop",
        "adops_raw_schema_crm": 'odd"name',
    }


def test_the_demo_sources_read_raw_shop_unless_the_var_names_another_schema():
    assert "{{ var('adops_raw_schema_shop', 'raw_shop') }}" in DEMO_SOURCES


SHOP_ORDERS_MODEL = "select id, status from {{ source('raw_shop', 'orders') }}"


def run_demo_sources(root: Path, warehouse, kind: str, **keywords) -> TransformResult:
    write_dbt_project(
        root,
        {"orders": SHOP_ORDERS_MODEL},
        schema="version: 2\n",
        **{"models/sources.yml": DEMO_SOURCES},
    )
    return DbtTransform(root, warehouse_kind=kind, **keywords).run(STEP, warehouse)


def seed_duckdb_raw(path: Path, schema: str, rows: str) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute(f"CREATE SCHEMA {schema}")
        connection.execute(
            f"CREATE TABLE {schema}.orders AS SELECT * FROM (VALUES {rows}) t(id, status)"
        )


def test_a_branch_run_on_duckdb_reads_the_raw_schema_from_vars_and_writes_its_own_analytics(
    root: Path, duck: ResolvedDuckDb
):
    seed_duckdb_raw(Path(duck.path), "raw_shop", "(1, 'stage')")
    seed_duckdb_raw(Path(duck.path), BRANCH_RAW, "(1, 'branch'), (2, 'branch')")

    result = run_demo_sources(
        root,
        duck,
        "duckdb",
        target_schema=BRANCH_ANALYTICS,
        raw_schemas={"shop": BRANCH_RAW},
    )

    assert (result.crashed, result.failed_models) == (False, ())
    with duckdb.connect(duck.path) as connection:
        branch = connection.execute(f"select id, status from {BRANCH_ANALYTICS}.orders").fetchall()
        schemas = connection.execute("select schema_name from duckdb_schemas()").fetchall()
    assert sorted(branch) == [(1, "branch"), (2, "branch")]
    assert ("analytics",) not in schemas


def test_the_demo_sources_fall_back_to_raw_shop_without_vars(root: Path, duck: ResolvedDuckDb):
    seed_duckdb_raw(Path(duck.path), "raw_shop", "(1, 'stage')")

    result = run_demo_sources(root, duck, "duckdb")

    assert (result.crashed, result.failed_models) == (False, ())
    with duckdb.connect(duck.path) as connection:
        rows = connection.execute("select id, status from analytics.orders").fetchall()
    assert rows == [(1, "stage")]


def test_a_branch_run_on_postgres_reads_the_raw_schema_from_vars_and_writes_its_own_analytics(
    root: Path, postgres_warehouse: PostgresWarehouse
):
    postgres_warehouse.execute(
        "CREATE SCHEMA raw_shop",
        "CREATE TABLE raw_shop.orders AS SELECT 1 AS id, 'stage' AS status",
        f"CREATE SCHEMA {BRANCH_RAW}",
        f"CREATE TABLE {BRANCH_RAW}.orders AS SELECT 2 AS id, 'branch' AS status",
    )

    result = run_demo_sources(
        root,
        postgres_warehouse.connection,
        "postgres",
        target_schema=BRANCH_ANALYTICS,
        raw_schemas={"shop": BRANCH_RAW},
    )

    assert (result.crashed, result.failed_models) == (False, ())
    assert postgres_warehouse.rows(f"SELECT id, status FROM {BRANCH_ANALYTICS}.orders") == [
        (2, "branch")
    ]
    assert "analytics" not in postgres_warehouse.schemas()
