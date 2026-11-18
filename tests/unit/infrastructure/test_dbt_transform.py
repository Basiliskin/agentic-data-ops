import ast
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from dagster import DagsterInstance
from dbt.cli.main import dbtRunner, dbtRunnerResult

import adops
from adops.application.ports import ResolvedConnection, StepStatus, Transform, TransformResult
from adops.application.run_pipeline import run_pipeline
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator
from adops.infrastructure.dbt_transform import (
    PASSWORD_ENV,
    DbtTransform,
    render_postgres_profile,
)

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


def duckdb_profile(database: Path):
    """A DuckDB stand-in for the Postgres profile, reading the password through env_var() too.

    external_root is used only by external materializations, which these projects lack, so the
    field only proves the variable is set while dbt runs: dbt refuses an env_var() with no value.
    """

    def render(profile_name: str, warehouse: ResolvedConnection) -> str:
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
def transform(tmp_path: Path, root: Path) -> DbtTransform:
    return DbtTransform(root, duckdb_profile(tmp_path / "warehouse.duckdb"))


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


class ExplodingRunner:
    def invoke(self, args: list[str]) -> dbtRunnerResult:
        assert os.environ[PASSWORD_ENV] == PASSWORD
        raise RuntimeError("dbt blew up")


def test_dbt_transform_is_a_transform(transform: DbtTransform):
    assert isinstance(transform, Transform)


def test_passing_build_reports_every_test_with_its_model_and_zero_failures(
    root: Path, transform: DbtTransform
):
    write_dbt_project(
        root,
        {"orders": HEALTHY_ORDERS},
        **{"tests/orders_have_ids.sql": "select * from {{ ref('orders') }} where id is null"},
    )

    result = transform.run(STEP, WAREHOUSE)

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
    root: Path, transform: DbtTransform
):
    write_dbt_project(
        root, {"orders": orders_sql((1, "paid"), (2, None), (3, None), (4, None))}
    )

    facts = facts_by_name(transform.run(STEP, WAREHOUSE))

    assert facts["not_null_orders_status"].status is DbtTestStatus.FAIL
    assert facts["not_null_orders_status"].failures == 3
    assert facts["not_null_orders_id"].status is DbtTestStatus.PASS


def test_severity_warn_test_reports_warn_with_its_failing_rows(
    root: Path, transform: DbtTransform
):
    write_dbt_project(root, {"orders": orders_sql((1, "paid"), (2, "lost"), (3, "stolen"))})

    result = transform.run(STEP, WAREHOUSE)

    warned = facts_by_name(result)["accepted_values_orders_status__paid__shipped"]
    assert warned.status is DbtTestStatus.WARN
    assert warned.failures == 2
    assert result.crashed is False
    assert result.failed_models == ()


def test_broken_model_is_a_failed_model_and_its_tests_are_skipped(
    root: Path, transform: DbtTransform
):
    schema = ORDERS_TESTS + "  - name: broken\n    columns:\n      - name: id\n"
    schema += "        data_tests: [not_null]\n"
    write_dbt_project(
        root,
        {"orders": HEALTHY_ORDERS, "broken": f"select * from {ERROR_SENTINEL}"},
        schema=schema,
    )

    result = transform.run(STEP, WAREHOUSE)

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
    root: Path, transform: DbtTransform
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

    result = transform.run(STEP, WAREHOUSE)

    assert result.crashed is False
    assert result.failed_models == ("orders_keep_their_ids",)


def test_missing_project_folder_is_a_crash(transform: DbtTransform):
    result = transform.run(STEP, WAREHOUSE)

    assert result == TransformResult(tests=(), crashed=True, failed_models=())


def test_same_adapter_twice_in_one_process_gives_independent_results(
    root: Path, transform: DbtTransform, monkeypatch: pytest.MonkeyPatch
):
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
    transform = DbtTransform(root, render_postgres_profile, runner=ExplodingRunner)

    with pytest.raises(RuntimeError, match="dbt blew up"):
        transform.run(STEP, WAREHOUSE)

    assert PASSWORD_ENV not in os.environ
    assert dict(os.environ) == before


def test_dbt_writes_nothing_into_the_project_and_no_file_holds_the_password(
    tmp_path: Path, root: Path
):
    project = write_dbt_project(root, {"orders": HEALTHY_ORDERS})
    transform = DbtTransform(
        root, duckdb_profile(tmp_path / "warehouse.duckdb"), runner=ScanningRunner
    )

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
    text = render_postgres_profile("adops", WAREHOUSE)

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
        transform=DbtTransform(root, duckdb_profile(tmp_path / "warehouse.duckdb")),
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
