import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import pytest
from typer.testing import CliRunner

from adops.application.ports import (
    Orchestrator,
    ResolvedConnection,
    RunPlan,
    RunResult,
    Sync,
    SyncResult,
)
from adops.application.run_pipeline import InOrderOrchestrator
from adops.domain.checks import LoadFacts, SourceFacts
from adops.domain.pipeline import SyncStep
from adops.infrastructure.dagster_orchestrator import DagsterOrchestrator, JobCompileError
from adops.infrastructure.dlt_sync import DltSync
from adops.interface import cli, run_command

runner = CliRunner()

SENTINEL = "s3cr3t-run-command-sentinel"

SETTINGS = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: ADOPS_TEST_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: ADOPS_TEST_SHOP_PASSWORD}
"""

PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
      - name: orders
        mode: incremental
        primary_key: id
        cursor: updated_at
"""

COLUMNS = {"customers": frozenset({"id"}), "orders": frozenset({"id", "updated_at"})}


@dataclass
class FakeSync:
    missing_tables: set[str] = field(default_factory=set)
    sync_error: Exception | None = None
    sources: list[ResolvedConnection] = field(default_factory=list)

    def preflight(self, step: SyncStep, source: ResolvedConnection) -> Mapping[str, SourceFacts]:
        self.sources.append(source)
        return {
            table.name: SourceFacts(
                table_exists=table.name not in self.missing_tables, columns=COLUMNS[table.name]
            )
            for table in step.tables
        }

    def sync(
        self, step: SyncStep, source: ResolvedConnection, warehouse: ResolvedConnection
    ) -> SyncResult:
        if self.sync_error is not None:
            raise self.sync_error
        facts = {
            "customers": LoadFacts(0, 20, 0, 0, source_rows=20),
            "orders": LoadFacts(98, 100, 0, 0, cursor_max_before=1, cursor_max_after=2),
        }
        return SyncResult({"customers": 20, "orders": 2}, facts)


@dataclass
class RecordingOrchestrator:
    error: Exception | None = None
    environ: list[dict[str, str]] = field(default_factory=list)

    def execute(self, plan: RunPlan, sync: Sync) -> RunResult:
        self.environ.append(dict(os.environ))
        if self.error is not None:
            raise self.error
        return InOrderOrchestrator().execute(plan, sync)


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "adops.yaml").write_text(SETTINGS)
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "shop_sync.yaml").write_text(PIPELINE)
    monkeypatch.setenv("ADOPS_TEST_SHOP_PASSWORD", SENTINEL)
    monkeypatch.setenv("ADOPS_TEST_WAREHOUSE_PASSWORD", SENTINEL)
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    return tmp_path


@pytest.fixture
def sync(monkeypatch):
    fake = FakeSync()
    monkeypatch.setattr(run_command, "build_sync", lambda project, pipeline: fake)
    return fake


@pytest.fixture
def orchestrator(monkeypatch):
    fake = RecordingOrchestrator()
    monkeypatch.setattr(run_command, "build_orchestrator", lambda: fake)
    return fake


def invoke(project, *args):
    return runner.invoke(cli.app, ["run", "shop_sync", "--project", str(project), *args])


def assert_clean_failure(result):
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert SENTINEL not in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_success_prints_steps_rows_and_checks(project):
    result = invoke(project)

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0] == "step sync_shop: passed — customers 20 rows, orders 2 rows"
    assert "  check customers source_tables_exist: pass" in lines
    assert "  check customers row_count_matches: pass" in lines
    assert "  check orders cursor_not_backwards: pass" in lines
    assert lines[-1] == "Pipeline shop_sync passed in Environment dev."


@pytest.mark.usefixtures("orchestrator")
def test_missing_source_table_fails_its_check_and_names_the_table(project, sync):
    sync.missing_tables.add("customers")

    result = invoke(project)

    assert_clean_failure(result)
    assert "step sync_shop: check_failed" in result.output
    assert "  check customers source_tables_exist: fail" in result.output
    assert "customers: source_tables_exist failed — table not found" in result.output


@pytest.mark.usefixtures("orchestrator")
def test_sync_failure_is_reported_without_the_secret(project, sync):
    sync.sync_error = RuntimeError(f"connection refused for password {SENTINEL}")

    result = invoke(project)

    assert_clean_failure(result)
    assert "step sync_shop: failed" in result.output
    assert "Step sync_shop: RuntimeError: connection refused for password ***" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_invalid_pipeline_lists_problems(project):
    (project / "pipelines" / "shop_sync.yaml").write_text("name: shop_sync\nsteps: []\n")

    result = invoke(project)

    assert_clean_failure(result)
    assert "steps" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_unknown_environment_names_the_available_ones(project):
    result = invoke(project, "--env", "prod")

    assert_clean_failure(result)
    assert "environments.prod: no Environment with this name (available: dev)" in result.output


@pytest.mark.usefixtures("sync", "orchestrator")
def test_missing_pipeline_file_is_one_line(project):
    result = runner.invoke(cli.app, ["run", "nope", "--project", str(project)])

    assert_clean_failure(result)
    assert result.output.strip() == "pipelines/nope.yaml: file not found"


@pytest.mark.usefixtures("sync", "orchestrator")
def test_missing_secret_names_the_variable(project, monkeypatch):
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    result = invoke(project)

    assert_clean_failure(result)
    assert "ADOPS_TEST_SHOP_PASSWORD" in result.output


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (JobCompileError("Pipeline shop_sync: bad name"), "Pipeline shop_sync: bad name"),
        (RuntimeError(f"boom {SENTINEL}"), "unexpected RuntimeError while running shop_sync"),
    ],
)
@pytest.mark.usefixtures("sync")
def test_orchestrator_errors_exit_without_traceback(project, orchestrator, error, expected):
    orchestrator.error = error

    result = invoke(project)

    assert_clean_failure(result)
    assert result.output.strip() == expected


@pytest.mark.usefixtures("orchestrator")
def test_process_environment_wins_over_dotenv(project, sync, monkeypatch):
    (project / ".env").write_text(
        "ADOPS_TEST_SHOP_PASSWORD=from_dotenv\nADOPS_TEST_WAREHOUSE_PASSWORD=from_dotenv\n"
    )
    monkeypatch.setenv("ADOPS_TEST_SHOP_PASSWORD", "from_process")
    monkeypatch.delenv("ADOPS_TEST_WAREHOUSE_PASSWORD")

    result = invoke(project)

    assert result.exit_code == 0, result.output
    assert [source.password for source in sync.sources] == ["from_process"]


@pytest.mark.usefixtures("orchestrator")
def test_dotenv_entries_without_value_are_ignored(project, sync, monkeypatch):
    (project / ".env").write_text("ADOPS_TEST_SHOP_PASSWORD\n")
    monkeypatch.delenv("ADOPS_TEST_SHOP_PASSWORD")

    result = invoke(project)

    assert_clean_failure(result)
    assert "ADOPS_TEST_SHOP_PASSWORD" in result.output


@pytest.mark.usefixtures("sync")
def test_dagster_yaml_scopes_dagster_home_and_dotenv_to_the_run(project, orchestrator):
    (project / "dagster.yaml").write_text("")
    (project / ".env").write_text("ADOPS_TEST_DAGSTER_PG_HOST=from_dotenv\n")
    before = dict(os.environ)

    result = invoke(project)

    assert result.exit_code == 0, result.output
    [during] = orchestrator.environ
    assert during["DAGSTER_HOME"] == str(project.resolve())
    assert during["ADOPS_TEST_DAGSTER_PG_HOST"] == "from_dotenv"
    assert dict(os.environ) == before


@pytest.mark.usefixtures("sync")
def test_dagster_home_unset_without_dagster_yaml(project, orchestrator):
    before = dict(os.environ)

    result = invoke(project)

    assert result.exit_code == 0, result.output
    [during] = orchestrator.environ
    assert "DAGSTER_HOME" not in during
    assert dict(os.environ) == before


def test_real_adapters_are_built_for_the_project(tmp_path):
    sync = run_command.build_sync(tmp_path, "shop_sync")
    orchestrator = run_command.build_orchestrator()

    assert isinstance(sync, DltSync)
    assert isinstance(sync, Sync)
    assert isinstance(orchestrator, DagsterOrchestrator)
    assert isinstance(orchestrator, Orchestrator)
