import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from pathlib import Path
from typing import Any

import yaml
from dbt.artifacts.schemas.results import NodeStatus
from dbt.cli.main import dbtRunner, dbtRunnerResult

from adops.application.ports import (
    ResolvedConnection,
    ResolvedDuckDb,
    ResolvedWarehouse,
    TransformResult,
)
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep
from adops.infrastructure.dlt_sync import UnknownConnectionKindError, duckdb_file

PASSWORD_ENV = "ADOPS_DBT_WAREHOUSE_PASSWORD"
PROFILE = "adops"
TARGET = "warehouse"
TARGET_SCHEMA = "analytics"
CRASHED = TransformResult(tests=(), crashed=True, failed_models=())
FAILED_NODE_STATUSES = frozenset({NodeStatus.Error, NodeStatus.Fail, NodeStatus.RuntimeErr})

PROFILE_TEMPLATE = """\
{profile}:
  target: {target}
  outputs:
    {target}:
      type: postgres
      host: {host}
      port: {port}
      dbname: {database}
      user: {user}
      password: "{{{{ env_var('{password_env}') }}}}"
      schema: {schema}
      threads: 1
"""

ProfileRenderer = Callable[[str, Any, Path], str]


def render_postgres_profile(
    profile_name: str, warehouse: ResolvedConnection, _project_root: Path
) -> str:
    """profiles.yml text for the Warehouse; the password is only an env_var() reference."""
    return PROFILE_TEMPLATE.format(
        profile=json.dumps(profile_name),
        target=TARGET,
        host=json.dumps(warehouse.host),
        port=warehouse.port,
        database=json.dumps(warehouse.database),
        user=json.dumps(warehouse.user),
        password_env=PASSWORD_ENV,
        schema=TARGET_SCHEMA,
    )


def render_duckdb_profile(profile_name: str, warehouse: ResolvedDuckDb, project_root: Path) -> str:
    """profiles.yml text for the Sync's DuckDB file.

    dbt-duckdb resolves a relative path against dbt's cwd, so the path is made absolute here; one
    thread keeps dbt to a single connection under DuckDB's single-writer lock.
    """
    target = {
        "type": "duckdb",
        "path": str(duckdb_file(project_root, warehouse.path).absolute()),
        "schema": TARGET_SCHEMA,
        "threads": 1,
    }
    return yaml.safe_dump({profile_name: {"target": TARGET, "outputs": {TARGET: target}}})


PROFILE_RENDERERS: dict[str, ProfileRenderer] = {
    "postgres": render_postgres_profile,
    "duckdb": render_duckdb_profile,
}


def profile_renderer(kind: str) -> ProfileRenderer:
    if kind not in PROFILE_RENDERERS:
        raise UnknownConnectionKindError("Warehouse", kind, PROFILE_RENDERERS)
    return PROFILE_RENDERERS[kind]


class DbtTransform:
    """Runs `dbt build` in-process on a Project's dbt project and reports only measured facts.

    profiles.yml, target/ and logs/ go to a temporary folder outside the Project, so neither the
    rendered profile nor dbt's artefacts are left behind.
    """

    def __init__(
        self,
        project_root: Path,
        *,
        warehouse_kind: str,
        render_profile: ProfileRenderer | None = None,
        runner: Callable[[], dbtRunner] = dbtRunner,
    ) -> None:
        renderer = profile_renderer(warehouse_kind)
        self.project_root = project_root
        self.render_profile = render_profile or renderer
        self.runner = runner

    def run(self, step: TransformStep, warehouse: ResolvedWarehouse) -> TransformResult:
        with tempfile.TemporaryDirectory(prefix="adops-dbt-") as tmp:
            work = Path(tmp)
            profile = self.render_profile(PROFILE, warehouse, self.project_root)
            (work / "profiles.yml").write_text(profile)
            with _warehouse_environ(warehouse):
                result = self.runner().invoke(self._build_args(step, work))
        return _measure(result)

    def _build_args(self, step: TransformStep, work: Path) -> list[str]:
        options = {
            "--project-dir": self.project_root / step.project,
            "--profiles-dir": work,
            "--profile": PROFILE,
            "--target": TARGET,
            "--target-path": work / "target",
            "--log-path": work / "logs",
        }
        pairs = [part for option, value in options.items() for part in (option, str(value))]
        return ["build", *pairs, "--quiet", "--no-send-anonymous-usage-stats"]


def _warehouse_environ(warehouse: ResolvedWarehouse) -> AbstractContextManager[None]:
    if isinstance(warehouse, ResolvedConnection):
        return _password_in_environ(warehouse.password)
    return nullcontext()


@contextmanager
def _password_in_environ(password: str) -> Iterator[None]:
    saved = dict(os.environ)
    os.environ[PASSWORD_ENV] = password
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _measure(result: dbtRunnerResult) -> TransformResult:
    if result.result is None or result.exception is not None:
        return CRASHED
    tests: list[DbtTestFacts] = []
    failed_models: list[str] = []
    for node_result in result.result:
        node = node_result.node
        if node.resource_type == "test":
            tests.append(
                DbtTestFacts(
                    unique_id=node.unique_id,
                    name=node.name,
                    table=_tested_model(node),
                    status=DbtTestStatus(node_result.status.value),
                    failures=node_result.failures,
                )
            )
        elif node_result.status in FAILED_NODE_STATUSES:
            failed_models.append(node.name)
    return TransformResult(tests=tuple(tests), crashed=False, failed_models=tuple(failed_models))


def _tested_model(node: Any) -> str:
    owner = getattr(node, "attached_node", None) or next(iter(node.depends_on.nodes), "")
    return owner.split(".")[2] if owner else ""
