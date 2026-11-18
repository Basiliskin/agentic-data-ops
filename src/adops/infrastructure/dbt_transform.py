import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.results import NodeStatus
from dbt.cli.main import dbtRunner, dbtRunnerResult

from adops.application.ports import ResolvedConnection, TransformResult
from adops.domain.transform import DbtTestFacts, DbtTestStatus, TransformStep

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

ProfileRenderer = Callable[[str, ResolvedConnection], str]


def render_postgres_profile(profile_name: str, warehouse: ResolvedConnection) -> str:
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


class DbtTransform:
    """Runs `dbt build` in-process on a Project's dbt project and reports only measured facts.

    profiles.yml, target/ and logs/ go to a temporary folder outside the Project, so neither the
    rendered profile nor dbt's artefacts are left behind.
    """

    def __init__(
        self,
        project_root: Path,
        render_profile: ProfileRenderer = render_postgres_profile,
        runner: Callable[[], dbtRunner] = dbtRunner,
    ) -> None:
        self.project_root = project_root
        self.render_profile = render_profile
        self.runner = runner

    def run(self, step: TransformStep, warehouse: ResolvedConnection) -> TransformResult:
        with tempfile.TemporaryDirectory(prefix="adops-dbt-") as tmp:
            work = Path(tmp)
            (work / "profiles.yml").write_text(self.render_profile(PROFILE, warehouse))
            with _password_in_environ(warehouse.password):
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
