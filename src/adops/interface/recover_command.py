"""`adops server-recover`: run once in the daemon container, before dagster-daemon starts."""

import os
from collections.abc import Callable, Mapping
from pathlib import Path

import typer

from adops.application.alerts import notify_stale_run
from adops.domain.loader import load_project_settings
from adops.domain.secret_reference import SecretReference
from adops.infrastructure.dagster_server import DagsterInstance, clear_stale_runs
from adops.infrastructure.webhook_alerts import WebhookAlertSender
from adops.interface.run_command import project_env, read_settings
from adops.interface.server import ALERTS_OFF, ENVIRONMENT_VARIABLE, PROJECT_VARIABLE


def server_recover() -> None:
    """Mark runs a Server crash left running as failed, and alert once for each.

    Always exits 0: the daemon starts after this command, and a problem here must never stop it.
    """
    try:
        cleared = recover(os.environ)
    except Exception as error:
        # Only the type: an unexpected error was never redacted and may hold a secret.
        typer.echo(f"stale run sweep failed ({type(error).__name__})", err=True)
        return
    for pipeline in cleared:
        typer.echo(f"Pipeline {pipeline}: a run left running by a Server crash was marked failed")


def recover(environ: Mapping[str, str]) -> list[str]:
    project = Path(environ.get(PROJECT_VARIABLE) or Path.cwd()).resolve()
    environment = environ.get(ENVIRONMENT_VARIABLE, "")
    webhook = load_project_settings(read_settings(project)).alert_webhook_env
    notify = _stale_run_notifier(project, environment, webhook)
    with DagsterInstance.get() as instance:
        return clear_stale_runs(instance, notify)


def _stale_run_notifier(
    project: Path, environment: str, webhook: SecretReference | None
) -> Callable[[str], None]:
    if webhook is None or not project_env(project).get(webhook.name):
        typer.echo(ALERTS_OFF)

    def notify(pipeline: str) -> None:
        url = project_env(project).get(webhook.name) if webhook else None
        if not url:
            return
        problem = notify_stale_run(pipeline, environment, WebhookAlertSender(url))
        if problem:
            typer.echo(problem, err=True)

    return notify
