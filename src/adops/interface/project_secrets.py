from pathlib import Path

from adops.application.secrets import SET, secret_reference_status, secret_references
from adops.domain.loader import load_project_settings
from adops.interface import run_command


def project_secret_values(project: Path) -> list[str]:
    """The values adops.yaml's set Secret References resolve to, for redaction only."""
    settings = load_project_settings(run_command.read_settings(project))
    references = [settings.alert_webhook_env] if settings.alert_webhook_env else []
    for environment in settings.environments.values():
        references += secret_references(
            environment.warehouse,
            list(environment.sources.values()),
            list(environment.targets.values()),
        )
    env = run_command.project_env(project)
    status = secret_reference_status(env, references)
    return [env[name] for name, state in status.items() if state == SET]
