"""Alerts the Server sends: names, fixed text and the sanitized Step error only."""

from dataclasses import dataclass
from typing import Protocol

from adops.application.ports import SUCCEEDED, RunResult


@dataclass(frozen=True)
class FailureAlert:
    pipeline: str
    environment: str
    step: str
    message: str


STALE_RUN_MESSAGE = "a run left running by a Server crash was marked failed"


@dataclass(frozen=True)
class StaleRunAlert:
    pipeline: str
    environment: str
    message: str = STALE_RUN_MESSAGE


Alert = FailureAlert | StaleRunAlert


class AlertSender(Protocol):
    def send(self, alert: Alert) -> None: ...


def failure_alert(result: RunResult) -> FailureAlert:
    failed = next(step for step in result.steps if step.status not in SUCCEEDED)
    return FailureAlert(
        pipeline=result.pipeline,
        environment=result.environment,
        step=failed.name,
        message=failed.error or str(failed.status),
    )


def notify_failure(result: RunResult, sender: AlertSender) -> str | None:
    return _notify(failure_alert(result), sender)


def notify_stale_run(pipeline: str, environment: str, sender: AlertSender) -> str | None:
    return _notify(StaleRunAlert(pipeline, environment), sender)


def _notify(alert: Alert, sender: AlertSender) -> str | None:
    """Returns a sanitized problem line instead of raising: a failing alert never changes a run."""
    try:
        sender.send(alert)
    except Exception as error:
        # Only the type: the error may carry the webhook URL, which is itself a secret.
        return f"alert could not be sent ({type(error).__name__})"
    return None
