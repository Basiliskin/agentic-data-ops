"""The alert sent when a Server run finally fails: names and the sanitized Step error only."""

from dataclasses import dataclass
from typing import Protocol

from adops.application.ports import SUCCEEDED, RunResult


@dataclass(frozen=True)
class FailureAlert:
    pipeline: str
    environment: str
    step: str
    message: str


class AlertSender(Protocol):
    def send(self, alert: FailureAlert) -> None: ...


def failure_alert(result: RunResult) -> FailureAlert:
    failed = next(step for step in result.steps if step.status not in SUCCEEDED)
    return FailureAlert(
        pipeline=result.pipeline,
        environment=result.environment,
        step=failed.name,
        message=failed.error or str(failed.status),
    )


def notify_failure(result: RunResult, sender: AlertSender) -> str | None:
    """Returns a sanitized problem line instead of raising: a failing alert never changes a run."""
    try:
        sender.send(failure_alert(result))
    except Exception as error:
        # Only the type: the error may carry the webhook URL, which is itself a secret.
        return f"alert could not be sent ({type(error).__name__})"
    return None
