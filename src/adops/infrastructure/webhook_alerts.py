import json
from dataclasses import asdict
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from adops.application.alerts import FailureAlert

TIMEOUT_SECONDS = 10.0
SCHEMES = ("http", "https")


class UnsupportedWebhookScheme(ValueError):
    def __init__(self) -> None:
        super().__init__("the alert webhook must be an http or https URL")


class WebhookAlertSender:
    """POSTs a Slack-compatible JSON alert. Raised errors may hold the URL; callers log the type."""

    def __init__(self, url: str, timeout: float = TIMEOUT_SECONDS) -> None:
        self._url = url
        self._timeout = timeout

    def send(self, alert: FailureAlert) -> None:
        if urlsplit(self._url).scheme not in SCHEMES:
            raise UnsupportedWebhookScheme()
        request = Request(
            self._url,
            data=json.dumps(payload(alert)).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=self._timeout):
            pass


def payload(alert: FailureAlert) -> dict[str, str]:
    text = (
        f"Pipeline {alert.pipeline} failed in Environment {alert.environment} "
        f"at Step {alert.step}: {alert.message}"
    )
    return {"text": text, **asdict(alert)}
