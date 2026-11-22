import json
import socket

import pytest

from adops.application.alerts import (
    Alert,
    FailureAlert,
    StaleRunAlert,
    notify_failure,
    notify_stale_run,
)
from adops.application.ports import RunResult, StepReport, StepStatus
from adops.infrastructure.webhook_alerts import (
    TIMEOUT_SECONDS,
    UnsupportedWebhookScheme,
    WebhookAlertSender,
)

ALERT = FailureAlert("shop_sync", "prod", "sync_shop", "Source shop is unreachable")
FAILED = RunResult(
    "shop_sync",
    "prod",
    (StepReport("sync_shop", StepStatus.FAILED, error="Source shop is unreachable"),),
)
STALE = StaleRunAlert("shop_sync", "prod")
TOKEN_PATH = "/services/SENTINEL_TOKEN"


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_one_json_post_carries_text_and_names(webhook):
    WebhookAlertSender(webhook.url()).send(ALERT)

    [(path, content_type, body)] = webhook.requests
    assert (path, content_type) == ("/hook", "application/json")
    assert json.loads(body) == {
        "text": (
            "Pipeline shop_sync failed in Environment prod at Step sync_shop: "
            "Source shop is unreachable"
        ),
        "pipeline": "shop_sync",
        "environment": "prod",
        "step": "sync_shop",
        "message": "Source shop is unreachable",
    }


def test_the_default_timeout_is_short():
    assert TIMEOUT_SECONDS <= 10


@pytest.mark.parametrize(
    ("status", "delay", "expected"),
    [(500, 0.0, "HTTPError"), (200, 1.0, "TimeoutError")],
)
def test_an_error_or_slow_webhook_is_reported_by_type_only(webhook, status, delay, expected):
    webhook.status, webhook.delay = status, delay
    sender = WebhookAlertSender(webhook.url(TOKEN_PATH), timeout=0.2)

    line = notify_failure(FAILED, sender)

    assert line == f"alert could not be sent ({expected})"
    assert len(webhook.requests) == 1


def test_an_unreachable_webhook_is_reported_without_the_url():
    url = f"http://127.0.0.1:{closed_port()}{TOKEN_PATH}"

    line = notify_failure(FAILED, WebhookAlertSender(url))

    assert line == "alert could not be sent (URLError)"
    assert "SENTINEL" not in line


@pytest.mark.parametrize("url", ["file:///etc/passwd", "hooks.example/SENTINEL_TOKEN"])
def test_only_http_and_https_urls_are_posted_to(url):
    with pytest.raises(UnsupportedWebhookScheme) as raised:
        WebhookAlertSender(url).send(ALERT)

    assert "SENTINEL" not in str(raised.value)
    assert "passwd" not in str(raised.value)


def test_a_stale_run_alert_posts_names_and_the_fixed_message_only(webhook):
    WebhookAlertSender(webhook.url(TOKEN_PATH)).send(STALE)

    [(path, content_type, body)] = webhook.requests
    assert (path, content_type) == (TOKEN_PATH, "application/json")
    sent = json.loads(body)
    assert sent == {
        "text": (
            "Pipeline shop_sync in Environment prod: "
            "a run left running by a Server crash was marked failed"
        ),
        "pipeline": "shop_sync",
        "environment": "prod",
        "message": "a run left running by a Server crash was marked failed",
    }
    assert all("http" not in value and "SENTINEL" not in value for value in sent.values())


def test_an_unreachable_webhook_for_a_stale_run_is_reported_without_the_url():
    url = f"http://127.0.0.1:{closed_port()}{TOKEN_PATH}"

    line = notify_stale_run("shop_sync", "prod", WebhookAlertSender(url))

    assert line == "alert could not be sent (URLError)"


@pytest.mark.parametrize("alert", [ALERT, STALE])
@pytest.mark.parametrize("url", ["ftp://hooks.example/SENTINEL_TOKEN", "file:///etc/passwd"])
def test_every_alert_type_is_posted_only_to_http_and_https(alert: Alert, url: str):
    with pytest.raises(UnsupportedWebhookScheme) as raised:
        WebhookAlertSender(url).send(alert)

    assert str(raised.value) == "the alert webhook must be an http or https URL"
