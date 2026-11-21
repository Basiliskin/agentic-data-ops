import json
from dataclasses import asdict

from adops.application.alerts import FailureAlert, failure_alert, notify_failure
from adops.application.ports import RunResult, StepReport, StepStatus

SENTINEL_DSN = "postgres://adops:SENTINEL_PASSWORD@db.internal/shop"
SENTINEL_TABLE = "SENTINEL_ROW_VALUE"


def failed_run() -> RunResult:
    return RunResult(
        "shop_sync",
        "prod",
        (
            StepReport("sync_shop", StepStatus.PASSED, rows_loaded={SENTINEL_TABLE: 3}),
            StepReport("transform", StepStatus.FAILED, error="dbt model orders failed"),
            StepReport("push_crm", StepStatus.FAILED, error=f"could not reach {SENTINEL_DSN}"),
        ),
    )


class RecordingSender:
    def __init__(self) -> None:
        self.sent: list[FailureAlert] = []

    def send(self, alert: FailureAlert) -> None:
        self.sent.append(alert)


class BrokenSender:
    def send(self, alert: FailureAlert) -> None:
        raise ConnectionRefusedError(f"https://hooks.example/{SENTINEL_DSN}")


def test_the_alert_holds_names_and_only_the_first_failing_steps_error():
    alert = failure_alert(failed_run())

    assert alert == FailureAlert("shop_sync", "prod", "transform", "dbt model orders failed")
    dumped = json.dumps(asdict(alert))
    assert "SENTINEL" not in dumped


def test_a_failure_without_an_error_message_uses_its_status():
    result = RunResult("shop_sync", "stage", (StepReport("checks", StepStatus.CHECK_FAILED),))

    assert failure_alert(result).message == "check_failed"


def test_notify_failure_sends_one_alert_and_reports_nothing():
    sender = RecordingSender()

    assert notify_failure(failed_run(), sender) is None
    assert sender.sent == [failure_alert(failed_run())]


def test_a_failing_sender_becomes_a_line_with_only_the_error_type():
    line = notify_failure(failed_run(), BrokenSender())

    assert line == "alert could not be sent (ConnectionRefusedError)"
