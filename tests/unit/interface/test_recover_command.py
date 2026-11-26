import socket
from collections.abc import Iterator
from pathlib import Path

import pytest
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.test_utils import create_run_for_test
from typer.testing import CliRunner

from adops.interface import cli, recover_command
from adops.interface.server import ALERTS_OFF

runner = CliRunner()

SECRET = "s3cr3t-recover-sentinel"
TOKEN_PATH = "/services/SENTINEL_TOKEN"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    path = tmp_path / "project"
    result = runner.invoke(cli.app, ["init", "--no-git", "--demo", str(path)])
    assert result.exit_code == 0, result.output
    return path


@pytest.fixture
def server_env(project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    monkeypatch.setenv("ADOPS_PROJECT", str(project))
    monkeypatch.setenv("ADOPS_ENVIRONMENT", "stage")
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    yield home


def add_run(job_name: str, status: DagsterRunStatus) -> str:
    with DagsterInstance.get() as instance:
        return create_run_for_test(instance, job_name=job_name, status=status).run_id


def status_of(run_id: str) -> DagsterRunStatus:
    with DagsterInstance.get() as instance:
        return instance.get_run_by_id(run_id).status


def recover() -> tuple[int, str, str]:
    result = runner.invoke(cli.app, ["server-recover"])
    return result.exit_code, result.stdout, result.stderr


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_with_the_webhook_unset_runs_are_cleared_and_alerts_are_off(server_env):
    run_id = add_run("shop_sync", DagsterRunStatus.STARTED)

    code, out, err = recover()

    assert code == 0
    assert out.splitlines() == [
        ALERTS_OFF,
        "Pipeline shop_sync: a run left running by a Server crash was marked failed",
    ]
    assert err == ""
    assert status_of(run_id) == DagsterRunStatus.FAILURE


def test_a_stale_run_sends_one_webhook_post(server_env, webhook, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", webhook.url(TOKEN_PATH))
    run_id = add_run("shop_sync", DagsterRunStatus.STARTED)

    code, out, err = recover()

    assert code == 0
    assert ALERTS_OFF not in out
    assert err == ""
    [(path, _, body)] = webhook.requests
    assert path == TOKEN_PATH
    assert b'"pipeline": "shop_sync"' in body
    assert b'"environment": "stage"' in body
    assert status_of(run_id) == DagsterRunStatus.FAILURE


def test_with_no_stale_runs_nothing_is_cleared_or_sent(server_env, webhook, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", webhook.url())
    add_run("shop_sync", DagsterRunStatus.SUCCESS)

    code, out, err = recover()

    assert (code, out, err) == (0, "", "")
    assert webhook.requests == []


def test_a_failing_webhook_is_a_sanitized_line_and_the_run_is_still_failed(server_env, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{closed_port()}{TOKEN_PATH}")
    run_id = add_run("shop_sync", DagsterRunStatus.STARTED)

    code, _, err = recover()

    assert code == 0
    assert err.splitlines() == ["alert could not be sent (URLError)"]
    assert "SENTINEL" not in err
    assert status_of(run_id) == DagsterRunStatus.FAILURE


def test_the_webhook_url_is_read_again_when_each_alert_is_sent(server_env, webhook, monkeypatch):
    reads = iter([{"ALERT_WEBHOOK_URL": webhook.url()}, {}])
    monkeypatch.setattr(recover_command, "project_env", lambda project: next(reads))
    run_id = add_run("shop_sync", DagsterRunStatus.STARTED)

    code, out, err = recover()

    assert code == 0
    assert ALERTS_OFF not in out
    assert err == ""
    assert webhook.requests == []
    assert status_of(run_id) == DagsterRunStatus.FAILURE


def test_an_unexpected_sweep_error_prints_only_its_type_and_exits_0(server_env, monkeypatch):
    def broken(instance, notify):
        raise ConnectionError(f"postgresql://dagster:{SECRET}@dagster_meta/dagster")

    monkeypatch.setattr(recover_command, "clear_stale_runs", broken)

    code, out, err = recover()

    assert code == 0
    assert err.splitlines() == ["stale run sweep failed (ConnectionError)"]
    assert SECRET not in out + err
    assert "Traceback" not in out + err


def test_a_project_without_a_webhook_setting_turns_alerts_off(server_env, project):
    settings = project / "adops.yaml"
    settings.write_text(settings.read_text().replace("alert_webhook_env: ALERT_WEBHOOK_URL\n", ""))
    add_run("shop_sync", DagsterRunStatus.STARTING)

    code, out, err = recover()

    assert code == 0
    assert out.splitlines()[0] == ALERTS_OFF
    assert err == ""
