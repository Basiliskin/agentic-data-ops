import sys

import pytest
from typer.testing import CliRunner

import adops
from adops.interface import cli

runner = CliRunner()


def test_version_flag_prints_package_version():
    result = runner.invoke(cli.app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == adops.__version__


def test_no_arguments_shows_help():
    result = runner.invoke(cli.app, [])

    assert "--version" in result.stdout


def test_version_callback_ignores_unset_flag():
    cli._print_version(False)


def test_main_runs_the_cli(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["adops", "--version"])

    with pytest.raises(SystemExit) as exit_info:
        cli.main()

    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == adops.__version__


def test_run_help_shows_the_all_option():
    result = runner.invoke(cli.app, ["run", "--help"], env={"COLUMNS": "200"})

    assert result.exit_code == 0
    assert "--all" in result.output
    assert "Run every Pipeline in name order; stop at the first failure." in result.output
