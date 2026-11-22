from datetime import UTC, datetime, timedelta, timezone

import pytest

from adops.domain.run_log import is_plain_name, is_run_log_name, run_log_file_name


def test_file_name_is_utc_timestamp_then_pipeline():
    timestamp = datetime(2026, 11, 21, 14, 30, 5, tzinfo=UTC)

    assert run_log_file_name(timestamp, "shop_sync") == "20261121T143005Z-shop_sync.log"


def test_non_utc_timestamp_is_converted_to_utc():
    timestamp = datetime(2026, 11, 21, 16, 30, 5, tzinfo=timezone(timedelta(hours=2)))

    assert run_log_file_name(timestamp, "shop_sync") == "20261121T143005Z-shop_sync.log"


def test_a_formatted_name_is_a_run_log_name():
    timestamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert is_run_log_name(run_log_file_name(timestamp, "orders"))


@pytest.mark.parametrize(
    "name",
    [
        "../x.log",
        "/tmp/20261121T143005Z-a.log",
        "a.log",
        "20261121T143005Z-Shop.log",
        "20261121T143005-a.log",
        "20261121T143005Z-a.log\n",
        "20261121T143005Z-.log",
        "20261121T143005Z-a.txt",
        "20261121T143005Z-../a.log",
    ],
)
def test_names_that_are_not_run_logs_are_rejected(name):
    assert not is_run_log_name(name)


@pytest.mark.parametrize("name", ["dev", "br_feat_x", "shop_sync"])
def test_plain_names_are_accepted(name):
    assert is_plain_name(name)


@pytest.mark.parametrize("name", ["", "Dev", "../x", "a/b", "1dev", "dev\n"])
def test_names_unsafe_in_a_path_are_rejected(name):
    assert not is_plain_name(name)
