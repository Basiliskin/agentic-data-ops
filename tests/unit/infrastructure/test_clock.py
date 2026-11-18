from datetime import UTC, datetime, timedelta

from adops.application.ports import Clock
from adops.infrastructure.clock import SystemClock


def test_system_clock_reads_the_current_utc_moment():
    before = datetime.now(UTC)

    now = SystemClock().now()

    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)
    assert before <= now <= datetime.now(UTC)
    assert isinstance(SystemClock(), Clock)
