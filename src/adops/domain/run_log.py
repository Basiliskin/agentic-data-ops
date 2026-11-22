import re
from datetime import UTC, datetime

PLAIN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
RUN_LOG_NAME = re.compile(r"^\d{8}T\d{6}Z-[a-z][a-z0-9_]*\.log$")
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def run_log_file_name(timestamp: datetime, pipeline: str) -> str:
    return f"{timestamp.astimezone(UTC).strftime(TIMESTAMP_FORMAT)}-{pipeline}.log"


def is_run_log_name(name: str) -> bool:
    return RUN_LOG_NAME.fullmatch(name) is not None


def is_plain_name(name: str) -> bool:
    """Environment and Pipeline names that may become part of a Run Log path."""
    return PLAIN_NAME.fullmatch(name) is not None
