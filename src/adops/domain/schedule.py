import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from adops.domain.errors import RuleViolation

# Not reverse_sync_diff.WRITING_ENVIRONMENTS: that rule decides who may write to a Target and only
# happens to name the same Environments.
SCHEDULED_ENVIRONMENTS = frozenset({"stage", "prod"})

UNKNOWN_SCHEDULE = (
    "not a known schedule; use hourly, daily at HH:MM, weekly on <day> at HH:MM or a 5-field cron"
)
BAD_TIME = "the time must be HH:MM in 24-hour form, from 00:00 to 23:59"
WEEKDAYS = ("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday")
BAD_DAY = "the day must be one of " + ", ".join(WEEKDAYS[1:] + WEEKDAYS[:1])

_TIME = re.compile(r"([01][0-9]|2[0-3]):([0-5][0-9])")
_DAILY = re.compile(r"daily at (\S+)")
_WEEKLY = re.compile(r"weekly on (\S+) at (\S+)")
_RANGE = re.compile(r"([0-9]+)(?:-([0-9]+))?")
_STEP = re.compile(r"[0-9]+")
_FEBRUARY = 2
_DAYS_FEBRUARY_NEVER_HAS = frozenset({30, 31})


@dataclass(frozen=True)
class _CronField:
    name: str
    low: int
    high: int


_CRON_FIELDS = (
    _CronField("minute", 0, 59),
    _CronField("hour", 0, 23),
    _CronField("day of month", 1, 31),
    _CronField("month", 1, 12),
    _CronField("weekday", 0, 6),
)


def runs_on_schedule(environment_name: str) -> bool:
    return environment_name in SCHEDULED_ENVIRONMENTS


class Schedule(BaseModel):
    """When a Pipeline runs on the Server: the user's text and its 5-field cron."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    text: str
    cron: str

    @classmethod
    def parse(cls, text: str) -> "Schedule":
        return cls.model_validate(text)

    @model_validator(mode="before")
    @classmethod
    def _from_text(cls, data: Any) -> Any:
        if not isinstance(data, str):
            raise RuleViolation([((), UNKNOWN_SCHEDULE)])
        return {"text": data, "cron": _compile(data)}


def _compile(text: str) -> str:
    if text == "hourly":
        return "0 * * * *"
    if daily := _DAILY.fullmatch(text):
        hour, minute = _time(daily.group(1))
        return f"{minute} {hour} * * *"
    if weekly := _WEEKLY.fullmatch(text):
        day, time = weekly.groups()
        if day not in WEEKDAYS:
            raise RuleViolation([((), BAD_DAY)])
        hour, minute = _time(time)
        return f"{minute} {hour} * * {WEEKDAYS.index(day)}"
    fields = text.split()
    if len(fields) != len(_CRON_FIELDS):
        raise RuleViolation([((), UNKNOWN_SCHEDULE)])
    _check_cron(fields)
    return " ".join(fields)


def _time(text: str) -> tuple[int, int]:
    match = _TIME.fullmatch(text)
    if match is None:
        raise RuleViolation([((), BAD_TIME)])
    return int(match.group(1)), int(match.group(2))


def _check_cron(fields: list[str]) -> None:
    values = [_field_values(text, field) for text, field in zip(fields, _CRON_FIELDS, strict=True)]
    days, months = values[2], values[3]
    if months == {_FEBRUARY} and days <= _DAYS_FEBRUARY_NEVER_HAS:
        raise RuleViolation([((), "this schedule never runs: February has no day 30 or 31")])


def _field_values(text: str, field: _CronField) -> set[int]:
    values: set[int] = set()
    for item in text.split(","):
        values |= _item_values(item, field)
    return values


def _item_values(item: str, field: _CronField) -> set[int]:
    base, slash, step_text = item.partition("/")
    step = _step(step_text, field) if slash else 1
    if base == "*":
        return set(range(field.low, field.high + 1, step))
    match = _RANGE.fullmatch(base)
    if match is None or (slash and match.group(2) is None):
        raise RuleViolation([((), _field_problem(field))])
    first = int(match.group(1))
    last = first if match.group(2) is None else int(match.group(2))
    if not field.low <= first <= last <= field.high:
        raise RuleViolation([((), _field_problem(field))])
    return set(range(first, last + 1, step))


def _step(text: str, field: _CronField) -> int:
    if _STEP.fullmatch(text) is None or int(text) == 0:
        raise RuleViolation([((), _field_problem(field))])
    return int(text)


def _field_problem(field: _CronField) -> str:
    return (
        f"in a 5-field cron, the {field.name} field must be *, a number, a range a-b, a list a,b "
        f"or a step */n or a-b/n, with values from {field.low} to {field.high}"
    )
