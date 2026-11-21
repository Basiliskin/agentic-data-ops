import pytest
from pydantic import ValidationError

from adops.domain.schedule import (
    BAD_DAY,
    BAD_TIME,
    SCHEDULED_ENVIRONMENTS,
    UNKNOWN_SCHEDULE,
    Schedule,
    runs_on_schedule,
)


def problem_of(text: object) -> str:
    with pytest.raises(ValidationError) as caught:
        Schedule.model_validate(text)
    (detail,) = caught.value.errors(include_input=False, include_url=False)
    (problem,) = detail["ctx"]["error"].problems
    location, message = problem
    assert location == ()
    return message


@pytest.mark.parametrize(
    ("text", "cron"),
    [
        ("hourly", "0 * * * *"),
        ("daily at 02:00", "0 2 * * *"),
        ("daily at 00:00", "0 0 * * *"),
        ("daily at 23:59", "59 23 * * *"),
        ("weekly on sunday at 06:30", "30 6 * * 0"),
        ("weekly on monday at 06:30", "30 6 * * 1"),
        ("weekly on tuesday at 06:30", "30 6 * * 2"),
        ("weekly on wednesday at 06:30", "30 6 * * 3"),
        ("weekly on thursday at 06:30", "30 6 * * 4"),
        ("weekly on friday at 06:30", "30 6 * * 5"),
        ("weekly on saturday at 19:05", "5 19 * * 6"),
    ],
)
def test_plain_words_compile_to_one_cron_and_keep_the_text(text, cron):
    schedule = Schedule.parse(text)

    assert (schedule.text, schedule.cron) == (text, cron)


@pytest.mark.parametrize(
    "text",
    ["daily at 24:00", "daily at 2:60", "daily at 2:05", "daily at 2", "daily at 02:5"],
)
def test_daily_with_a_bad_time_is_rejected(text):
    assert problem_of(text) == BAD_TIME


def test_weekly_with_a_bad_time_is_rejected():
    assert problem_of("weekly on monday at 7:00") == BAD_TIME


@pytest.mark.parametrize("text", ["weekly on funday at 06:00", "weekly on Monday at 06:00"])
def test_weekly_with_an_unknown_day_is_rejected(text):
    assert problem_of(text) == BAD_DAY
    assert BAD_DAY.startswith("the day must be one of monday, tuesday,")
    assert BAD_DAY.endswith("saturday, sunday")


@pytest.mark.parametrize(
    "text",
    ["every day", "Daily at 02:00", "Hourly", "daily", "weekly on monday", "", "   "],
)
def test_text_outside_the_grammar_that_is_not_a_cron_is_rejected(text):
    assert problem_of(text) == UNKNOWN_SCHEDULE


def test_the_unknown_schedule_message_lists_every_allowed_form():
    for form in ("hourly", "daily at HH:MM", "weekly on <day> at HH:MM", "5-field cron"):
        assert form in UNKNOWN_SCHEDULE


@pytest.mark.parametrize("value", [5, None, ["hourly"], {"text": "hourly", "cron": "0 * * * *"}])
def test_a_schedule_that_is_not_text_is_rejected(value):
    assert problem_of(value) == UNKNOWN_SCHEDULE


@pytest.mark.parametrize(
    "cron",
    [
        "*/15 * * * *",
        "0 9-17 * * 1-5",
        "0 0 1,15 * *",
        "0-30/10 * * * *",
        "0 0 30 1,3 *",
        "0 0 30 * *",
        "0 0 29 2 *",
        "0 0 1,30 2 *",
        "59 23 31 12 6",
        "00 02 * * *",
    ],
)
def test_a_valid_cron_is_kept_as_written(cron):
    schedule = Schedule.parse(cron)

    assert (schedule.text, schedule.cron) == (cron, cron)


def test_a_cron_with_extra_spaces_is_normalised_to_single_spaces():
    assert Schedule.parse("0  2 * *  *").cron == "0 2 * * *"


@pytest.mark.parametrize("cron", ["* * * *", "* * * * * *"])
def test_a_cron_without_exactly_five_fields_is_rejected(cron):
    assert problem_of(cron) == UNKNOWN_SCHEDULE


@pytest.mark.parametrize(
    ("cron", "field", "bounds"),
    [
        ("60 * * * *", "minute", "0 to 59"),
        ("* 24 * * *", "hour", "0 to 23"),
        ("* * 0 * *", "day of month", "1 to 31"),
        ("* * 32 * *", "day of month", "1 to 31"),
        ("* * * 13 *", "month", "1 to 12"),
        ("* * * 0 *", "month", "1 to 12"),
        ("* * * * 7", "weekday", "0 to 6"),
        ("*/0 * * * *", "minute", "0 to 59"),
        ("*/ * * * *", "minute", "0 to 59"),
        ("*/x * * * *", "minute", "0 to 59"),
        ("5-1 * * * *", "minute", "0 to 59"),
        ("1,,2 * * * *", "minute", "0 to 59"),
        ("1, * * * *", "minute", "0 to 59"),
        ("5/2 * * * *", "minute", "0 to 59"),
        ("0 0 * jan *", "month", "1 to 12"),
        ("0 0 * * mon", "weekday", "0 to 6"),
        ("0 0 * * 1-", "weekday", "0 to 6"),
        ("0 0 * * 1-7", "weekday", "0 to 6"),
        ("0 0 * * 0-6/0", "weekday", "0 to 6"),
        ("? * * * *", "minute", "0 to 59"),
    ],
)
def test_a_cron_field_out_of_shape_or_range_names_the_field_and_its_bounds(cron, field, bounds):
    problem = problem_of(cron)

    assert problem == (
        f"in a 5-field cron, the {field} field must be *, a number, a range a-b, a list a,b "
        f"or a step */n or a-b/n, with values from {bounds}"
    )


@pytest.mark.parametrize("cron", ["0 0 30 2 *", "0 0 31 2 *", "0 0 30,31 2 *", "0 0 30-31 2 *"])
def test_a_cron_that_asks_for_february_30_or_31_only_is_rejected(cron):
    assert problem_of(cron) == "this schedule never runs: February has no day 30 or 31"


@pytest.mark.parametrize(
    "text",
    [
        "SENTINEL_9f3 daily",
        "daily at SENTINEL_9f3",
        "weekly on SENTINEL_9f3 at 06:00",
        "SENTINEL_9f3 * * * *",
        "* * * * SENTINEL_9f3",
        "0 0 30 2 SENTINEL_9f3",
    ],
)
def test_no_message_echoes_the_rejected_text(text):
    with pytest.raises(ValidationError) as caught:
        Schedule.parse(text)

    assert "SENTINEL_9f3" not in problem_of(text)
    assert "SENTINEL_9f3" not in str(caught.value.errors(include_input=False, include_url=False))


@pytest.mark.parametrize("attribute", ["text", "cron"])
def test_a_schedule_cannot_be_changed_after_parsing(attribute):
    schedule = Schedule.parse("hourly")

    with pytest.raises(ValidationError):
        setattr(schedule, attribute, "daily at 02:00")


@pytest.mark.parametrize("name", ["stage", "prod"])
def test_stage_and_prod_run_on_a_schedule(name):
    assert runs_on_schedule(name) is True


@pytest.mark.parametrize(
    "name", ["dev", "Stage", "PROD", " stage", "prod ", "production", "staging", "feature-x", ""]
)
def test_no_other_environment_runs_on_a_schedule(name):
    assert runs_on_schedule(name) is False


def test_the_scheduled_environments_are_exactly_stage_and_prod():
    assert SCHEDULED_ENVIRONMENTS == frozenset({"stage", "prod"})
