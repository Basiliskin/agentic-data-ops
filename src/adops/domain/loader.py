from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

from adops.domain.environment import WAREHOUSE_TYPES, Environment, ProjectSettings
from adops.domain.errors import Location, PipelineValidationError, RuleViolation
from adops.domain.pipeline import DECLARED_CHECK_TYPES, STEP_TYPES, Pipeline, SyncStep

ROOT = "(root)"


def load_pipeline(text: str) -> Pipeline:
    return _load(Pipeline, text)


def load_project_settings(text: str) -> ProjectSettings:
    return _load(ProjectSettings, text)


def ensure_sources_defined(pipeline: Pipeline, environment: Environment) -> None:
    problems = [
        f"{format_path(('steps', index, 'source'))}: no Source with this name in the Environment"
        for index, step in enumerate(pipeline.steps)
        if isinstance(step, SyncStep) and step.source not in environment.sources
    ]
    if problems:
        raise PipelineValidationError(problems)


def format_path(location: Location) -> str:
    path = ""
    for part in location:
        if isinstance(part, int):
            path += f"[{part}]"
        else:
            path += f".{part}" if path else part
    return path or ROOT


def _load[M: BaseModel](model: type[M], text: str) -> M:
    document = _parse(text)
    try:
        return model.model_validate(document)
    except ValidationError as error:
        problems = _describe(error)
    # Raised outside `except` so the pydantic error, which holds input values, is not chained.
    raise PipelineValidationError(problems)


def _parse(text: str) -> dict[str, Any]:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as error:
        problem = _describe_yaml_error(error)
    else:
        if isinstance(document, dict):
            return document
        problem = f"{ROOT}: must be a mapping of settings"
    raise PipelineValidationError([problem])


def _describe_yaml_error(error: yaml.YAMLError) -> str:
    mark = getattr(error, "problem_mark", None)
    if mark is None:
        return f"{ROOT}: not valid YAML"
    return f"{ROOT}: not valid YAML (line {mark.line + 1}, column {mark.column + 1})"


def _describe(error: ValidationError) -> list[str]:
    lines = []
    for detail in error.errors(include_input=False, include_url=False):
        location = _without_union_tags(tuple(detail["loc"]))
        cause = detail.get("ctx", {}).get("error")
        if isinstance(cause, RuleViolation):
            lines.extend(
                f"{format_path(location + sub_location)}: {problem}"
                for sub_location, problem in cause.problems
            )
        else:
            lines.append(f"{format_path(location)}: {detail['msg']}")
    return lines


_UNION_TAGS = {"steps": STEP_TYPES, "checks": DECLARED_CHECK_TYPES}


def _without_union_tags(location: Location) -> Location:
    # pydantic inserts the matched tag after `steps[i]`, `checks[k]` and an Environment's
    # `warehouse`; it is not a YAML key.
    return tuple(
        part for index, part in enumerate(location) if not _is_union_tag(location[:index], part)
    )


def _is_union_tag(before: Location, part: str | int) -> bool:
    if len(before) == 3 and before[0] == "environments" and before[2] == "warehouse":
        return part in WAREHOUSE_TYPES
    return (
        len(before) >= 2
        and isinstance(before[-1], int)
        and part in _UNION_TAGS.get(str(before[-2]), ())
    )
