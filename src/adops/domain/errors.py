from collections.abc import Sequence

Location = tuple[str | int, ...]


class PipelineValidationError(Exception):
    """Invalid Pipeline or Project settings, as `path: problem` lines that never echo values."""

    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = tuple(problems)
        super().__init__("\n".join(self.problems))


class RuleViolation(ValueError):
    """Raised from model validators to report problems at paths relative to the model."""

    def __init__(self, problems: Sequence[tuple[Location, str]]) -> None:
        self.problems = tuple(problems)
        super().__init__("; ".join(problem for _, problem in self.problems))
