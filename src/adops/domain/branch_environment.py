import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from adops.domain.errors import PipelineValidationError
from adops.domain.pipeline import Step, SyncStep
from adops.domain.transform import TransformStep

STAGE = "stage"
FINGERPRINT_ENVIRONMENTS = frozenset({STAGE, "prod"})
BRANCH_PREFIX = "br_"
# Postgres silently truncates identifiers longer than 63 bytes.
MAX_IDENTIFIER_BYTES = 63
# The loader creates a "<schema>_staging" companion next to every raw schema it merges into.
STAGING_SUFFIX = "_staging"
HASH_SUFFIX_LENGTH = 8
SHORTEST_TRUNCATED_NAME = len(BRANCH_PREFIX) + 1 + 1 + HASH_SUFFIX_LENGTH


def branch_environment_name(branch: str, longest_source: str) -> str:
    sanitized = re.sub(r"[^a-z0-9]+", "_", branch.lower()).strip("_")
    if not sanitized:
        raise PipelineValidationError(
            ["--branch: the branch name has no letters or digits to name a Branch Environment"]
        )
    name = BRANCH_PREFIX + sanitized
    longest_schema = SchemaLayout.for_branch(name).raw(longest_source) + STAGING_SUFFIX
    excess = len(longest_schema.encode()) - MAX_IDENTIFIER_BYTES
    if excess <= 0:
        return name
    budget = len(name) - excess
    if budget < SHORTEST_TRUNCATED_NAME:
        raise PipelineValidationError(
            [
                f"sources: a Source name is too long to fit in a Branch Schema of "
                f"{MAX_IDENTIFIER_BYTES} bytes"
            ]
        )
    digest = hashlib.sha256(branch.encode()).hexdigest()[:HASH_SUFFIX_LENGTH]
    kept = name[: budget - HASH_SUFFIX_LENGTH - 1].rstrip("_")
    return f"{kept}_{digest}"


def drop_prefix(env_name: str) -> str:
    return f"{env_name}__"


def saves_stage_fingerprint(environment_name: str) -> bool:
    return environment_name in FINGERPRINT_ENVIRONMENTS


@dataclass(frozen=True)
class SchemaLayout:
    """The Warehouse schemas one Environment writes: shared names, or a Branch's prefixed ones."""

    prefix: str = ""

    @classmethod
    def default(cls) -> "SchemaLayout":
        return cls()

    @classmethod
    def for_branch(cls, env_name: str) -> "SchemaLayout":
        return cls(drop_prefix(env_name))

    def raw(self, source: str) -> str:
        return f"{self.prefix}raw_{source}"

    @property
    def analytics(self) -> str:
        return f"{self.prefix}analytics"

    @property
    def adops(self) -> str:
        return f"{self.prefix}adops"


@dataclass(frozen=True)
class StepReads:
    """The raw schema per Source and the analytics schema one Step reads in a Branch run."""

    raw: Mapping[str, str]
    analytics: str


def read_layout(
    branch_layout: SchemaLayout, first_changed_index: int, steps: Sequence[Step]
) -> dict[str, StepReads]:
    """Map each Step that runs in the Branch to what it reads.

    A Source's raw schema is the Branch's once a rebuilt Sync Step of it ran earlier in the
    Branch, and stage's otherwise; analytics likewise follows the first rebuilt Transform Step.
    """
    stage = SchemaLayout.default()
    sources = dict.fromkeys(step.source for step in steps if isinstance(step, SyncStep))
    raw = {source: stage.raw(source) for source in sources}
    analytics = stage.analytics
    reads = {}
    for step in steps[first_changed_index:]:
        reads[step.name] = StepReads(dict(raw), analytics)
        if isinstance(step, SyncStep):
            raw[step.source] = branch_layout.raw(step.source)
        elif isinstance(step, TransformStep):
            analytics = branch_layout.analytics
    return reads


def canonical_step_json(step: Step) -> str:
    return json.dumps(step.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def step_fingerprint(step_json: str, files: Mapping[str, bytes]) -> str:
    digest = hashlib.sha256()

    def add(part: bytes) -> None:
        digest.update(f"{len(part)}:".encode())
        digest.update(part)

    add(step_json.encode())
    for path in sorted(files):
        add(path.encode())
        add(files[path].replace(b"\r\n", b"\n"))
    return digest.hexdigest()


def first_changed_step(current: Sequence[tuple[str, str]], stage: Mapping[str, str]) -> int | None:
    """Index of the first Step that differs from stage's, or None when the Branch matches stage.

    `current` and `stage` both pair Step names with fingerprints in Step order, so a new,
    renamed, edited or moved Step all count as changed. Steps stage has beyond the Branch's
    last one change nothing the Branch runs.
    """
    stage_steps = list(stage.items())
    for index, step in enumerate(current):
        if index >= len(stage_steps) or stage_steps[index] != step:
            return index
    return None


def raw_schema_var(source: str) -> str:
    return f"adops_raw_schema_{source}"


def uses_raw_schema_var(files: Mapping[str, bytes], source: str) -> bool:
    var = re.compile(rb"\b" + re.escape(raw_schema_var(source).encode()) + rb"\b")
    return any(
        path.endswith((".yml", ".yaml")) and var.search(content) is not None
        for path, content in files.items()
    )
