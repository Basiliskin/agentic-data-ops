import hashlib
import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum

from adops.domain.branch_environment import first_changed_step
from adops.domain.environment import ProjectSettings
from adops.domain.pipeline import Pipeline


class DeployPlanKind(Enum):
    FULL = "full"
    FROM_STEP = "from_step"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class DeployState:
    """What an Environment recorded after its last passing deploy run of one Pipeline."""

    settings_marker: str | None
    seen_record_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class DeployPlan:
    """How much of one Pipeline a stage or prod deploy reruns."""

    kind: DeployPlanKind
    start_index: int | None = None
    step_name: str | None = None

    def __post_init__(self) -> None:
        has_start = self.start_index is not None and self.step_name is not None
        if has_start != (self.kind is DeployPlanKind.FROM_STEP):
            raise ValueError("only a from_step plan names the Step it starts from")
        if self.start_index is not None and self.start_index < 0:
            raise ValueError("a from_step plan starts at a Step index of 0 or more")

    @classmethod
    def full(cls) -> "DeployPlan":
        return cls(DeployPlanKind.FULL)

    @classmethod
    def unchanged(cls) -> "DeployPlan":
        return cls(DeployPlanKind.UNCHANGED)

    @classmethod
    def from_step(cls, start_index: int, step_name: str) -> "DeployPlan":
        return cls(DeployPlanKind.FROM_STEP, start_index, step_name)


def settings_marker(settings: ProjectSettings, env: str) -> str:
    """Hash of the parsed adops.yaml settings one Environment's runs depend on.

    Only Secret Reference names are hashed, so a rotated Secret value does not change it.
    """
    payload = {
        "environment": settings.environments[env].model_dump(mode="json"),
        "shared": settings.model_dump(mode="json", include={"timezone", "alert_webhook_env"}),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def plan_deploy(
    pipeline: Pipeline,
    current_fingerprints: Sequence[tuple[str, str]],
    saved_fingerprints: Mapping[str, str],
    marker_changed: bool,
    unseen_record_ids: Collection[str],
) -> DeployPlan:
    """Apply the Deploy Rerun Rules, in their binding order, to one Pipeline.

    `unseen_record_ids` are this Pipeline's Accepted-Change Record ids the Environment has not
    run with yet. Saved rows of Steps the Pipeline no longer has are ignored, not trusted.
    """
    if marker_changed or unseen_record_ids or pipeline.schedule is None or not saved_fingerprints:
        return DeployPlan.full()
    step_names = {step.name for step in pipeline.steps}
    saved = {name: fp for name, fp in saved_fingerprints.items() if name in step_names}
    index = first_changed_step(current_fingerprints, saved)
    if index is None:
        return DeployPlan.unchanged()
    return DeployPlan.from_step(index, pipeline.steps[index].name)
