import dataclasses
import re

import pytest

from adops.domain.deploy_rerun import (
    DeployPlan,
    DeployPlanKind,
    DeployState,
    plan_deploy,
    settings_marker,
)
from adops.domain.loader import load_project_settings
from adops.domain.pipeline import Pipeline

STEPS = ("sync_shop", "transform", "push")
CURRENT = [("sync_shop", "fp-sync"), ("transform", "fp-transform"), ("push", "fp-push")]
SAVED = dict(CURRENT)


def shop_pipeline(step_names=STEPS, schedule: str | None = "daily at 02:00") -> Pipeline:
    steps = {
        "sync_shop": {
            "type": "sync",
            "name": "sync_shop",
            "source": "shop",
            "tables": [{"name": "orders", "mode": "full_refresh", "primary_key": "id"}],
        },
        "transform": {"type": "transform", "name": "transform", "project": "dbt"},
        "push": {
            "type": "reverse_sync",
            "name": "push",
            "model": "customer_stats",
            "target": "crm",
            "target_table": "customers",
            "key": "id",
            "mapping": {"id": "id"},
        },
    }
    data = {"name": "shop", "steps": [steps[name] for name in step_names]}
    if schedule is not None:
        data["schedule"] = schedule
    return Pipeline.model_validate(data)


def plan(
    pipeline=None,
    current=CURRENT,
    saved=SAVED,
    marker_changed=False,
    unseen=frozenset(),
) -> DeployPlan:
    return plan_deploy(pipeline or shop_pipeline(), current, saved, marker_changed, unseen)


FULL = DeployPlan.full()
UNCHANGED = DeployPlan.unchanged()


def test_nothing_changed_is_unchanged():
    assert plan() == UNCHANGED


def test_a_changed_settings_marker_reruns_in_full():
    assert plan(marker_changed=True) == FULL


def test_an_unseen_accepted_change_record_reruns_in_full():
    assert plan(unseen={"orders-drop-email"}) == FULL


def test_no_unseen_records_with_every_other_condition_clear_is_not_a_full_rerun():
    assert plan(unseen=set()) == UNCHANGED


def test_a_pipeline_without_a_schedule_always_reruns_in_full():
    assert plan(shop_pipeline(schedule=None)) == FULL


def test_no_saved_fingerprints_reruns_in_full():
    assert plan(saved={}) == FULL


def test_a_changed_middle_step_reruns_from_that_step():
    current = [CURRENT[0], ("transform", "fp-transform-edited"), CURRENT[2]]

    result = plan(current=current)

    assert result == DeployPlan.from_step(1, "transform")
    assert (result.kind, result.start_index, result.step_name) == (
        DeployPlanKind.FROM_STEP,
        1,
        "transform",
    )


def test_a_new_last_step_reruns_from_it():
    saved = dict(CURRENT[:2])

    assert plan(saved=saved) == DeployPlan.from_step(2, "push")


FULL_CONDITIONS = {
    "marker_changed": {"marker_changed": True},
    "unseen_record": {"unseen": {"r1"}},
    "no_schedule": {"pipeline": shop_pipeline(schedule=None)},
    "no_saved": {"saved": {}},
}


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("marker_changed", "unseen_record"),
        ("unseen_record", "no_schedule"),
        ("no_schedule", "no_saved"),
        ("marker_changed", "no_saved"),
    ],
)
def test_adjacent_full_rules_together_still_rerun_in_full(first, second):
    assert plan(**FULL_CONDITIONS[first], **FULL_CONDITIONS[second]) == FULL


@pytest.mark.parametrize("condition", sorted(FULL_CONDITIONS))
def test_every_full_rule_wins_over_a_changed_step(condition):
    current = [CURRENT[0], ("transform", "fp-transform-edited"), CURRENT[2]]
    assert plan(current=current, **FULL_CONDITIONS[condition]) == FULL


@pytest.mark.parametrize("condition", sorted(FULL_CONDITIONS))
def test_every_full_rule_wins_over_no_changed_step(condition):
    assert plan(**FULL_CONDITIONS[condition]) == FULL


def test_a_stale_row_of_a_removed_step_is_ignored():
    pipeline = shop_pipeline(("sync_shop", "push"))
    current = [CURRENT[0], CURRENT[2]]

    assert plan(pipeline, current=current, saved=SAVED) == UNCHANGED


def test_a_renamed_step_reruns_from_its_new_name_and_its_old_row_is_ignored():
    saved = {"sync_shop": "fp-sync", "transform_old": "fp-transform", "push": "fp-push"}

    assert plan(saved=saved) == DeployPlan.from_step(1, "transform")


def test_saved_rows_only_for_removed_steps_rerun_from_the_first_step():
    saved = {"gone": "fp-gone"}

    assert plan(saved=saved) == DeployPlan.from_step(0, "sync_shop")


def test_the_saved_rows_passed_in_are_not_changed():
    saved = {"gone": "fp-gone", **SAVED}

    plan(saved=saved)

    assert saved == {"gone": "fp-gone", **SAVED}


@pytest.mark.parametrize(
    ("kind", "start_index", "step_name"),
    [
        (DeployPlanKind.FROM_STEP, None, None),
        (DeployPlanKind.FROM_STEP, 1, None),
        (DeployPlanKind.FROM_STEP, None, "transform"),
        (DeployPlanKind.FULL, 1, "transform"),
        (DeployPlanKind.UNCHANGED, 0, "sync_shop"),
    ],
)
def test_a_plan_names_a_start_step_exactly_when_it_reruns_from_a_step(kind, start_index, step_name):
    with pytest.raises(ValueError, match="only a from_step plan"):
        DeployPlan(kind, start_index, step_name)


def test_a_from_step_plan_cannot_start_before_the_first_step():
    with pytest.raises(ValueError, match="index of 0 or more"):
        DeployPlan.from_step(-1, "sync_shop")


def test_plans_and_deploy_state_are_frozen():
    state = DeployState(settings_marker=None)

    with pytest.raises(dataclasses.FrozenInstanceError):
        FULL.kind = DeployPlanKind.UNCHANGED
    with pytest.raises(dataclasses.FrozenInstanceError):
        state.settings_marker = "x"
    assert state.seen_record_ids == frozenset()


ENVIRONMENTS = """
# Settings for every Environment.
timezone: Europe/Berlin
environments:
  dev:
    warehouse: {type: duckdb, path: dev.duckdb}
    sources:
      shop: {type: postgres, host: h, port: 5432, database: dev, user: u, password_env: SHOP_PW}
  stage:
    warehouse: {type: postgres, host: wh, port: 5432, database: d, user: u, password_env: WH_PW}
    sources:
      shop: {type: postgres, host: h, port: 5432, database: d, user: u, password_env: SHOP_PW}
    targets:
      crm: {type: postgres, host: c, port: 5432, database: d, user: u, password_env: CRM_PW}
  prod:
    warehouse: {type: postgres, host: wh, port: 5432, database: p, user: u, password_env: WH_PW}
    sources:
      shop: {type: postgres, host: h, port: 5432, database: p, user: u, password_env: SHOP_PW}
"""

REFORMATTED = """
environments:
  prod:
    sources:
      shop:
        password_env: "SHOP_PW"
        user: u
        database: p
        port: 5432
        host: h
        type: postgres
    warehouse: {type: postgres, host: wh, port: 5432, database: p, user: u, password_env: WH_PW}
  stage:
    targets:
      crm: {type: 'postgres', host: 'c', port: 5432, database: d, user: u, password_env: CRM_PW}
    sources:
      shop:   {type: postgres, host: h,   port: 5432, database: d, user: u, password_env: SHOP_PW}
    warehouse:
      type: postgres
      host: wh
      port: 5432
      database: d
      user: u
      password_env: WH_PW
  dev:
    warehouse: {type: duckdb, path: dev.duckdb}
    sources:
      shop: {type: postgres, host: h, port: 5432, database: dev, user: u, password_env: SHOP_PW}
timezone: "Europe/Berlin"
"""


def marker(text: str, env: str) -> str:
    return settings_marker(load_project_settings(text), env)


def edited(old: str, new: str, text: str = ENVIRONMENTS) -> str:
    assert text.count(old) == 1
    return text.replace(old, new)


def test_the_marker_is_a_sha256_hex_digest():
    assert re.fullmatch(r"[0-9a-f]{64}", marker(ENVIRONMENTS, "stage"))


def test_a_comment_edit_leaves_the_marker_unchanged():
    commented = edited("# Settings for every Environment.", "# Reviewed on Monday.")
    commented = edited("  stage:\n", "  stage:  # the shared test Server\n", commented)

    assert marker(commented, "stage") == marker(ENVIRONMENTS, "stage")


def test_indentation_quoting_and_key_order_leave_the_marker_unchanged():
    for env in ("dev", "stage", "prod"):
        assert marker(REFORMATTED, env) == marker(ENVIRONMENTS, env)


def test_an_edit_to_another_environment_changes_only_that_environments_marker():
    prod_edited = edited(
        "database: p, user: u, password_env: SHOP_PW",
        "database: p2, user: u, password_env: SHOP_PW",
    )

    assert marker(prod_edited, "stage") == marker(ENVIRONMENTS, "stage")
    assert marker(prod_edited, "prod") != marker(ENVIRONMENTS, "prod")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "      shop: {type: postgres, host: h, port: 5432, database: d,",
            "      shop: {type: postgres, host: h2, port: 5432, database: d,",
        ),
        (
            "    warehouse: {type: postgres, host: wh, port: 5432, database: d,",
            "    warehouse: {type: postgres, host: wh2, port: 5432, database: d,",
        ),
        ("crm: {type: postgres, host: c,", "crm: {type: postgres, host: c2,"),
    ],
    ids=["source", "warehouse", "target"],
)
def test_an_edit_in_the_environments_own_block_changes_its_marker(old, new):
    assert marker(edited(old, new), "stage") != marker(ENVIRONMENTS, "stage")
    assert marker(edited(old, new), "prod") == marker(ENVIRONMENTS, "prod")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("timezone: Europe/Berlin", "timezone: UTC"),
        ("timezone: Europe/Berlin", "timezone: Europe/Berlin\nalert_webhook_env: ALERT_URL"),
    ],
    ids=["timezone", "alert_webhook_env"],
)
def test_a_shared_setting_changes_every_environments_marker(old, new):
    for env in ("stage", "prod"):
        assert marker(edited(old, new), env) != marker(ENVIRONMENTS, env)


def test_the_marker_holds_secret_reference_names_not_secret_values(monkeypatch):
    monkeypatch.setenv("SHOP_PW", "first-value")
    before = marker(ENVIRONMENTS, "stage")
    monkeypatch.setenv("SHOP_PW", "rotated-value")

    assert marker(ENVIRONMENTS, "stage") == before
    renamed = edited(
        "database: d, user: u, password_env: SHOP_PW",
        "database: d, user: u, password_env: SHOP_PW2",
    )
    assert marker(renamed, "stage") != before
