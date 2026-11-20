import dataclasses
import re

import pytest

from adops.domain.branch_environment import (
    SchemaLayout,
    StepReads,
    branch_environment_name,
    canonical_step_json,
    drop_prefix,
    first_changed_step,
    read_layout,
    saves_stage_fingerprint,
    step_fingerprint,
    uses_raw_schema_var,
)
from adops.domain.errors import PipelineValidationError
from adops.domain.names import IDENTIFIER
from adops.domain.pipeline import Pipeline


def staging_schema(name: str, source: str) -> str:
    return SchemaLayout.for_branch(name).raw(source) + "_staging"


@pytest.mark.parametrize(
    ("branch", "expected"),
    [
        ("feature/ABC-12.fix", "br_feature_abc_12_fix"),
        ("main", "br_main"),
        ("--/Hotfix//", "br_hotfix"),
        ("release/1.2.3", "br_release_1_2_3"),
        ("fix-über-bug", "br_fix_ber_bug"),
        ("a__b", "br_a_b"),
        ("Café.Menu ☕", "br_caf_menu"),
    ],
)
def test_branch_names_are_sanitized_into_identifiers(branch, expected):
    name = branch_environment_name(branch, "shop")

    assert name == expected
    assert re.fullmatch(IDENTIFIER, name)


@pytest.mark.parametrize("branch", ["///", "", "---", "☕"])
def test_a_branch_name_without_letters_or_digits_is_a_usage_error(branch):
    with pytest.raises(PipelineValidationError) as error:
        branch_environment_name(branch, "shop")

    assert error.value.problems == (
        "--branch: the branch name has no letters or digits to name a Branch Environment",
    )


@pytest.mark.parametrize("source", ["s", "shop", "x" * 30])
@pytest.mark.parametrize(
    "branch",
    ["a" * 200, "feature/" + "very-long-name-" * 12, "Ünïcødé/" * 30, "x" * 48 + "/" + "y" * 10],
)
def test_long_names_are_cut_so_the_longest_branch_schema_fits_63_bytes(branch, source):
    name = branch_environment_name(branch, source)

    assert len(staging_schema(name, source).encode()) <= 63
    assert re.fullmatch(IDENTIFIER, name)
    assert re.fullmatch(IDENTIFIER, staging_schema(name, source))
    assert not name.endswith("_")
    assert "___" not in staging_schema(name, source)


def test_a_name_that_exactly_fits_is_kept_whole():
    source = "shop"
    room = 63 - len("__raw_shop_staging")
    branch = "b" * (room - len("br_"))

    assert branch_environment_name(branch, source) == "br_" + branch
    assert branch_environment_name(branch + "b", source) != "br_" + branch + "b"


def test_two_long_names_differing_only_in_their_last_character_stay_distinct():
    first = branch_environment_name("f" * 199 + "a", "x" * 30)
    second = branch_environment_name("f" * 199 + "b", "x" * 30)

    assert first != second
    assert re.fullmatch(r"br_f+_[0-9a-f]{8}", first)


def test_truncation_never_leaves_a_separator_before_the_hash_suffix():
    branch = "a" * 34 + "/" + "b" * 48
    name = branch_environment_name(branch, "shop")

    assert re.fullmatch(r"br_a+_[0-9a-f]{8}", name)


def test_the_same_branch_always_gets_the_same_name():
    branch = "feature/" + "z" * 100

    assert branch_environment_name(branch, "shop") == branch_environment_name(branch, "shop")


def test_a_source_too_long_to_leave_room_for_a_branch_name_is_reported():
    with pytest.raises(PipelineValidationError) as error:
        branch_environment_name("feature/" + "a" * 40, "s" * 40)

    assert error.value.problems == (
        "sources: a Source name is too long to fit in a Branch Schema of 63 bytes",
    )


def test_a_short_branch_name_fits_beside_a_long_source():
    assert branch_environment_name("ab", "s" * 40) == "br_ab"


def test_the_default_layout_keeps_dev_and_stage_schema_names():
    layout = SchemaLayout.default()

    assert layout.raw("shop") == "raw_shop"
    assert layout.analytics == "analytics"
    assert layout.adops == "adops"


def test_a_branch_layout_prefixes_every_schema_with_the_drop_prefix():
    layout = SchemaLayout.for_branch("br_feature")

    assert layout.raw("shop") == "br_feature__raw_shop"
    assert layout.analytics == "br_feature__analytics"
    assert layout.adops == "br_feature__adops"
    assert drop_prefix("br_feature") == "br_feature__"
    for schema in (layout.raw("shop"), layout.analytics, layout.adops):
        assert schema.startswith(drop_prefix("br_feature"))


def test_a_schema_layout_is_frozen():
    layout = SchemaLayout.default()

    with pytest.raises(dataclasses.FrozenInstanceError):
        layout.prefix = "br_x__"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        layout.analytics = "elsewhere"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("name", "saves"),
    [
        ("stage", True),
        ("Stage", False),
        ("br_stage", False),
        ("stage_test", False),
        ("prod", False),
        ("dev", False),
    ],
)
def test_only_the_environment_named_exactly_stage_saves_fingerprints(name, saves):
    assert saves_stage_fingerprint(name) is saves


def shop_pipeline(*step_names: str) -> Pipeline:
    steps = {
        "sync_shop": {
            "type": "sync",
            "name": "sync_shop",
            "source": "shop",
            "tables": [{"name": "orders", "mode": "full_refresh", "primary_key": "id"}],
        },
        "sync_crm": {
            "type": "sync",
            "name": "sync_crm",
            "source": "crm",
            "tables": [{"name": "leads", "mode": "full_refresh", "primary_key": "id"}],
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
    return Pipeline.model_validate({"name": "shop", "steps": [steps[name] for name in step_names]})


BRANCH = SchemaLayout.for_branch("br_x")


def test_rebuilding_from_the_first_step_reads_only_branch_schemas_after_each_rebuild():
    steps = shop_pipeline("sync_shop", "transform", "push").steps

    assert read_layout(BRANCH, 0, steps) == {
        "sync_shop": StepReads({"shop": "raw_shop"}, "analytics"),
        "transform": StepReads({"shop": "br_x__raw_shop"}, "analytics"),
        "push": StepReads({"shop": "br_x__raw_shop"}, "br_x__analytics"),
    }


def test_a_transform_after_an_unchanged_sync_reads_stage_raw_then_push_reads_branch_analytics():
    steps = shop_pipeline("sync_shop", "transform", "push").steps

    assert read_layout(BRANCH, 1, steps) == {
        "transform": StepReads({"shop": "raw_shop"}, "analytics"),
        "push": StepReads({"shop": "raw_shop"}, "br_x__analytics"),
    }


def test_a_reverse_sync_after_an_unchanged_transform_reads_stage_analytics():
    steps = shop_pipeline("sync_shop", "transform", "push").steps

    assert read_layout(BRANCH, 2, steps) == {
        "push": StepReads({"shop": "raw_shop"}, "analytics"),
    }


def test_only_the_rebuilt_sources_move_to_branch_raw_schemas():
    steps = shop_pipeline("sync_shop", "sync_crm", "transform").steps

    assert read_layout(BRANCH, 1, steps)["transform"] == StepReads(
        {"shop": "raw_shop", "crm": "br_x__raw_crm"}, "analytics"
    )


def test_nothing_runs_when_the_first_changed_index_is_past_the_last_step():
    steps = shop_pipeline("sync_shop", "transform").steps

    assert read_layout(BRANCH, 2, steps) == {}


def test_canonical_step_json_sorts_keys_and_includes_defaults():
    (step,) = shop_pipeline("push").steps

    text = canonical_step_json(step)

    assert text.startswith('{"delete_missing":false,"key":"id",')
    assert '"max_change_share":0.5' in text


STEP_JSON = '{"name":"transform","project":"dbt","type":"transform"}'
FILES = {"models/a.sql": b"select 1\n", "dbt_project.yml": b"name: shop\n"}


def test_a_fingerprint_ignores_file_order():
    reordered = dict(reversed(list(FILES.items())))

    assert step_fingerprint(STEP_JSON, FILES) == step_fingerprint(STEP_JSON, reordered)


def test_a_fingerprint_ignores_crlf_line_endings():
    windows = {"models/a.sql": b"select 1\r\n", "dbt_project.yml": b"name: shop\r\n"}

    assert step_fingerprint(STEP_JSON, FILES) == step_fingerprint(STEP_JSON, windows)


def test_a_fingerprint_is_a_sha256_hex_digest_and_stable():
    fingerprint = step_fingerprint(STEP_JSON, FILES)

    assert re.fullmatch(r"[0-9a-f]{64}", fingerprint)
    assert fingerprint == step_fingerprint(STEP_JSON, dict(FILES))


@pytest.mark.parametrize(
    ("step_json", "files"),
    [
        (STEP_JSON, {**FILES, "models/a.sql": b"select 2\n"}),
        (STEP_JSON, {"models/b.sql": b"select 1\n", "dbt_project.yml": b"name: shop\n"}),
        (STEP_JSON.replace('"dbt"', '"dbt2"'), FILES),
        (STEP_JSON, {**FILES, "macros/m.sql": b""}),
        (STEP_JSON, {"dbt_project.yml": b"name: shop\n"}),
    ],
    ids=["changed byte", "renamed file", "changed step field", "added empty file", "removed file"],
)
def test_a_fingerprint_changes_with_any_real_change(step_json, files):
    assert step_fingerprint(step_json, files) != step_fingerprint(STEP_JSON, FILES)


def test_moving_bytes_across_a_file_boundary_changes_the_fingerprint():
    before = {"a.sql": b"select 1; select", "b.sql": b" 2"}
    after = {"a.sql": b"select 1;", "b.sql": b" select 2"}

    assert step_fingerprint(STEP_JSON, before) != step_fingerprint(STEP_JSON, after)


def test_moving_bytes_between_a_path_and_its_content_changes_the_fingerprint():
    assert step_fingerprint(STEP_JSON, {"a.sq": b"lselect"}) != step_fingerprint(
        STEP_JSON, {"a.sql": b"select"}
    )


def test_moving_bytes_between_the_step_json_and_a_file_changes_the_fingerprint():
    assert step_fingerprint("{}a", {"b": b""}) != step_fingerprint("{}", {"ab": b""})


STAGE_FINGERPRINTS = {"sync_shop": "f1", "transform": "f2", "push": "f3"}


def test_no_step_changed_when_every_step_matches_stage():
    current = [("sync_shop", "f1"), ("transform", "f2"), ("push", "f3")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) is None


def test_the_first_step_with_a_different_fingerprint_is_the_first_changed_step():
    current = [("sync_shop", "f1"), ("transform", "f2-edited"), ("push", "f3-edited")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 1


def test_a_step_stage_has_never_run_is_changed():
    current = [("sync_shop", "f1"), ("new_step", "f9"), ("transform", "f2"), ("push", "f3")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 1


def test_a_renamed_step_is_changed():
    current = [("sync_shop", "f1"), ("transform_v2", "f2"), ("push", "f3")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 1


def test_two_swapped_steps_with_unchanged_content_are_changed_from_the_first_moved_one():
    current = [("transform", "f2"), ("sync_shop", "f1"), ("push", "f3")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 0


def test_every_step_is_changed_when_stage_has_no_fingerprints():
    assert first_changed_step([("sync_shop", "f1")], {}) == 0


def test_a_step_appended_after_stage_last_step_is_changed():
    current = [("sync_shop", "f1"), ("transform", "f2"), ("push", "f3"), ("extra", "f4")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 3


def test_removing_stage_trailing_steps_leaves_nothing_to_rebuild():
    current = [("sync_shop", "f1"), ("transform", "f2")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) is None


def test_removing_a_middle_step_changes_the_steps_after_it():
    current = [("sync_shop", "f1"), ("push", "f3")]

    assert first_changed_step(current, STAGE_FINGERPRINTS) == 1


@pytest.mark.parametrize(
    ("files", "uses"),
    [
        (
            {"models/sources.yml": b"schema: \"{{ var('adops_raw_schema_shop', 'raw_shop') }}\""},
            True,
        ),
        ({"models/sources.yaml": b"schema: \"{{ var('adops_raw_schema_shop') }}\""}, True),
        ({"models/sources.yml": b"schema: raw_shop"}, False),
        ({"models/sources.yml": b"var('adops_raw_schema_shop_eu')"}, False),
        ({"models/sources.yml": b"var('adops_raw_schema_sho')"}, False),
        ({"models/model.sql": b"var('adops_raw_schema_shop')"}, False),
        ({}, False),
    ],
    ids=["yml", "yaml", "hard-coded", "longer source", "shorter source", "sql file", "empty"],
)
def test_a_dbt_project_uses_the_raw_schema_var_only_from_a_yml_file(files, uses):
    assert uses_raw_schema_var(files, "shop") is uses


def test_the_raw_schema_var_check_escapes_the_source_name():
    files = {"sources.yml": b"var('adops_raw_schema_aXb')"}

    assert uses_raw_schema_var(files, "a.b") is False
    assert uses_raw_schema_var(files, "aXb") is True
