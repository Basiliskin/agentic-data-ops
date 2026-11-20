from dataclasses import dataclass, field

import pytest
from fakes import StrictEnv

from adops.application.drop_branch import (
    BranchState,
    DroppedBranch,
    WarehouseSchemas,
    drop_branch,
)
from adops.application.ports import ResolvedConnection, ResolvedDuckDb, ResolvedWarehouse
from adops.application.secrets import MissingSecretError
from adops.domain.errors import PipelineValidationError

STAGE_PASSWORD = "s3cr3t-stage-drop-sentinel"
STAGE_ENV = StrictEnv({"STAGE_WAREHOUSE_PASSWORD": STAGE_PASSWORD})

POSTGRES = """
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w, user: w,
                password_env: DEV_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: s, user: s,
             password_env: SHOP_DB_PASSWORD}
  stage:
    warehouse: {type: postgres, host: stage-db, port: 5432, database: wh, user: adops,
                password_env: STAGE_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: stage-src, port: 5432, database: s, user: s,
             password_env: STAGE_SHOP_PASSWORD}
  prod:
    warehouse: {type: postgres, host: prod-db, port: 5432, database: wh, user: adops,
                password_env: PROD_WAREHOUSE_PASSWORD}
    sources: {}
"""

DUCKDB = """
environments:
  dev:
    warehouse: {type: duckdb, path: dev.duckdb}
    sources: {}
  stage:
    warehouse: {type: duckdb, path: stage.duckdb}
    sources:
      shop: {type: postgres, host: stage-src, port: 5432, database: s, user: s,
             password_env: STAGE_SHOP_PASSWORD}
"""

NO_STAGE = POSTGRES.split("  stage:")[0]

STAGE_SCHEMAS = ["raw_shop", "raw_shop_staging", "analytics", "adops", "public"]
NEIGHBOURS = [
    "br_feat2__analytics",
    "br_fea__analytics",
    "br_featx_analytics",
    "br_feat_analytics",
    "br_feat_%_analytics",
    "br_other__adops",
    "xbr_feat__analytics",
]
FEAT = ["br_feat__raw_shop", "br_feat__raw_shop_staging", "br_feat__analytics", "br_feat__adops"]


@dataclass
class FakeWarehouseSchemas:
    schemas: list[str]
    calls: list[tuple[str, ResolvedWarehouse, str | None]] = field(default_factory=list)

    def list_schemas(self, warehouse: ResolvedWarehouse) -> list[str]:
        self.calls.append(("list", warehouse, None))
        return list(self.schemas)

    def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
        self.calls.append(("drop", warehouse, schema))
        if schema in self.schemas:
            self.schemas.remove(schema)


@dataclass
class FakeBranchState:
    events: list[tuple[str, ResolvedWarehouse | None, str | None]]
    deleted: list[str] = field(default_factory=list)

    def delete(self, environment: str) -> None:
        self.events.append(("delete-state", None, environment))
        self.deleted.append(environment)


def ports(schemas: list[str]) -> tuple[FakeWarehouseSchemas, FakeBranchState]:
    warehouse = FakeWarehouseSchemas(schemas)
    return warehouse, FakeBranchState(warehouse.calls)


def test_only_the_branchs_exactly_prefixed_schemas_are_dropped():
    warehouse, state = ports(STAGE_SCHEMAS + NEIGHBOURS + FEAT)

    dropped = drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)

    assert dropped == DroppedBranch("br_feat", tuple(sorted(FEAT)))
    assert sorted(warehouse.schemas) == sorted(STAGE_SCHEMAS + NEIGHBOURS)
    assert state.deleted == ["br_feat"]


def test_the_prefix_comes_from_the_sanitized_branch_name():
    warehouse, state = ports(["br_feature_x__analytics", "analytics"])

    dropped = drop_branch(POSTGRES, "Feature/X", STAGE_ENV, warehouse, state)

    assert dropped.schemas == ("br_feature_x__analytics",)
    assert warehouse.schemas == ["analytics"]
    assert state.deleted == ["br_feature_x"]


def test_a_branch_with_nothing_left_succeeds_and_still_clears_local_state():
    warehouse, state = ports(list(STAGE_SCHEMAS))

    first = drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)
    second = drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)

    assert first == second == DroppedBranch("br_feat", ())
    assert warehouse.schemas == STAGE_SCHEMAS
    assert [call[0] for call in warehouse.calls] == ["list", "delete-state"] * 2


def test_schemas_are_dropped_before_the_local_state_is_deleted():
    warehouse, state = ports(["br_feat__analytics", "br_feat__raw_shop"])

    drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)

    assert [call[0] for call in warehouse.calls] == ["list", "drop", "drop", "delete-state"]


def test_a_failed_drop_keeps_the_local_state():
    class FailingDrop(FakeWarehouseSchemas):
        def drop(self, warehouse: ResolvedWarehouse, schema: str) -> None:
            raise RuntimeError("drop failed")

    warehouse = FailingDrop(["br_feat__analytics"])
    state = FakeBranchState(warehouse.calls)

    with pytest.raises(RuntimeError):
        drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)

    assert state.deleted == []


def test_only_stages_warehouse_secret_is_resolved():
    warehouse, state = ports(["br_feat__analytics"])

    drop_branch(POSTGRES, "feat", STAGE_ENV, warehouse, state)

    resolved = {call[1] for call in warehouse.calls if call[1] is not None}
    assert resolved == {
        ResolvedConnection(
            host="stage-db", port=5432, database="wh", user="adops", password=STAGE_PASSWORD
        )
    }


def test_a_duckdb_stage_needs_no_secret():
    warehouse, state = ports(["br_feat__analytics"])

    drop_branch(DUCKDB, "feat", StrictEnv({}), warehouse, state)

    assert warehouse.calls[0][1] == ResolvedDuckDb("stage.duckdb")


def test_a_missing_stage_secret_drops_nothing():
    warehouse, state = ports(["br_feat__analytics"])

    with pytest.raises(MissingSecretError) as raised:
        drop_branch(POSTGRES, "feat", {"DEV_WAREHOUSE_PASSWORD": "dev"}, warehouse, state)

    assert raised.value.names == ("STAGE_WAREHOUSE_PASSWORD",)
    assert warehouse.calls == [] and state.deleted == []


def test_a_branch_name_without_letters_or_digits_drops_nothing():
    warehouse, state = ports(["br___analytics", "analytics"])

    with pytest.raises(PipelineValidationError):
        drop_branch(POSTGRES, "///", STAGE_ENV, warehouse, state)

    assert warehouse.calls == [] and state.deleted == []


def test_a_project_without_a_stage_environment_is_rejected():
    warehouse, state = ports(["br_feat__analytics"])

    with pytest.raises(PipelineValidationError) as raised:
        drop_branch(NO_STAGE, "feat", STAGE_ENV, warehouse, state)

    assert "stage" in raised.value.problems[0]
    assert warehouse.calls == []


def test_the_fakes_satisfy_the_ports():
    warehouse, state = ports([])

    assert isinstance(warehouse, WarehouseSchemas)
    assert isinstance(state, BranchState)
