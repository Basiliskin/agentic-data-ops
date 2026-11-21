import dataclasses

import pytest

from adops.domain.assistant_access import (
    AssistantRefusal,
    AssistantTarget,
    assistant_target,
    refuse_shared_dev_connection,
)
from adops.domain.environment import DuckDbConnection, Environment, PostgresConnection


def refusal_of(environment, branch) -> str:
    with pytest.raises(AssistantRefusal) as caught:
        assistant_target(environment, branch)
    return str(caught.value)


def test_dev_is_accepted():
    assert assistant_target("dev", None) == AssistantTarget(branch=None)


def test_a_branch_is_accepted_and_recorded():
    assert assistant_target(None, "feature-x").branch == "feature-x"


def test_stage_is_refused():
    refusal_of("stage", None)


def test_prod_is_refused():
    refusal_of("prod", None)


def test_qa_is_refused():
    refusal_of("qa", None)


def test_production_is_refused():
    refusal_of("production", None)


def test_dev_in_another_case_is_refused():
    refusal_of("Dev", None)


def test_dev_with_trailing_space_is_refused():
    refusal_of("dev ", None)


def test_a_custom_name_is_refused_with_the_exact_dev_rule():
    message = refusal_of("development", None)

    assert "exactly `dev`" in message
    assert "Branch Environment" in message
    assert "'development'" in message


def test_an_environment_and_a_branch_together_are_refused():
    assert "not both" in refusal_of("dev", "x")


def test_neither_an_environment_nor_a_branch_is_refused():
    assert "exactly `dev`" in refusal_of(None, None)


def test_an_empty_branch_is_refused():
    refusal_of(None, "")


def test_a_target_is_frozen():
    target = assistant_target("dev", None)

    with pytest.raises(dataclasses.FrozenInstanceError):
        target.branch = "x"


def postgres(host="localhost", port=5432, database="db", user="adops", secret="DB_PASSWORD"):
    return PostgresConnection(
        type="postgres", host=host, port=port, database=database, user=user, password_env=secret
    )


def env(warehouse, **targets):
    return Environment(warehouse=warehouse, sources={}, targets=targets)


def refuses(environments, duckdb_identity=str) -> str | None:
    try:
        refuse_shared_dev_connection(environments, duckdb_identity)
    except AssistantRefusal as error:
        return str(error)
    return None


def test_the_same_warehouse_as_stage_is_refused():
    message = refuses({"dev": env(postgres(database="wh")), "stage": env(postgres(database="wh"))})

    assert message is not None
    assert "stage" in message


def test_the_same_warehouse_as_prod_is_refused():
    message = refuses(
        {
            "dev": env(postgres(database="wh")),
            "stage": env(postgres(database="stage_wh")),
            "prod": env(postgres(database="wh")),
        }
    )

    assert message is not None
    assert "prod" in message


def test_the_same_target_as_stage_is_refused():
    assert refuses(
        {
            "dev": env(postgres(database="dev_wh"), shop=postgres(database="shop")),
            "stage": env(postgres(database="stage_wh"), shop=postgres(database="shop")),
        }
    )


def test_a_dev_target_equal_to_stages_warehouse_is_refused():
    assert refuses(
        {
            "dev": env(postgres(database="dev_wh"), shop=postgres(database="stage_wh")),
            "stage": env(postgres(database="stage_wh")),
        }
    )


@pytest.mark.parametrize("stage_host", ["127.0.0.1", "::1"])
def test_loopback_aliases_are_one_host(stage_host):
    assert refuses(
        {
            "dev": env(postgres(host="LOCALHOST", database="wh")),
            "stage": env(postgres(host=stage_host, database="wh")),
        }
    )


def test_host_case_does_not_matter():
    assert refuses(
        {
            "dev": env(postgres(host="DB.Example.com", database="wh")),
            "stage": env(postgres(host="db.example.com", database="wh")),
        }
    )


def test_user_and_secret_are_not_part_of_the_connection():
    assert refuses(
        {
            "dev": env(postgres(database="wh", user="dev_user", secret="DEV_PASSWORD")),
            "stage": env(postgres(database="wh", user="stage_user", secret="STAGE_PASSWORD")),
        }
    )


def test_an_equal_duckdb_identity_is_refused():
    identity = {
        "warehouse.duckdb": "/p/warehouse.duckdb",
        "./warehouse.duckdb": "/p/warehouse.duckdb",
    }

    assert refuses(
        {
            "dev": env(DuckDbConnection(type="duckdb", path="warehouse.duckdb")),
            "stage": env(DuckDbConnection(type="duckdb", path="./warehouse.duckdb")),
        },
        identity.__getitem__,
    )


def test_different_duckdb_identities_pass():
    assert (
        refuses(
            {
                "dev": env(DuckDbConnection(type="duckdb", path="dev.duckdb")),
                "stage": env(DuckDbConnection(type="duckdb", path="stage.duckdb")),
            }
        )
        is None
    )


def test_a_duckdb_path_never_matches_a_postgres_database():
    assert (
        refuses(
            {
                "dev": env(DuckDbConnection(type="duckdb", path="wh")),
                "stage": env(postgres(database="wh")),
            }
        )
        is None
    )


def test_a_different_port_is_a_different_connection():
    assert (
        refuses(
            {
                "dev": env(postgres(port=5433, database="wh")),
                "stage": env(postgres(port=5432, database="wh")),
            }
        )
        is None
    )


def test_a_different_database_is_a_different_connection():
    assert (
        refuses({"dev": env(postgres(database="dev_wh")), "stage": env(postgres(database="wh"))})
        is None
    )


def test_dev_alone_passes():
    assert refuses({"dev": env(postgres(database="wh"))}) is None


def test_fully_separate_connections_pass():
    assert (
        refuses(
            {
                "dev": env(postgres(database="dev_wh"), shop=postgres(database="shop_dev")),
                "stage": env(postgres(database="stage_wh"), shop=postgres(database="shop")),
                "prod": env(postgres(host="prod-db", database="wh"), shop=postgres(host="prod-db")),
                "qa": env(postgres(database="dev_wh")),
            }
        )
        is None
    )


def test_the_shared_connection_refusal_carries_no_connection_values():
    shared = postgres(host="leak-host-zz9", database="leak-db-zz9", user="leak-user-zz9")

    message = refuses({"dev": env(shared), "stage": env(shared)})

    assert message is not None
    for value in ("leak-host-zz9", "leak-db-zz9", "leak-user-zz9"):
        assert value not in message
