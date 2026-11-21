import dataclasses

import pytest

from adops.domain.assistant_access import (
    OUTSIDE_PROJECT_FILES,
    WRITES_ONLY_UNDER_ROOTS,
    AssistantRefusal,
    AssistantTarget,
    allowed_project_path,
    assistant_target,
    plain_pipeline_name,
    refuse_branch_schema_acceptance,
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


@pytest.mark.parametrize("name", ["shop_sync", "a-b", "v2", "orders.v2"])
def test_a_plain_pipeline_name_is_returned_unchanged(name):
    assert plain_pipeline_name(name) == name


@pytest.mark.parametrize(
    "name", ["", "..", "a/b", "a\\b", "../x", "x..y", "/etc/passwd", "shop\0sync"]
)
def test_a_pipeline_name_that_could_leave_pipelines_is_refused_without_echoing_it(name):
    with pytest.raises(AssistantRefusal) as caught:
        plain_pipeline_name(name)

    message = str(caught.value)
    assert "plain Pipeline name" in message
    if name:
        assert name not in message


def test_accepting_a_schema_in_a_branch_is_refused():
    with pytest.raises(AssistantRefusal, match="only in `dev`"):
        refuse_branch_schema_acceptance("feature")


def test_accepting_a_schema_without_a_branch_is_allowed():
    refuse_branch_schema_acceptance(None)


@pytest.mark.parametrize(
    ("relative", "expected"),
    [
        ("adops.yaml", "adops.yaml"),
        ("pipelines/a.yaml", "pipelines/a.yaml"),
        ("dbt/models/x.sql", "dbt/models/x.sql"),
        ("pipelines\\a.yaml", "pipelines/a.yaml"),
        ("./pipelines//a.yaml", "pipelines/a.yaml"),
        ("pipelines", "pipelines"),
    ],
)
def test_an_allowed_project_path_is_returned_normalized(relative, expected):
    assert allowed_project_path(relative) == expected


@pytest.mark.parametrize(
    "relative",
    [
        "",
        ".",
        "/etc/passwd",
        "\\etc\\passwd",
        "C:/Windows/x",
        "../x",
        "pipelines/../adops.yaml",
        ".env",
        ".ENV",
        ".env.local",
        ".Env.local",
        "pipelines/.env",
        "dbt/.Env.prod",
        "pipelines/.env/x.yaml",
        ".adops/dlt/x",
        ".github/workflows/ci.yml",
        "server/x",
        "Pipelines/a.yaml",
        "pipelines/a\0.yaml",
    ],
)
def test_a_path_outside_the_project_files_is_refused_with_a_fixed_message(relative):
    with pytest.raises(AssistantRefusal) as caught:
        allowed_project_path(relative)

    assert str(caught.value) == OUTSIDE_PROJECT_FILES


def test_an_env_lookalike_that_is_not_dotenv_is_allowed():
    assert allowed_project_path("pipelines/.environment.yaml") == "pipelines/.environment.yaml"


@pytest.mark.parametrize(
    ("relative", "expected"),
    [
        ("pipelines/a.yaml", "pipelines/a.yaml"),
        ("dbt/models/x.sql", "dbt/models/x.sql"),
        ("dbt/profiles.yml", "dbt/profiles.yml"),
        ("pipelines\\new\\b.yaml", "pipelines/new/b.yaml"),
    ],
)
def test_a_write_under_pipelines_or_dbt_is_allowed(relative, expected):
    assert allowed_project_path(relative, write=True) == expected


@pytest.mark.parametrize("relative", ["adops.yaml", "./adops.yaml", "pipelines", "dbt/"])
def test_a_write_outside_a_pipelines_or_dbt_file_is_refused_as_read_only(relative):
    with pytest.raises(AssistantRefusal) as caught:
        allowed_project_path(relative, write=True)

    assert str(caught.value) == WRITES_ONLY_UNDER_ROOTS


@pytest.mark.parametrize(
    "relative",
    [
        ".env",
        ".ENV",
        ".env.local",
        "pipelines/.env",
        "dbt/.Env.prod",
        ".adops/x",
        ".github/workflows/ci.yml",
        "server/x",
        "../x",
        "dbt/../adops.yaml",
        "pipelines/../adops.yaml",
        "/etc/passwd",
        "/abs/pipelines/a.yaml",
        "C:/Windows/x",
        "pipelines/a\0.yaml",
    ],
)
def test_every_read_refusal_also_refuses_a_write(relative):
    with pytest.raises(AssistantRefusal) as caught:
        allowed_project_path(relative, write=True)

    assert str(caught.value) == OUTSIDE_PROJECT_FILES


def test_adops_yaml_stays_readable_while_it_is_not_writable():
    assert allowed_project_path("adops.yaml") == "adops.yaml"
    with pytest.raises(AssistantRefusal):
        allowed_project_path("adops.yaml", write=True)

