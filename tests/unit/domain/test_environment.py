import pytest
from pydantic import ValidationError

from adops.domain.environment import DuckDbConnection, PostgresConnection
from adops.domain.errors import PipelineValidationError
from adops.domain.loader import load_project_settings
from adops.domain.secret_reference import SecretReference

CONNECTION = "{type: postgres, host: localhost, port: 5432, database: db, user: adops, %s}"


def settings(
    warehouse_secret="password_env: WAREHOUSE_DB_PASSWORD",
    source_secret="password_env: SHOP_DB_PASSWORD",
    environment="dev",
):
    return f"""
environments:
  {environment}:
    warehouse: {CONNECTION % warehouse_secret}
    sources:
      shop: {CONNECTION % source_secret}
"""


def problems_of(text: str) -> tuple[str, ...]:
    with pytest.raises(PipelineValidationError) as caught:
        load_project_settings(text)
    return caught.value.problems


def test_valid_settings_load_the_dev_environment():
    environment = load_project_settings(settings()).environments["dev"]

    assert environment.warehouse.password_env == SecretReference(name="WAREHOUSE_DB_PASSWORD")
    shop = environment.sources["shop"]
    assert (shop.type, shop.host, shop.port, shop.database, shop.user) == (
        "postgres",
        "localhost",
        5432,
        "db",
        "adops",
    )
    assert shop.password_env.name == "SHOP_DB_PASSWORD"


def test_dev_environment_is_required():
    assert problems_of(settings(environment="prod")) == (
        "environments.dev: this Environment is required",
    )


@pytest.mark.parametrize("name", ["shop_db_password", "1PASSWORD", "SHOP-DB", "", "A B"])
def test_secret_reference_must_be_an_upper_snake_case_variable_name(name):
    text = settings(source_secret=f"password_env: '{name}'")

    assert problems_of(text) == (
        "environments.dev.sources.shop.password_env: "
        "must be an environment variable name in UPPER_SNAKE_CASE",
    )


def test_secret_reference_given_as_a_mapping_is_rejected():
    text = settings(source_secret="password_env: {value: hunter2}")

    assert problems_of(text) == (
        "environments.dev.sources.shop.password_env.name: Field required",
        "environments.dev.sources.shop.password_env.value: Extra inputs are not permitted",
    )


def test_secret_reference_built_directly_still_checks_the_name():
    with pytest.raises(ValidationError):
        SecretReference(name="lower")

    assert SecretReference.model_validate("SHOP_DB_PASSWORD").name == "SHOP_DB_PASSWORD"


def test_secret_reference_holds_only_a_name():
    assert set(SecretReference.model_fields) == {"name"}


@pytest.mark.parametrize("where", ["warehouse", "source"])
def test_literal_password_is_rejected_pointing_at_password_env(where):
    literal = "password: hunter2, password_env: SHOP_DB_PASSWORD"
    text = settings(**{f"{where}_secret": literal})
    path = "environments.dev.warehouse" if where == "warehouse" else "environments.dev.sources.shop"

    (problem,) = problems_of(text)

    assert problem.startswith(f"{path}.password: literal passwords are not allowed")
    assert "password_env" in problem
    assert "hunter2" not in problem


def test_misspelled_connection_key_is_rejected_instead_of_ignored():
    text = settings(source_secret="pasword_env: SHOP_DB_PASSWORD")

    assert problems_of(text) == (
        "environments.dev.sources.shop.password_env: Field required",
        "environments.dev.sources.shop.pasword_env: Extra inputs are not permitted",
    )


@pytest.mark.parametrize("warehouse", ["{type: mysql, path: x}", "{path: x}"])
def test_an_unknown_warehouse_type_names_the_supported_kinds(warehouse):
    text = settings().replace(CONNECTION % "password_env: WAREHOUSE_DB_PASSWORD", warehouse)

    assert problems_of(text) == (
        "environments.dev.warehouse: type must be one of: postgres, duckdb",
    )


def duckdb_settings(warehouse):
    return settings().replace(CONNECTION % "password_env: WAREHOUSE_DB_PASSWORD", warehouse)


def test_a_duckdb_warehouse_needs_only_a_path():
    environment = load_project_settings(
        duckdb_settings("{type: duckdb, path: warehouse.duckdb}")
    ).environments["dev"]

    assert environment.warehouse == DuckDbConnection(type="duckdb", path="warehouse.duckdb")


@pytest.mark.parametrize(
    ("warehouse", "problem"),
    [
        (
            "{type: duckdb, path: x, host: localhost}",
            "environments.dev.warehouse.host: Extra inputs are not permitted",
        ),
        ("{type: duckdb}", "environments.dev.warehouse.path: Field required"),
        (
            "{type: duckdb, path: ''}",
            "environments.dev.warehouse.path: String should have at least 1 character",
        ),
    ],
)
def test_a_malformed_duckdb_warehouse_is_rejected(warehouse, problem):
    assert problems_of(duckdb_settings(warehouse)) == (problem,)


def test_sources_stay_postgres_only():
    text = settings().replace(
        CONNECTION % "password_env: SHOP_DB_PASSWORD", "{type: duckdb, path: x}"
    )

    assert "environments.dev.sources.shop.type: Input should be 'postgres'" in problems_of(text)


TARGETS = f"""
    targets:
      crm: {CONNECTION % "password_env: CRM_DB_PASSWORD"}
"""


def test_an_environment_has_no_targets_unless_it_declares_them():
    assert load_project_settings(settings()).environments["dev"].targets == {}


def test_targets_are_postgres_connections_kept_apart_from_sources():
    environment = load_project_settings(settings() + TARGETS).environments["dev"]

    crm = environment.targets["crm"]
    assert isinstance(crm, PostgresConnection)
    assert crm.password_env.name == "CRM_DB_PASSWORD"
    assert "crm" not in environment.sources
    assert "shop" not in environment.targets


def test_a_target_with_a_literal_password_is_rejected():
    text = settings() + TARGETS.replace("password_env: CRM", "password: hunter2, password_env: CRM")

    (problem,) = problems_of(text)

    assert problem.startswith("environments.dev.targets.crm.password: literal passwords")
    assert "hunter2" not in problem


def test_a_target_cannot_be_a_duckdb_file():
    text = settings() + "    targets:\n      crm: {type: duckdb, path: x}\n"

    assert "environments.dev.targets.crm.type: Input should be 'postgres'" in problems_of(text)


@pytest.mark.parametrize("name", ["../x", "..", "a/b", "Stage", "PROD", "1st", "dev-2"])
def test_an_environment_name_that_could_leave_its_state_folder_is_rejected(name):
    text = settings() + settings(environment=f'"{name}"').removeprefix("\nenvironments:\n")

    assert problems_of(text) == (
        f"environments.{name}.[key]: String should match pattern '^[a-z][a-z0-9_]*$'",
    )


def test_lowercase_environment_names_load():
    text = settings() + settings(environment="stage_2").removeprefix("\nenvironments:\n")

    assert set(load_project_settings(text).environments) == {"dev", "stage_2"}


def test_the_project_time_zone_defaults_to_utc():
    assert load_project_settings(settings()).timezone == "UTC"


@pytest.mark.parametrize("name", ["Europe/Berlin", "America/New_York", "UTC", "Etc/GMT+2"])
def test_a_known_time_zone_is_kept(name):
    assert load_project_settings(settings() + f"timezone: {name}\n").timezone == name


@pytest.mark.parametrize(
    "name",
    [
        "Mars/SENTINEL_tz",
        "SENTINEL_tz",
        "utc",
        "europe/berlin",
        "Europe",
        "Europe/",
        "../SENTINEL_tz",
        "/etc/SENTINEL_tz",
        '""',
        "SENTINEL_tz" * 40,
    ],
)
def test_an_unknown_time_zone_is_one_value_free_problem_at_timezone(name):
    assert problems_of(settings() + f"timezone: {name}\n") == (
        "timezone: not a known time zone; use an IANA name such as Europe/Berlin or UTC",
    )


def test_a_time_zone_that_is_not_text_is_rejected_without_its_value():
    (problem,) = problems_of(settings() + "timezone: [SENTINEL_tz]\n")

    assert problem.startswith("timezone: ")
    assert "SENTINEL_tz" not in problem


def test_alerts_are_off_unless_a_webhook_secret_reference_is_set():
    assert load_project_settings(settings()).alert_webhook_env is None

    loaded = load_project_settings(settings() + "alert_webhook_env: ALERT_WEBHOOK_URL\n")

    assert loaded.alert_webhook_env == SecretReference(name="ALERT_WEBHOOK_URL")


@pytest.mark.parametrize(
    "value", ["https://hooks.example/SENTINEL_hook", "sentinel_lower", "{name: SENTINEL_lower}"]
)
def test_a_webhook_url_pasted_in_place_of_its_variable_name_is_refused(value):
    problems = problems_of(settings() + f"alert_webhook_env: '{value}'\n")

    assert problems
    assert all(problem.startswith("alert_webhook_env") for problem in problems)
    assert not any("SENTINEL" in problem or "sentinel" in problem for problem in problems)
