import pytest
from pydantic import ValidationError

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


def test_only_postgres_connections_are_accepted():
    text = settings().replace("type: postgres", "type: mysql", 1)

    assert problems_of(text) == ("environments.dev.warehouse.type: Input should be 'postgres'",)
