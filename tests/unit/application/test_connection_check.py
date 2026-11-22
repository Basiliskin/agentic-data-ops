from dataclasses import dataclass, field

from adops.application.connection_check import (
    WAREHOUSE_UNREACHABLE,
    ConnectionKind,
    ConnectionResult,
    check_connections,
    source_unreachable,
    target_unreachable,
)
from adops.application.ports import ResolvedConnection
from adops.application.secrets import MissingSecretError
from adops.domain.environment import Environment

PASSWORD = "s3cr3t-pa55"


def postgres(host: str, password_env: str) -> dict:
    return {
        "type": "postgres",
        "host": host,
        "port": 5432,
        "database": "db",
        "user": "adops",
        "password_env": password_env,
    }


ENVIRONMENT = Environment.model_validate(
    {
        "warehouse": postgres("warehouse.internal", "WAREHOUSE_PASSWORD"),
        "sources": {
            "shop": postgres("shop.internal", "SHOP_PASSWORD"),
            "crm": postgres("crm.internal", "CRM_PASSWORD"),
        },
        "targets": {
            "hubspot": postgres("hubspot.internal", "HUBSPOT_PASSWORD"),
            "ads": postgres("ads.internal", "ADS_PASSWORD"),
        },
    }
)
ENV_VALUES = {
    name: PASSWORD
    for name in (
        "WAREHOUSE_PASSWORD",
        "SHOP_PASSWORD",
        "CRM_PASSWORD",
        "HUBSPOT_PASSWORD",
        "ADS_PASSWORD",
    )
}


@dataclass
class FakeProbe:
    unreachable_hosts: set[str] = field(default_factory=set)
    duckdb_ok: bool = True
    postgres_calls: list[ResolvedConnection] = field(default_factory=list)
    duckdb_calls: list[str] = field(default_factory=list)

    def postgres(self, connection: ResolvedConnection) -> bool:
        self.postgres_calls.append(connection)
        return connection.host not in self.unreachable_hosts

    def duckdb(self, path: str) -> bool:
        self.duckdb_calls.append(path)
        return self.duckdb_ok


def ok(kind: ConnectionKind, name: str) -> ConnectionResult:
    return ConnectionResult(kind, name, True)


def test_every_connection_reachable_in_order_with_sources_sorted():
    probe = FakeProbe()

    results = check_connections(ENVIRONMENT, ["shop", "crm", "shop"], ENV_VALUES, probe)

    assert results == [
        ok(ConnectionKind.WAREHOUSE, "warehouse"),
        ok(ConnectionKind.SOURCE, "crm"),
        ok(ConnectionKind.SOURCE, "shop"),
        ok(ConnectionKind.TARGET, "hubspot"),
        ok(ConnectionKind.TARGET, "ads"),
    ]
    assert [c.host for c in probe.postgres_calls] == [
        "warehouse.internal",
        "crm.internal",
        "shop.internal",
        "hubspot.internal",
        "ads.internal",
    ]
    assert {c.password for c in probe.postgres_calls} == {PASSWORD}


def test_an_unreachable_warehouse_still_checks_every_source_and_target():
    probe = FakeProbe(unreachable_hosts={"warehouse.internal"})

    results = check_connections(ENVIRONMENT, ["shop", "crm"], ENV_VALUES, probe)

    assert [(r.kind, r.name, r.ok) for r in results] == [
        (ConnectionKind.WAREHOUSE, "warehouse", False),
        (ConnectionKind.SOURCE, "crm", True),
        (ConnectionKind.SOURCE, "shop", True),
        (ConnectionKind.TARGET, "hubspot", True),
        (ConnectionKind.TARGET, "ads", True),
    ]
    assert results[0].message == "Warehouse is unreachable" == WAREHOUSE_UNREACHABLE


def test_each_unreachable_kind_has_its_fixed_message():
    probe = FakeProbe(unreachable_hosts={"warehouse.internal", "shop.internal", "ads.internal"})

    results = check_connections(ENVIRONMENT, ["shop"], ENV_VALUES, probe)

    assert [r.message for r in results] == [
        "Warehouse is unreachable",
        "Source shop is unreachable",
        None,
        "Target ads is unreachable",
    ]
    assert source_unreachable("shop") == "Source shop is unreachable"
    assert target_unreachable("ads") == "Target ads is unreachable"


def test_a_missing_secret_fails_only_that_source_with_the_existing_text():
    env_values = {k: v for k, v in ENV_VALUES.items() if k != "SHOP_PASSWORD"}
    probe = FakeProbe()

    results = check_connections(ENVIRONMENT, ["shop", "crm"], env_values, probe)

    assert [r.ok for r in results] == [True, True, False, True, True]
    assert results[2] == ConnectionResult(
        ConnectionKind.SOURCE, "shop", False, str(MissingSecretError(["SHOP_PASSWORD"]))
    )
    assert "shop.internal" not in [c.host for c in probe.postgres_calls]


def test_a_missing_secret_on_the_warehouse_or_a_target_fails_only_that_entry():
    env_values = {
        k: v for k, v in ENV_VALUES.items() if k not in {"WAREHOUSE_PASSWORD", "ADS_PASSWORD"}
    }

    results = check_connections(ENVIRONMENT, [], env_values, FakeProbe())

    assert [(r.name, r.message) for r in results] == [
        ("warehouse", str(MissingSecretError(["WAREHOUSE_PASSWORD"]))),
        ("hubspot", None),
        ("ads", str(MissingSecretError(["ADS_PASSWORD"]))),
    ]


def test_a_source_not_in_the_environment_is_a_failed_result():
    probe = FakeProbe()

    results = check_connections(ENVIRONMENT, ["billing", "shop"], ENV_VALUES, probe)

    assert results[1] == ConnectionResult(
        ConnectionKind.SOURCE, "billing", False, "Source billing is unreachable"
    )
    assert results[2] == ok(ConnectionKind.SOURCE, "shop")


def test_a_duckdb_warehouse_is_probed_by_path_and_zero_targets_report_nothing_more():
    environment = Environment.model_validate(
        {
            "warehouse": {"type": "duckdb", "path": "warehouse.duckdb"},
            "sources": {"shop": postgres("shop.internal", "SHOP_PASSWORD")},
        }
    )
    probe = FakeProbe(duckdb_ok=False)

    results = check_connections(environment, ["shop"], ENV_VALUES, probe)

    assert results == [
        ConnectionResult(ConnectionKind.WAREHOUSE, "warehouse", False, WAREHOUSE_UNREACHABLE),
        ok(ConnectionKind.SOURCE, "shop"),
    ]
    assert probe.duckdb_calls == ["warehouse.duckdb"]


def test_no_message_carries_a_password_host_or_dsn():
    env_values = {k: v for k, v in ENV_VALUES.items() if k != "CRM_PASSWORD"}
    probe = FakeProbe(unreachable_hosts={"warehouse.internal", "shop.internal", "hubspot.internal"})

    results = check_connections(ENVIRONMENT, ["shop", "crm", "billing"], env_values, probe)

    messages = [r.message for r in results if r.message]
    assert len(messages) == 5
    for message in messages:
        assert PASSWORD not in message
        assert "postgresql://" not in message
        assert ".internal" not in message
