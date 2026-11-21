from pathlib import Path

from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CallToolResult

from adops.domain.environment import PostgresConnection
from adops.domain.loader import load_project_settings
from adops.interface.mcp_server import build_server

SETTINGS = """
alert_webhook_env: ADOPS_MCP_ALERT_WEBHOOK
environments:
  dev:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w_dev, user: w,
                password_env: ADOPS_MCP_DEV_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: shop, user: s,
             password_env: ADOPS_MCP_DEV_SHOP_PASSWORD}
    targets:
      crm: {type: postgres, host: localhost, port: 5434, database: crm_dev, user: c,
            password_env: ADOPS_MCP_DEV_CRM_PASSWORD}
  stage:
    warehouse: {type: postgres, host: localhost, port: 5433, database: w_stage, user: w,
                password_env: ADOPS_MCP_STAGE_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: localhost, port: 5432, database: shop, user: s,
             password_env: ADOPS_MCP_STAGE_SHOP_PASSWORD}
    targets:
      crm: {type: postgres, host: localhost, port: 5434, database: crm, user: c,
            password_env: ADOPS_MCP_STAGE_CRM_PASSWORD}
  prod:
    warehouse: {type: postgres, host: db.internal, port: 5432, database: w_prod, user: w,
                password_env: ADOPS_MCP_PROD_WAREHOUSE_PASSWORD}
    sources:
      shop: {type: postgres, host: db.internal, port: 5432, database: shop, user: s,
             password_env: ADOPS_MCP_PROD_SHOP_PASSWORD}
    targets:
      crm: {type: postgres, host: db.internal, port: 5432, database: crm, user: c,
            password_env: ADOPS_MCP_PROD_CRM_PASSWORD}
"""

PIPELINE = """
name: shop_sync
steps:
  - name: sync_shop
    source: shop
    tables:
      - name: customers
        mode: full_refresh
        primary_key: id
"""


def secret_references(settings_text: str) -> list[str]:
    """Every Secret Reference adops.yaml names: each password_env, and alert_webhook_env."""
    settings = load_project_settings(settings_text)
    references = [settings.alert_webhook_env] if settings.alert_webhook_env else []
    for environment in settings.environments.values():
        connections = [environment.warehouse, *environment.sources.values()]
        connections += environment.targets.values()
        references += [c.password_env for c in connections if isinstance(c, PostgresConnection)]
    return [reference.name for reference in references]


def secret_values(settings_text: str = SETTINGS) -> dict[str, str]:
    return {name: f"s3cr3t-{name.lower()}-value" for name in secret_references(settings_text)}


def write_project(project: Path, settings_text: str = SETTINGS) -> dict[str, str]:
    """A Project whose .env gives every Secret Reference a distinctive value; returns them."""
    (project / "adops.yaml").write_text(settings_text)
    (project / "pipelines").mkdir(exist_ok=True)
    (project / "pipelines" / "shop_sync.yaml").write_text(PIPELINE)
    secrets = secret_values(settings_text)
    (project / ".env").write_text("".join(f"{name}={value}\n" for name, value in secrets.items()))
    return secrets


async def call_tool(project: Path, tool: str, arguments: dict[str, object]) -> CallToolResult:
    async with create_connected_server_and_client_session(build_server(project)) as session:
        return await session.call_tool(tool, arguments)


def error_text(result: CallToolResult) -> str:
    assert result.isError
    return "\n".join(block.text for block in result.content if block.type == "text")


def assert_no_secret(result: CallToolResult, secrets: dict[str, str] | None = None) -> None:
    """Fail if any secret value appears anywhere in the serialized response."""
    serialized = result.model_dump_json()
    leaked = [name for name, value in (secrets or secret_values()).items() if value in serialized]
    assert not leaked, f"MCP response carries the value of {leaked}"
