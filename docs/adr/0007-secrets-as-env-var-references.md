# Secrets are referenced by environment-variable name; tools never see values

Pipeline YAML refers to secrets only by name (`${APP_DB_PASSWORD}`); values come from a git-ignored `.env` locally or from Docker/CI secrets, with optional Vault/cloud secret-store plug-ins later. The MCP server and CLI output may show a Secret Reference's name and whether it is set, never its value, so the Assistant (and any LLM behind it) cannot leak credentials.
