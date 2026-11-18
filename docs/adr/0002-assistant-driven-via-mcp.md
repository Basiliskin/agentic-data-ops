# The user's own AI assistant drives the product via MCP; built-in agent is optional

"Agentic" primarily means the user's existing coding assistant (Claude Code, Cursor, etc.) operates the product through an MCP server and skills, so the core needs no LLM key and stays a deterministic CLI/library. A built-in Pydantic AI + Wren analytics agent (the spec's main agent) is an optional add-on limited to answering questions about data.

**Considered Options:** built-in agent only (requires LLM key and a chat UI, harder setup); both equally (doubles surface area in v1).
