# Analytics Agent and Wren AI deferred to v1.1

v1 covers Sync, Transform, Reverse Sync, Environments, MCP and skills; it needs no LLM key and no Wren containers. The Analytics Agent (Pydantic AI + Wren semantic layer) ships in v1.1 as an optional docker-compose extra with an `/ask` skill and metric-definition flow. This reverses the spec's emphasis, where governed analytics is central, because the semantic layer is effectively a second product and would jeopardise the 100%-coverage and easy-setup goals.
