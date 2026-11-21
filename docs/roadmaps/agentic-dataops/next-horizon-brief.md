# Planning Brief — horizon 10 (written by horizon 9 PLAN, 2026-11-21)

## Recommended scope
Finish the remaining vision item-7 MCP tools on top of the horizon-9 server. The drop-branch, accept-schema (dev only), project-view and list/read/edit-file tools follow this horizon's pattern: one synchronous tool per phase that calls existing code (drop_project_branch, accept_project_schema, loaders plus a small secret_reference_status helper, a resolve_project_file check on top of the pure path rule), sits behind the same guard and error mapping, and has its own refusal and leak tests. That suits a cheap `plan --like 9` horizon, with phases 5 and 6 from stage3-phases.json lifted almost as written. Run logs plus read-logs, and test-connection, need new ports and adapters (a run-log writer using the Clock port, called from run_project, which changes CLI and E2E behaviour; a connection-check port with a psycopg adapter). Plan them as a separate horizon or a clearly split second half. Settle read_file secret handling and dbt/-edit safety before the file tools ship. Keep stdio-only transport, the dev/branch allow-list and the open Promotion/ADR-0015 blockers as stated.

## Unknowns
- Does horizon 9 land as planned? The next horizon builds on mcp_server.py's guard helper, error mapping and mcp_harness.assert_no_secret. Check their real names and signatures before reusing them.
- drop_project_branch takes stage's Warehouse lock but has no lock_timeout path. Can it fail fast like run/preview without changing the CLI's 30-minute wait?
- accept_project_schema takes no Warehouse lock. Is it safe for the Assistant to accept a dev schema while a dev run is going on, or does the MCP tool need a lock?
- Run-log identity is undefined: what <run> is (timestamp, run id, pipeline name), retention and pruning, and whether runs that fail before a RunResult exists (refusal, MissingSecretError) write a log at all.
- read_file can return a literal password a user typed into adops.yaml or dbt/. Should read/list responses go through secret redaction, refuse the file, or return it as is?
- Can dbt/ contain a profiles.yml or other connection config, so that edit under dbt/ redirects dev to stage/prod data and the adops.yaml shared-connection rule never sees it?
- What does test-connection mean for a DuckDB Warehouse, and for a Branch Environment, which would open stage's Warehouse and stage's operational Reverse Sync targets?
- Claude Code's real MCP tool-call timeout versus how long a dev or Branch run takes. Fail-fast covers the lock wait only, not long dlt/dbt runs.

## Research
- Read the finished horizon-9 code: src/adops/interface/mcp_server.py, tests/unit/interface/mcp_harness.py and src/adops/domain/assistant_access.py, to get the tool, guard and error-mapping template a plan --like horizon would copy.
- Read stage3-phases.json phases 5 (project-file-tools) and 6 (project-view-tool). They are already specified in detail and can be lifted almost unchanged.
- Find out how Path.resolve() and case-insensitive APFS treat `.ENV`, `Pipelines/` and symlinked parents, so the path rule's post-resolve recheck is proven against real filesystem behaviour.
- Check whether dbt_transform.py generates profiles at run time or reads them from dbt/, which decides whether dbt/ edits are a connection-redirect hole.
- Find every connect point (warehouse_lock._connect, postgres_reverse_sync._connected, DltSync.preflight) and its fixed sanitized message, to shape a connection-check port without driver text.
- Look at how Dagster/dbt/dlt output is silenced today (dlt_sync.py:41, dbt_transform.py:143-150, dagster_orchestrator) so a run-log writer logs only RunResult-derived text, never engine text.

## Decisions needed
- One template horizon for the four reuse tools (drop-branch, accept-schema, project-view, file tools) and a separate horizon for run logs, read-logs and test-connection, or one mixed horizon.
- Secret handling for read_file contents: redact known secret values, refuse, or return the raw text.
- May edit change the stage/prod Environment blocks in adops.yaml, or only dev's block? Promotion and Git review are the only other safeguard.
- Run-log format and lifecycle: file naming, what goes in it, retention, and whether CLI runs log by default (this changes existing CLI behaviour and E2E).
- Whether drop-branch and accept-schema through MCP fail fast on a held lock like run/preview, and whether accept-schema gains a lock at all.
- Whether open blockers (Promotion hardening, STARTED-run cleanup, ADR-0015 accepted-change records, accept-schema --branch) keep waiting behind the MCP tools or get scheduled next.

## Artifacts to inspect
- src/adops/interface/mcp_server.py (horizon-9 tool and guard template)
- tests/unit/interface/mcp_harness.py (secret-leak harness every new tool must pass)
- src/adops/domain/assistant_access.py (Environment allow-list; target for allowed_project_path)
- src/adops/interface/drop_branch_command.py and src/adops/application/drop_branch.py (drop_prefix match, stage lock)
- src/adops/interface/accept_schema_command.py (accept_project_schema and its error mapping at lines 26-36)
- src/adops/application/secrets.py (secret_references, resolve_secret_references; home for secret_reference_status)
- src/adops/interface/run_command.py (run_project, project_env, _scoped_environ; where a run-log hook would go)
- src/adops/infrastructure/warehouse_lock.py, postgres_reverse_sync.py, dlt_sync.py (connect points for test-connection)
- src/adops/infrastructure/dbt_transform.py (dbt profile and connection origin, which decides whether dbt/ edits are safe)
- src/adops/templates/plain/ (no gitignore yet; needed before .adops/logs exists) and .github/workflows/ci.yml wheel list
