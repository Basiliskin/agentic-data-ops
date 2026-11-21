# Planning Brief — horizon 9 (written by horizon 8 PLAN, 2026-11-21)

## Recommended scope
The next horizon should first make the horizon-8 Promotion path sturdy, then add one new capability. It should close the horizon-7 STARTED-run blocker on the Server and choose whether to prove the workflow commands with a live E2E (PR checkout mount plus Dagster reload) inside the 20-minute budget, using any first user feedback. Then it should take on ADR-0015 accepted-change records (accept-schema writes a record that stage/prod apply once), but only after the record format and applied-once decisions above are settled. Leave the MCP server, the six skills, the Verified Source E2E suite and the single-VM playbook for later horizons, since the skills depend on the MCP server and Promotion. Keep lock-timeout config and branch-name collision fixes out unless feedback asks for them.

## Unknowns
- Do the generated stage/prod workflows actually work on a real self-hosted runner: does docker compose run with the PR checkout mounted over the Server folder's .adops/ and .env build Branch schemas in the real stage Warehouse?
- Does the Dagster code-location reload the workflow sends really pick up added or removed Pipelines and Schedule changes while an in-progress run finishes on the old version?
- Do files the root Server container creates in .adops/ (DuckDB file, .lock, dlt state) break git checkout or later runs started as the runner user?
- What is in an ADR-0015 accepted-change record: the full accepted TableShape from where accept-schema ran, or 'accept whatever the Source shows next run', given prod's Source can differ from dev's?
- How does an Environment know it already applied an accepted-change record, and what happens if the Source has drifted again by the time the record is applied?
- After a Server crash, how should runs left STARTED be cleared so the skip-overlap rule does not skip every later tick: Dagster run monitoring, a startup sweep, or both?
- What does the Assistant need from the MCP server beyond edit/validate/test-connection/run/Preview/read-logs, and how does server code tell a dev/Branch request from stage/prod reliably?
- What did the first user feedback on Promotion say (runner setup, fork policy, 30-minute lock), and does any of it change horizon-8 choices?

## Research
- Read the finished horizon-8 roadmap and the new ADR-0016 to see what the workflows, the lock and the reload step actually became, versus what was planned.
- Try the generated adops-pull-request.yml and adops-stage.yml commands by hand on a Linux host with the demo stack: docker compose run with the mounts, then the Dagster reload, then run --all --env stage.
- Dagster 1.13 docs on run_monitoring in dagster.yaml (start/cancel timeouts, max_resume_run_attempts) and how it acts with QueuedRunCoordinator max_concurrent_runs: 1.
- Read PlannedStep._raise_if_breaking and accept_schema.py to find where an accepted-change record would be applied before the snapshot comparison.
- Read ADR-0002 and ADR-0009, then check MCP Python SDK options (stdio server, tool schemas) and how to test refusal of stage/prod at 100% coverage.
- Time the current Docker E2E (Postgres and DuckDB, plus the Server and lock-overlap cases) against the 20-minute limit before adding live workflow or MCP E2E cases.
- Re-read blockers.md items from horizons 6-7 (fork reach, changed-only, concurrent --branch, pinned install) and mark which ones horizon 8 closed.

## Decisions needed
- Whether the next horizon adds a live E2E of the workflow commands (docker compose run with the PR checkout mount, Dagster reload) or keeps checking the generated YAML only.
- Format and location of the accepted-change record, what accept-schema --env stage/prod does now, and how applied-once is tracked per Environment.
- How STARTED-after-crash runs are handled on the Server (run monitoring config vs cleanup on startup) and whether that alerts through the webhook.
- Which vision item comes next: ADR-0015 records, the MCP server, the six skills, or the Verified Source E2E suite, and whether the skills wait for the MCP server.
- Whether to fix short-branch-name Branch Environment collisions now that drop-branch runs automatically on every PR close.
- Whether the lock timeout stays a fixed 30 minutes or becomes configurable, decided from real feedback rather than up front.

## Artifacts to inspect
- docs/adr/0014-schedules-run-on-a-pull-based-server.md
- docs/adr/0015-breaking-changes-accepted-through-git.md
- docs/adr/0006-branch-environments-as-warehouse-schemas.md
- docs/adr/0002-assistant-driven-via-mcp.md and docs/adr/0009-assistant-acts-only-in-dev-and-branch-environments.md
- src/adops/templates/github/workflows/ (adops-pull-request.yml, adops-stage.yml, adops-prod.yml, as built in horizon 8)
- src/adops/infrastructure/warehouse_lock.py
- src/adops/interface/run_command.py (run_project, run_in_environment, _run_branch, --all, RUN_ERRORS)
- src/adops/interface/server.py and src/adops/infrastructure/dagster_server.py
- src/adops/templates/server/dagster.yaml (QueuedRunCoordinator max_concurrent_runs: 1, no run_monitoring)
- src/adops/application/accept_schema.py and src/adops/interface/accept_schema_command.py
- src/adops/application/run_pipeline.py (PlannedStep._raise_if_breaking) and src/adops/infrastructure/schema_snapshot_store.py
- src/adops/domain/branch_environment.py (branch_environment_name, hashing of long names only)
- tests/e2e/conftest.py, tests/e2e/test_demo_sync.py, tests/e2e/test_server.py
- docs/playbooks/promotion.md and docs/roadmaps/agentic-dataops/blockers.md
