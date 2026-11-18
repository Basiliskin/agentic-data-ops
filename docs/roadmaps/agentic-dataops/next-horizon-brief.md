# Planning Brief — horizon 2 (written by horizon 1 PLAN, 2026-11-17)

## Recommended scope
Start with the rest of the Check system (user decision 2026-11-17): Breaking Change detection (schema snapshot vs last successful run, halting the Step and everything after it) and user-declared per-table Checks in YAML (not_null, accepted_values, min_rows, freshness), built on horizon 1's SourceFacts/LoadFacts and evaluate_*_checks rules. Then extend the working demo from "Sync loads" to "Sync + Transform with safe failure": a dbt Transform Step (dbt project in the demo scaffold, models + dbt tests over the dlt raw schema) and the Step-dependency model it needs; a Breaking Change halts the affected Step and everything downstream; dedicated sanitized errors for unreachable Source/Warehouse and empty Source table — dbt tests should report as Checks too; all proven in the Docker E2E job under the 100% coverage gate. Optionally the DuckDB Warehouse if the dbt-duckdb lock and defer questions resolve cheaply. Leave Reverse Sync/Preview, stage/prod/Branch Environments + Promotion, PyAirbyte, MCP + skills, OTel and playbooks for later.

## Unknowns
- Do dbt-core/dbt-postgres/dbt-duckdb (and later PyAirbyte) fit the same uv lock as dlt + dagster + dagster-postgres?
- How do dbt models reference dlt's `raw_<source>` tables (normalized identifiers, `_dlt_*` columns/tables)? Does adops generate `sources.yml`?
- Can dlt schema contracts detect renamed/dropped/retyped Postgres columns as a Breaking Change, or is an adops-owned schema snapshot needed?
- How does "halt this Step and downstream" work once Steps form a graph rather than a linear chain?
- How do Pipelines run on a schedule when Dagster runs in-process only (daemon/code location vs cron/CI)?
- How do Branch Environment schemas line up with dlt datasets and dbt `--defer --state`; where does stage dbt state live; same on Postgres and DuckDB?
- How can PR workflows reach the stage Warehouse without exposing secrets to forked PRs?
- Which engine performs Reverse Sync into Postgres and how is the Preview diff computed without writing; delete semantics?
- Does the single `adops.yaml` Environment model hold up for stage/prod deployments and generated Branch Environments?
- How long/stable is the Docker E2E job on GitHub Actions as Steps are added?

## Research
- Spec invariants vs ADR-0001..0012 for the next slices.
- dlt (locked version): schema contracts/evolution, dataset naming, dlt as writer into operational Postgres.
- dbtRunner vs dagster-dbt inside a Dagster op; dbt `--defer`/`--state`/`state:modified+` on Postgres and DuckDB.
- Scratch `uv lock` with dbt packages, then with airbyte added.
- Dagster: scheduled jobs without user-written Dagster code; failure/skip propagation through a graph.
- Python MCP SDK (stdio): checking target Environment before a tool runs.
- Record the e2e CI job wall-clock baseline from a real PR.

## Decisions needed
- Step dependencies in YAML: implicit linear order vs explicit (`needs:` / dbt-ref).
- dbt integration: dagster-dbt assets vs dbtRunner in an adops op; dbt project/profiles location; profiles generated from adops.yaml?
- Breaking Change detection: inside the Sync port contract vs an adops-owned schema snapshot around any adapter.
- Environment config for stage/prod/Branch: one adops.yaml vs deployment-level env vars; Branch schema naming.
- Reverse Sync engine, key/merge strategy, deletes in v1?
- DuckDB Warehouse with Transform now or later.
- Scheduled/production runs: in-process + cron/CI vs Dagster daemon/webserver in compose.

## Artifacts to inspect
pyproject.toml, uv.lock, src/adops/domain/{pipeline,environment,loader,errors,checks}.py, src/adops/application/{ports,run_pipeline}.py, src/adops/infrastructure/{dlt_sync,dagster_orchestrator}.py, src/adops/interface/{cli,run_command,init_command}.py, src/adops/templates/demo/, tests/e2e/test_demo_sync.py, .github/workflows/ci.yml, docs/agentic_dataops_production_spec.md
