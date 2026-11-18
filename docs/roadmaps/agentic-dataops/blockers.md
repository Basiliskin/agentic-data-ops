# Blockers

- 2026-11-17 | horizon 1 | Do dbt-core/dbt-postgres/dbt-duckdb (and later PyAirbyte) fit the same uv lock as dlt + dagster + dagster-postgres?
- 2026-11-17 | horizon 1 | How do dbt models reference dlt's raw_<source> tables (normalized names, _dlt_* columns), and does adops generate sources.yml?
- 2026-11-17 | horizon 1 | Can dlt schema contracts detect renamed/dropped/retyped Postgres columns as a Breaking Change, or is an adops schema snapshot needed?
- 2026-11-17 | horizon 1 | How does 'halt this Step and downstream' work once Steps form a graph rather than a linear chain?
- 2026-11-17 | horizon 1 | How do Pipelines run on a schedule when Dagster runs in-process only (daemon/code location vs cron/CI)?
- 2026-11-17 | horizon 1 | How do Branch Environment schemas line up with dlt datasets and dbt --defer --state, and where does stage dbt state live?
- 2026-11-17 | horizon 1 | How can PR workflows reach the stage Warehouse without exposing secrets to forked PRs?
- 2026-11-17 | horizon 1 | Which engine performs Reverse Sync into Postgres, how is the Preview diff computed without writing, and what are delete semantics?
- 2026-11-17 | horizon 1 | Does the single adops.yaml Environment model hold up for stage/prod deployments and generated Branch Environments?
- 2026-11-17 | horizon 1 | How long and how stable is the Docker E2E job on GitHub Actions as Steps are added?
