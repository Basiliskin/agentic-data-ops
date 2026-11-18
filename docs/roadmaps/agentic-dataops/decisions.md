# Decisions

- 2026-11-17 | horizon 1 | dlt syncs Verified Sources and PyAirbyte only Docker-free Community Sources, both behind one Sync port — because PyAirbyte runs source-postgres only via Docker (ADR-0012)
- 2026-11-17 | horizon 1 | Horizon 1 keeps all 7 phases so the demo loads end-to-end — because the user chose to see a working demo over a smaller foundations-only horizon
- 2026-11-17 | horizon 1 | Domain and application layers import no framework (dlt, dagster, typer, os.environ); engines sit behind Sync and orchestrator ports — because 100% branch coverage needs fakes
- 2026-11-17 | horizon 1 | No `pragma: no cover` except TYPE_CHECKING; secrets reach Dagster ops only by closure, never run config — because coverage and secret-safety must be real, not gamed
- 2026-11-17 | horizon 1 | Every Sync table gets built-in Checks; a failed Check stops later Steps; skipping needs a written reason (ADR-0013) — because every flow must be guarded
- 2026-11-17 | horizon 1 | Check verdicts come only from pure domain rules over SourceFacts/LoadFacts; adapters only measure — because same facts must give the same verdict
- 2026-11-18 | horizon 2 | Schema snapshots live in adops._adops_schema_snapshots in each Environment's Warehouse, saved only after every Check of the Step passes — because each Environment needs its own baseline
- 2026-11-18 | horizon 2 | An intended Breaking Change is accepted only via `adops accept-schema <pipeline> <table> --env`, never automatically — because schema changes must be a deliberate choice
- 2026-11-18 | horizon 2 | dbt runs through dbt-core's dbtRunner behind a Transform port, not dagster-dbt; dbt test failure counts are measured, verdicts stay in domain rules — because of ADR-0013
- 2026-11-18 | horizon 2 | Steps run in linear YAML order; 'downstream' means every later Step — because no Step graph exists yet
- 2026-11-18 | horizon 2 | Domain code never reads the clock; 'now' comes from an injected Clock port — because freshness verdicts must be deterministic
- 2026-11-18 | horizon 2 | Horizon 2 finishes the Check system only; horizon 3 does dbt Transform, then dedicated errors, then DuckDB — because the user chose that order
