# Decisions

- 2026-11-17 | horizon 1 | dlt syncs Verified Sources and PyAirbyte only Docker-free Community Sources, both behind one Sync port — because PyAirbyte runs source-postgres only via Docker (ADR-0012)
- 2026-11-17 | horizon 1 | Horizon 1 keeps all 7 phases so the demo loads end-to-end — because the user chose to see a working demo over a smaller foundations-only horizon
- 2026-11-17 | horizon 1 | Domain and application layers import no framework (dlt, dagster, typer, os.environ); engines sit behind Sync and orchestrator ports — because 100% branch coverage needs fakes
- 2026-11-17 | horizon 1 | No `pragma: no cover` except TYPE_CHECKING; secrets reach Dagster ops only by closure, never run config — because coverage and secret-safety must be real, not gamed
- 2026-11-17 | horizon 1 | Every Sync table gets built-in Checks; a failed Check stops later Steps; skipping needs a written reason (ADR-0013) — because every flow must be guarded
- 2026-11-17 | horizon 1 | Check verdicts come only from pure domain rules over SourceFacts/LoadFacts; adapters only measure — because same facts must give the same verdict
