# Discoveries
- 2026-11-18 | horizon 1 | coverage.py 7.17 (branch=true) does not flag a one-line `if cond: pass` as a missed branch; a gate-bite probe must put the body on its own line
- 2026-11-18 | horizon 1 | run-pipeline | InOrderOrchestrator catches only StepError; a raising Check rule (mixed-type cursors) escapes run_pipeline [src/adops/application/run_pipeline.py] → adapters must emit same-typed cursor values
- 2026-11-18 | horizon 1 | dlt-sync | DltSync runs one dlt load per table so a SyncFailedError can name the table; Source row count is measured in sync() into LoadFacts.source_rows [src/adops/infrastructure/dlt_sync.py]
- 2026-11-18 | horizon 1 | dlt-sync | dlt's 'dlt' logger has propagate=False, so caplog cannot prove dlt itself never logs cursor values; only adops' own logging is tested [tests/unit/infrastructure/test_dlt_sync.py]
- 2026-11-18 | horizon 1 | run-command | dagster.yaml resolves `env:` values from os.environ only, so adops run overlays .env into os.environ (plus DAGSTER_HOME) for the run and restores it [src/adops/interface/run_command.py]
