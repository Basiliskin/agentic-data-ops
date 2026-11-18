# Planning Brief — horizon 3 (written by horizon 2 PLAN, 2026-11-18)

## Recommended scope
Start with the dbt Transform Step, following the user-confirmed order. It needs a TransformStep model in the Pipeline union, a pure dbt-test verdict rule, a Transform port bound into the planned Step, a dbtRunner adapter in infrastructure/ (with dbt-core 1.12.7, dbt-postgres 1.11.0 and dbt-duckdb added to runtime deps and re-locked), a secret-free generated profile, and the demo dbt project (hand-written sources.yml over raw_shop, with DEMO_FILES and the CI grep list updated). Prove it in Docker E2E: Sync, then Transform, then dbt tests pass, and a failing dbt test stops later Steps. Then reuse the already-specified dedicated-errors phase (unreachable Source, unreachable Warehouse, empty Source table; unit tests, no E2E). Then add the DuckDB Warehouse (DuckDbConnection union, Warehouse with no secret, Warehouse-kind wiring, dlt[duckdb] at runtime) if it fits the phase cap; otherwise defer it explicitly to horizon 4. Keep Reverse Sync, Branch/stage Environments, needs: graphs, dagster-dbt and sources.yml generation out of scope.

## Already settled (do not re-ask)
- dbt runs via dbt's Python dbtRunner behind a Transform port in infrastructure/ — not dagster-dbt.
- The dbt adapter only measures (test id, failures count, crash flag); a domain rule decides the verdict.
- Steps keep linear YAML order.
- dbt-core 1.12.7 + dbt-postgres 1.11.0 + dbt-duckdb resolve in the current uv lock (verified 2026-11-18).

## Unknowns
- Horizon 2 is planned but not yet implemented. Do the planned names hold after execute (SchemaSnapshotStore, Clock, PlannedStep closures, CheckResult.check: str)?
- How do you tell a dbt crash from a test failure in the dbtRunner result (success/exception fields, run_results statuses error/fail/warn)? Is `failures` ever null?
- Is dbtRunner safe to call several times in one process, and inside an in-process Dagster op? Is it thread-safe?
- Which dbt outputs echo profile values or the host (logs/, target/ compiled SQL, run_results messages, exception text)? Which does adops surface or leave on disk?
- Does DuckDB's single-writer lock break when dlt, the snapshot store and dbt-duckdb open the same file one after another in one run?
- Which dbt-duckdb version did the lock resolve to, and does it pin duckdb against the installed 1.5.6?
- Will a dbt build plus the failure scenarios fit the 20-minute Docker E2E job? There is no wall-clock baseline yet.
- Do dlt destination connection failures surface a stable exception type that can be classified as "Warehouse unreachable"?
- Do dlt duckdb type mapping and identifier normalization differ enough from Postgres to break sources.yml, the models or the declared-Check measurements?

## Research
- dbt-core 1.12 programmatic invocation: dbtRunner, dbtRunnerResult, `build` vs `run`+`test`, --profiles-dir/--project-dir, log and target path flags.
- run_results.json schema: per-node unique_id, status, failures, message. Which fields can carry SQL or connection text?
- dbt-duckdb profile options and its file locking next to other DuckDB connections in the same process.
- dbt env_var() and profiles.yml, so the profile lives in a temp dir outside the Project with no password on disk (ADR-0007).
- dlt duckdb destination: credentials/path convention, raw_<source> naming and _dlt_* columns, compared with Postgres.
- Horizon-2 execution outcome (discoveries.md, ledger) to confirm what actually landed.

## Decisions needed
- TransformStep YAML shape (dbt project path, select/tags), and how the tagged union keeps `steps[i].field` error paths.
- One `dbt build` vs separate run + test. Is a failing model (not a test) a StepError?
- dbt test → CheckResult mapping: check name, table (model vs source tests), warn severity, and whether skip_checks can target dbt tests.
- Where the dbt profile is generated and how resolved Warehouse credentials reach it.
- Transform port plumbing: closure-bound into the planned Step, or widen Orchestrator/StepTask beyond a single Sync.
- Should `adops validate` also run `dbt parse`?
- DuckDB path relative to the Project or absolute? How is the connection-closing order enforced?
- How build_sync/build_snapshot_store get the Warehouse kind from settings instead of hard-coding 'postgres'.
- Phase budget: dbt ≥4 phases, errors 1, DuckDB 1–2. Decide up front whether DuckDB fits or moves to horizon 4.

## Artifacts to inspect
horizons/horizon-02-breaking-changes-and-declared-checks-roadmap.json `deferred` (the fully specified errors phase), src/adops/application/{ports,run_pipeline}.py, src/adops/domain/{pipeline,checks,environment}.py, src/adops/infrastructure/{dlt_sync,dagster_orchestrator}.py, src/adops/interface/run_command.py, src/adops/templates/demo/ + tests/unit/interface/test_init_command.py, .github/workflows/ci.yml, tests/e2e/test_demo_sync.py, tests/unit/{domain/test_domain_purity,application/test_application_purity}.py, pyproject.toml / uv.lock
