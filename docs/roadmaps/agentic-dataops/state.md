# State

- 2026-11-17 | horizon 1 | planned | 7 phases (demo shop loads end-to-end); gate passed after heal (1 blocker, 2 majors fixed)
- 2026-11-17 | horizon 1 | replanned | revise-phases, 6 phases revised — built-in deterministic Checks on every Sync (ADR-0013) and adops validate folded in
- 2026-11-18 | horizon 1 | completed | 7 phases, 1 amendment, 0 overrides — demo shop loads end-to-end via adops run; Docker E2E job in CI (no PR run yet)
- 2026-11-18 | horizon 2 | planned | 6 phases (Breaking Changes + accept-schema, user-declared Checks); gate passed after heal (0 blockers, 2 majors fixed, 1 phase split)
- 2026-11-18 | horizon 2 | completed | 6 phases | 2 amendments (accept-schema relaxes dropped NOT NULL columns; demo seed email nullable) | 0 overrides
- 2026-11-18 | horizon 3 | planned | 6 phases (dbt Transform Step + dedicated Sync errors); gate passed after fixes (0 blockers, 2 majors fixed)
- 2026-11-18 | horizon 3 | completed | 6 phases | 1 amendment — dbt Transform Step, dbt tests as Checks, empty Source table Check, fixed Source/Warehouse unreachable and load-failed errors, dlt output silenced
- 2026-11-18 | horizon 4 | planned | 6 phases (DuckDB Warehouse runs the demo end to end); gate passed after fixes (0 blockers, 1 major fixed)
- 2026-11-19 | horizon 4 | completed | 6 phases | 1 amendment — DuckDB Warehouse kind, kind read from settings, dlt[duckdb] + dbt-duckdb at runtime, demo E2E passes on Postgres and DuckDB in one stack (92s)
- 2026-11-19 | horizon 5 | planned | 7 phases (Reverse Sync + Preview, stage Environment, demo E2E on both Warehouses); gate passed after fixes (0 blockers, 3 majors fixed)
- 2026-11-19 | horizon 5 | completed | 7 phases | 1 amendment — Reverse Sync + Preview (writes only in stage/prod), per-Environment dlt state, demo stage Environment; E2E passes on Postgres and DuckDB (110s)
