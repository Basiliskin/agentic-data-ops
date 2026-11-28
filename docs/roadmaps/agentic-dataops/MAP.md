# Map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an Apache-2.0 CLI that scaffolds a Project whose YAML
Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev,
stage, prod and Branch Environments, operated by the user's own Assistant through MCP and six Skills,
with Promotion through GitHub Actions.

**As of:** 2026-11-27 · **Ordering:** whatever unblocks the most remaining work comes first; ties go
to whatever closes an open question, then to the earliest visible capability. The user's own order
for the production-readiness themes (decisions.md, 2026-11-26) is kept. · **User goal:** none given;
the vision is the whole goal.

This file is regenerated on every run. It is a view, not a plan. Only the "Done" rows are fact; every
row marked `candidate` is a current best guess.

## Where the project stands

All 18 horizons are complete and nothing is in flight. Since the last map, #18 landed as two
horizons: deploys switch release folders (horizon 17) and deploys rerun only Changed Steps
(horizon 18). adops is on PyPI as 0.3.1. What remains for v1 is a full plain (non-demo) scaffold, the
OTel/Grafana profile, more Sources (including PyAirbyte), the playbooks, and two Assistant-safety
questions. The user's production-readiness themes, led by freshness alerts, sit alongside them.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–16 | Horizons 1–16 | Everything listed under Done below | — | Done (fact) |
| 17 | Promotion proven on a real self-hosted runner | PR → Branch run, merge → stage, release → prod on the user's own Server machine | Closed the h8 real-runner, reload, root-owned `.adops/` and prod-gate questions (not yet marked resolved in blockers.md) | Done (trials 2026-11-26/27, outside a horizon) |
| 18 | Deploys never change files under a run in progress, and rerun only Changed Steps | Release folders with an atomic `current` switch (horizon 17); `adops run --all --changed` in both deploy workflows (horizon 18) | Safe Promotion with real data; no full reload on every merge | Done (horizons 17–18) |
| 19 | Real-runner trial of changed-only deploys, plus first-run fixes | A merge or release on a real runner reruns only Changed Steps, and a new repo's first runs explain themselves instead of showing unexplained red runs | h18's recorded follow-up (no real-runner trial yet); h17 question on a deploy whose rerun fails partway; discovery: a new repo's first PR always gets a red Branch run ("stage has no Step Fingerprints"); discovery: pushing a new repo starts adops-stage before any runner exists; inputs for the rollback command and the playbooks (#25) | candidate · likely |
| 20 | DuckDB Server trial on AWS, plus closing DuckDB gaps | A DuckDB Warehouse proven on a real Linux Server: one AWS Free-plan `m7i-flex.large` (8 GB) running stage and prod, auto-stopping after 72 hours, created and deleted by `scripts/trial-ec2.sh up` and `down`; PR → Branch, merge → stage, release → prod, two Scheduled runs, a Schedule/Branch collision on the same DuckDB file, a `kill -9` crash, and a Breaking Change accepted by record; peak memory, CPU and disk are measured per check so the playbooks can state a minimum Server size | "Full DuckDB support" proven, not only tested in CI; closes blockers.md lines 17 and 21 (single-writer lock, type mapping) and adds the DuckDB case of h18's Postgres-only stage e2e; must come before the Mongo Source (ADR-0017/0018) so DuckDB problems are not mixed with Mongo ones | candidate · likely · user-requested |
| 21 | Freshness alerts | Each table can declare how stale it may get; the Server alerts when a table has not loaded in time, including when a Schedule silently stops or is paused, and sends a recovery notice | The most common real data incident; staleness now depends only on Schedules, since unchanged scheduled Pipelines no longer refresh on merge (h18); partly answers the h7 no-Schedule run question | candidate · likely · user-requested (was #19 on the previous map) |
| 22 | Assistant dev-safety hardening | The Assistant can't point dev at stage/prod data through connection config inside `dbt/`, and long dev/Branch runs don't silently hit the tool-call timeout | h9 `dbt/profiles.yml` redirect question; h9 MCP timeout question; makes the read-only query tool (beyond visibility) safe to add | candidate · possible |
| 23 | More Verified Sources plus CDC Sync | MongoDB (designed in ADR-0017, Heavy reads in ADR-0018, which needs its own DuckDB Heavy-read check), MySQL, SQL Server and at least one SaaS API (such as Stripe) through dlt with E2E tests, and CDC Sync so deleted rows are seen | Vision criterion 10 meaning more than "Postgres only"; the README's "Sources are Postgres today" gap; the Sync port proven across Source kinds before PyAirbyte (#25). Gated by the E2E time budget (h1/h2 question) | candidate · possible · user-requested |
| 24 | Run metrics, volume-anomaly Checks and an OpenTelemetry/Grafana profile | Run metrics over time, a Check that halts on abnormal load volume, and an optional compose profile with tests that no span carries a secret | Vision criteria 2 (OTel/Grafana profile) and 6 (no secret in spans); volume-anomaly Checks need the metric history as a baseline | candidate · possible · user-requested |
| 25 | Full plain Project scaffold and an upgrade path for existing Projects | Plain `adops init` gives a dbt project and starter Pipelines, and an existing Project can take on new Skills and templates without starting over | Vision criterion 1; h15 deferred dbt project in the plain template; h15 deferred upgrade command; the h12 gap that existing Projects get stale-run recovery only by hand; the h1 dbt raw-table question and the h3 source-test naming bug | candidate · speculative |
| 26 | Docker-free Community Sources through PyAirbyte | Sync from a Docker-free Airbyte Community Source behind the same Sync port and Checks as dlt (ADR-0012) | The vision's PyAirbyte objective. Best after #23 proves the Sync port across Source kinds | candidate · speculative |

Numbers 19–26 are positions on this path, not horizon numbers. A PLAN run assigns the real number.
Freshness alerts moved from #19 to #20 because the real-runner trial now ranks above it, then to #21 when the DuckDB Server trial (user-requested 2026-11-28) was placed right after #19.

## Done

- Horizon 1 — demo shop loads end to end via `adops run` (7 phases)
- Horizon 2 — Breaking Changes, `accept-schema`, user-declared Checks (6)
- Horizon 3 — dbt Transform Step and dedicated Sync errors (6)
- Horizon 4 — DuckDB Warehouse runs the demo end to end (6)
- Horizon 5 — Reverse Sync with Preview, stage Environment (7)
- Horizon 6 — Branch Environments with `--branch` and `drop-branch` (7)
- Horizon 7 — Schedules on a Server, run rules, webhook alerts (7)
- Horizon 8 — GitHub Actions Promotion workflows, Warehouse lock, `run --all`, ADR-0016 (6)
- Horizon 9 — Assistant MCP server: run, Preview, validate for dev/Branch only (5)
- Horizon 10 — MCP drop-branch, accept-schema, project view, file tools (5)
- Horizon 11 — Run Logs, read-logs, test-connection (5)
- Horizon 12 — Server recovers runs a crash left running (3)
- Horizon 13 — Breaking Changes accepted through Git records (5)
- Horizon 14 — `accept-schema` writes accepted-change records (7)
- Horizon 15 — six Assistant Skills shipped by `adops init` (5)
- Horizon 16 — stage and prod run locally on a Mac (3)
- Horizon 17 — deploys switch release folders; a held run keeps its folder (5)
- Horizon 18 — deploys rerun only Changed Steps; prod saves fingerprints; Deploy State table (6)
- Outside a horizon — Promotion proven on real self-hosted runners (2026-11-26/27); adops on PyPI (0.1.0 → 0.3.1)

## Open questions blocking the path

| Question (from blockers.md) | Gates |
|---|---|
| h17: when a changed-only rerun fails partway, does the next deploy rerun from the right Step? | 19 |
| h8: what did first user feedback on Promotion say? | 19, 24 |
| h7: do runs started right after a version is accepted queue behind scheduled runs, and does a no-Schedule Pipeline run only then? | 20 |
| h9: can `dbt/` hold a `profiles.yml` that redirects dev to stage/prod data? | 21 |
| h9: Claude Code's MCP tool-call timeout versus long dev/Branch runs | 21 |
| h1/h2: how long and stable is the Docker E2E job, and does it fit the 20-minute budget as Sources are added? (horizon 18: 15:51) | 22 |
| h1: how do dbt models reference dlt's raw tables, and does adops generate `sources.yml`? | 24 |
| h3: DbtTransform reports a dbt source test under the source name, not its table | 24 |
| h13: how often is a PR's accepted-change record actually exercised in its Branch before merge? | — |

**Housekeeping for the next PLAN run:** blockers.md still lists as open several questions that
later work answered. These are the h8 runner, reload, root-owned-files and prod-gate questions
(answered by the 2026-11-26/27 trials), and the three h17 questions on deploy state, the settings
marker and partial failures (the first two answered by horizon 18's decisions). There are also
h1/h2 questions on dbtRunner reuse, dbt output leaks, the DuckDB single-writer lock, dlt error
types, DuckDB type mapping and NOT NULL relaxation (they look settled by horizons 3–6 and 14). The
next PLAN run should mark them `— resolved`. MAP does not edit memory.

## Beyond current visibility

- **Still unplanned from the 2026-11-26 production-readiness review, and not tied to a v1
  criterion:** resuming a failed run from its failed Step, a backfill command, more than one run at
  a time per Server, and whether dlt restores the Incremental cursor from the Warehouse when the
  Server's `.adops/` is lost. Also unplanned: a read-only row-limited `select` tool for the Assistant
  (user-requested, after #22), a rollback command (after #19), a Step dependency graph, and pruning
  stale fingerprint rows. Whether v1 needs any of these is the user's call.
- **Analytics Agent:** ADR-0010 defers it to v1.1 (user-requested).
- **Snowflake, BigQuery and Redshift Warehouses or a managed cloud service** are named in the
  README as not a fit yet. The vision fixes Warehouses to Postgres or DuckDB and keeps adops
  self-hosted. Going after those ELT/ETL searches would need REVISE-VISION first, not a reordering.
- This tail is re-derived on every run and gets sharper as horizons land.
