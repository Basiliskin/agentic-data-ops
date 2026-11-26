# Map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an Apache-2.0 CLI that scaffolds a Project whose YAML
Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev,
stage, prod and Branch Environments, operated by the user's own Assistant through MCP and six Skills,
with Promotion through GitHub Actions.

**As of:** 2026-11-26 · **Ordering:** whatever unblocks the most remaining work comes first; ties go
to whatever closes an open question. · **User goal:** none given; the vision is the whole goal.

This file is regenerated on every run. It is a view, not a plan. Only the "Done" rows are fact; every
row marked `candidate` is a current best guess.

## Where the project stands

All 16 horizons are complete; nothing is in flight and nothing is briefed as next. The core is
built: Sync, dbt Transform, Reverse Sync with Preview, Postgres and DuckDB Warehouses, Branch
Environments, Schedules on a Server, crash recovery, Breaking Changes accepted through Git, the MCP
server, six Skills, and stage and prod running locally on a Mac. What remains for v1 is proof that the
GitHub Actions Promotion works on a real runner, a full plain (non-demo) scaffold, production
playbooks, and three smaller gaps (Assistant safety edges, OTel/Grafana, PyAirbyte).

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–16 | Horizons 1–16 | Everything listed under Done below | — | Done (fact) |
| 17 | Promotion proven on a real self-hosted runner | PR → Branch run, merge → stage, release → prod actually run on the user's own Server machine | 4 open horizon-8 questions (real runner, Dagster reload, root-owned `.adops/` files, prod gate without Enterprise), the horizon-7 run-slot question, the horizon-13 record-in-Branch question, the live release → prod run, automatic Branch drop on PR close, the single-VM playbook. **2026-11-26 trial: PR → Branch run, merge → stage and drop on close passed on a Mac runner; reload-name bug fixed; release → prod still open** | candidate · likely |
| 18 | Deploys never change files under a run in progress | A stage/prod deploy no longer runs `git checkout` in the Server folder while a scheduled run is going (a Transform could pick up new dbt models halfway through an old run): deploy into a new folder and switch, or wait for the Server to be idle; a deploy reruns only Changed Steps instead of `run --all` | Safe Promotion with real data volumes; the "full reload on every merge" cost; finding 2.1 of the 2026-11-26 production-readiness review | candidate · likely (after 17) · user-requested |
| 19 | Freshness alerts | Each Pipeline table can declare how stale it may get (e.g. `fresh_within: 26h`); the Server alerts when a table has not loaded in time, including when a Schedule silently stops or is paused, and sends a recovery notice | The most common real data incident goes unnoticed today; only failed runs alert | candidate · likely · user-requested |
| 20 | More Verified Sources plus CDC Sync | MySQL and SQL Server Sources (and at least one SaaS API such as Stripe) through dlt as Verified Sources with E2E tests, and CDC Sync (defined in GLOSSARY.md, not built) so deleted rows are seen | adops useful beyond Postgres shops; Incremental Sync's blind spot for deletes; vision criterion 10 meaning more than "Postgres only" | candidate · likely · user-requested |
| 21 | Run metrics, OpenTelemetry and an optional Grafana profile | Rows loaded, duration and Check results per run kept over time; volume-anomaly Checks; an optional compose profile that shows runs as traces and dashboards, with tests that no span carries a secret | Vision criteria 2 (OTel/Grafana profile) and 6 (no secret in spans), log shipping, seeing a 40% drop without a hand-written Check | candidate · possible · user-requested (merges the earlier OTel/Grafana candidate) |
| 22 | Read-only query tool for the Assistant | An MCP tool that runs a row-limited `select` in dev or a Branch Environment only, so "show me the top customers" works; refuses stage/prod in server code like the other tools | The README Assistant example can show its result (2026-11-24 try); the query-tool discovery entry | candidate · possible · user-requested |
| 23 | Full plain Project scaffold | `adops init` without `--demo` gives `pipelines/`, a dbt project and `.env.example`, so a user can write a first Pipeline without copying the demo | Vision criterion 1, raw-schema parameterizing for Branch rebuilds in the plain template, the horizon-1 dbt raw-table/`sources.yml` question, the horizon-3 source-test naming bug, Skills that assume a `dbt/` exists | candidate · likely |
| 24 | Close the Assistant's dev-redirect and long-run gaps | The Assistant can't point dev at stage/prod data through config inside `dbt/`, and long dev/Branch runs don't silently hit the tool-call timeout | Horizon-9 `dbt/profiles.yml` redirect question, horizon-9 MCP timeout question, background runs or progress for long runs | candidate · possible |
| 25 | Single-VM production and Environments/Promotion playbooks | Written guides to run stage and prod on one Linux VM with Promotion end to end | Vision criterion 11, the horizon-8 "first user feedback on Promotion" question, a playbook line for existing Projects to turn on stale-run recovery | candidate · possible (after 17) |
| 26 | Docker-free Community Sources through PyAirbyte | Sync from a Docker-free Airbyte Community Source behind the same Sync port as dlt (ADR-0012) | The vision's PyAirbyte objective, test-connection for Community Sources | candidate · speculative |
| 27 | Analytics Agent | The optional built-in AI that answers business questions about Warehouse data in plain words, read-only (ADR-0010 defers it to v1.1) | The Assistant-first idea paying off for people who are not engineers | candidate · speculative · after v1 · user-requested |

Numbers 17–27 are positions on this path, not horizon numbers. A PLAN run assigns the real number.

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
- Horizon 16 — stage and prod run locally on a Mac; demo prod Environment, isolation E2E, Mac guide (3)

## Open questions blocking the path

| Question (from blockers.md) | Gates |
|---|---|
| h8: do the stage/prod workflows work on a real self-hosted runner? | 17 |
| h8: does the Dagster code-location reload pick up added/removed Pipelines and Schedules mid-run? | 17 |
| h8: do root-owned files in `.adops/` break `git checkout` or later runner-user runs? | 17 |
| h8: without GitHub Enterprise, is "only writers publish releases" an acceptable prod gate? | 17 |
| h7: do runs started right after a version is accepted queue behind scheduled runs? | 17 |
| h13: how often is a PR's accepted-change record actually exercised in its Branch before merge? | 17 |
| h8: what did first user feedback on Promotion say? | 25 |
| h1: how do dbt models reference dlt's raw tables, and does adops generate `sources.yml`? | 23 |
| h3: DbtTransform reports a dbt source test under the source name, not its table | 23 |
| h9: can `dbt/` hold a `profiles.yml` that redirects dev to stage/prod data? | 24 |
| h9: Claude Code's MCP tool-call timeout versus long dev/Branch runs | 24 |

Several older horizon-1/2 questions in blockers.md (dbtRunner reuse, DuckDB single-writer, dlt error
types, E2E timing) look settled by horizons 3–6 but were never marked `— resolved`. The next PLAN run
should close them.

## Beyond current visibility

- **Is publishing `adops` to PyPI part of v1?** The Server Dockerfile and the fork validate job
  install a pinned `adops==<version>` from the index, and 0.1.0 is not there. Memory doesn't settle
  this; it affects criterion 1 and candidate 17.
- **Vision criterion 5 says Branch Environments are "dropped on merge/close"**, but horizon 6 decided
  `drop-branch` is manual and deferred the automation. It is folded into candidate 17 here; whether
  v1 requires it is the user's call.
- Criterion 10's "every Verified Source passes the E2E suite" is met only because Postgres is the
  only Verified Source; candidate 20 (user-requested 2026-11-26) adds more Sources and CDC.
  CSV/Parquet and streaming Reverse Sync diffs are still left off.
- Candidates 18–22 and 27 came from a production-readiness review on 2026-11-26 (decisions.md).
  Still unplanned from that review: resuming a failed run from its failed Step, a backfill command,
  more than one run at a time per Server, and whether dlt restores the Incremental cursor from the
  Warehouse when the Server's `.adops/` is lost.
- This tail is re-derived on every run and gets sharper as horizons land.
