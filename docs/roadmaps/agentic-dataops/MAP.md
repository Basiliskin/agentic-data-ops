# Map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, a self-hosted Python CLI/library whose YAML Pipelines (dlt/PyAirbyte Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev/stage/prod and Branch Environments, operated by the user's own Assistant through an MCP server and six skills, with GitOps Promotion, so a SQL-literate user needs no Git or Docker knowledge.

**As of:** 2026-11-21 · **Ordering:** whatever unblocks the most remaining work comes first; ties go to whatever closes an open question · **User goal:** none given, so the vision is the whole goal.

> This page is regenerated from project memory on every `map` run. It is a view, not a plan. Only **Done** is fact. **Candidates** are guesses about what remains, not planned work.

## Where the project stands

All ten planning horizons are finished, and every phase is done in its ledger. Nothing is in flight. The demo runs end to end on both Warehouses with Checks, Breaking Changes, dbt, Reverse Sync with Preview, Branch Environments, a Server running Schedules and GitHub Actions Promotion. The Assistant's MCP server has nine tools.

What remains before v1: the last two MCP tools (run logs/read-logs and test-connection), the six skills, a full non-demo `adops init`, carrying accepted schema changes through Git, Promotion hardening with the production playbooks, Community Sources and optional tracing.

There is no brief for the next horizon: the `next-horizon-brief.md` on disk was written for horizon 10, which is now finished.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1 | Demo shop loads end to end | dlt Sync, built-in Checks, `adops run`/`validate`, Docker E2E in CI | everything after | done (7 phases) |
| 2 | Breaking Changes and declared Checks | schema snapshots, `accept-schema`, not_null/accepted_values/min_rows/freshness | 3, 6 | done (6) |
| 3 | dbt Transform and dedicated errors | dbt models + tests as Checks, fixed unreachable/empty/load-failed errors | 4, 5 | done (6) |
| 4 | DuckDB Warehouse | same Pipelines on DuckDB, E2E on both kinds | 5, 6 | done (6) |
| 5 | Reverse Sync and Preview | writes only in stage/prod, Preview elsewhere, stage Environment | 6, 9 | done (7) |
| 6 | Branch Environments | `run --branch` deferring to stage by Step Fingerprint, `drop-branch` | 8, 9 | done (7) |
| 7 | Server runs Schedules | Schedules, run rules, Server profile, webhook alerts | 8 | done (7) |
| 8 | GitHub Actions Promotion | `run --all`, Warehouse lock, PR/stage/prod workflows, promotion playbook | 9 | done (6) |
| 9 | Assistant MCP server | `adops mcp` over stdio: run, preview, validate; dev/branch only; `.mcp.json` | 10 | done (5) |
| 10 | MCP project and file tools | drop_branch, accept_schema (dev), project_view, list/read/edit files | 11 | done (5) |
| 11 | Finish the MCP tool set: run logs, read-logs, test-connection | every run writes a secret-free log the Assistant can read; connections checked with fixed messages; closes vision item 7 | the six skills; run-log identity question; test-connection-for-DuckDB/branch question; template `.gitignore` for init; log secret-safety tests | candidate — likely |
| 12 | Accept a Breaking Change across Environments (ADR-0015 records, branch acceptance) | an accepted schema change travels through Git into stage and prod; a PR branch's Breaking Change has an unattended path | four open record/applied/drift/PR-branch questions; the `accept-schema --branch` gap; a schema-change flow the skills can narrate | candidate — possible (record format needs a decision first) |
| 13 | The six SKILL.md skills | the Assistant checks prerequisites, does Git itself and narrates plainly (vision item 8) | full `adops init`; real MCP timeout evidence; first user feedback on Promotion | candidate — possible |
| 14 | Full non-demo `adops init` scaffold | plain `init` gives a complete Project: pipelines, dbt project, settings, compose, `.env.example`, skills, workflows, `.mcp.json` (vision item 1) | single-VM playbook; sources.yml generation if pursued | candidate — possible |
| 15 | Server and Promotion hardening, then production playbooks | workflows proven on a real runner, Dagster reload, `.adops/` ownership, prod approval gate, stale STARTED runs cleared; Environments/Promotion and single-VM playbooks (vision item 11) | five open runner/reload/ownership/prod-gate/STARTED questions; abandoned `br_` schema cleanup | candidate — speculative (needs live infrastructure and user feedback) |
| 16 | Source breadth: PyAirbyte Community Sources, every Verified Source under E2E | Docker-free Community Sources, CSV/Parquet Source, the dbt source-test name fix (vision items 2, 10) | CSV/Parquet Source; dbt source-test question; E2E duration question | candidate — speculative |
| 17 | OpenTelemetry tracing and optional Grafana profile | traced runs with no secret in any span (vision items 2, 6) | span half of the secret-safety criterion | candidate — speculative |

## Done

- Horizon 1: 7 phases (demo shop loads end to end; 1 replan, 1 amendment)
- Horizon 2: 6 phases (Breaking Changes, declared Checks; 2 amendments)
- Horizon 3: 6 phases (dbt Transform, dedicated errors; 1 amendment)
- Horizon 4: 6 phases (DuckDB Warehouse; 1 amendment)
- Horizon 5: 7 phases (Reverse Sync and Preview; 1 amendment)
- Horizon 6: 7 phases (Branch Environments; 1 amendment)
- Horizon 7: 7 phases (Server runs Schedules)
- Horizon 8: 6 phases (GitHub Actions Promotion; 2 amendments)
- Horizon 9: 5 phases (Assistant MCP server)
- Horizon 10: 5 phases (MCP project and file tools)

## Open questions blocking the path

- **Gates #11:** What is a run log identified by (timestamp, run id, Pipeline), how long is it kept, and does a run that fails before a result exists write one?
- **Gates #11:** What does test-connection mean for a DuckDB Warehouse, and for a Branch Environment that opens stage's Warehouse and targets?
- **Gates #11, #13:** How does Claude Code's real MCP tool-call timeout compare with a long dev or branch run?
- **Gates #12:** What is in an accepted-change record: the full accepted shape, or "accept whatever the Source shows next run", given prod's Source can differ from dev's?
- **Gates #12:** How does an Environment know it already applied a record, and what happens if the Source drifted again?
- **Gates #12:** How is a Breaking Change in a PR branch handled unattended?
- **Gates #15:** Do the generated stage/prod workflows work on a real self-hosted runner, and does the Dagster code-location reload pick up changes?
- **Gates #15:** Do root-owned files in `.adops/` break checkout or later runs as the runner user?
- **Gates #15:** Without GitHub Enterprise, is "only writers can publish releases" an acceptable prod gate?
- **Gates #15:** After a Server crash, how are runs left STARTED cleared: run monitoring, a startup sweep, or both?
- **Gates #15:** What did the first user feedback on Promotion say?
- **Gates #16:** DbtTransform reports a dbt source test under the source name, not its table. This needs a fix before users write source tests.
- **Gates #16:** Will a growing Docker E2E suite stay inside the fixed 20-minute CI timeout?
- **Already answered, not marked resolved:** these horizon-9 entries in `blockers.md` were settled in horizon 10:
  - drop-branch fails fast on a held lock
  - accept-schema is dev only, with no lock
  - read_file blanks out secret values
  - plain Pipeline names are required
  - `dbt/profiles.yml` cannot change a connection, because dbt runs with a temporary profiles dir

  Older horizon 2–3 questions about dbt and dlt behaviour are also likely answered by the code that shipped. The next PLAN run should close all of these.

## Beyond current visibility

Past #14 the order is a guess. Promotion hardening and the playbooks depend on live runner use and user feedback that has not happened yet. "Every Verified Source passes E2E" has no defined list of v1 Verified Sources beyond the demo Postgres and the deferred CSV/Parquet Source. The tail is re-derived on every run and gets sharper as horizons land.
