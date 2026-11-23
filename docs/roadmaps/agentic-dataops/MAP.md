# Project map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an open-source CLI that scaffolds a Project whose YAML Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev/stage/prod and Branch Environments. The user's own Assistant operates it through an MCP server and six skills, and Promotion runs through GitHub Actions. A SQL-literate person with no Git or Docker knowledge should be able to use it.

**As of:** 2026-11-23 · **Ordering:** work that unblocks the most remaining work comes first. Ties go to the item that answers an open question, then to the earliest thing a user can see. · **User goal text:** none given.

This file is regenerated on demand. It is a view, not a plan. Only **Done** is fact. Nothing is in flight and no next horizon is briefed. Everything under candidates is a best guess from deferred work and open questions, and no candidate has been planned.

## Where the project stands

All 14 horizons are complete (82 phases), and nothing is in flight. Horizon 14 closed the Breaking Change loop. In dev, `accept-schema` now writes the accepted-change record you commit. With any other `--env` it only reads and writes the record. `--branch` hand-accepts a Branch Environment under stage's lock, and the Assistant's tool returns the record's path. About five gaps to v1 remain: the six Assistant skills, Promotion proven on a real runner, a dbt project in the plain scaffold, the playbooks, PyAirbyte, and observability. One more is still unclear: which Verified Sources v1 must ship.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–14 | Horizons 1–14 | See "Done" below | — | done (fact) |
| 15 | The six Assistant skills shipped by `adops init` | Ask the Assistant to build, test, Preview and promote Pipelines, and to handle a Breaking Change end to end. The skills check prerequisites, do the Git work (branch, commit, open the PR carrying a record) and explain each step plainly | Vision items 8 and the skills half of 1. The deferred Git and PR work for records (h13, h14). Skills using read-logs/test-connection (h11). Real use that answers the MCP tool-call timeout question (h09), whether a PR's record gets exercised in its Branch (h13), and first user feedback on Promotion (h08) | candidate · likely. h14 recorded it as the user-confirmed next horizon |
| 16 | Promotion proven on a real self-hosted runner | Trust that the generated merge→stage and release→prod workflows really deploy on a Linux runner and Server | Five open questions: real runner with the PR checkout mount (h08), Dagster reload picking up changes (h08), root-owned `.adops/` files (h08), the prod gate without GitHub Enterprise (h08), runs queuing after a version is accepted (h07). Also vision item 9 (checked only by parsing YAML so far) and the playbook | candidate · possible |
| 17 | A dbt project in the plain (non-demo) Project template | Plain `adops init` gives a working dbt project set up for Branch Environments, and dbt source tests are reported under the right table | Vision item 1 for plain Projects (h01). Raw-schema variables in the plain template (h06). The open h03 source-test naming question | candidate · possible |
| 18 | Closing the remaining Assistant safety gaps | Files under `dbt/` can no longer point dev at stage/prod data, and long dev/Branch runs don't fail on an MCP tool-call timeout | Open h09 questions: `profiles.yml` under `dbt/`, real tool-call timeout. Deferred long-run handling (h09, h10, h11) | candidate · speculative. The timeout half needs #15's real use first |
| 19 | Playbooks for Environments/Promotion and single-VM production | Written steps for setting up stage/prod, Promotion and a single-VM production Server | Vision item 11. The full single-VM playbook deferred in h07/h08 | candidate · speculative. Should follow #16 so it describes a validated setup |
| 20 | Docker-free Community Sources through PyAirbyte | Sync from a Docker-free Community Source with the same Pipeline YAML | The PyAirbyte half of the vision objective (h01–h03). Community Source test-connection (h11) | candidate · speculative |
| 21 | OpenTelemetry tracing and the optional Grafana compose profile | Optional run traces in Grafana, with tests that no span holds a secret | Vision item 2 (Grafana/OTel profile) and the span half of item 6. Log shipping deferred in h11 | candidate · speculative. Nothing else waits on it |

**Why this order.** The skills come first because the user confirmed them and because every Git and PR item lands there: the decision to keep Git out of the MCP server deferred all of that work to the skills. Validation on a real runner retires the biggest group of open questions (five, from h07 and h08), and the production playbook depends on it. The plain dbt template closes a vision-1 gap and one open question. The safety gaps follow the skills because the timeout half needs real use. Playbooks, PyAirbyte and OTel are vision requirements that nothing else depends on. No candidate contradicts a binding decision. The skills do the Git work while the MCP server stays Git-free and limited to dev/Branch (h09, ADR-0009). Acceptance stays a committed record or a hand-run command, never automatic (h13).

## Done

- Horizon 1 — demo shop loads end to end via `adops run` (7 phases)
- Horizon 2 — Breaking Changes + accept-schema, user-declared Checks (6)
- Horizon 3 — dbt Transform Step and dedicated Sync errors (6)
- Horizon 4 — DuckDB Warehouse runs the demo (6)
- Horizon 5 — Reverse Sync + Preview, stage Environment (7)
- Horizon 6 — Branch Environments, drop-branch (7)
- Horizon 7 — scheduling Server: Schedules, run rules, webhook alerts (7)
- Horizon 8 — GitHub Actions Promotion, Warehouse lock, run --all, ADR-0016 (6)
- Horizon 9 — Assistant MCP server: run, preview, validate (5)
- Horizon 10 — MCP drop-branch, accept-schema (dev), project view, file tools (5)
- Horizon 11 — Run Logs, read-logs, test-connection (5)
- Horizon 12 — Server recovers from runs a crash left running (3)
- Horizon 13 — accepted-change records applied in stage/prod/Branch, applied-records table, checked by validate (5)
- Horizon 14 — accept-schema writes the record in dev, read-only record-only for other `--env`, CLI `--branch` under stage's lock, MCP `record_path`, demo E2E uses the dev record (7)

## Open questions blocking the path

- **Gate #15:** Claude Code's real MCP tool-call timeout against long dev/Branch runs (h09). How often will a PR's record actually be exercised in its Branch before merge (h13)? What did first user feedback on Promotion say (h08)? Real use through the skills answers all three.
- **Gate #16:** do the workflows work on a real self-hosted runner? Does the Dagster reload pick up Pipeline and Schedule changes? Do root-owned `.adops/` files break checkout? Is "only writers can publish releases" an acceptable prod gate? (all h08) Do runs started right after a version is accepted queue behind scheduled runs (h07)?
- **Gate #17:** dbt source tests are reported under the source name, not the table (h03).
- **Gate #18:** can `dbt/` hold a `profiles.yml` that points dev at stage/prod data (h09)?

About 13 older blockers.md lines are answered by later decisions but not yet marked resolved: the h1 Environment-model and sources.yml questions; the h2 dbtRunner, DuckDB lock, dlt error, type-mapping, E2E-budget and NOT NULL questions; and the h8/h9 MCP questions answered in h9/h10. The horizon-13 brief's unknowns were all settled in h14. Marking these resolved is a PLAN job.

## Beyond current visibility

Nothing was cut by the 7-candidate cap. Past #17 the tail is vague, and #19–#21 come from the vision text alone. One vision item can't be mapped yet. Item 10 says "every Verified Source passes the E2E suite", but memory never lists which Verified Sources v1 must ship beyond the demo Postgres, and a CSV/Parquet Source was deferred as demo-only. That list is a user decision to make before the item can become a candidate. The tail is re-derived on every MAP run and gets sharper as horizons land.
