# Project map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an open-source CLI that scaffolds a Project whose YAML Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev/stage/prod and Branch Environments. The user's own Assistant operates it through an MCP server and six skills, and Promotion runs through GitHub Actions. A SQL-literate person with no Git or Docker knowledge should be able to use it.

**As of:** 2026-11-22 · **Ordering:** work that unblocks the most remaining work comes first. Ties go to the item that answers an open question, then to the earliest thing a user can see. · **User goal text:** none given.

This file is regenerated on demand. It is a view, not a plan. Only **Done** is fact. Everything under candidates is a best guess from deferred work and open questions, and no candidate has been planned.

## Where the project stands

All 12 horizons are complete (70 phases), and nothing is in flight. Shipped so far: the demo loads end to end; Checks and Breaking Changes; dbt; DuckDB; Reverse Sync with Preview; Branch Environments; the scheduling Server; GitHub Actions Promotion; the full MCP tool set; Run Logs and connection checks; and Server crash recovery. `next-horizon-brief.md` is stale (it briefs horizon 10, which is done). An unfinished planning draft for the next horizon has picked the six Assistant skills. The rest of the gap to v1 is at the edges: accepting Breaking Changes in stage/prod through Git, the skills, proving Promotion on a real Server, the plain scaffold, PyAirbyte, observability and the playbooks.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–12 | Horizons 1–12 | See "Done" below | — | done (fact) |
| 13 | Breaking Changes in stage/prod accepted through Git (ADR-0015 accepted-change records) | Accept an intended Breaking Change once, commit it, and have stage then prod apply it on Promotion, with no hand-run command on the Server | Deferred from h07, h08, h09, h10, h11 and h12. Answers 5 open questions: what a record holds, how an Environment knows it applied one, whether records are removed, a Source that drifted again, and an unattended Breaking Change in a PR branch. Also lets the Breaking Change skill cover stage/prod | candidate · likely |
| 14 | The six Assistant skills written by `adops init` / `init --demo` | The user asks the Assistant to change, check, run, Preview and promote. The skills check prerequisites, do Git themselves and explain in plain words | Vision item 8 and the skills half of vision item 1. Lets the real MCP tool-call timeout be seen in use (h09 open question) | candidate · likely. **This is what the unfinished horizon-13 draft picked.** Five skills can ship now. The Breaking Change skill can only use dev-only accept_schema until #13 lands |
| 15 | Promotion proven on a real self-hosted runner, plus dropping a PR's Branch Environment on merge/close | Trust that the generated workflows really deploy on a Linux runner, and branch schemas no longer need a manual drop | Four open h08 questions: does it work on a real runner, does the Dagster reload pick up changes, do root-owned `.adops/` files cause trouble, is the prod gate acceptable without GitHub Enterprise. Also the "drop on merge/close" part of vision item 5 and h08's first-user-feedback question | candidate · possible |
| 16 | Complete plain (non-demo) `adops init` layout | A real Project gets a dbt project, a stage Environment and working Promotion workflows out of the box | Vision item 1 for plain Projects. Fixes the h08 finding that the stage workflow fails on a plain Project, which has no stage | candidate · possible |
| 17 | PyAirbyte adapter for Docker-free Community Sources | Sync from a Community Source with the same Pipeline YAML | Community Source test-connection (h11) and Community Source E2E (h01) | candidate · speculative |
| 18 | OpenTelemetry tracing and the optional Grafana compose profile | Optional run traces in Grafana with no secret in any span | Vision items 2 and 6 (the span half), and the playbook's monitoring section | candidate · speculative |
| 19 | Environments/Promotion and single-VM production playbooks | Written guides for setting up Environments and Promotion and running on one VM | Closes vision item 11. Should come after #13, #15 and #18 so the guides describe finished behaviour | candidate · speculative |

**#13 versus #14.** Ranked by what it unblocks, the Git-acceptance work comes first: it answers more open questions and decides the Breaking Change skill's format. The skills are a close second and are the earliest thing a user can see, which is why the draft picked them. Doing the skills first is fine, but the Breaking Change skill will then need a second pass once #13 lands. No binding decision is broken either way. The Assistant can never accept a change for stage/prod itself (ADR-0009 and the h09 allow-list), and acceptance is never automatic (h02).

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

## Open questions blocking the path

- What an accepted-change record holds, how an Environment knows it applied one, whether records are removed, and what happens if the Source drifted again (blockers.md h07, h08). These gate **#13**.
- How a Breaking Change in a PR branch is handled unattended (blockers.md h06). This gates **#13**.
- Claude Code's real MCP tool-call timeout against long dev/Branch runs (blockers.md h09). Only real use answers it, so it waits on **#14**.
- Whether the generated workflows work on a real self-hosted runner, whether the Dagster reload picks up Pipeline and Schedule changes, whether root-owned `.adops/` files break checkout, whether "only writers can publish releases" is an acceptable prod gate, and what first user feedback said (blockers.md h08). These gate **#15**.
- Not tied to any candidate yet, so they belong to a future PLAN run:
  - Can `dbt/` hold a profiles.yml that points dev edits at stage/prod data (h09)?
  - Is a dropped NOT NULL in the Source relaxed in the Warehouse, or reported as a Breaking Change (h02)?
  - dbt source tests are reported under the source name, not the table (h03).
  - How long and how stable is the Docker E2E job as Steps are added (h01)?

Many older blockers.md lines are settled by later horizons but not yet marked resolved. They include the h02 dbt/DuckDB/dlt questions, the single adops.yaml Environment model, the h07 run-slot question, and the h09 MCP questions answered by h10/h11 decisions. Marking them resolved is a PLAN job.

## Beyond current visibility

Nothing was cut by the 7-candidate cap. "Every Verified Source passes the E2E suite" (vision item 10) seems met only for the Postgres Source today. A CSV/Parquet file Source was deferred as demo-only and is not ranked. Past #15, confidence drops quickly: what #13's record means and what real-runner testing in #15 turns up could both reshape the tail. The tail is re-derived on every MAP run and gets sharper as horizons land.
