# Project map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an open-source CLI that scaffolds a Project whose YAML Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev/stage/prod and Branch Environments. The user's own Assistant operates it through an MCP server and six skills, and Promotion runs through GitHub Actions. A SQL-literate person with no Git or Docker knowledge should be able to use it.

**As of:** 2026-11-22 · **Ordering:** work that unblocks the most remaining work comes first. Ties go to the item that answers an open question, then to the earliest thing a user can see. · **User goal text:** none given.

This file is regenerated on demand. It is a view, not a plan. Only **Done** is fact. **Next** is a briefed scope, and everything under candidates is a best guess from deferred work and open questions. No candidate has been planned.

## Where the project stands

All 13 horizons are complete (75 phases), and nothing is in flight. Horizon 13 added accepted-change records: a committed `pipelines/accepted/<id>.yaml` file lets stage, prod and Branch runs get past an intended Breaking Change once, and `adops validate` checks those files. The briefed next step finishes that loop: `accept-schema` writes the record for you, and gets `--branch`. After that, the biggest gaps to v1 are the six Assistant skills, Promotion proven on a real runner, the plain scaffold, PyAirbyte, observability and the playbooks.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–13 | Horizons 1–13 | See "Done" below | — | done (fact) |
| 14 | accept-schema writes the record file, `--env stage\|prod` becomes read-only and only writes the record, CLI-only `accept-schema --branch`, demo E2E extension, ADR-0015/0014 text | Accept a Breaking Change in dev with one command that writes the record to commit. stage/prod are never written by hand. A Branch Breaking Change can be accepted by hand | Closes the h13 gap where `--env stage\|prod` still writes those Warehouses, and the h06 `--branch` gap. Fixes the record format the Breaking Change skill will commit. Owns the 5 open h13 questions | next (briefed, not planned) |
| 15 | The six Assistant skills shipped by `adops init` | Ask the Assistant to change, check, run, Preview, accept a Breaking Change and promote. The skills check prerequisites, do the Git work and explain each step plainly | Vision item 8 and the skills half of item 1. The deferred "Assistant opens the PR carrying a record" (h13). Skills using read-logs/test-connection (h11). Answers the h09 question about the MCP tool-call timeout and enables the h08 first-user-feedback question | candidate · likely. Needs #14's record format |
| 16 | Promotion proven on a real self-hosted runner, plus automatic drop of a PR's Branch Environment on merge/close | Trust that the generated workflows really deploy on a Linux runner, and branch schemas go away with no manual drop-branch | Four open h08 questions: real runner, Dagster reload picking up changes, root-owned `.adops/` files, prod gate without GitHub Enterprise. Also h12 root-owned file cleanup, h08 live workflow E2E, and the drop-on-merge part of vision items 5 and 9 | candidate · possible. Swapping with #15 is defensible |
| 17 | Complete plain (non-demo) `adops init` Project | A real Project gets a dbt project, a stage Environment, the skills and working Promotion workflows out of the box | Vision item 1 for plain Projects (h01). Raw-schema parameterizing in the plain template (h06) | candidate · possible. Should follow #15 so it can ship the skills |
| 18 | Validation and safety hardening of authored files | `adops validate` catches a Pipeline whose name differs from its file name, colliding Branch names, and dbt-side connection config that could point dev at stage/prod. dbt source tests are reported against the right table | The one unlanded h13 discovery (name vs file name). Open h09 question (profiles.yml in `dbt/`). Open h03 question (source tests). h08 short-branch-name collisions | candidate · possible. More urgent once skills let users edit files freely |
| 19 | PyAirbyte adapter for Docker-free Community Sources | Sync from a Community Source with the same Pipeline YAML | Community Source test-connection (h11), Community Source E2E (h01), and the PyAirbyte half of the vision objective | candidate · speculative |
| 20 | OpenTelemetry tracing and the optional Grafana compose profile | Optional run traces in Grafana, with tests that no span holds a secret | Vision items 2 and 6 (the span half), and the playbook's monitoring section | candidate · speculative |
| 21 | Environments/Promotion and single-VM production playbooks | Written guides for setting up Environments and Promotion and running on one VM | Closes vision item 11 | candidate · speculative. Comes after #16 and #20 so the guides describe finished behaviour |

**Why this order.** Once #14 fixes the record format, the skills have the most deferred work waiting on them (from h09, h10, h11 and h13). They are also the only way to answer the tool-call timeout question and to get real user feedback. The real-runner work retires more open questions (four from h08), but it needs real infrastructure and gets clearer once users drive it through the skills. No binding decision depends on the order between them. The skills do the Git work while the MCP server stays Git-free and limited to dev/Branch (h09, ADR-0009). Acceptance stays a committed record or a hand-run command, never automatic (h13). Automatic drop-on-merge reverses nothing: h06 only kept the drop manual for that horizon. A later PLAN run should confirm that reading.

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

## Open questions blocking the path

- **Gate #14** (owned by its brief): is the stage/prod preflight truly read-only? Are record ids unique enough? Should accept-schema skip writing when an unapplied record already covers the changes? Will a PR's record actually be exercised in its Branch before merge? Is the parenthesised change wording settled? Does the 20-minute E2E budget still fit?
- **Gate #15:** Claude Code's real MCP tool-call timeout against long dev/Branch runs (h09). Only real use answers it.
- **Gate #16:** do the workflows work on a real self-hosted runner? Does the Dagster reload pick up Pipeline and Schedule changes? Do root-owned `.adops/` files break checkout? Is "only writers can publish releases" an acceptable prod gate? What did first user feedback say? (all h08)
- **Gate #18:** can `dbt/` hold a profiles.yml that points dev at stage/prod data (h09)? dbt source tests are reported under the source name, not the table (h03).

About 16 older blockers.md lines are answered by later decisions but not yet marked resolved. Examples are the h02 dbt/DuckDB/dlt questions, the single adops.yaml Environment model, the h07 run-slot question, and the h09 MCP questions answered in h10/h11. Marking them resolved is a PLAN job.

## Beyond current visibility

Nothing was cut by the 7-candidate cap. Past #16 the tail is vague. #19–#21 come from the vision text alone, and no horizon has explored them. What real-runner testing and first user feedback turn up could reshape them. "Every Verified Source passes the E2E suite" (vision item 10) is met only for the Postgres Source today. It is unclear whether v1 needs more Verified Sources. A CSV/Parquet Source was deferred as demo-only. The tail is re-derived on every MAP run and gets sharper as horizons land.
