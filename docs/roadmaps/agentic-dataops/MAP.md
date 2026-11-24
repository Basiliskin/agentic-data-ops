# Project map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an open-source CLI that scaffolds a Project whose YAML Pipelines (Sync, dbt Transform, Reverse Sync with Preview) run on Postgres or DuckDB across dev/stage/prod and Branch Environments. The user's own Assistant operates it through an MCP server and six skills, and Promotion runs through GitHub Actions. A SQL-literate person with no Git or Docker knowledge should be able to use it.

**As of:** 2026-11-23 · **Ordering:** work that unblocks the most remaining work comes first. Ties go to the item that answers an open question, then to the earliest thing a user can see. · **User goal text:** none given.

This file is regenerated on demand. It is a view, not a plan. Only **Done** is fact. Nothing is in flight and no next horizon is briefed: `next-horizon-brief.md` is the horizon-14 brief and is out of date. Everything under candidates is a best guess from deferred work and open questions, and no candidate has been planned.

## Where the project stands

All 15 horizons are complete (87 phases), and nothing is in flight. Horizon 15 shipped the six Assistant skills with `adops init`: setup, build, test-in-dev, share, Breaking Change and promote. They check prerequisites, do the Git work themselves and explain each step. An offline test replays their Git steps against a local repository. What remains for v1 is mostly proof and coverage, not new core features: Promotion has never run on a real runner, the plain scaffold has no dbt project, the playbooks are missing, and PyAirbyte, the Verified Source list and OTel/Grafana are untouched.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–15 | Horizons 1–15 | See "Done" below | — | done (fact) |
| 16 | Promotion proven on a real self-hosted runner | The generated PR, merge→stage and release→prod workflows run for real on a Linux runner next to a Server. Evidence settles the checkout mount, the Dagster reload, `.adops/` file ownership and the prod approval gate | Five h08 questions (real runner + checkout mount, Dagster reload, root-owned `.adops/` files, first user feedback, prod gate without GitHub Enterprise). The h07 run-slot queuing question. The h13 question of whether a PR's record gets exercised in its Branch. The live release→prod run deferred in h08. The playbooks (#19) | candidate · likely. Needs a real Linux host and a GitHub repo with runner rights, set up by hand |
| 17 | Closing the Assistant and dbt secret-safety gaps | The Assistant can't point dev at stage/prod data through connection config under `dbt/`. Tests settle which dbt outputs can echo profile values or the host. A decision on long dev/Branch runs against Claude Code's MCP tool-call timeout | h09 `profiles.yml` under `dbt/`. h02 dbt outputs echoing secrets. h09 tool-call timeout. The long-run handling deferred in h09–h11 | candidate · possible. The timeout part needs a person to try the shipped skills, because there is no automated LLM evaluation (vision item 12) |
| 18 | A dbt project in the plain `init` template, with correct dbt source-test reporting | Plain `adops init` gives a working dbt project that follows the raw-schema convention, so Branch Environments work. dbt source tests are reported under their table | h03 source-test naming question. Raw-schema parameters in the plain template (h06). The skills working on plain Projects (h15). Possible `sources.yml` generation (h02–h04) | candidate · possible. No outside dependency |
| 19 | Playbooks for Environments/Promotion and single-VM production | Written guidance for setting up stage/prod, Branch Environments, Promotion, the runner/fork/prod gate and a single-VM Server, plus the h12 notes on upgrading an existing Project | Vision item 11. The playbook deferred in h01, h02, h07, h08 and h15. The h12 manual `server/dagster.yaml` upgrade path. The h08 branch-name collision notes | candidate · possible. Should follow #16 so it describes a proven setup |
| 20 | Docker-free Community Sources through PyAirbyte | A Pipeline can Sync from a Docker-free Community Source behind the same Sync port, with the same Checks, Breaking Change detection and sanitized errors | The PyAirbyte part of the vision objective (deferred in h01–h03). Community Source test-connection (h11) | candidate · speculative. Memory never names which Source to prove it on |
| 21 | The v1 Verified Source list and its E2E coverage | A user-decided list of Verified Sources v1 ships, each passing the Docker E2E suite within the CI budget | Vision item 10. The CSV/Parquet and CDC paths deferred in h01. The h01 question about E2E job length and stability | candidate · speculative. Mostly exists to force the decision on the list |
| 22 | OpenTelemetry tracing and the optional Grafana compose profile | An optional Grafana/OTel profile in `docker compose`, with tests that no span holds a secret | Vision item 2 and the span part of item 6. Observability deferred in h01–h04. Log shipping deferred in h11 | candidate · speculative. Nothing else depends on it |

**Why this order.** Proving Promotion on a real runner answers the most open questions (five from h08, one from h07, one from h13), and the playbooks depend on it. The safety gaps answer three open questions that touch vision items 6 and 7. They must not widen the MCP allow-list or let `edit_file` reach `adops.yaml` (decisions from h9 and h10). The plain dbt template answers one open question and needs nothing external. The playbooks close a vision item but nothing depends on them. PyAirbyte, the Verified Source list and OTel are vision requirements with little leverage and no design yet. No candidate contradicts a binding decision.

**Correction to the ordering subagent:** it proposed adding automatic Branch Environment removal on PR merge/close to #16 for vision item 5. The generated `adops-pull-request.yml` already runs `adops drop-branch "$BRANCH"` on PR `closed`, so item 5 is met and #16 leaves that part out.

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
- Horizon 15 — six Assistant skills shipped by `adops init` (Claude Code only), with structural checks and an offline Git replay test (5)

## Open questions blocking the path

- **Gate #16:** do the workflows work on a real self-hosted runner with the PR checkout mounted? Does the Dagster reload pick up Pipeline and Schedule changes? Do root-owned `.adops/` files break checkout? Is "only writers can publish releases" an acceptable prod gate? What did first user feedback on Promotion say? (all h08) Do runs started right after a version is accepted queue behind scheduled runs (h07)? How often is a PR's record exercised in its Branch before merge (h13)?
- **Gate #17:** can `dbt/` hold a `profiles.yml` that points dev at stage/prod data (h09)? Which dbt outputs can echo profile values or the host (h02)? What is Claude Code's real MCP tool-call timeout against long runs (h09)?
- **Gate #18:** dbt source tests are reported under the source name, not the table (h03).
- **Gate #21:** how long and how stable is the Docker E2E job as Sources are added (h01)? Which Verified Sources must v1 ship? That one is a user decision and isn't recorded yet.

About 13 older `blockers.md` lines are answered by later decisions but not yet marked resolved: the h1 Environment-model and `sources.yml` questions, and the h2 dbtRunner, DuckDB lock, dlt error type, type mapping, E2E budget, NOT NULL and naming questions. Marking them resolved is a PLAN job.

## Beyond current visibility

Nothing was cut by the 7-candidate cap. #16–#19 rest on recorded open questions and deferred work. #20–#22 rest mainly on the vision text: PyAirbyte's design hasn't been explored, and the Verified Source list is undecided. The tail is re-derived on every MAP run and gets sharper as horizons land.
