# Map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, an open-source Python CLI that scaffolds YAML Pipelines (Sync, dbt Transform, Reverse Sync with Preview) running on Postgres or DuckDB across dev/stage/prod and Branch Environments, operated by the user's own Assistant through an MCP server and six skills, with Promotion through GitHub Actions. The target user knows SQL but not Git or Docker.

**Date:** 2026-11-22 · **Ordering:** whatever unblocks the most remaining work goes first. Ties go to whatever retires an open question, then to whatever gives the earliest visible result. · **User goal:** none given, so the vision is the whole goal.

> This page is regenerated from project memory on each run. It is not a plan. Only "Done" is fact. There is no "In flight" or "Next" horizon right now. Everything under "Candidates" is a guess.

## Where the project stands

All 11 horizons are complete (68 phases). The engine works end to end: Sync, dbt Transform, Reverse Sync with Preview, both Warehouse kinds, Branch Environments, scheduled Servers, GitHub Actions Promotion, and every MCP tool the vision names. What remains is mostly the layer the target user touches (the six skills, and accepting Breaking Changes through Git), plus proof that the production setup works on a real machine.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1–11 | Horizons 1–11 | Demo end to end → Checks → dbt → DuckDB → Reverse Sync → Branch Envs → Server Schedules → Promotion → MCP server → MCP project/file tools → Run Logs + test-connection | — | **done** (fact) |
| 12 | Accepting Breaking Changes in stage and prod through a reviewed Git change (ADR-0015) | Accept an intended schema change for stage/prod by merging a recorded change; each Environment applies it on its next run | 4 open questions on the accepted-change record (contents, applied-once tracking, drift since acceptance, unattended Breaking Changes in a branch); the skills' "fix a Breaking Change" flow | next candidate · likely |
| 13 | The six Assistant skills, shipped in every scaffolded Project | The Assistant changes, tests and promotes a Pipeline, does the Git steps itself and explains them in plain words | Vision items 1 and 8; the Assistant opening PRs (Git stays in the skills, not the MCP server); skills using read-logs and test-connection; a full plain `adops init` with a dbt project; the real use the timeout question needs | candidate · likely (can overlap #12; only the Breaking Change skill waits on it) |
| 14 | Server and Promotion hardening on a real self-hosted machine | A Server recovers after a crash; stage/prod workflows shown to work on a real runner | 5 open questions: runs stuck as STARTED, real-runner checkout over `.adops/` and `.env`, Dagster code-location reload, root-owned `.adops/` files, a prod gate without Enterprise; the production playbook | candidate · possible |
| 15 | Closing the Assistant's remaining safety and long-run gaps | dev can't be pointed at stage/prod data through a dbt `profiles.yml`; long runs don't fail on the MCP tool-call timeout | 2 open questions (dbt/ connection redirect, tool-call timeout); long-run progress or background run handles | candidate · possible (the timeout half needs #13 first) |
| 16 | Single-VM production playbook | One guide to stand up a stage/prod Server on one Linux machine | Vision item 11; the Environments/Promotion playbook beyond promotion.md | candidate · possible (after #14) |
| 17 | Community Sources through PyAirbyte | Sync from a Docker-free Community connector with the same Pipeline YAML | The PyAirbyte adapter (ADR-0012); test-connection for Community Sources | candidate · speculative |
| 18 | Optional tracing and Grafana profile | An optional compose profile shows runs as traces in Grafana, with tests that no span carries a secret | Vision item 2's Grafana/OTel profile; vision item 6's "no span" clause | candidate · speculative |

Rows 12–18 are themes, not planned horizons. Their numbers only continue the sequence. A PLAN run decides what actually comes next.

## Done

- Horizon 1: demo shop loads end to end (7 phases)
- Horizon 2: Breaking Changes + accept-schema, user-declared Checks (6)
- Horizon 3: dbt Transform Step and dedicated errors (6)
- Horizon 4: DuckDB Warehouse runs the demo (6)
- Horizon 5: Reverse Sync and Preview, stage Environment (7)
- Horizon 6: Branch Environments (7)
- Horizon 7: the Server runs Schedules (7)
- Horizon 8: GitHub Actions Promotion (6)
- Horizon 9: Assistant MCP server with run, preview and validate (5)
- Horizon 10: MCP project and file tools (5)
- Horizon 11: Run Logs, read-logs and test-connection (5)

## Open questions blocking the path

- **Gates #12:** What goes into an accepted-change record: the full accepted table shape, or "accept whatever the Source shows next run"? (h7, h8)
- **Gates #12:** How does an Environment know it already applied a record, and are records ever removed? (h7, h8)
- **Gates #12:** What happens when the Source has drifted again before the record is applied? (h7, h8)
- **Gates #12:** How is a Breaking Change in a PR branch handled when nobody is watching, given accept-schema --branch is a known gap? (h6)
- **Gates #14:** Runs left STARTED after a Server crash make every later tick skip: Dagster run monitoring, a startup sweep, or both? (h7, h8)
- **Gates #14:** Do the stage/prod workflows work on a real self-hosted runner, with the PR checkout mounted over `.adops/` and `.env`? (h8)
- **Gates #14:** Does the Dagster code-location reload pick up added or removed Pipelines while a run finishes on the old version? (h8)
- **Gates #14:** Do root-owned files in `.adops/` break git checkout or later runs started as the runner user? (h8)
- **Gates #14:** Without GitHub Enterprise, is "only writers can publish releases" an acceptable prod gate? (h8)
- **Gates #15:** Can a `profiles.yml` under dbt/ redirect dev to stage/prod data without the shared-connection rule seeing it? (h9)
- **Gates #15:** Claude Code's real MCP tool-call timeout versus how long a dev or Branch run takes. (h9)
- **Gates nothing specific:** dbt source tests are reported under the source name, not the table. Fix this before users write source tests. (h3)
- **Waits on users:** What did the first user feedback on Promotion say? (h8)

19 older entries in blockers.md look already settled by later decisions or delivered work, but they are not marked `— resolved` (for example: dbt in-process safety, read_file secret handling, drop_branch fail-fast, plain Pipeline names). The next PLAN run should mark them resolved.

## Beyond current visibility

- CSV/Parquet file Verified Source: ADR-0012 names it, and vision item 10 would then need it in E2E. Whether v1 needs it is unclear.
- Generating dbt `sources.yml` from the Pipeline YAML (convenience).
- accept-schema --branch. It may fold into #12.
- Branch Environment name collisions for short branch names; listing and cleaning up abandoned `br_` schemas.
- Running only changed Pipelines on merge to stage (`--all` was chosen).

The order past #13 depends on real use and real self-hosted runs, which nobody has done yet. This list is rebuilt on every run and gets sharper as horizons land.
