# Project map — agentic-dataops

**Goal (vision.md):** ship v1 of `adops`, a self-hosted CLI whose YAML Pipelines (Sync, Transform,
Reverse Sync) run on Postgres or DuckDB across dev/stage/prod and Branch Environments, operated by the
user's own Assistant through an MCP server and six skills, with Promotion via GitHub Actions.

**Generated:** 2026-11-21 · **Ordering:** what unblocks the most remaining work first; ties go to
closing an open question, then to the earliest visible capability · **User goal:** none given (the
vision is the whole goal).

> This is a regenerated view, not a plan. Only **Done** and **In flight** are fact. **Next** is a
> briefed scope hint, and every **candidate** is a guess derived from deferred work and open
> questions. It is not planned and not numbered as a horizon.

## Where the project stands

Eight horizons are complete. The demo loads, checks, transforms and reverse-syncs on Postgres and
DuckDB, Branch Environments work, a Server runs Schedules, and generated GitHub Actions workflows
promote PR → stage → prod. Nothing is in flight. The biggest gap left is the Assistant: the MCP server
and the six skills have never been started, and they are the "agentic" part of the product. After
that come a second Verified Source with its E2E suite, the optional tracing profile, Community
Sources, and release work.

## The ordered path

| # | What | Delivers | Unblocks | Status / Confidence |
|---|---|---|---|---|
| 1 | Demo shop loads end to end | `init --demo`, `run` Syncs into Postgres, built-in Checks, Docker E2E in CI | everything after | done (fact) |
| 2 | Breaking Changes and declared Checks | schema snapshots, `accept-schema`, YAML Checks | Transform, stage/prod | done (fact) |
| 3 | dbt Transform and clear errors | dbt Step, dbt tests as Checks, unreachable/empty errors | DuckDB, Reverse Sync | done (fact) |
| 4 | DuckDB Warehouse | same demo on a DuckDB file | vision item 3 | done (fact) |
| 5 | Reverse Sync and Preview | writes only in stage/prod, Preview elsewhere | Branch Environments | done (fact) |
| 6 | Branch Environments | `run --branch` with defer, `drop-branch` | Promotion | done (fact) |
| 7 | Scheduling Server | Schedules, run rules, alerts | Promotion deploys | done (fact) |
| 8 | GitHub Actions Promotion | PR/stage/prod workflows, Warehouse lock, setup guide | Assistant hand-off via PRs | done (fact) |
| 9 | Harden Promotion, then accepted-change records | recovery for runs left STARTED after a Server crash, optional live check of the workflow commands, ADR-0015 records applied once per Environment | Breaking Changes in branches; skills that hand stage/prod off to a PR | **next** (briefed) |
| 10 | Assistant MCP server, dev and Branch only | the Assistant can edit, validate, test connections, run, Preview and read logs; server code refuses stage/prod | the six skills, vision items 6 (MCP part) and 7 | candidate · likely |
| 11 | Six skills and the full `adops init` layout | init scaffolds the six skills and a dbt project; skills check prerequisites, do Git for the user, narrate plainly | vision items 1 and 8 | candidate · possible |
| 12 | Verified Source E2E suite with a CSV/Parquet Source | a second Verified Source; CI proves every Verified Source within the time budget | vision item 10; E2E-budget questions | candidate · possible |
| 13 | Breaking Changes inside Branch Environments | a defined outcome instead of the `accept-schema --branch` gap | the horizon-6 unattended-Breaking-Change question | candidate · possible |
| 14 | Tracing and the optional Grafana/OTel profile | traces of runs, with tests that no span holds a secret | vision items 2 and 6 (span part); the playbook's monitoring part | candidate · speculative |
| 15 | PyAirbyte Community Sources | Sync from Docker-free Community Sources through the same Sync port | the vision objective's Community Source clause | candidate · speculative |
| 16 | v1 release readiness | install adops as a published package; a single-VM production playbook | vision items 1 and 11 | candidate · speculative |

Rows 10 and 11 must stay in that order: the skills call the MCP server. Rows 12, 14 and 15 do not
depend on the Assistant chain and could move earlier if you want something visible sooner.

## Done

- Horizon 1: demo shop loads end to end (7 phases, 1 amendment)
- Horizon 2: Breaking Changes and declared Checks (6 phases, 2 amendments)
- Horizon 3: dbt Transform and dedicated errors (6 phases, 1 amendment)
- Horizon 4: DuckDB Warehouse runs the demo (6 phases, 1 amendment)
- Horizon 5: Reverse Sync and Preview (7 phases, 1 amendment)
- Horizon 6: Branch Environments (7 phases, 1 amendment)
- Horizon 7: Scheduling Server (7 phases)
- Horizon 8: GitHub Actions Promotion (6 phases, 2 amendments)

## Open questions blocking the path

Questions that gate a specific step:

| Question (from blockers.md) | Gates |
|---|---|
| After a Server crash, how are runs left STARTED cleared: Dagster run monitoring, a startup sweep, or both? (h7, h8) | #9 |
| Do the generated workflows work on a real self-hosted runner, with the PR checkout mounted over the Server folder? (h8) | #9 |
| Does the Dagster reload pick up added or removed Pipelines and Schedules while a run finishes on the old version? (h8) | #9 |
| Do root-owned files the Server container writes in `.adops/` break `git checkout` or later runs as the runner user? (h8) | #9 |
| What is in an accepted-change record: the full accepted table shape, or "accept what the Source shows next run"? (h7, h8) | #9, #13 |
| How does an Environment know it already applied a record, and what if the Source drifted again? (h7, h8) | #9, #13 |
| Do runs started right after a version is accepted queue behind scheduled runs in the single run slot? (h7) | #9 |
| What first user feedback on Promotion said, and whether it changes horizon-8 choices (h8) | #9 |
| Without GitHub Enterprise, is "only writers can publish releases" an acceptable prod gate? (h8) | #9 |
| What does the Assistant need from the MCP server, and how does server code reliably tell dev/Branch from stage/prod? (h8) | #10 |
| How is a Breaking Change in a PR branch handled unattended, given `accept-schema --branch` is a gap? (h6) | #13 |
| How long and how stable is the Docker E2E job as Steps are added, and does it fit 20 minutes? (h1, h2) | #12 |
| DbtTransform reports a dbt source test under the source name, not its table (h3) | #12, if users write source tests |

Another 15 open questions from horizons 1–2 (dbtRunner reuse, DuckDB single-writer locking, dlt
exception types, type mapping and others) look answered by later horizons. They gate nothing here.
They are still open in `blockers.md`. A PLAN run's memory compaction, or you, can close them.

## Beyond current visibility

Left out on purpose: a configurable lock timeout and a fix for short branch names that collide (both
wait for user feedback, by decision), CDC Sync (outside v1), and generating dbt `sources.yml` (a
convenience that unblocks nothing; it could ride along with #12). The Analytics Agent and Wren are
v1.1 (ADR-0010). The least specified part of the remaining path is how the skills should narrate Git
and Docker for a non-technical user (ADR-0011), and real user feedback may reorder the tail. This
tail is re-derived on every MAP run and gets sharper as horizons land.
