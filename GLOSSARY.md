# Agentic DataOps

An open-source, self-hosted tool for moving data into a warehouse, transforming it, and pushing results back out, operated interactively through the user's AI assistant.

## Language

### Pipelines

**Pipeline**:
A version-controlled definition of a chain of Steps that run together on a schedule or on demand.
_Avoid_: job, DAG, flow, workflow

**Schedule**:
How often a Pipeline runs by itself, written once in the Pipeline file (in plain words such as `daily at 02:00`, or cron) and read in the Project's time zone. It is used by both stage and prod and starts the whole Pipeline. dev and Branch Environments never run on a Schedule. A run that is due while the same Pipeline is still running is skipped.
_Avoid_: cron, trigger, timer

**Server**:
The always-on Linux machine that runs one Environment's Schedules (one for stage, one for prod) from its Project folder, a Git clone. It does not pull from Git: on a merge or release, its Self-hosted runner validates the new version, moves the folder to it and reloads the Server (ADR-0016). Its Dagster page is bound to 127.0.0.1 because Dagster's open-source page cannot be limited to "Run now" and "Pause" (ADR-0014 clarification).
_Avoid_: host, VM, worker, runner (when meaning the Server itself; the GitHub agent on it is the Self-hosted runner)

**Self-hosted runner**:
GitHub's runner program installed on a Server machine, labelled `adops-stage` or `adops-prod`, that runs the Project's workflow jobs there so they reach that Server's Warehouse with the secrets in its `.env`.
_Avoid_: agent, CI worker, Server (the Server is the machine and its Dagster)

**Run all**:
`adops run --all`: running every Pipeline of the Project one after another, in name order, stopping at the first failure. Branch runs and stage/prod deploys use it.
_Avoid_: full run, run everything, batch

**Step**:
One unit of work inside a Pipeline: a Sync, a Transform, or a Reverse Sync.
_Avoid_: task, node, op

**Project**:
A user's own Git repository holding their Pipelines, dbt models and Environment settings, created by `adops init`.
_Avoid_: workspace, repo (when meaning this), app

**Source**:
An external system data is copied from, such as an application database.
_Avoid_: input, connector (a connector is the code, the Source is the configured system)

**Verified Source**:
A kind of Source covered by the tool's full end-to-end test suite in CI.
_Avoid_: supported, official, certified

**Community Source**:
A kind of Source that can be used through PyAirbyte but is not covered by the tool's tests.
_Avoid_: unsupported, experimental, beta

**Warehouse**:
The analytical database where synced and transformed data lives.
_Avoid_: destination (ambiguous with Reverse Sync targets), DWH, cache

**Sync**:
Copying selected tables/streams from a Source into the Warehouse (through dlt for Verified Sources, PyAirbyte for Community Sources). A document database's collection counts as a table; its nested fields become columns and its lists become child tables.
_Avoid_: ingestion run, extract, load, import

**Incremental Sync**:
A Sync that copies only rows changed since the last run, found by a cursor column such as `updated_at`; it does not see deleted rows.
_Avoid_: delta load, append sync

**Full Refresh**:
A Sync that copies a whole table every run, replacing the previous copy.
_Avoid_: reload, snapshot sync

**CDC Sync**:
A Sync that reads the Source database's own change log, so it also captures deletes.
_Avoid_: replication, streaming sync

**Heavy read**:
A Sync table read larger than the Project's limit (by default more than 1 million rows or documents, or 1 GB), from any Source (ADR-0018). It runs only when its Pipeline entry carries a written approval reason, committed through Git and recording the size it was approved for. The approval must be renewed once the table grows past three times that size. Only one runs per Server at a time, and a Branch Environment reads only a slice of it. Any run over the limit, including an unplanned big Incremental Sync, is written batch by batch into a hidden copy that replaces the visible table only once every batch has passed; only a Full Refresh or a first run needs the approval.
_Avoid_: big sync, large load, expensive query

**Child table**:
The Warehouse table made from a list inside a document collection's documents (for example `orders__items`), one row per list item. Its key is the parent document's key plus the item's position in the list, unless the Pipeline names its own key (such as the parent key plus `sku`).
_Avoid_: nested table, sub-table, array table

**Breaking Change**:
A change to a Source table's shape (renamed or dropped column, changed type) since the last successful run. It halts the whole Sync Step before any Warehouse table changes, and every later Step, until resolved. In dev it is resolved by running `accept-schema` by hand; in stage, prod and Branch Environments, by a committed Accepted-change record, which dev's `accept-schema` writes. A Branch Environment can also be hand-accepted with `accept-schema --branch <name>`, which writes no record. An added column is not a Breaking Change. For a document database, a field seen with a new type is always a Breaking Change, but a field missing from the documents read counts as dropped only on a Full Refresh; a field present in only some documents is never one.
_Avoid_: schema drift (covers harmless changes too), schema error

**Accepted-change record**:
A YAML file in `pipelines/accepted/`, committed through Git, that names one Pipeline table and the Breaking Changes accepted for it, written as `<column> dropped` or `<column> retyped (<from> -> <to>)`. Its id is the file name. Each Environment applies a record at most once, and records are never removed. `accept-schema` in dev writes one (as does any other `--env`, read-only); it can also be written by hand and checked with `adops validate`.
_Avoid_: schema approval, migration, waiver

**accept-schema**:
The command (`adops accept-schema <pipeline> <table>`) that accepts an intended Breaking Change. In dev it writes an Accepted-change record and saves the table's current shape as dev's baseline. With any other `--env` it only reads that Environment and writes the record. With `--branch <name>` it saves the Branch Environment's baseline under stage's Warehouse lock and writes no record. The Assistant's `accept_schema` tool runs it in dev only.
_Avoid_: approve schema, schema migrate

**Transform**:
Turning Warehouse data into new tables using dbt models and their tests.
_Avoid_: model run, job

**Reverse Sync**:
Copying data from the Warehouse back into an operational database.
_Avoid_: reverse ETL, writeback, activation

**Reverse Sync target**:
The existing table in an operational Postgres database that a Reverse Sync writes to, matched row by row on a key column. adops never creates it.
_Avoid_: destination, sink

**Diff**:
The rows a Reverse Sync would insert, update or delete to make its target match the dbt result, matched by key. Rows missing from the dbt result are deleted only when the Step sets `delete_missing: true`; otherwise they are kept and reported. The Preview and the real write use the same Diff.
_Avoid_: changeset, delta, plan

**Max-change guard**:
The `max_change` Check: it stops a Reverse Sync when the rows it would update or delete exceed a set share of the target's rows (half by default). An empty target always passes.
_Avoid_: circuit breaker, blast-radius limit

**Check**:
A fixed rule run before or after a Step that gives the same pass or fail for the same data (for example: the primary key is unique, no rows were lost). A failed Check fails its Step and stops every later Step; a Check can be skipped for one table only with a written reason.
_Avoid_: test, assertion, validation, data quality rule

**Preview**:
A Reverse Sync run that reports the changes it would make, with a sample, without writing anything. Only the Environments named exactly `stage` and `prod` write; every other Environment, including every Branch Environment, gets a Preview.
_Avoid_: dry run, simulation, plan

### Environments

**Environment**:
A fully separate place a Pipeline runs, with its own Warehouse location and secrets: dev, stage or prod.
_Avoid_: stage (as a generic word), target, profile

**Branch Environment**:
A disposable Environment for a single Git branch or pull request, built from stage's settings by `adops run --branch` and removed with `adops drop-branch` when it is no longer needed. It lives in Branch Schemas in stage's Warehouse.
_Avoid_: preview env, ephemeral env, sandbox

**Branch Schema**:
One of a Branch Environment's schemas in stage's Warehouse, named `br_<branch>__<schema>` (e.g. `br_add_refunds__analytics`), beside stage's own schema of the same kind.
_Avoid_: env schema, branch database

**Step Fingerprint**:
A hash of everything that defines a Step: its definition in the Pipeline file and, for a Transform, every file of its dbt project. stage saves one per Step after the Step's Checks pass.
_Avoid_: checksum, version, hash (alone)

**Changed Step**:
A Step whose Step Fingerprint differs from the one saved by stage's last successful run. A Branch Environment rebuilds the first Changed Step and every Step after it.
_Avoid_: dirty Step, modified Step

**Defer**:
What a Branch Environment does with a Step that has not changed: it skips the Step and reads stage's result instead. A Branch where no Step changed defers every Step and uses stage as it is.
_Avoid_: skip (a skipped Step is one that did not run because an earlier one failed), reuse, cache

**Drop**:
Removing a Branch Environment with `adops drop-branch <branch>`: every Branch Schema starting with exactly `br_<branch>__` and its local dlt state. stage's own schemas are never dropped.
_Avoid_: delete, clean up, teardown

**Promotion**:
Moving a reviewed change from one Environment to the next (Branch Environment → stage → prod) through Git.
_Avoid_: deploy (when meaning this), release, push to prod

**Fork PR**:
A pull request whose branch lives in a copy of the repository under another account. It gets only `adops validate`, on a GitHub-hosted machine, with no secrets and no Branch run; GitHub's fork approval setting, not the workflow, keeps its code off the Servers.
_Avoid_: external PR, untrusted PR

**Warehouse lock**:
The lock every Warehouse-writing run takes on one Warehouse, per Pipeline, so two runs never write to it at once. A run waits up to 30 minutes for it, then fails without retry. Branch runs and Drop use stage's lock.
_Avoid_: mutex, run lock, concurrency limit

**Secret Reference**:
The name of a secret (e.g. `APP_DB_PASSWORD`) used in configuration in place of its value.
_Avoid_: credential, secret (when meaning the name)

### Actors

**Assistant**:
The user's own AI coding tool (e.g. Claude Code) that operates the product through its MCP server and skills.
_Avoid_: agent (ambiguous), bot

**Skill**:
A SKILL.md file that `adops init` writes to `.claude/skills/<name>/` and that tells the Assistant how to carry out one workflow step by step in plain words.
_Avoid_: playbook, prompt

**Analytics Agent**:
The optional built-in AI that answers business questions about warehouse data; it can read, never write.
_Avoid_: agent (unqualified), chatbot
