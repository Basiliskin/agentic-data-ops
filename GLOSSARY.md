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
The always-on machine that runs one Environment's Schedules (one for stage, one for prod). It pulls the Project from Git and switches to a new version only when that version passes validation.
Until horizon 8 a Server runs the Project version in its bind-mounted folder rather than pulling from Git, and its Dagster page is bound to 127.0.0.1 because Dagster's open-source page cannot be limited to "Run now" and "Pause" (ADR-0014 clarification).
_Avoid_: host, VM, worker, runner

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
Copying selected tables/streams from a Source into the Warehouse (through dlt for Verified Sources, PyAirbyte for Community Sources).
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

**Breaking Change**:
A change to a Source table's shape (renamed or dropped column, changed type) since the last successful run. It halts the whole Sync Step before any table loads, and every later Step, until resolved. An added column is not a Breaking Change.
_Avoid_: schema drift (covers harmless changes too), schema error

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

**Secret Reference**:
The name of a secret (e.g. `APP_DB_PASSWORD`) used in configuration in place of its value.
_Avoid_: credential, secret (when meaning the name)

### Actors

**Assistant**:
The user's own AI coding tool (e.g. Claude Code) that operates the product through its MCP server and skills.
_Avoid_: agent (ambiguous), bot

**Analytics Agent**:
The optional built-in AI that answers business questions about warehouse data; it can read, never write.
_Avoid_: agent (unqualified), chatbot
