# Agentic DataOps

An open-source, self-hosted tool for moving data into a warehouse, transforming it, and pushing results back out, operated interactively through the user's AI assistant.

## Language

### Pipelines

**Pipeline**:
A version-controlled definition of a chain of Steps that run together on a schedule or on demand.
_Avoid_: job, DAG, flow, workflow

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
A disposable Environment created for a single Git branch or pull request and deleted when it is merged or closed.
_Avoid_: preview env, ephemeral env, sandbox

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
