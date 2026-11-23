# Breaking Changes in stage and prod are accepted through Git

A Breaking Change halts scheduled runs, often when nobody is watching, and today resolving it means running `adops accept-schema <pipeline> <table> --env prod` on a machine that can reach that Environment's Warehouse. The target user cannot be expected to do that (ADR-0011) and the Assistant may not touch stage or prod (ADR-0009). So accepting a Breaking Change becomes a reviewed change in the Project: the Assistant opens a pull request adding an accepted-change record for the table, a human merges (and releases, for prod), the Server pulls it (ADR-0014), and the next run records the new shape and continues.

**Considered Options:** a person runs `accept-schema` on the Server (needs server access and skills the user lacks); an "Accept" button on the Dagster page (fast, but unreviewed and leaves no record in Git).

**Consequences:** `accept-schema` changes from writing the snapshot directly into one Environment's Warehouse to producing a record in the Project that each Environment applies on its next run; running it by hand stays available for dev and Branch Environments.

**Clarification (2026-11-23):** what `adops accept-schema <pipeline> <table>` does as shipped in horizons 13 and 14:
- **dev** (the default, or `--env dev`): finds the table's Breaking Changes, writes an accepted-change record for them, then saves the table's current Source shape as dev's baseline (relaxing NOT NULL on dropped columns). The record is written before the baseline changes, so a refused record leaves dev as it was. dev runs never apply records; the record is for stage, prod and Branch Environments.
- **Any other `--env`** (stage, prod, ci or any other name in `adops.yaml`): opens that Environment's Source and snapshots read-only and writes only the record file. Commit the record; the Environment accepts the change on its next run. It never saves a baseline there.
- **`--branch <name>`** (cannot be combined with `--env`): accepts the current shape directly as that Branch Environment's baseline, under stage's Warehouse lock, and writes no record.
- **The Assistant's MCP `accept_schema` tool** works only in dev and returns the written record's path as `record_path` (`null` when none was written). For a Branch it tells the user to run `adops accept-schema --branch <name>` by hand.
- **Record id:** `<UTC %Y%m%dT%H%M%SZ>-<pipeline>-<table>`, stored as `pipelines/accepted/<id>.yaml`. A table name that is not plain (a-z, 0-9, `_`) is refused.
- **Duplicates and clashes:** when existing records already cover every change found, no record is written and their paths are printed. Otherwise a new record lists every change found. It is created exclusively, so a file with the same name is never overwritten and the command stops with nothing accepted.
