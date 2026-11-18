# Branch Environments are schemas in the shared stage Warehouse

Beyond the fixed dev/stage/prod Environments, every Git branch / pull request gets a disposable Branch Environment implemented as its own schema (e.g. `env_add_refunds`) in the stage Warehouse, created by CI and dropped on merge/close. Schemas were chosen over a separate database or docker stack per branch because they are cheap, fast to create, and work identically on Postgres and DuckDB.

**Consequences:** a Branch Environment does not re-sync Sources by default. It defers to the stage Environment's synced tables and rebuilds only changed Steps and their downstream dependents; a full re-sync happens only when the branch changes a Sync Step itself.
