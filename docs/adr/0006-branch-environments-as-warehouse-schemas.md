# Branch Environments are schemas in the shared stage Warehouse

Beyond the fixed dev/stage/prod Environments, every Git branch / pull request gets a disposable Branch Environment implemented as its own set of schemas in the stage Warehouse, named `br_<branch>__<schema>` (e.g. `br_add_refunds__analytics`). It is created by `adops run <pipeline> --branch <branch>` and dropped by `adops drop-branch <branch>`. Schemas were chosen over a separate database or docker stack per branch because they are cheap, fast to create, and work identically on Postgres and DuckDB.

**Consequences:** a Branch Environment does not re-sync Sources by default. It defers to the stage Environment's synced tables and rebuilds only changed Steps and their downstream dependents; a full re-sync happens only when the branch changes a Sync Step itself.

## Clarification (2026-11-20)

The original text named a single `env_<branch>` schema created by CI and dropped on merge/close. What shipped:

- **One schema per kind, not one per branch.** A Branch gets `br_<branch>__raw_<source>` (only when it re-syncs that Source), `br_<branch>__analytics` and `br_<branch>__adops`, beside stage's own `raw_<source>`, `analytics` and `adops`. dlt also creates `<schema>_staging` companions; they carry the same prefix.
- **The `__` separator is kept by turning off dlt's dataset-name normalization** for non-default schemas only, because dlt otherwise collapses `__` to `_`. dev and stage schema names are unchanged.
- **The Branch name** is the Git branch lower-cased with every run of non-alphanumerics replaced by `_`, truncated with a hash suffix when `br_<name>__raw_<longest source>_staging` would exceed Postgres's 63 bytes.
- **A Changed Step** is one whose Step Fingerprint differs from stage's last successful run. Stage saves fingerprints only after a Step's Checks pass; a Branch run never writes stage's tables, fingerprints or schema snapshots.
- **Dropping is manual.** `adops drop-branch <branch>` drops every schema starting with exactly `br_<name>__` (CASCADE) and the Branch's local dlt state; stage's schemas are never touched. Running it from CI on merge/close is deferred.
- **dbt projects must read their raw schema through a variable**, so a Branch that re-syncs a Source can point Transforms at its own copy. In `sources.yml`:

  ```yaml
  schema: "{{ var('adops_raw_schema_shop', 'raw_shop') }}"
  ```

  where `shop` is the Source name. A Branch run that rebuilds a Sync fails fast when the dbt sources do not use the variable.
- **Known gap:** `adops accept-schema` has no `--branch` option. A Branch's schema snapshots start fresh, so a Breaking Change inside a Branch is rare.

## Clarification (2026-11-21)

- **Dropping now runs from CI.** The generated `adops-pull-request.yml` runs `adops drop-branch <branch>` on the stage Server when a pull request is closed or merged (ADR-0016). `adops drop-branch` by hand still works.
- **Branch names that sanitize to the same name collide.** Two branches such as `feature/new-orders` and `feature-new/orders` both become `feature_new_orders` and share one Branch Environment: each run overwrites the other's tables, and closing either pull request drops both. This is recorded, not fixed; `docs/playbooks/promotion.md` gives the recovery steps.
