# Planning Brief — horizon 18 (rerun only Changed Steps on deploy)

Prepared 2026-11-27 by horizon 17 PLAN. Horizon 17 = release folders + atomic `current` switch; it deferred MAP #18's second half.

## Recommended scope

Finish MAP #18 by delivering the held Outcome B whole, built on h17's release folders. This means four parts. (1) A domain rule that picks, for each Pipeline in a stage or prod deploy: full run, rerun from the first Changed Step, or 'unchanged, nothing rerun', using the user's fixed defaults. (2) Prod saves its own Step Fingerprints, and rows for Steps no longer in a Pipeline are pruned. (3) Per-Environment saved deploy state for the adops.yaml marker and seen accepted-change record ids, reusing existing applied-record storage where it fits. (4) A deploy-only changed-only run option wired into both generated deploy workflows, with YAML-parsing tests and dated clarifications to ADR-0006, ADR-0014 and ADR-0016 plus promotion.md, including the documented change that unchanged scheduled Pipelines no longer refresh on merge. Hand `adops run --all`, Branch behaviour, scheduled runs, rollback and freshness alerts (MAP #19) stay out of scope. First confirm that h17 actually landed.

## Unknowns

- Did horizon 17 ship as planned? At brief time src/adops/infrastructure/release_folders.py and src/adops/interface/release_command.py do not exist yet. Horizon 18 assumes `adops server-release`, the per-run release-folder tag, and deploys that run `adops run --all --env <env> --project /project/current` are merged. It also assumes the e2e held-run case is green.
- Does a deploy need a new store for 'new accepted-change record'? The Warehouse may already be enough: schema_snapshot_store.py and accept_schema.py hold applied record ids per Environment. A record that is applied only when a Sync Step runs may count as 'new' until then. It is unknown whether that is good enough, or whether the deploy needs its own 'seen ids' (B3).
- What should count as 'any adops.yaml change'? A raw-bytes hash would rerun everything on whitespace or comment edits. A canonical hash of the parsed settings would not. It is also unknown whether only the target Environment's section should count.
- What happens to the deploy's changed-only run when a rerun Step fails partway? Steps before it saved fingerprints, later ones did not. Will the next deploy correctly rerun from the failed Step, and does the 3x300 s RetryRequested path (Server runs only) matter for in-process deploy runs?
- When the first deploy after upgrade runs everything in prod, does saving fingerprints there have any side effects on the prod Warehouse adops schema? Examples: new table creation rights, or a Postgres role without CREATE. Nobody has checked this.
- Can a deploy's changed-only run and a scheduled full run of the same Pipeline overlap in a way that leaves fingerprints out of step with data? For example, the scheduled run finishes on the old release after the deploy saved new fingerprints (per-Pipeline Warehouse lock plus run pinning from h17).

## Research before planning

- Read the h17 phase outcomes and any replan notes under docs/roadmaps/agentic-dataops/ (decisions.md, discoveries.md, both uncommitted now) to confirm what h17 actually delivered and any new discoveries about run pinning or cleanup.
- Trace how run_pipeline's `first` slicing and _deferred() behave outside a Branch. Today first=None returns every Step as DEFERRED with Branch wording. A Pipeline where no Step changed needs an 'unchanged, nothing rerun' Run Log line instead (src/adops/application/run_pipeline.py:549-600, 645, 694-710).
- Read step_fingerprint_store.py _load/_save (saved_order = max+1, never deleted) and the first_changed_step contract. Then choose between pruning and by-name comparison. Check that pruning in stage does not change how Branch deferral behaves today.
- Read how accepted-change records are applied and recorded: domain/accepted_change_record.py (applied_ids), application/accept_schema.py, infrastructure/schema_snapshot_store.py. This decides whether B3 needs a new table.
- Read ADR-0006 (Changed Step, stage only), ADR-0014 (2026-11-21 clarification 'deploy runs every Pipeline' and its 'Pipeline with no Schedule' consequence) and ADR-0016. Each needs a dated clarification.
- Read the h17-rewritten DEPLOY_WORKFLOWS tests in tests/unit/interface/test_init_command.py. They pin `adops run --all --env <env>` as the run step, and every swap to `--changed` must be asserted by parsing YAML (g7).

## Decisions needed

- Where the changed-only plan rule lives: a new domain function next to first_changed_step (branch_environment.py) or a separate domain module, and how it gets its inputs (fingerprints, adops.yaml marker, record ids, Schedule presence) without framework imports (g6).
- Whether to rename or keep saves_stage_fingerprint now that prod saves too, and whether the fingerprint store and port get an Environment-neutral name.
- Stale fingerprint rows: prune rows for Steps no longer in the Pipeline after a passing run, or compare by name and ignore extra rows.
- Where per-Environment deploy state (adops.yaml marker, seen record ids) is stored: a new table in that Environment's Warehouse adops schema, or reuse of existing applied-record storage. Also when it is written: before the run, or only after every Pipeline passes.
- The CLI surface: `adops run --changed` valid only with --env stage|prod (and its error when used with --branch or dev), or a separate deploy-only command. Also whether it implies --all.
- Run Log and output wording for Steps skipped because they did not change in an Environment's own deploy, as opposed to Branch 'deferred to stage'. Also the StepStatus value they get.
- Whether this horizon includes a real-runner or Mac retrial of the changed-only deploy. The h16 decision keeps prod out of CI e2e.

## Artifacts to inspect

- src/adops/domain/branch_environment.py
- src/adops/application/run_pipeline.py
- src/adops/application/ports.py
- src/adops/infrastructure/step_fingerprint_store.py
- src/adops/infrastructure/schema_snapshot_store.py
- src/adops/domain/accepted_change_record.py
- src/adops/domain/schedule.py
- src/adops/interface/run_command.py
- src/adops/templates/github/workflows/adops-stage.yml
- src/adops/templates/github/workflows/adops-prod.yml
- tests/unit/interface/test_init_command.py
- tests/e2e/test_server.py
- docs/adr/0006-branch-environments-as-warehouse-schemas.md
- docs/adr/0014-schedules-run-on-a-pull-based-server.md
- docs/adr/0016-promotion-runs-on-a-self-hosted-runner-per-server.md
- docs/playbooks/promotion.md
- docs/roadmaps/agentic-dataops/MAP.md
- docs/roadmaps/.tmp/h17/stage3-phases.json

## Deferred candidates

- B1 domain: Add the changed-only deploy plan rule (adops.yaml change -> every Pipeline in full; new accepted-change record -> that Pipeline in full; no Schedule -> full; no saved fingerprints -> full; else from first_changed_step; none changed -> 'unchanged, nothing rerun') and widen saves_stage_fingerprint to stage and prod — held: horizon ceiling
- B2 infrastructure: Save prod Step Fingerprints in prod's Warehouse adops schema and prune rows for Steps no longer in a Pipeline (step_fingerprint_store.py) — held: horizon ceiling
- B3 infrastructure: Add per-Environment saved deploy state (adops.yaml hash, seen accepted-change record ids) — held: horizon ceiling
- B4 application+interface: Add `adops run --changed` (only with --env stage|prod) through run_pipeline's `first` slicing, switch both deploy workflows to it, and add dated clarifications to ADR-0014 and ADR-0006 — held: horizon ceiling
