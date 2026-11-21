# Planning Brief — horizon 8 (written by horizon 7 PLAN, 2026-11-20)

## Recommended scope
Make the horizon-7 Server pull its Project from Git instead of reading a bind-mounted folder: main for stage, the latest release tag for prod. It switches to a new version only after `adops validate` passes, keeps the last good version and sends a webhook alert otherwise. Once it accepts a version, it queues runs of the Pipelines whose files changed, so stage's Step Fingerprints stay current. Then make ADR-0015 real. `accept-schema` for stage/prod writes an accepted-change record into the Project instead of writing to the Warehouse. Each Environment applies the record once, on its next run, before checking for Breaking Changes, and keeps the dropped-column nullable step. The horizon-2 decision line is superseded. Settle the record format, the applied-once tracking, Git credentials and the hot-swap mechanism up front. Prove it end to end with a local Git remote in the E2E. Add a prod Environment to the templates only if needed to test the release-tag path. Leave GitHub Actions workflows, the MCP server and the skills for later.

## Unknowns
- What exactly is in an accepted-change record: the full accepted TableShape copied from where accept-schema ran, or just 'accept whatever shape the Source shows on the next run'? prod's Source can differ from dev's.
- How does an Environment know it already applied a record? Is it tracked in the Warehouse next to _adops_schema_snapshots, and are records ever removed from the Project once every Environment has applied them?
- What happens if a record's shape does not match the Source shape seen on the next run (it drifted again)? Halt with a new Breaking Change, or apply it anyway?
- How does the Server get Git credentials for a private repo, and from where? A deploy key, a token as a Secret Reference, or SSH agent? And does the Server image need git, or does a Python library do the pull?
- How does the Server find 'the latest release tag': semver ordering, GitHub Releases, or the newest annotated tag by date? What counts as a release when there are pre-release or malformed tags?
- How does a new version reach the running Dagster code location: reload the code location in place, restart containers, or switch a symlink to a checkout? How are in-flight and queued runs on the old version handled?
- Which Pipelines count as 'changed' when a version is accepted: a git diff of pipeline and dbt files, or Step Fingerprints compared with stage's saved ones (decided in horizon 6 with no Git)?
- Do runs started right after a version is accepted share the single run slot and queue behind scheduled runs, and does a Pipeline with no Schedule run only at that moment?
- In horizon 8, does the bind-mounted folder from horizon 7 stay supported (dev/demo) next to Git-pull mode, or does Git-pull replace it on stage/prod?
- How often does the Server poll Git, and who sets that? A Dagster sensor/schedule inside the daemon, or a separate loop/container?

## Research
- Dagster 1.13 code-location reload: workspace.yaml python_module vs gRPC server, the reload_workspace GraphQL mutation, and how the daemon picks up new definitions. Also whether a sensor can trigger the reload.
- Ways to run git in containers: a git binary in python:slim vs dulwich/pygit2, shallow fetch of a tag or branch, deploy-key handling, known_hosts, and keeping secrets out of logs.
- How release-tag-to-deploy flows pick 'latest' (semver vs creation date) in tools like Argo CD, Flux or watchtower, to borrow a simple rule.
- How horizon 7 actually built it, by reading the finished horizon-7 roadmap and its decisions: the Server code location, how run_pipeline is wired for the Server, the alert port, the ADR-0014 clarification, and the E2E server scenario.
- How run_pipeline's PlannedStep._raise_if_breaking loads and saves snapshots, to find where an accepted-change record would be applied before the comparison.
- Open blockers 25-27 in blockers.md (CI reachability, merge→stage changed-only, concurrent --branch). Check which ones Git-pull plus run-changed-on-accept closes.

## Decisions needed
- Format and location of the accepted-change record in the Project (file path, schema, one file per table vs one log), and whether `adops validate` checks it.
- What accept-schema --env stage/prod does now: write a record, refuse a direct write, or keep the direct write as an escape hatch. Also what --env dev/--branch keep doing.
- How a record becomes 'applied once per Environment' (an applied-record ledger in the Warehouse vs comparing shapes) and whether records are cleaned up or kept forever.
- How the Server pulls and switches versions: git binary or library, polling interval, where checkouts live, and how the last good version is kept and switched atomically.
- How Git credentials and the repo URL reach the Server (Secret References in .env?) and what a public repo needs.
- Which events alert when a version is rejected or a pull fails, using the horizon-7 webhook, and how to keep validate output in an alert free of secrets and values.
- How 'changed Pipelines' is found on accept (git diff vs Step Fingerprints) and in what order those runs are queued.
- Whether init templates gain a prod Environment, so the release-tag path can be tested end to end.
- How the E2E proves the Git loop inside the 20-minute CI limit: a local bare repo or file:// remote, and a fast poll interval.

## Artifacts to inspect
- docs/adr/0014-schedules-run-on-a-pull-based-server.md (plus the horizon-7 clarification on the bind-mounted folder)
- docs/adr/0015-breaking-changes-accepted-through-git.md
- src/adops/application/accept_schema.py (accept_schema saves straight to the snapshot store and makes dropped columns nullable)
- src/adops/interface/accept_schema_command.py
- src/adops/application/run_pipeline.py (PlannedStep._raise_if_breaking, BreakingChangeError, run_pipeline, FingerprintSavingStep)
- src/adops/infrastructure/schema_snapshot_store.py (_adops_schema_snapshots table)
- src/adops/application/ports.py (SchemaSnapshotStore, StepFingerprintStore, StepReport, Orchestrator)
- src/adops/domain/schema_snapshot.py and src/adops/domain/branch_environment.py (step_fingerprint, first_changed_step, saves_stage_fingerprint)
- src/adops/interface/validate_command.py (validate_project Report, which acceptance needs to call)
- adops.interface.server and the Dagster Definitions builder from horizon 7 (src/adops/interface/server*, src/adops/infrastructure/dagster_*)
- src/adops/templates/demo/ (docker-compose.yml server profile, Dockerfile, dagster.yaml, adops.yaml with dev+stage only) and src/adops/templates/plain/
- tests/e2e/test_demo_sync.py (stack fixture, horizon-7 server scenario) and tests/unit/interface/test_init_command.py
- docs/roadmaps/agentic-dataops/decisions.md (horizon-2 accept-schema line to supersede; horizon-6 fingerprint lines) and blockers.md
