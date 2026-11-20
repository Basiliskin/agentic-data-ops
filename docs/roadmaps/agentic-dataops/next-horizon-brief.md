# Planning Brief — horizon 7 (written by horizon 6 PLAN, 2026-11-19)

## Recommended scope
Close vision criterion (5) and start (9): have `adops init` generate GitHub Actions workflows — PR opened/updated runs `adops validate` + `adops run --branch`, PR merged/closed runs `adops drop-branch`, merge to main runs stage, release/approval runs prod — as thin wrappers over the horizon-6 CLI, verified by a lint or snapshot test rather than a live GitHub run. Settle CI-to-Warehouse reachability, the fork-PR secret policy and run serialization first; they decide whether the workflows can work at all. Add a prod Environment to the templates if Promotion needs one. Keep the MCP server, skills, playbooks and Verified Source E2E for later unless the CI work turns out small.

## Unknowns
- How does a GitHub-hosted runner reach the stage Warehouse, when the demo's stage is localhost:15433 Postgres or a local .adops DuckDB file no CI run can share?
- Which GitHub event and secret setup lets a PR workflow run --branch without exposing stage secrets to forked PRs (open blocker since horizon 1)?
- Does merge→stage run every Pipeline or only changed ones? Stage writes the fingerprints branches compare against, so a stale/partial stage run makes branches rebuild the wrong Steps.
- What does prod look like? The demo has only dev and stage (a test asserts {'dev','stage'}); does the single adops.yaml model hold for a prod deploy?
- With no locking and single-writer DuckDB, what happens when two PRs run --branch at once against the same stage Warehouse?
- How do generated workflows install adops, at which version, while keeping ADR-0008's "no tool code copied into the Project"?
- What does "tests" mean in "PR→Branch Environment+tests" (validate, the branch run's Checks, more)? Should a Preview be posted to the PR?
- How is a Breaking Change in a PR branch handled, given accept-schema --branch is a recorded gap and CI cannot "drop and re-run" interactively?

## Research
- The finished horizon-6 roadmap, decisions.md, discoveries.md: final drop-branch verb, separator, raw-schema var convention, E2E wall-clock, any replans.
- GitHub Actions: pull_request closed with merged, pull_request vs pull_request_target secrets for forks, environment protection/approvals, release triggers, concurrency groups.
- How to verify generated workflow YAML in the tool repo's CI (actionlint, schema snapshot, act) under the 100% coverage gate.
- Python MCP SDK: does it resolve in the current uv lock with dlt, dagster, dbt pins; stdio transport only?
- Branch-name sources (GITHUB_HEAD_REF) that exceed the truncation cap or collide after sanitizing.
- The open Dagster scheduling blocker: cron CI vs Dagster daemon decides what the merge→stage workflow does.

## Decisions needed
- Which remaining vision item(s) horizon 7 takes on: generated GitHub Actions, the MCP server, the skills, or a combination.
- How CI connects to the stage/prod Warehouse: self-hosted runner, network-reachable Postgres + repo secrets, or no CI support for a DuckDB stage.
- Fork-PR policy: skip Branch Environments for forks, require a label/approval, or pull_request_target with its risks.
- Prod Promotion trigger: release tag, manual environment approval, or both; whether a prod Environment is added to the templates.
- Whether workflows ship in both plain and demo templates via adops init, and how DEMO_FILES and the ci.yml wheel grep change.
- How to serialize concurrent branch and stage runs: GitHub concurrency groups only, or Warehouse-side locking.
- Whether the accept-schema --branch gap is closed now that unattended CI branch runs make it reachable.

## Artifacts to inspect
src/adops/interface/run_command.py, src/adops/interface/init_command.py + tests/unit/interface/test_init_command.py, src/adops/templates/demo/{adops.yaml,docker-compose.yml}, src/adops/templates/plain/adops.yaml, .github/workflows/ci.yml, src/adops/domain/reverse_sync_diff.py, src/adops/application/secrets.py, docs/adr/0006, 0008, 0009, 0011, tests/e2e/test_demo_sync.py
