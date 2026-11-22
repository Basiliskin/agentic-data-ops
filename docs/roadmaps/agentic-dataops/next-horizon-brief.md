# Planning Brief — horizon 14 (written by horizon 13 PLAN, 2026-11-22)

## Recommended scope
Close the record loop that h13 left half-done. Make the Breaking Change lookup a read-only step that both accept-schema and runs can use. Make adops accept-schema (CLI and the dev-only MCP tool) write the pipelines/accepted/ record file in dev. Turn --env stage|prod into read-only, record-only commands, which closes the known gap where they still write those Warehouses. Add the CLI-only accept-schema --branch <name> under stage's Warehouse lock as the first phase, as already decided. Extend the demo E2E so stage halts, dev writes the record, stage applies it once and a second run does not apply it again. Finish with the ADR-0015/0014 text and the decisions.md supersessions. Pull in the Breaking Change Assistant skill (or the #14 skills set) only if the planner judges it fits under the phase ceiling. Otherwise leave the skills as the following horizon, now unblocked by a stable record format.

## Unknowns
- Does sync.preflight for stage/prod build or touch anything that writes, such as dlt local state in .adops/dlt/<env> or dataset creation? If it does, find_table_changes is not truly read-only for --env stage|prod.
- Did h13 settle on the parenthesised BreakingChange.message wording ('<col> retyped (<from> -> <to>)') for record files? The analysis draft used a form without parens. Check the shipped phase 0 before writing dump/halt text.
- Is a UTC-timestamp record id (<ts>-<pipeline>-<table>) unique enough? Two accept-schema runs in the same second, or on two machines, could collide. Nothing yet says what happens if the target file already exists.
- When dev accept-schema finds changes that an existing unapplied record already covers, should it write a second, duplicate record? No rule exists yet.
- With fresh Branch baselines and deferred Sync Steps, how often will a PR's record actually be exercised in its Branch before merge? Is the extended E2E enough proof?
- Does the 20-minute e2e budget (g4) still have room for the extra stage runs in test_demo_incremental_breaking_change_and_missing_table on both Postgres and DuckDB?

## Research
- Read the h13 code as it shipped, not the plan: AcceptedChangeRecord, cover(), the SchemaSnapshotStore applied_record_ids/save contract, load_project_records, ACCEPTED_RECORDS_DIR and the halt-message wording.
- Read drop-branch's lock and name handling (stage_branch_name, SchemaLayout.for_branch, _holding_lock on STAGE). It is the model for accept-schema --branch.
- Trace sync.preflight and build_sync/build_snapshot_store in run_command.py to see what an --env stage|prod read-only path must avoid building.
- Read the decisions.md lines to supersede: h2 line ~10 (accepted only via accept-schema --env) and h6 line ~39 (--branch is a known gap). Also read ADR-0015 Consequences and ADR-0014's stale 'horizon 8' pointer.
- Read MAP.md candidate #14 (six Assistant skills) to judge whether the Breaking Change skill could ship in the same horizon once record writing exists.

## Decisions needed
- Does accept-schema --branch use the same Breaking Change detection and dropped-column NOT NULL relaxation as dev (sharing accept_schema), or only overwrite the Branch snapshot?
- Does dev accept-schema (CLI and MCP) start taking dev's Warehouse lock? Today it takes none (h10 decision), while --branch will take stage's lock.
- What does accept-schema --env stage|prod do when that Environment has no previous snapshot or sees no Breaking Change? Error, plain message, or point to a hand-written record?
- Collision policy for record ids and files: refuse, add a suffix, or overwrite. Also whether accept-schema should skip writing when an unapplied record already covers the changes.
- Horizon scope: only the deferred accept-schema work (orders 5-6 plus --branch), or also the Breaking Change skill (or all six skills) from MAP candidate #14 now that the record format is fixed?
- Shape of the MCP AcceptedSchemaReply change: an optional record_path, plus how a 'dev saw no Breaking Change' result is reported (field or distinct reply).

## Artifacts to inspect
- src/adops/application/accept_schema.py (accept_schema, AcceptedSchema; split into find_table_changes)
- src/adops/interface/accept_schema_command.py (accept_project_schema, --env handling; add --branch and record writing)
- src/adops/interface/mcp_server.py:142-149,229-241,296-309 (accept_schema tool, refuse_branch_schema_acceptance, AcceptedSchemaReply)
- tests/unit/interface/test_accept_schema_command.py:154,268,317-345 (tests that assert --env stage writes the Warehouse; must be rewritten)
- tests/unit/interface/test_mcp_server.py:1044-1142 (accept_schema MCP tests)
- tests/e2e/test_demo_sync.py:145,255-283 (Breaking Change demo scenario to extend)
- src/adops/domain/accepted_change_record.py and src/adops/application/run_pipeline.py (h13 record type, cover(), shared relaxation, halt text)
- src/adops/infrastructure/warehouse_lock.py and src/adops/application/drop_branch.py (lock pattern for --branch)
- src/adops/interface/run_command.py:113-116,241,436-489,569-572 (store/sync factories, --env/--branch exclusion, Branch layout and name)
- docs/adr/0015-breaking-changes-accepted-through-git.md, docs/adr/0014-*.md, docs/roadmaps/agentic-dataops/decisions.md, GLOSSARY.md
