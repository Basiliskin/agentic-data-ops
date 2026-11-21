# Horizon 10 — Assistant MCP: project and file tools

Adds the remaining reuse-only Assistant tools from vision item 7 to the horizon-9 MCP server: drop-branch, dev-only accept-schema, a read-only project view, and list/read/edit file tools. Each sits behind the same dev/Branch allow-list, sanitized error mapping and secret-leak tests, so the Assistant can inspect and change a Project without ever reaching stage/prod or a secret value.

**Modelled on horizon 9** (`horizon-09-assistant-mcp-server-roadmap.json`) via `plan --like 9`.

## Phase 1 — Add the drop-branch MCP tool

**Goal:** A drop_branch MCP tool that fails fast on a held stage lock, with the CLI default unchanged, covered by in-memory tests with secret-leak assertions.

**Why:** This is one small slice across layers (rule, use case, adapter, MCP entry point and tests) built on the pattern horizon 9 already settled, instead of spreading it over several phases. It also gives drop_project_branch the lock_timeout path that run_project already has, so the Assistant never hangs on a held lock while the CLI behaviour stays the same.

**Changes:**
- Add `lock_timeout: float = DEFAULT_LOCK_TIMEOUT` to drop_project_branch in drop_branch_command.py and pass it to run_command.build_warehouse_lock(warehouse, project, STAGE, lock_timeout). The `drop_branch` CLI command passes nothing, so it keeps the 30-minute wait.
- Add a synchronous `drop_branch(branch: str)` tool in build_server. It calls assistant_target(None, branch) first, then drop_project_branch(project, branch, lock_timeout=ASSISTANT_LOCK_TIMEOUT), inside `_sanitized_errors()`. It never calls the CLI function and never takes an Environment argument.
- Add a DroppedBranchReply TypedDict (environment, dropped schema names) and a small serializer from DroppedBranch.
- Extend _sanitized_errors with WarehouseUnreachable ('stage's Warehouse is unreachable or locked by another process' style fixed text, naming no host or DSN), because it is not in RUN_ERRORS and would otherwise reply 'unexpected WarehouseUnreachable'.
- Tests in test_mcp_server.py: a branch drop returns the dropped schema names; an empty branch name is refused; a held lock gives the busy reply with FakeLocks.timeouts == [ASSISTANT_LOCK_TIMEOUT]; a missing stage Warehouse Secret Reference names only the variable; an unreachable Warehouse gives the fixed text; an unexpected error names only its type; stage's own schemas are untouched (fake WarehouseSchemas records only br_ names). Every call_tool is followed by assert_no_secret.
- Tests in test_drop_branch_command.py: the CLI drop_branch passes DEFAULT_LOCK_TIMEOUT to the lock (FakeLocks.timeouts == [DEFAULT_LOCK_TIMEOUT]); update any stub of drop_project_branch or build_warehouse_lock that rejects the new keyword (the horizon-9 run phase broke conftest.py and test_run_command.py the same way).

**Files:** `src/adops/interface/drop_branch_command.py`, `src/adops/interface/mcp_server.py`, `tests/unit/interface/test_mcp_server.py`, `tests/unit/interface/test_drop_branch_command.py`, `tests/unit/interface/conftest.py (only if the FakeLocks or stub signatures need the new keyword)`

**How to verify:**
- The MCP drop fails fast while the CLI still waits 30 minutes (min 9): A test holds the lock via FakeLocks(error=WarehouseLocked('stage', 0)) and asserts the exact busy reply text and FakeLocks.timeouts == [ASSISTANT_LOCK_TIMEOUT].
- The tool can only drop br_<branch>__ schemas and refuses a bad branch name (min 8): A test lists stage schemas plus br_<branch>__ schemas in the fake and asserts only the br_ ones were dropped.
- Every response and error is sanitized and scanned for secrets (min 9): `grep -c 'call_tool(' tests/unit/interface/test_mcp_server.py` is not larger than `grep -c 'assert_no_secret(' tests/unit/interface/test_mcp_server.py` for the new tests (every call is scanned).
- Rules live in domain/application, the entry point only wires (min 8): `uv run pytest tests/unit/domain/test_domain_purity.py tests/unit/application/test_application_purity.py` passes.

**Done when:** A drop_branch MCP tool that fails fast on a held stage lock, with the CLI default unchanged, covered by in-memory tests with secret-leak assertions.

## Phase 2 — Add the dev-only accept-schema MCP tool

**Goal:** An accept_schema MCP tool for dev only, a plain_pipeline_name rule applied to every pipeline-taking tool, and tests with refusals and secret-leak assertions.

**Why:** This is one small slice across layers (rule, use case, adapter, MCP entry point and tests) built on the pattern horizon 9 already settled, instead of spreading it over several phases. The plain-Pipeline-name rule is planned here because accept_schema is the next tool with a pipeline argument, and it closes the open blocker that run's pipeline argument reads pipelines/<name>.yaml unchecked.

**Changes:**
- Add `plain_pipeline_name(name: str) -> str` to domain/assistant_access.py (strings only, no pathlib). It returns the name or raises AssistantRefusal with a fixed message when the name is empty, or contains '/', '\\', '..' or a NUL character. The message does not echo the supplied name.
- In mcp_server.py call plain_pipeline_name first in run, preview, validate (when pipeline is given) and the new tool, before the guard reads adops.yaml and before any file is read.
- Add a synchronous `accept_schema(pipeline, table, environment=None, branch=None)` tool. After plain_pipeline_name it calls guarded_run_arguments(project, environment, branch), and refuses when a branch was given (accept-schema is dev only), then calls accept_project_schema(project, pipeline, table, DEV). It takes no Warehouse lock and never passes a timeout.
- Add an AcceptedSchemaReply TypedDict (pipeline, table, step, environment, column count, nullable column names) and a serializer from AcceptedSchema; no column types or values beyond names already in the Warehouse shape.
- Extend _sanitized_errors with StepError (its message is already free of secrets) so a failing Source or Step has a fixed reply; keep unexpected errors as the type name only.
- Domain tests: one test per refused name ('', '..', 'a/b', 'a\\b', '../x', 'x..y', a NUL name) and accepted names ('shop_sync', 'a-b', 'v2'), and the refusal text does not contain the supplied name.
- MCP tests: accept_schema in dev succeeds and records the shape; stage, prod, qa, production, Dev, 'dev ', a custom name and any branch each get a refusal with zero build_* calls; a dev sharing a database with stage is refused; FakeLocks records no lock call (documenting the no-lock decision); Source unreachable, a missing Secret Reference and a StepError map to fixed text; run, preview, validate and accept_schema each refuse '../adops' and 'a/b' with no file read. Every call_tool is followed by assert_no_secret.

**Files:** `src/adops/domain/assistant_access.py`, `tests/unit/domain/test_assistant_access.py`, `src/adops/interface/mcp_server.py`, `tests/unit/interface/test_mcp_server.py`, `tests/unit/interface/mcp_harness.py (only if the shared Project needs a Source table fixture)`

**How to verify:**
- The tool refuses with the shared dev/Branch guard before any side effect (min 8): Tests call the tool with 'stage', 'prod', 'qa', 'production', 'Dev' and 'dev ' and each is a sanitized refusal.
- Every pipeline-taking tool refuses a non-plain name before any file is read (min 9): `grep -n 'plain_pipeline_name' src/adops/interface/mcp_server.py` shows a call in run, preview, validate and accept_schema.
- accept_schema writes only dev's recorded shape and takes no lock (min 8): A test asserts accept_project_schema (spied) was called with environment 'dev' exactly.
- Every response and error is sanitized and scanned for secrets (min 9): `grep -c 'call_tool(' tests/unit/interface/test_mcp_server.py` is not larger than `grep -c 'assert_no_secret(' tests/unit/interface/test_mcp_server.py` for the new tests (every call is scanned).
- Rules live in domain/application, the entry point only wires (min 8): `uv run pytest tests/unit/domain/test_domain_purity.py tests/unit/application/test_application_purity.py` passes.

**Done when:** An accept_schema MCP tool for dev only, a plain_pipeline_name rule applied to every pipeline-taking tool, and tests with refusals and secret-leak assertions.

## Phase 3 — Add the read-only project-view MCP tool

**Goal:** A project_view MCP tool and a secret_reference_status helper, with tests proving no value and no connection detail ever reaches the reply.

**Why:** This is one small slice across layers (rule, use case, adapter, MCP entry point and tests) built on the pattern horizon 9 already settled, instead of spreading it over several phases. Knowing the Project's shape and which Secret References are missing saves the Assistant from reading raw files, and the status helper gives later tools one place that decides 'set' or 'missing'.

**Changes:**
- Add `secret_reference_status(env: Mapping[str, str], refs: Iterable[SecretReference]) -> dict[str, str]` to application/secrets.py. It maps each reference name to 'set' or 'missing' using the same rule as resolve_secret_references (an empty value is missing) and never returns a value. env is passed in; the module reads no os.environ.
- Add a synchronous `project_view()` tool. It reads adops.yaml with load_project_settings and every pipelines/*.yaml with load_pipeline (through the existing run_command read helpers or a small loop), and builds a ProjectViewReply: per Pipeline its name, Schedule text (or None) and Steps (name and kind); per Environment its name, Warehouse type, Source and target names and its Secret References with status from secret_reference_status(run_command.project_env(project), ...), plus alert_webhook_env's status when configured.
- The reply carries no host, port, database, user, path or password; Environment names are listed but no connection detail.
- Reuse the schedule text already built for validate (validate_command._schedule_line, made public if needed) instead of rewriting it.
- Invalid adops.yaml or Pipeline YAML maps to the existing sanitized problem lines (PipelineValidationError is in RUN_ERRORS); no new exception mapping is needed.
- Tests: secret_reference_status for set, unset and empty-string values and for a name used twice; project_view on the harness Project with a distinctive value for every Secret Reference returns the structure and passes assert_no_secret; a Project with some references unset shows 'missing' for exactly those; a Pipeline with and without a Schedule; no Pipelines folder; invalid YAML returns the problem lines; the reply contains none of the connection hosts or users from SETTINGS.

**Files:** `src/adops/application/secrets.py`, `tests/unit/application/test_secrets.py`, `src/adops/interface/mcp_server.py`, `tests/unit/interface/test_mcp_server.py`, `src/adops/interface/validate_command.py (only if the schedule summary helper _schedule_line must be made public)`

**How to verify:**
- Secret References show only 'set' or 'missing' (min 9): Unit tests of secret_reference_status cover set, unset, empty-string and a name referenced twice.
- The overview lists Pipelines, Steps, Environments and Schedules (min 8): A test asserts the exact Step names and kinds in YAML order for a Pipeline with Sync, Transform and Reverse Sync Steps.
- Each refusal and failure is asserted, and the tests can fail (min 8): Each refusal test asserts the sanitized message text and that build_* spies, FakeLocks and the file tree are untouched.
- Rules live in domain/application, the entry point only wires (min 8): `uv run pytest tests/unit/domain/test_domain_purity.py tests/unit/application/test_application_purity.py` passes.

**Done when:** A project_view MCP tool and a secret_reference_status helper, with tests proving no value and no connection detail ever reaches the reply.

## Phase 4 — Add the list-files and read-file MCP tools

**Goal:** list_files and read_file MCP tools whose path rule, resolve check and redaction are each unit-tested, with the .env refusal and the no-symlink-escape rule proven on real files.

**Why:** This is one small slice across layers (rule, use case, adapter, MCP entry point and tests) built on the pattern horizon 9 already settled, instead of spreading it over several phases. Reading is split from editing because the path rule, the resolve check and the redaction are the risky parts, and the edit tool then reuses them.

**Changes:**
- Add `allowed_project_path(relative: str) -> str` to domain/assistant_access.py. It works on strings only (no pathlib), normalizes separators, and returns the Project-relative path when it is adops.yaml or lies under pipelines/ or dbt/; it raises AssistantRefusal for an empty path, an absolute path, any '..' part, any part that is .env or starts with .env. (compared in lower case, so .ENV and .Env.local are refused), and anything else including .adops/, .github/ and server/. The refusal never echoes more than the supplied relative path.
- Add `redact_secret_values(text: str, values: Iterable[str]) -> str` to application/secrets.py: replaces each non-empty value with *** (longest value first, empty values skipped). Pure function, env passed in by the caller.
- Create interface/assistant_files.py with `resolve_project_file(project, relative) -> Path`: apply allowed_project_path to the request, resolve the path, require it to be inside project.resolve(), and apply allowed_project_path again to the resolved path relative to the Project, so a symlink pointing at .env or outside the Project is refused. Add `list_project_files(project, directory) -> list[str]` (sorted relative paths of regular files under the allowed roots, skipping any entry that fails resolve_project_file) and `read_project_file(project, relative, secret_values) -> str` (UTF-8 text, size capped, redacted; a non-text or oversized file gets a fixed refusal).
- Add synchronous `list_files(directory: str = "")` and `read_file(path: str)` tools. read_file computes the secret values from adops.yaml's Secret References resolved against run_command.project_env(project) (reading adops.yaml itself goes through the same redaction). Both run inside _sanitized_errors; a missing file is a fixed 'file not found' reply naming the supplied relative path.
- Domain tests per accepted and refused case: adops.yaml, pipelines/a.yaml, dbt/models/x.sql, '', '/etc/passwd', '../x', 'pipelines/../adops.yaml', '.env', '.ENV', '.env.local', 'pipelines/.env', 'dbt/.Env.prod', '.adops/dlt/x', '.github/workflows/ci.yml', 'server/x', 'Pipelines/a.yaml'.
- Secrets tests: redaction of one value, two values, overlapping values (a shorter value inside a longer one), a value repeated, an empty value (text unchanged), and no values.
- Interface tests (tmp_path): a symlink in pipelines/ pointing at .env, at a file outside the Project and at a directory outside are each refused; a symlink to an allowed file inside the Project works; a resolved path that lands in .adops/ is refused.
- MCP tests: read_file returns an allowed file with a distinctive secret value typed into it replaced by ***, and the same value typed into adops.yaml is replaced; .env and .ENV refused and the .env text is absent from every response; list_files lists only allowed paths and omits .env; every call_tool is followed by assert_no_secret.

**Files:** `src/adops/domain/assistant_access.py`, `tests/unit/domain/test_assistant_access.py`, `src/adops/application/secrets.py`, `tests/unit/application/test_secrets.py`, `src/adops/interface/assistant_files.py (new)`, `tests/unit/interface/test_assistant_files.py (new)`, `src/adops/interface/mcp_server.py`, `tests/unit/interface/test_mcp_server.py`

**How to verify:**
- Paths stay inside the allowed Project files after resolve (min 9): Domain tests include '.ENV', '.Env.local', 'pipelines/.env', 'Pipelines/a.yaml', 'pipelines/../adops.yaml' and an absolute path, each refused with a fixed message.
- Every resolved Secret Reference value in file text becomes *** (min 9): A test writes a distinctive value into pipelines/x.yaml, dbt/models/m.sql and adops.yaml and asserts each reply holds *** and not the value.
- Each refusal and failure is asserted, and the tests can fail (min 8): Each refusal test asserts the sanitized message text and that build_* spies, FakeLocks and the file tree are untouched.
- Rules live in domain/application, the entry point only wires (min 8): `uv run pytest tests/unit/domain/test_domain_purity.py tests/unit/application/test_application_purity.py` passes.

**Done when:** list_files and read_file MCP tools whose path rule, resolve check and redaction are each unit-tested, with the .env refusal and the no-symlink-escape rule proven on real files.

## Phase 5 — Add the edit-file MCP tool

**Goal:** An edit_file MCP tool limited to pipelines/ and dbt/, with real-filesystem tests that every refusal leaves files unchanged.

**Why:** This is one small slice across layers (rule, use case, adapter, MCP entry point and tests) built on the pattern horizon 9 already settled, instead of spreading it over several phases. It reuses the path rule and resolve check from the read phase, adding only the narrower write allow-list and the write step.

**Changes:**
- Extend allowed_project_path with a keyword `write: bool = False`. When write is True only paths under pipelines/ and dbt/ are accepted; adops.yaml is refused with a fixed message saying it is read-only for the Assistant. Every other refusal of the read rule (including .env in any case) still applies.
- Add `write_project_file(project, relative, content) -> bool` to interface/assistant_files.py. It applies allowed_project_path(write=True) to the request, resolves the deepest existing ancestor and requires it inside project.resolve() and allowed_project_path(write=True) again on the resolved target, refuses a target that is an existing directory or a symlink leaving the allowed folders, creates missing parent folders inside the allowed root, writes the UTF-8 text, and returns whether the file was created.
- Add a synchronous `edit_file(path: str, content: str)` tool and an EditFileReply TypedDict (path, bytes written, created). The reply never echoes the content. A content larger than a fixed size cap gets a fixed refusal before any write. The tool does not run validate; the Assistant calls validate itself.
- Domain tests: write accepted for pipelines/a.yaml, dbt/models/x.sql and dbt/profiles.yml; refused for adops.yaml, .env, .ENV, '.env.local', pipelines/.env, .adops/x, .github/workflows/ci.yml, server/x, '../x' and an absolute path; read acceptance of adops.yaml is unchanged (tested in the same file).
- Interface tests (tmp_path): create a new file with a new subfolder; overwrite an existing file; a symlink in pipelines/ to a file outside the Project is not written through; a symlinked parent folder pointing outside is refused; a symlink to adops.yaml is refused; a target that is a directory is refused; no file is changed after any refusal (compare bytes before and after).
- MCP tests: a successful edit then validate round trip on the harness Project; adops.yaml and .env refused with the file bytes unchanged; the reply holds no content (a distinctive secret-looking string in the content does not appear in the reply); oversized content refused; every call_tool is followed by assert_no_secret.

**Files:** `src/adops/domain/assistant_access.py`, `tests/unit/domain/test_assistant_access.py`, `src/adops/interface/assistant_files.py`, `tests/unit/interface/test_assistant_files.py`, `src/adops/interface/mcp_server.py`, `tests/unit/interface/test_mcp_server.py`

**How to verify:**
- Writes land only under pipelines/ and dbt/ and never through a symlink out (min 9): Domain tests include adops.yaml (refused for write, accepted for read), .ENV, 'pipelines/.env', 'dbt/../adops.yaml' and an absolute path.
- A refused or failed edit changes nothing on disk (min 8): Each refusal test asserts the Project tree (paths and bytes) is identical before and after.
- Every response and error is sanitized and scanned for secrets (min 9): `grep -c 'call_tool(' tests/unit/interface/test_mcp_server.py` is not larger than `grep -c 'assert_no_secret(' tests/unit/interface/test_mcp_server.py` for the new tests (every call is scanned).
- Rules live in domain/application, the entry point only wires (min 8): `uv run pytest tests/unit/domain/test_domain_purity.py tests/unit/application/test_application_purity.py` passes.

**Done when:** An edit_file MCP tool limited to pipelines/ and dbt/, with real-filesystem tests that every refusal leaves files unchanged.

**Depends on:** list-read-file-tools

## Out of scope

- Run logs and the read-logs tool: they need a new log writer and a new adapter, so they get their own horizon.
- The test-connection tool: it needs new ports and adapters that open every Source, Warehouse and target.
- accept-schema --branch: branch baselines start fresh (known gap, horizon 6).
- ADR-0015 accepted-change records and Git-based Breaking Change acceptance: the record format is undecided.
- Hardening Promotion, clearing runs left STARTED after a Server crash: Server concerns, deferred with the Promotion blockers.
- Any non-stdio MCP transport or MCP authentication: v1 is local stdio only.
- Claude Code's real MCP tool-call timeout against long runs: needs real use first.
- Editing adops.yaml, .env, .adops/, .github/ or server/ through the Assistant: deliberately read-only or refused.
- Git operations from the MCP server: the skills do Git (ADR-0009, ADR-0011).
- read-logs tool and run logs — need a new log writer and a new adapter (new ports), so they are a separate horizon
- test-connection tool — needs new ports and adapters that open every Source, Warehouse and target
- accept-schema --branch — known gap, branch baselines start fresh (horizon 6)
- ADR-0015 accepted-change records, Promotion hardening, STARTED-run cleanup and non-stdio transport — outside this batch by the user's scoping

## Quality gate

None ran. This horizon used the template path (`plan --like 9`): no critic, healer or gate. Its quality rests on horizon 9 having worked and on the user's confirmation on 2026-11-21. Run `/dima-plan-roadmap-ddd-v5-11 review docs/roadmaps/agentic-dataops/horizons/horizon-10-assistant-mcp-project-and-file-tools-roadmap.json` for an independent read before executing.

## Cost

1 Agent call (sonnet).
