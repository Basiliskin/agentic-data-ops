# Try the Assistant on the demo

A repeatable check that the README's quickstart and its Assistant example still work: a fresh
demo Project, the README request typed into Claude Code, and a grade on five points. Run it after
changing the README, a Skill or anything the Assistant calls. It takes about 10 minutes; the
optional share part takes 5 more and creates a private GitHub repository.

## Before you start

- **Docker is running.** Open OrbStack (or Docker Desktop) and wait until it says it is running.
- **adops is installed from the version you want to test.** From a checkout of this repository:
  `uv tool install --force --reinstall .` (or `uv tool install git+https://github.com/Basiliskin/agentic-data-ops`
  for the published `main`). Check with `adops --version`.
- **Claude Code is installed** (`claude --version`).
- **Ports 15432–15434 are free**: `lsof -iTCP:15432-15434 -sTCP:LISTEN` prints nothing.
- For the share part only: the GitHub CLI is signed in (`gh auth status`).

## Step 1 — Create a fresh demo Project

In a normal terminal, not inside another Claude Code session:

```sh
cd ~
adops init --demo adops-try
cd adops-try
cp .env.example .env
docker compose up --wait
adops run --all
```

- `adops init` prints `Saved these starting files in a new Git repository, on branch main.`
  If it says Git could not save them, set your Git name and email and finish the commit as it
  says before going on; otherwise the share part's first pull request holds the whole Project.
- The last line must be `Pipeline shop_sync passed in Environment dev.` This is the state a
  README reader is in after the quickstart.

## Step 2 — Start Claude Code in the Project

```sh
claude
```

- If it asks whether to trust the folder, say yes.
- If it asks whether to use the `adops` MCP server from `.mcp.json`, say yes.
- Type `/mcp` and check that `adops` shows as connected. If it does not, stop: that is the
  first finding.

## Step 3 — Type the README request exactly

> Add a dbt model with the top 10 customers by total spent, and test it in dev.

Answer each permission prompt and write it down. Allow the adops tools (`mcp__adops__...`) and
the Skills. If it asks for anything else (Bash, `git`, `docker`), say no and note what it asked
for: the README request should need none of them.

## Step 4 — Save the transcript and look at the rows

- Type `/export` and save the conversation to a file, for example `~/adops-try/transcript.txt`.
- Look at the rows yourself, using the model name the Assistant reports:

  ```sh
  docker compose exec warehouse psql -U warehouse -d warehouse \
    -c 'select * from analytics.top_customers'
  ```

- Compare with the source of truth:

  ```sh
  docker compose exec warehouse psql -U warehouse -d warehouse \
    -c 'select customer_id, total_amount from analytics.customer_stats order by total_amount desc, customer_id limit 10'
  ```

## Step 5 — Grade it

| # | Check | How to tell |
|---|---|---|
| 1 | The model file exists | `git status` shows a new file in `dbt/models/` and a changed `schema.yml` |
| 2 | It validated, and the dev run passed | The transcript shows `validate`, then one `run` with `environment="dev"`, and no separate `preview` call |
| 3 | Nothing outside dev was touched | No `stage`/`prod` in any tool call; `git status` shows only `dbt/` changes; no `storage/` folder |
| 4 | The explanation is in plain words | A reader who knows SQL but not adops understands every sentence |
| 5 | The rows are right | Step 4's two queries list the same 10 customers in the same order |

Also note any permission prompt for something other than adops tools or Skills, any query hint
without the `analytics.` schema, and anything that felt slow or confusing.

## Optional: share the change

This creates a **private GitHub repository** under your account and opens a pull request in
it, not in the tool's repository. In the same Claude Code session, say `share it`.

What to expect today, on a demo with no stage Server:

- The Assistant uses `git` and `gh` through Bash; that is expected (ADR-0011).
- It tries the Branch run **before** pushing. It is refused because stage has never run
  `shop_sync` ("stage has no Step Fingerprints"); the Assistant explains this and asks whether
  to open the pull request anyway.
- Before creating the repository it says the checks need a self-hosted runner; after pushing
  it reports that no runner labelled `adops-stage` exists.
- The pull request holds only the change (the model and `schema.yml`), because `adops init`
  saved the starting files on main. Find it with `gh pr list --repo <you>/adops-try`.

To see the pull request's checks actually run, the Mac must act as the stage Server with a
self-hosted runner: see [Promotion](promotion.md) and
[Run stage and prod on a Mac](run-stage-and-prod-on-a-mac.md).

## Step 6 — Clean up

```sh
cd ~/adops-try && docker compose down -v && cd ~ && rm -rf ~/adops-try
gh repo delete <you>/adops-try    # only if you did the share part; asks you to confirm
```

`gh` is not allowed to delete repositories by default. If `gh repo delete` fails with
`HTTP 403 ... needs the "delete_repo" scope`, grant that once with
`gh auth refresh -h github.com -s delete_repo` (it opens the browser) and run the delete again.

## Running it without a person (for maintainers)

Steps 2–3 can run headless, which shows whether it works but not what a person sees (no
permission prompts):

```sh
claude -p "Add a dbt model with the top 10 customers by total spent, and test it in dev." \
  --mcp-config .mcp.json --strict-mcp-config \
  --allowedTools "mcp__adops__*" "Skill" "Read" \
  --disallowedTools "Bash" "Edit" "Write" "WebFetch" "WebSearch" \
  --output-format stream-json --verbose > transcript.jsonl
```

Each line of `transcript.jsonl` is one event; the tool calls are the `tool_use` entries of
`assistant` events, and the last line (`"type":"result"`) holds the final answer, turns and cost.

## Recorded trials

| Date | How | Result |
|---|---|---|
| 2026-11-24 | Headless, before fixes | All 5 checks passed (20 turns, 85 s, ~$0.33). Found: the Pipeline ran twice (preview, then run); runs left `storage/` in the Project. Both fixed. |
| 2026-11-24 | Headless, after fixes | All 5 checks passed (20 turns, 61 s, ~$0.29); one run, no `storage/`. |
| 2026-11-26 | A person in Claude Code, plus share | All 5 checks passed. Found: query hint without `analytics.`; first pull request held the whole Project and its Branch run was refused only after the repository and pull request were created. All three fixed the same day (`adops init` makes the starting save; `adops-share` tries the Branch run before pushing). The share part has not been re-run since. |
