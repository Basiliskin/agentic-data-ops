---
name: adops-test-in-dev
description: Try a Pipeline in the dev Environment - checks the dev connections, previews what it would change, runs it and explains any failure in plain words using the Run Logs. Use it after building or changing a Pipeline, or when someone asks why a dev run failed.
---

# Try a Pipeline in dev

dev is the person's own sandbox: running there changes only their local dev databases, so it is
the safe place to try a change. Every call in this Skill passes `environment="dev"` explicitly,
because the adops tools refuse a call that names no Environment. The stage and prod Environments
are never reached from here; they are reached only through Git, once the change is shared.

Before each step, say in one plain sentence what you are doing and why. Explain results in plain
words rather than pasting the raw reply.

## 1. Check the dev connections

Say: "First I'm checking that adops can reach the dev databases, so a failure later is about
the Pipeline and not about a stopped database."

- Call `mcp__adops__test_connection` with `environment="dev"`. It only opens and closes each
  connection; it reads and writes nothing.
- For each failed connection, say which database it is (the Warehouse, a Source or a target) and
  the likely fix: the database is not running (`docker compose up --wait`), a wrong password in
  `.env`, or a host or port in `adops.yaml` that does not match. Stop here until it passes.

## 2. Preview what the Pipeline would change

Say: "Now I'm previewing what this Pipeline would write to the target systems, without writing
it."

- Call `mcp__adops__preview` with the Pipeline's name and `environment="dev"`.
- For each entry in `previews`, tell the person in plain numbers how many rows would be
  inserted, updated and deleted, and show a few of the sample keys.
- If `errors` is not empty, explain each one as in step 4.

## 3. Run the Pipeline in dev

Say: "Now I'm running the Pipeline in dev. This loads the dev Warehouse and runs every Step."

- Call `mcp__adops__run` with the Pipeline's name and `environment="dev"`.
- If `succeeded` is true, tell the person which Steps ran and how many rows each loaded
  (`rows_loaded`), and that the change is ready to share with the adops-share Skill.

## 4. Explain a failure

When `succeeded` is false, do not just say "it failed". Read the reply and explain it:

- Look through `steps`. A Step whose `status` is not a success has its reason in `error`. Say
  which Step failed and what the error means in plain words. For example: "The Sync Step could
  not find the `refunds` table in the shop database, so either the table name is mistyped or
  the table does not exist there yet."
- Look at each Step's `checks`. Each one names a `table`, the `check`, its `status` and a
  `message`. Explain every failed check, for example: "The check says `customer_id` is empty in
  3 rows of `orders`, which the Pipeline does not allow."
- For more detail, call `mcp__adops__read_logs` with `environment="dev"` to list the Run Logs,
  newest first, then call it again with `environment="dev"` and the `name` of the newest log to
  read it.
- If an `error` mentions a Breaking Change (a Source column was removed or changed type), do not
  try to fix it here. Explain which table and column changed, and point the person to the
  adops-breaking-change Skill, which handles that change.
- If the fix is in a Pipeline file, go back to the adops-build Skill, then come back and run
  step 3 again.

## When a run takes a long time

A long run may stop answering in the Assistant before it finishes, while it keeps running in
the background. That is not a failure, and you must not start the run again, because two runs
would then work on the same tables. Tell the person the run is still going. Then call
`mcp__adops__read_logs` with `environment="dev"`: the newest log shows whether the run has
finished and how it ended. Check again after a short while if it is not done yet.
