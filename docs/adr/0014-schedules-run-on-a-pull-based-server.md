# Schedules run on an always-on, pull-based Server running Dagster's scheduler

stage and prod each get a Server: an always-on machine running Dagster's scheduler and web page next to the Warehouse, started with `docker compose --profile server up` from the generated Project. A Pipeline's Schedule is one `schedule:` line in its Pipeline file (plain words such as `daily at 02:00`, or cron), read in the Project's time zone (`adops.yaml`, default UTC), and starts the whole Pipeline; dev and Branch Environments never run on a Schedule. The Server pulls the Project from Git itself (`main` for stage, the latest release tag for prod) and switches to a new version only after `adops validate` passes; otherwise it keeps the last good version and alerts. This changes ADR-0005, where Dagster ran only in-process per `adops run`, and closes the horizon-1 blocker on how Pipelines run on a schedule.

**Considered Options:** a GitHub Actions cron (a GitHub-hosted runner cannot reach a localhost Postgres or a DuckDB file Warehouse, and stage/prod secrets would live in GitHub); GitHub pushing to the Server over SSH (the Server must accept inbound connections); a self-hosted GitHub runner on the Server (a forked PR could run code on it); a bare `adops` loop or system cron (no run history or page a non-technical user can look at).

**Consequences:**
- Accepting a new version on a Server also runs the Pipelines whose files changed right away, so stage's Step Fingerprints stay current for Branch Environments; a Pipeline with no Schedule runs only then.
- One run at a time per Server (other Pipelines queue); a run due while the same Pipeline is still running is skipped; after downtime the Server runs once to catch up, not once per missed run.
- Only Source/Warehouse-unreachable failures are retried (3 times, 5 minutes apart); a failed Check or a Breaking Change is never retried, because it gives the same result for the same data.
- Failures are shown on the Dagster page and sent to Slack/email through a webhook Secret Reference, sanitized like every other error.
- The web page allows only "Run now" and "Pause"; the Schedule itself changes only through Git, and the Assistant has no access to a Server (ADR-0009).

**Clarification (2026-11-20):**
- Until horizon 8, a Server runs the Project version in its bind-mounted folder instead of pulling it from Git. The Git pull, the switch after `adops validate` passes, and Git-based acceptance of Breaking Changes (ADR-0015) come with horizon 8.
- Dagster's open-source web page has no per-action permissions, so it cannot be limited to "Run now" and "Pause". Instead it is bound to 127.0.0.1 (`DAGSTER_WEBSERVER_HOST`), so only someone on the Server machine can open it.

**Clarification (2026-11-21):**
- The Server no longer pulls from Git or polls for new versions. The generated merge and release workflows (`adops-stage.yml`, `adops-prod.yml`) run on a self-hosted runner on the Server machine (ADR-0016). They validate a fresh checkout, then push that version into the Server Project folder with a local `git fetch` and `git checkout --detach`, then reload Dagster's code location. Restarting the Server was rejected because it kills runs in progress; the daemon picks up Schedule changes within about 60 seconds of the reload.
- Accepting a new version on stage runs every Pipeline (`adops run --all --env stage`), not only those whose files changed; prod does the same on a release.
- The rejected option "a self-hosted GitHub runner on the Server (a forked PR could run code on it)" is now the chosen one, with a fork policy that answers that risk (ADR-0016).

**Clarification (2026-11-23):**
- Git-based acceptance of Breaking Changes (ADR-0015) did not come with horizon 8. It shipped in horizons 13 and 14: a committed accepted-change record in `pipelines/accepted/` is applied by stage and prod on their next run, and `adops accept-schema` in dev writes that record. `adops accept-schema --env stage|prod` only reads those Environments and writes the record, so nobody needs to run it on the Server.
