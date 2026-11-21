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
