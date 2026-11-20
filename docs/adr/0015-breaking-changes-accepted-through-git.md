# Breaking Changes in stage and prod are accepted through Git

A Breaking Change halts scheduled runs, often when nobody is watching, and today resolving it means running `adops accept-schema <pipeline> <table> --env prod` on a machine that can reach that Environment's Warehouse. The target user cannot be expected to do that (ADR-0011) and the Assistant may not touch stage or prod (ADR-0009). So accepting a Breaking Change becomes a reviewed change in the Project: the Assistant opens a pull request adding an accepted-change record for the table, a human merges (and releases, for prod), the Server pulls it (ADR-0014), and the next run records the new shape and continues.

**Considered Options:** a person runs `accept-schema` on the Server (needs server access and skills the user lacks); an "Accept" button on the Dagster page (fast, but unreviewed and leaves no record in Git).

**Consequences:** `accept-schema` changes from writing the snapshot directly into one Environment's Warehouse to producing a record in the Project that each Environment applies on its next run; running it by hand stays available for dev and Branch Environments.
