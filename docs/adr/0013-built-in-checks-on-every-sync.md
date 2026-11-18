# Built-in Checks guard every Sync; skipping one needs a written reason

Every flow must be guarded by fixed rules, not by the user remembering to add them. So every Sync table gets built-in Checks by default: listed tables and key/cursor columns exist before anything is copied; after the load the primary key is unique and never empty; a Full Refresh leaves the Warehouse with the same row count as the Source; an Incremental Sync never loses Warehouse rows and its cursor never goes backwards. The rules are pure functions over measured table facts (same facts, same verdict, no AI). A failed Check fails its Step and stops every later Step. A user can skip a Check for one table only with `skip_checks` plus a non-empty `reason`, and `adops validate` lists every skip.

Breaking Change detection (schema snapshot vs the last successful run) and user-declared Checks (not_null, accepted_values, min_rows, freshness) build on this and come next.

**Considered Options:** user-declared checks only (new flows would run unguarded); built-in Checks that can never be skipped (a Source that deletes rows on purpose would fail every run).
