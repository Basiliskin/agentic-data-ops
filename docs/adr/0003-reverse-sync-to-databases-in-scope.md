# Reverse Sync is in scope, limited to database destinations

The spec lists reverse ETL as a non-goal because writing into operational systems is risky. We include it because it is a core user need, but v1 only writes to databases (e.g. Postgres) and every Reverse Sync supports a dry-run preview before writing. SaaS destinations (CRMs etc.) are deferred.

**Consequences:** a Reverse Sync performs real writes only in the stage and prod Environments. In dev and every Branch Environment it runs as a Preview, reporting the inserts/updates/deletes it would make plus a sample, without writing.
