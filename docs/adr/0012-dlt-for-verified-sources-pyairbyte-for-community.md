# dlt syncs Verified Sources; PyAirbyte only for Docker-free Community Sources

PyAirbyte 0.71.3 can run `source-postgres` (a Java connector) only as a Docker image, which would require giving Sync jobs the Docker socket, rejected in ADR-0004. Verified Sources (Postgres, CSV/Parquet files) therefore sync through dlt, a pure-Python Apache-2.0 loader with incremental loads and merge into Postgres and DuckDB. PyAirbyte is kept for Community Sources whose connectors run without Docker (Python or YAML-manifest connectors, which covers most SaaS APIs). Both engines sit behind one project-owned Sync port.

**Considered Options:** mounting the Docker socket (host-level control, heavier setup); dlt only (fewer ready-made SaaS connectors).
