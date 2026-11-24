# DuckDB for laptop quickstart, Postgres as the Docker warehouse

The warehouse is pluggable, but v1 supports exactly two: DuckDB (single file, zero setup, for quick local trials) and Postgres (the default warehouse in docker compose). DuckDB alone was rejected as the main warehouse because it permits only one writing process, so loads and analytics queries would block each other. ClickHouse was rejected because PyAirbyte has no direct cache for it and would need a Docker-based destination connector, which the spec forbids (no Docker socket in connector jobs). Both chosen engines work natively with PyAirbyte caches, dbt and Wren.

**Amended by ADR-0012:** Verified Sources sync through dlt rather than PyAirbyte caches; PyAirbyte remains for Docker-free Community Sources.

**Clarification (2026-11-24):** "Laptop quickstart" means the Warehouse alone needs no setup. The demo Project still needs Docker, because its Source (the shop database) is a Postgres container whichever Warehouse is chosen. A Docker-free demo is not built; the README says Docker is required.
