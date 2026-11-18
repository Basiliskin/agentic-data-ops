# Pipelines are YAML files in Git, compiled to Dagster

Users and the Assistant author Pipelines as declarative YAML (e.g. `pipelines/shop_orders.yml`), which the tool validates and compiles into Dagster assets/jobs; a Python escape hatch exists for advanced steps. YAML was chosen over Python-first because it is safe for an AI assistant to edit, easy to validate against a schema, and reviewable in pull requests. Dagster stays the orchestrator but is an implementation detail behind the YAML format.
