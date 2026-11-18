# Self-hosted, single-team product first; multi-tenancy deferred

The production spec (`docs/agentic_dataops_production_spec.md`) describes a hosted platform serving many isolated customer organizations. We are instead building an open-source tool that one team clones and runs itself (`docker compose up`), because ease of setup is the primary goal and tenant isolation, approval services and enterprise security gates would dominate v1. Code is organized so multi-tenancy can be added later, and we keep the spec's core rules: AI proposes and deterministic code executes, secrets never reach a model, and quality checks gate data readiness.

**Consequences:** spec sections 3 (tenant model), most of 9.6 (approval-gated ops), 14 (enterprise checklist) and 16 (tenant-scoped API) are out of scope for v1. dev/stage/prod are separate deployments of the same tool, not tenants.
