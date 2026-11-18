# The Assistant acts directly only in dev and Branch Environments

Through MCP the Assistant may edit files, validate, test connections, run Pipelines, show Previews, and read logs, but only against dev or a Branch Environment. For stage and prod it can only open a pull request; a human merge triggers deployment. This is the simplest form of the spec's "AI proposes; deterministic code and humans approve" rule, enforced in the MCP server rather than by prompt instructions.

**Consequences:** Promotion is GitOps: a merge to `main` deploys to stage automatically; prod is deployed only by publishing a release tag or a manual CI approval. `adops init` generates the GitHub Actions workflows for PR (Branch Environment + tests), stage, and prod.
