# The tool is an installable package; users' work lives in a separate Project

Agentic DataOps ships as a Python package/CLI (`adops`); `adops init` scaffolds the user's own Project repo (pipelines, dbt, environments, docker-compose, `.env.example`, skills), and `adops init --demo` scaffolds the demo shop the same way. Forking the tool repo was rejected because it mixes user pipelines with tool code and makes upgrades painful.
