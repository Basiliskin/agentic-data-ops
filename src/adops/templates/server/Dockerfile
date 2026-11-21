FROM python:3.12-slim

# Override to install a local wheel placed in the Project folder:
#   docker compose build --build-arg ADOPS_PACKAGE=/build/adops-<version>-py3-none-any.whl
ARG ADOPS_PACKAGE=adops=={{ADOPS_VERSION}}
RUN --mount=type=bind,source=.,target=/build pip install --no-cache-dir "$ADOPS_PACKAGE"

WORKDIR /project
