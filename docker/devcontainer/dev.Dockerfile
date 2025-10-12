# Stage providing uv binaries (avoid unsupported variable expansion in COPY --from)
ARG PYTHON_VERSION=3.12.5
ARG UV_VERSION=0.9.2

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uvbin

FROM python:${PYTHON_VERSION} AS base

ARG POSTGRES_JDBC_VERSION=42.7.8
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/workspace/.venv

# System deps: Java 17 (Spark 4 requirement), curl, libpq for psycopg2-binary runtime.
RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    ca-certificates \
    curl \
    libpq5 \
    tini && \
    rm -rf /var/lib/apt/lists/*

# Install uv (copy from named stage) pinned version
COPY --from=uvbin /uv /uvx /bin/

WORKDIR /workspace

# Fetch JDBC driver (could alternatively apt install libpostgresql-jdbc-java; using direct jar for explicit versioning)
RUN mkdir -p /workspace/lib && \
    curl -fsSL -o /workspace/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    ln -s /workspace/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar /workspace/lib/postgresql.jar && \
    sha256sum /workspace/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar | awk '{print "Postgres JDBC jar sha256=" $1}'

# Copy only dependency metadata first for caching
COPY pyproject.toml uv.lock dbt_project.yml profiles.yml packages.yml ./

# Sync dependencies into an .venv (uv creates) using cache mounts
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked && \
    uv run dbt deps

# Default command is noop (devcontainer overrides). Use tini as init when run directly.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
