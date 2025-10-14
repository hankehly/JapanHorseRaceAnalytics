# Stage providing uv binaries (avoid unsupported variable expansion in COPY --from)
ARG PYTHON_VERSION=3.10
ARG UV_VERSION=0.9.2

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uvbin

FROM python:${PYTHON_VERSION}-bookworm AS base

ARG POSTGRES_JDBC_VERSION=42.7.8
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    VIRTUAL_ENV=/opt/venv \
    POSTGRES_JDBC_DIR=/opt/jdbc \
    POSTGRES_JDBC_JAR=/opt/jdbc/postgresql.jar \
    CLASSPATH=/opt/jdbc/postgresql.jar:$CLASSPATH

RUN apt-get update && \
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

# Copy only dependency metadata first for caching
COPY pyproject.toml uv.lock dbt_project.yml profiles.yml packages.yml ./

# Sync dependencies into /opt/venv (uv creates)
RUN uv sync --locked && \
    uv run dbt deps

# Fetch JDBC driver into non-mounted image layer directory to avoid it being hidden by bind mount
RUN mkdir -p "$POSTGRES_JDBC_DIR" && \
    curl -fsSL -o "$POSTGRES_JDBC_DIR/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    ln -s "postgresql-${POSTGRES_JDBC_VERSION}.jar" "$POSTGRES_JDBC_DIR/postgresql.jar" && \
    sha256sum "$POSTGRES_JDBC_DIR/postgresql-${POSTGRES_JDBC_VERSION}.jar" | awk '{print "Postgres JDBC jar sha256=" $1}'

# (Optional) Create compatibility symlink path inside workspace at container runtime via entrypoint script if ever needed

# Default command is noop (devcontainer overrides). Use tini as init when run directly.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
