# docker/app/Dockerfile

# ------------------------------------------------------------------------------
# Kronicle application image
# Multi-arch: amd64 + arm64
# ------------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM python:3.12-slim AS builder

# -----------------------------
# Build dependencies
# -----------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        build-essential \
        curl \
        git \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Set workdir
# -----------------------------
WORKDIR /opt/kronicle

# -----------------------------
# uv — version read from .uv-version (single source of truth)
# -----------------------------
COPY .uv-version ./
RUN pip install --no-cache-dir "uv==$(cat .uv-version)"

# -----------------------------
# Install the locked runtime dependencies (cache layer)
# -----------------------------
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock ./
# --no-install-project: the app runs from source via PYTHONPATH (no wheel needed),
# so the project metadata (README/LICENCE) is not required at this layer.
RUN uv sync --frozen --no-install-project --no-dev

# -----------------------------
# Runtime paths
# -----------------------------
ENV PYTHONPATH="/opt/kronicle/src:/opt/kronicle"
ENV PATH="/opt/kronicle/.venv/bin:$PATH"


# ------------------------------------------------------------------------------
# Copy application sources
# ------------------------------------------------------------------------------
COPY . .

# ------------------------------------------------------------------------------
# Generate build info
# ------------------------------------------------------------------------------
ARG GIT_COMMIT
ARG BUILD_DATE
RUN /opt/kronicle/docker/app/generate_build_info.sh

# ------------------------------------------------------------------------------
# Entrypoint (venv already on PATH, app imported from source via PYTHONPATH)
# ------------------------------------------------------------------------------
ENTRYPOINT ["python", "/opt/kronicle/docker/app/entrypoint.py"]
