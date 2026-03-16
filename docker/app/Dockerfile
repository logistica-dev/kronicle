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
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Set workdir
# -----------------------------
ARG KRONICLE_HOME=/opt/kronicle
WORKDIR $KRONICLE_HOME

# -----------------------------
# Copy requirements first (cache layer)
# -----------------------------
COPY requirements.txt .

# -----------------------------
# Create venv and install dependencies
# -----------------------------
RUN python -m venv .venv
ENV PYTHONPATH="$KRONICLE_HOME/src:$KRONICLE_HOME"
ENV PATH="$KRONICLE_HOME/.venv/bin:$PATH"
RUN . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt


# ------------------------------------------------------------------------------
# Copy init scripts
# ------------------------------------------------------------------------------
COPY . .

# ------------------------------------------------------------------------------
# Generate build info
# ------------------------------------------------------------------------------
ARG APP_VERSION
ARG GIT_COMMIT
RUN $KRONICLE_HOME/docker/app/generate_build_info.sh

# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
# Activate venv and run Python bootstrap script
ENTRYPOINT ["python", "$KRONICLE_HOME/docker/app/entrypoint.py"]
