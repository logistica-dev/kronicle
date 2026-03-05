# docker/app/Dockerfile

# ------------------------------------------------------------------------------
# Kronicle application image
# Multi-arch: amd64 + arm64
# ------------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM python:3.12-slim as builder
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
WORKDIR /opt/kronicle

# -----------------------------
# Copy requirements first (cache layer)
# -----------------------------
COPY requirements.txt .

# -----------------------------
# Create venv and install dependencies
# -----------------------------
RUN python -m venv .venv
ENV PATH="/opt/kronicle/.venv/bin:$PATH"
RUN . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt


# ------------------------------------------------------------------------------
# Copy init scripts
# ------------------------------------------------------------------------------
COPY . .

# Make scripts executable (if you want to run shell wrappers too)
RUN chmod -R +x scripts/init

# Temporary debug
# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
# Activate venv and run Python bootstrap script
ENTRYPOINT ["/opt/kronicle/.venv/bin/python", "/opt/kronicle/scripts/init/init.py"]
