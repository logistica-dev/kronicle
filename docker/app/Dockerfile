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
WORKDIR /opt/kronicle

# -----------------------------
# Copy requirements first (cache layer)
# -----------------------------
COPY requirements.txt .

# -----------------------------
# Create venv and install dependencies
# -----------------------------
RUN python -m venv .venv
ENV PYTHONPATH="/opt/kronicle/src:/opt/kronicle"
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

ARG APP_VERSION=unknown
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

RUN printf "__version__='%s'\n__commit__='%s'\n__build_date__='%s'\n" \
    "$APP_VERSION" "$GIT_COMMIT" "$BUILD_DATE" \
    > /opt/kronicle/src/kronicle/_build.py

# Temporary debug
# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
# Activate venv and run Python bootstrap script
ENTRYPOINT ["python", "/opt/kronicle/docker/app/entrypoint.py"]
