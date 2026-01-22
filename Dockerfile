# -------------------------------------------------------------
# Kronicle DB/init image
# -------------------------------------------------------------
# Multi-arch: amd64 + arm64
FROM --platform=$BUILDPLATFORM python:3.12-slim as builder

# Set workdir
WORKDIR /opt/kronicle

# Copy requirements first to leverage caching
COPY requirements.txt .

# Install venv and pip packages
RUN python -m venv .venv \
    && . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

# -------------------------------------------------------------
# Copy init scripts
# -------------------------------------------------------------
COPY scripts/ scripts/

# Make scripts executable (if you want to run shell wrappers too)
RUN chmod -R +x scripts/init

# -------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------
# Activate venv and run Python bootstrap script
ENTRYPOINT ["/opt/kronicle/.venv/bin/python", "/opt/kronicle/scripts/init/init.py"]
