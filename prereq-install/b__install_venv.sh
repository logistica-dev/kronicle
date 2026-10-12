#!/bin/sh
# Create the local .venv and install dependencies from uv.lock.
set -e

if ! command -v uv >/dev/null 2>&1; then
    echo "error: 'uv' not found. Install it: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi

# Runtime + dev (default) + test dependency groups, exactly as locked.
uv sync --frozen --group test
