#!/bin/bash
set -euo pipefail

VENV_DIR=".venv"
PYTHON_VERSION="3.12"

REQS_FILE="requirements.txt"
DEV_REQS_FILE="requirements-dev.txt"

echo "----- Recreating virtual environment"
rm -rf "$VENV_DIR"
python$PYTHON_VERSION -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

echo "----- Upgrading pip/setuptools/wheel"
pip install --upgrade pip setuptools wheel

echo "----- Installing dev tools"
pip install --upgrade pip-chill pre-commit black pytest

echo "----- Installing runtime dependencies (if any existing lockfile)"
if [[ -f "$REQS_FILE" ]]; then
    pip install --upgrade -r "$REQS_FILE"
fi

echo "----- Generating full dependency snapshot"
FULL_DEPS=$(pip-chill -v)

# Extract runtime deps (prod)
RUNTIME_DEPS=$(echo "$FULL_DEPS" | grep -v -E "black|pre-commit|pytest|pip-chill")
echo "$RUNTIME_DEPS" > "$REQS_FILE"

# Extract dev deps
echo "$FULL_DEPS" | grep -E "black|pre-commit|pytest|pip-chill" > "$DEV_REQS_FILE"

echo "----- All dependencies synced:"
echo "Runtime: $REQS_FILE"
echo "Dev: $DEV_REQS_FILE"

echo "----- Autoupdating pre-commit hooks"
pre-commit autoupdate

echo "----- Done!"
