#!/bin/sh
# docker/db/init.sh

set -euo pipefail

# --- Optional: source environment variables if you use a .env ---
# source /path/to/.env

# Activate virtual environment if exists
if [ -d "/opt/kronicle/.venv" ]; then
    source /opt/kronicle/.venv/bin/activate
fi

SCRIPTS=(
    "01_bootstrap_db.py"
    "02_create_tables.py"
    "03_create_superuser.py"
)

for script in "${SCRIPTS[@]}"; do
    echo "[init] Running $script ..."
    python3 "/opt/kronicle/scripts/init/$script"
    echo "[init] Finished $script"
done

echo "[init] DB initialization completed successfully."
