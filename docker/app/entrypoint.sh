#!/usr/bin/env bash
# docker/app/entrypoint.sh

set -e

# Function to wait for DB readiness
wait_for_db() {
    echo "Waiting for database at $DATABASE_URL ..."
    MAX_WAIT=60
    WAITED=0
    while ! psql "$DATABASE_URL" -c '\q' 2>/dev/null; do
        sleep 1
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$MAX_WAIT" ]; then
            echo "Database did not become ready within ${MAX_WAIT}s"
            exit 1
        fi
    done
    echo "Database is ready."
}

# Run DB wait
wait_for_db

# Check if DB is initialized (example: channels table exists)
if ! psql "$DATABASE_URL" -c '\dt' | grep -q "channels"; then
    echo "Database not initialized. Running init script..."
	/opt/kronicle/.venv/bin/python /opt/kronicle/scripts/init/init.py
else
    echo "Database already initialized. Skipping init."
fi

# Start Kronicle app
exec uvicorn kronicle.main:app --host 0.0.0.0 --port 8080 --log-level debug --access-log
