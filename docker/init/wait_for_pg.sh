#!/bin/sh
# docker/init/wait_for_pg.sh
set -e

MAX_WAIT=60
WAITED=0


echo "[init] Waiting for PostgreSQL at $KRONICLE_DB_HOST:$KRONICLE_DB_PORT..."

while ! pg_isready -h "$KRONICLE_DB_HOST" -p "$KRONICLE_DB_PORT" -U "$POSTGRES_USER" >/dev/null 2>&1
do
    WAITED=$((WAITED + 1))

    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        echo "ERROR: PostgreSQL not ready after ${MAX_WAIT}s" >&2
        exit 1
    fi

    echo "Waiting for DB... ${WAITED}s"
    sleep 1
done

echo "[init] PostgreSQL is ready"

exec python -m scripts.init.init
