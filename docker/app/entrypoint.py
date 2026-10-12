# docker/app/entrypoint.py
"""Container entrypoint: wait for PostgreSQL, then serve the FastAPI app.

DB provisioning and migration are handled by the application itself at startup
(see ``KronicleApp.lifespan`` / ``MigrationOrchestrator``), so this entrypoint
only waits for the database to accept connections before launching uvicorn.
"""

import os
from asyncio import run, sleep
from sys import exit, stderr

import uvicorn
from asyncpg import (
    CannotConnectNowError,
    ConnectionDoesNotExistError,
    InvalidCatalogNameError,
    TooManyConnectionsError,
)

from scripts.utils.read_conf import KronicleConf  # type: ignore

# --------------------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------------------
conf = KronicleConf.read_conf()


# --------------------------------------------------------------------------------------------------
# Wait for DB server
# --------------------------------------------------------------------------------------------------
async def wait_for_db_server(timeout: int = 60):
    db = conf.db
    waited = 0
    while waited < timeout:
        try:
            async with db.session(db_name="postgres") as conn:
                await conn.fetchval("SELECT 1")
                print("[entry] PostgreSQL server is ready")
                return
        except (
            CannotConnectNowError,
            ConnectionDoesNotExistError,
            InvalidCatalogNameError,
            TooManyConnectionsError,
            ConnectionRefusedError,
            ConnectionResetError,
            TimeoutError,
        ):
            await sleep(1)
            waited += 1
            print(f"[entry] Waiting for DB server... {waited}s")
    print(f"[entry] ERROR: DB server not ready after {timeout}s", file=stderr)
    exit(1)


if __name__ == "__main__":  # pragma: no-cover
    run(wait_for_db_server())

    print("[entry] Launching Uvicorn/FastAPI server...")
    from kronicle.main import app  # FastAPI app factory

    uvicorn.run(
        app,
        factory=True,
        host="0.0.0.0",
        port=int(os.environ.get("KRONICLE_PORT", 8000)),
        log_level="error",
    )
