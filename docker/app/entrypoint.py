# docker/app/entrypoint.py
import os
from asyncio import run, sleep
from sys import exit, stderr

import uvicorn
from asyncpg import CannotConnectNowError, ConnectionDoesNotExistError, InvalidCatalogNameError, PostgresError

from kronicle.db.core.models.channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.zone import Zone
from kronicle.db.data.channel_repository import ChannelMetadata
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_user import RbacUser
from scripts.init.init import main as init_script  # type: ignore
from scripts.utils.read_conf import KronicleConf  # type: ignore

# --------------------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------------------
conf = KronicleConf.read_conf()
DB_NAME = conf.db.name


SCHEMAS_TO_CHECK = {
    CoreEntity.namespace(): [Channel.tablename(), Zone.tablename()],  # example key tables in core schema
    RbacEntity.namespace(): [RbacUser.tablename()],  # example key tables in rbac schema
    ChannelMetadata.namespace(): [ChannelMetadata.tablename()],  # example key tables in data schema
}


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
        ):
            await sleep(1)
            waited += 1
            print(f"[entry] Waiting for DB server... {waited}s")
    print(f"[entry] ERROR: DB server not ready after {timeout}s", file=stderr)
    exit(1)


# --------------------------------------------------------------------------------------------------
# Check if target DB exists
# --------------------------------------------------------------------------------------------------
async def db_exists() -> bool:
    try:
        async with conf.db.session(db_name="postgres") as conn:
            exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", DB_NAME)
            return exists is not None
    except PostgresError as e:
        print(f"[entry] Error checking database existence: {e}")
        return False


# --------------------------------------------------------------------------------------------------
# Check if required tables exist
# --------------------------------------------------------------------------------------------------
async def tables_exists():
    """Return True if the DB is already initialized (example: channel metadata table exists)."""
    try:
        async with conf.db.session() as conn:
            for schema, tables in SCHEMAS_TO_CHECK.items():
                for table in tables:
                    qualified = f"{schema}.{table}"
                    exists = await conn.fetchval("SELECT to_regclass($1)", qualified)
                    if exists is None:
                        print(f"[entry] Missing table: {qualified}")
                        return False

    except PostgresError as e:
        print(f"[entry] Error checking schema objects: {e}")
        return False

    return True


# --------------------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------------------
async def wait_and_init():
    print("[entry] Waiting for Postrgesql...")
    await wait_for_db_server()

    print("[entry] Initialize DB if needed (synchronous scripts)")
    init_needed = False
    if not await db_exists():
        print("[entry] DB does not exist.")
        init_needed = True
    elif not await tables_exists():
        print("[entry] DB not initialized.")
        init_needed = True
    if init_needed:
        print(" [entry] Running init script...")
        try:
            init_script()
        except Exception as e:
            print(f"[entry] DB init script failed: {e}", file=stderr)
            exit(1)
    else:
        print(f"[entry] Database '{DB_NAME}' already exists. Skipping init.")


if __name__ == "__main__":
    run(wait_and_init())

    print("[entry] Launching Uvicorn/FastAPI server...")
    from kronicle.main import app  # your FastAPI instance

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("KRONICLE_PORT", 8000)),
        log_level="error",
    )
