# docker/app/entrypoint.py
import os
from asyncio import run, sleep

import uvicorn
from asyncpg import exceptions

from kronicle.db.core.models.channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.zone import Zone
from kronicle.db.data.channel_repository import ChannelMetadata
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.main import app  # your FastAPI instance
from scripts.init.init import main as init_script  # type: ignore
from scripts.utils.read_conf import KronicleConf  # type: ignore

conf = KronicleConf.read_conf()


async def wait_for_db(timeout: int = 60):
    db = conf.db
    waited = 0
    while waited < timeout:
        try:
            async with db.session() as conn:
                await conn.fetchval("SELECT 1")
                print("DB is ready")
                return
        except exceptions.CannotConnectNowError:
            await sleep(1)
            waited += 1
    raise RuntimeError(f"DB not ready after {timeout}s")


SCHEMAS_TO_CHECK = {
    CoreEntity.namespace(): [Channel.tablename(), Zone.tablename()],  # example key tables in core schema
    RbacEntity.namespace(): [RbacUser.tablename()],  # example key tables in rbac schema
    ChannelMetadata.namespace(): [ChannelMetadata.tablename()],  # example key tables in data schema
}


async def db_initialized():
    """Return True if the DB is already initialized (example: channel metadata table exists)."""
    conf = KronicleConf.read_conf()
    async with conf.db.session() as conn:
        for schema, tables in SCHEMAS_TO_CHECK.items():
            for table in tables:
                qualified_name = f"{schema}.{table}"
                try:
                    result = await conn.fetchrow(f"SELECT to_regclass('{qualified_name}') AS table_exists;")
                    if result["table_exists"] is None:
                        print(f"[entry] Table missing: {qualified_name}")
                        return False
                except exceptions.PostgresError as e:
                    print(f"Error checking table {qualified_name}: {e}")
                    return False
    return True


async def main():
    print("[entry] Awaiting for DB...")
    await wait_for_db()

    print("[entry] Initialize DB if needed (synchronous scripts)")
    if not await db_initialized():
        print("[entry] Database not initialized. Running init script...")
        init_script()

    print("[entry] Launching the FastAPI server...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("KRONICLE_PORT", 8000)),
        log_level="info",
    )


if __name__ == "__main__":
    run(main())
