# docker/app/entrypoint.py
import os
from asyncio import run, sleep

import uvicorn
from asyncpg import exceptions

from kronicle.main import app  # your FastAPI instance
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


if __name__ == "__main__":
    run(wait_for_db())
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("KRONICLE_PORT", 8000)),
        log_level="info",
    )
