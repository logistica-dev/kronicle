# docker/app/entrypoint.py
import asyncio

from asyncpg import exceptions

from scripts.utils.read_conf import KronicleConf  # type: ignore


async def wait_for_db(timeout: int = 60):
    conf = KronicleConf.read_conf()
    db = conf.db
    waited = 0
    while waited < timeout:
        try:
            async with db.session() as conn:
                await conn.fetchval("SELECT 1")
                print("DB is ready")
                return
        except exceptions.CannotConnectNowError:
            await asyncio.sleep(1)
            waited += 1
    raise RuntimeError(f"DB not ready after {timeout}s")


async def main():
    await wait_for_db()
    # run your init or app code here


if __name__ == "__main__":
    asyncio.run(main())
