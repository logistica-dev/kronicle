# kronicle/core/deps.py
import asyncio

from kronicle.controller.db_wrapper import DatabaseWrapper
from kronicle.controller.sensor_controller import SensorController
from kronicle.types.errors import AppStartupError, DatabaseConnectionError
from kronicle.utils.dev_logs import log_d, log_w

_db_wrapper: DatabaseWrapper | None = None
_sensor_controller: SensorController | None = None
_lock = asyncio.Lock()  # prevent race conditions


async def get_sensor_controller(retries: int = 5, delay: float = 2.0) -> SensorController:
    """
    Return the singleton SensorController, ensuring DB is reachable with retries.
    The DatabaseManager is initialized once and kept alive for the app lifetime.
    """
    global _db_wrapper, _sensor_controller
    here = "deps"

    async with _lock:  # prevent multiple concurrent initializations
        if _sensor_controller:
            return _sensor_controller

        if _db_wrapper is None:
            # Retry loop to ensure DB is reachable
            last_exception: DatabaseConnectionError | None = None
            for attempt in range(1, retries + 1):
                try:
                    _db_wrapper = await DatabaseWrapper.init_async()
                    log_d(here, f"DB connection successful on attempt {attempt}")
                    break
                except DatabaseConnectionError as e:
                    last_exception = e
                    log_w(here, f"DB connection failed on attempt {attempt}: {e}")
                    if attempt < retries:
                        await asyncio.sleep(delay)
                    else:
                        raise AppStartupError("Cannot connect to database after multiple retries") from last_exception
        assert _db_wrapper
        # DB is reachable, create singleton controller
        _sensor_controller = SensorController(db=_db_wrapper)
        return _sensor_controller


async def close_db():
    """
    Safely close the global DatabaseManager connection/pool.
    Can be called on shutdown or in tests.
    """
    global _db_wrapper, _sensor_controller
    async with _lock:
        if _db_wrapper is not None:
            try:
                await _db_wrapper.close_connection()
                log_d("deps", "Database connection/pool closed successfully")
            except Exception as e:
                log_w("deps", f"Error closing database: {e}")
            finally:
                _db_wrapper = None
                _sensor_controller = None


async def main():
    try:
        log_d("test_main", "Starting test of SensorController and DBManager...")
        controller = await get_sensor_controller(retries=3, delay=1.0)
        log_d("test_main", f"SensorController initialized: {controller}")

        # Optionally test a simple fetch
        rows = await controller.fetch_all_metadata()
        log_d("test_main", f"Fetched {len(rows)} metadata rows")

    finally:
        await close_db()
        log_d("test_main", "DB connection closed, test complete.")


if __name__ == "__main__":
    asyncio.run(main())
