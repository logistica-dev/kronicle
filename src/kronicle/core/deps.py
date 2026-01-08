# kronicle/core/deps.py
import asyncio

from kronicle.controller.db_wrapper import DatabaseWrapper
from kronicle.controller.operation_gate import OperationGate
from kronicle.errors.error_types import AppStartupError, DatabaseConnectionError
from kronicle.utils.dev_logs import log_d, log_w

_db_wrapper: DatabaseWrapper | None = None
_operation_gate: OperationGate | None = None
_lock = asyncio.Lock()  # prevent race conditions


async def get_operation_gate(retries: int = 5, delay: float = 2.0) -> OperationGate:
    """
    Return the singleton OperationGate, ensuring DB is reachable with retries.
    The DatabaseManager is initialized once and kept alive for the app lifetime.
    """
    global _db_wrapper, _operation_gate
    here = "deps"

    async with _lock:  # prevent multiple concurrent initializations
        if _operation_gate:
            return _operation_gate

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
        _operation_gate = OperationGate(db=_db_wrapper)
        return _operation_gate


async def close_db():
    """
    Safely close the global DatabaseManager connection/pool.
    Can be called on shutdown or in tests.
    """
    global _db_wrapper, _operation_gate
    async with _lock:
        if _db_wrapper is not None:
            try:
                await _db_wrapper.close_connection()
                log_d("deps", "Database connection/pool closed successfully")
            except Exception as e:
                log_w("deps", f"Error closing database: {e}")
            finally:
                _db_wrapper = None
                _operation_gate = None


async def main():
    try:
        log_d("test_main", "Starting test of OperationGate and DBManager...")
        controller = await get_operation_gate(retries=3, delay=1.0)
        log_d("test_main", f"OperationGate initialized: {controller}")

        # Optionally test a simple fetch
        rows = await controller.fetch_all_metadata()
        log_d("test_main", f"Fetched {len(rows)} metadata rows")

    finally:
        await close_db()
        log_d("test_main", "DB connection closed, test complete.")


if __name__ == "__main__":
    asyncio.run(main())
