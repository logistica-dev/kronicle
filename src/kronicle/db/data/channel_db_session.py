# kronicle/db/data/channel_db_session.py
from __future__ import annotations

from contextlib import asynccontextmanager
from json import dumps, loads
from typing import Any, AsyncIterator, Callable, Optional

from asyncpg import Connection, Pool, create_pool
from asyncpg.exceptions import PostgresError
from asyncpg.pool import PoolConnectionProxy

from kronicle.errors.error_types import DatabaseConnectionError
from kronicle.utils.dev_logs import log_d, log_e, log_w

mod = "db_session"


class ChannelDbSession:
    """
    Singleton DB session manager with async init, error interception, and transactions.

    Responsibilities:
    - Connection pooling
    - Async initialization
    - JSONB codec setup
    - Transaction context manager
    - Optional automatic error interception/logging
    """

    _instance: Optional[ChannelDbSession] = None
    _initialized: bool = False

    def __new__(cls, db_url: str | None = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        db_url: str | None = None,
        *,
        min_size: int = 1,
        max_size: int = 10,
        intercept_errors: bool = True,
        logger: Callable[[str, str], None] | None = None,
    ):
        """
        Initialize the singleton DB session manager.

        Args:
            db_url: Database URL (postgresql://user:pass@host/db)
            min_size: Minimum pool connections
            max_size: Maximum pool connections
            intercept_errors: Whether to catch and log DB errors automatically
            logger: Optional logging callable (module, message)
        """
        if getattr(self, "_initialized", False):
            return

        self.db_url = db_url or getattr(self, "_db_url", None)
        if not self.db_url:
            raise ValueError("DBSession must be initialized with a db_url")

        self.min_size = min_size
        self.max_size = max_size
        self.intercept_errors = intercept_errors
        self.logger = logger or log_w

        self._pool: Optional[Pool] = None
        self._initialized = True

    # ----------------------------------------------------------------------------------------------
    # Async init / connection setup
    # ----------------------------------------------------------------------------------------------
    async def init_async(self):
        """
        Async initialization: establishes connection/pool and sets JSONB codec.
        """
        self._pool = await create_pool(
            dsn=self.db_url,
            min_size=self.min_size,
            max_size=self.max_size,
            statement_cache_size=0,
            init=self._set_jsonb_codec,
        )
        log_d(mod, "DBSession pool initialized")

    async def _set_jsonb_codec(self, conn: Connection | PoolConnectionProxy):
        """
        Initialize JSONB codec for a connection.

        Args:
            conn: Connection or PoolConnectionProxy
        """
        await conn.set_type_codec(
            "jsonb",
            schema="pg_catalog",
            encoder=dumps,  # converts Python dict -> JSON string # asyncpg handles dict -> JSONB
            decoder=loads,  # converts JSON string -> Python dict
        )

    # ----------------------------------------------------------------------------------------------
    # Connection / transaction
    # ----------------------------------------------------------------------------------------------
    @asynccontextmanager
    async def connection(self) -> AsyncIterator[PoolConnectionProxy]:
        """
        Yield a live connection from the pool

        Yields:
            asyncpg.Connection
        """
        if not self._pool:
            raise DatabaseConnectionError("DBSession is not initialized")

        async with self._pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[PoolConnectionProxy]:
        """
        Acquire a connection inside a transaction context.

        Yields:
            asyncpg.Connection
        """
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    # ----------------------------------------------------------------------------------------------
    # Health check
    # ----------------------------------------------------------------------------------------------
    async def ping(self) -> bool:
        """
        Check if the database connection is alive.
        Returns:
            True if successful, False if an error occurs.
        """
        try:
            async with self.connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except PostgresError as e:
            log_e(f"{mod}.ping", f"Database ping failed: {e}")
            return False
        except Exception as e:
            log_e(f"{mod}.ping", f"Unexpected ping error: {e}")
            return False

    # ----------------------------------------------------------------------------------------------
    # Utility: execute with error interception
    # ----------------------------------------------------------------------------------------------
    async def execute(
        self,
        func: Callable[[PoolConnectionProxy], Any],
        *,
        catch_errors: bool | None = None,
    ) -> Any:
        """
        Execute a coroutine using a connection, optionally intercepting DB errors.

        Example:
            result = await db_session.execute(lambda conn: MyModel.fetch_all(conn))
        """
        catch = self.intercept_errors if catch_errors is None else catch_errors
        try:
            async with self.connection() as conn:
                return await func(conn)
        except PostgresError as e:
            if catch:
                self.logger(mod, f"Database error intercepted: {e}")
                return None
            raise
        except Exception as e:
            if catch:
                self.logger(mod, f"Unexpected error intercepted: {e}")
                return None
            raise

    # ----------------------------------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------------------------------
    async def close(self):
        """Close connection/pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            log_d(mod, "DBSession pool closed")
