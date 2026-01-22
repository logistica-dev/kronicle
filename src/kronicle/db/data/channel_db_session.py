# kronicle/db/data/channel_db_session.py
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Optional

import asyncpg
from asyncpg import Connection, Pool
from asyncpg.exceptions import PostgresError

from kronicle.errors.error_types import DatabaseConnectionError
from kronicle.utils.dev_logs import log_d, log_e, log_w

mod = "db_session"


class ChannelDbSession:
    """
    Singleton DB session manager with async init, error interception, and transactions.

    Responsibilities:
    - Maintain a single connection or pool
    - Async initialization
    - JSONB codec setup
    - Transaction context manager
    - Optional automatic error interception/logging
    """

    _instance: Optional["ChannelDbSession"] = None
    _initialized: bool = False

    def __new__(cls, db_url: str | None = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        db_url: str | None = None,
        *,
        use_pool: bool = True,
        min_size: int = 1,
        max_size: int = 10,
        intercept_errors: bool = True,
        logger: Callable[[str, str], None] | None = None,
    ):
        if getattr(self, "_initialized", False):
            return

        self.db_url = db_url or getattr(self, "_db_url", None)
        if not self.db_url:
            raise ValueError("DBSession must be initialized with a db_url")

        self.use_pool = use_pool
        self.min_size = min_size
        self.max_size = max_size
        self.intercept_errors = intercept_errors
        self.logger = logger or log_w

        self._pool: Optional[Pool] = None
        self._conn: Optional[Connection] = None
        self._initialized = True

    # ----------------------------------------------------------------------------------------------
    # Async init
    # ----------------------------------------------------------------------------------------------
    async def init_async(self):
        """Async initialization: establishes connection/pool and sets JSONB codec."""
        if self.use_pool:
            self._pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=self.min_size,
                max_size=self.max_size,
                statement_cache_size=0,
                init=self._set_jsonb_codec,
            )
            async with self._pool.acquire() as conn:
                await self._set_jsonb_codec(conn)
        else:
            self._conn = await asyncpg.connect(self.db_url)
            if not self._conn:
                raise DatabaseConnectionError("Could not establish DB connection")
            await self._set_jsonb_codec(self._conn)
        # log_d(mod, "DBSession initialized")

    async def _set_jsonb_codec(self, conn: Connection):
        await conn.set_type_codec(
            "jsonb",
            schema="pg_catalog",
            encoder=lambda v: v,  # asyncpg handles dict -> JSONB
            decoder=lambda v: v,
        )

    # ----------------------------------------------------------------------------------------------
    # Connection / transaction
    # ----------------------------------------------------------------------------------------------
    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        """Yield a live connection (pool or single)."""
        if self._pool:
            async with self._pool.acquire() as conn:
                yield conn
        elif self._conn:
            yield self._conn
        else:
            raise DatabaseConnectionError("DBSession is not initialized")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Connection]:
        """Yield a connection inside a transaction context."""
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    # ----------------------------------------------------------------------------------------------
    # Health check
    # ----------------------------------------------------------------------------------------------
    async def ping(self) -> bool:
        """
        Check if the database connection is alive.
        Returns True if successful, False if an error occurs.
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
        func: Callable[[Connection], Any],
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
        if self._conn:
            await self._conn.close()
            self._conn = None
            log_d(mod, "DBSession connection closed")
