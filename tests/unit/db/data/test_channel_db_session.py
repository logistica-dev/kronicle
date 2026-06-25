# tests/unit/db/data/test_channel_db_session.py
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asyncpg.exceptions import PostgresError

from kronicle.db.data.channel_db_session import ChannelDbSession
from kronicle.errors.error_types import DatabaseConnectionError

pytestmark = pytest.mark.asyncio

TEST_DB_URL = "postgresql://user:pass@localhost:5432/testdb"


@pytest.fixture(autouse=True)
def reset_singleton():
    ChannelDbSession._instance = None
    ChannelDbSession._initialized = False
    yield
    ChannelDbSession._instance = None
    ChannelDbSession._initialized = False


@pytest.fixture
def mock_conn():
    return AsyncMock()


@pytest.fixture
def mock_pool(mock_conn):
    pool = MagicMock()
    acquire_cm = AsyncMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = acquire_cm
    return pool


@pytest.fixture
def session():
    return ChannelDbSession(db_url=TEST_DB_URL)


class TestSingleton:
    async def test_new_returns_same_instance(self):
        s1 = ChannelDbSession(db_url=TEST_DB_URL)
        s2 = ChannelDbSession(db_url=TEST_DB_URL)
        assert s1 is s2

    async def test_new_reuses_instance_with_different_db_url(self):
        s1 = ChannelDbSession(db_url=TEST_DB_URL)
        s2 = ChannelDbSession(db_url="postgresql://other:pass@localhost:5432/otherdb")
        assert s1 is s2
        assert s1.db_url == TEST_DB_URL


class TestInit:
    async def test_raises_value_error_without_db_url(self):
        with pytest.raises(ValueError, match="DBSession must be initialized with a db_url"):
            ChannelDbSession(db_url=None)

    async def test_sets_attributes_correctly(self):
        logger = MagicMock()
        s = object.__new__(ChannelDbSession)
        ChannelDbSession.__init__(s, db_url=TEST_DB_URL, min_size=2, max_size=5, intercept_errors=False, logger=logger)
        assert s.db_url == TEST_DB_URL
        assert s.min_size == 2
        assert s.max_size == 5
        assert s.intercept_errors is False
        assert s.logger is logger
        assert s._pool is None
        assert s._initialized is True

    async def test_uses_defaults(self, session):
        assert session.min_size == 1
        assert session.max_size == 10
        assert session.intercept_errors is True
        assert session.logger is not None

    async def test_returns_early_if_already_initialized(self):
        s1 = object.__new__(ChannelDbSession)
        ChannelDbSession.__init__(s1, db_url=TEST_DB_URL, min_size=5)
        ChannelDbSession._instance = s1
        s2 = ChannelDbSession(db_url=TEST_DB_URL)
        assert s2 is s1
        assert s2.min_size == 5


class TestInitAsync:
    async def test_creates_pool_via_create_pool(self, session):
        mock_pool = MagicMock()
        with patch(
            "kronicle.db.data.channel_db_session.create_pool", new_callable=AsyncMock, return_value=mock_pool
        ) as mock_create_pool:
            await session.init_async()
            mock_create_pool.assert_awaited_once_with(
                dsn=TEST_DB_URL,
                min_size=1,
                max_size=10,
                statement_cache_size=0,
                init=session._set_jsonb_codec,
            )
            assert session._pool is mock_pool

    async def test_set_jsonb_codec_calls_set_type_codec(self, session):
        mock_co = AsyncMock()
        await session._set_jsonb_codec(mock_co)
        mock_co.set_type_codec.assert_awaited_once_with(
            "jsonb",
            schema="pg_catalog",
            encoder=__import__("json").dumps,
            decoder=__import__("json").loads,
        )


class TestConnection:
    async def test_raises_database_connection_error_when_pool_is_none(self, session):
        session._pool = None
        with pytest.raises(DatabaseConnectionError, match="DBSession is not initialized"):
            async with session.connection():
                pass

    async def test_acquires_connection_from_pool(self, session, mock_pool, mock_conn):
        session._pool = mock_pool
        async with session.connection() as conn:
            assert conn is mock_conn
        mock_pool.acquire.assert_called_once()


class TestTransaction:
    async def test_acquires_connection_and_enters_transaction(self, session, mock_pool, mock_conn):
        mock_transaction = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=mock_transaction)
        session._pool = mock_pool
        async with session.transaction() as conn:
            assert conn is mock_conn
        mock_transaction.__aenter__.assert_awaited_once()
        mock_transaction.__aexit__.assert_awaited_once()

    async def test_logs_and_re_raises_postgres_error(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=PostgresError("connection lost"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool
        with patch("kronicle.db.data.channel_db_session.log_w") as mock_log:
            with pytest.raises(PostgresError, match="connection lost"):
                async with session.transaction():
                    pass
            mock_log.assert_called_once()


class TestPing:
    async def test_returns_true_on_success(self, session, mock_pool, mock_conn):
        session._pool = mock_pool
        result = await session.ping()
        assert result is True
        mock_conn.execute.assert_awaited_once_with("SELECT 1")

    async def test_returns_false_on_postgres_error(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=PostgresError("db down"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool
        with patch("kronicle.db.data.channel_db_session.log_e") as mock_log:
            result = await session.ping()
            assert result is False
            mock_log.assert_called_once()

    async def test_returns_false_on_generic_exception(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("unexpected"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool
        with patch("kronicle.db.data.channel_db_session.log_e") as mock_log:
            result = await session.ping()
            assert result is False
            mock_log.assert_called_once()


class TestExecute:
    async def test_calls_func_with_connection_and_returns_result(self, session, mock_pool, mock_conn):
        session._pool = mock_pool

        async def my_func(conn):
            return 42

        result = await session.execute(my_func)
        assert result == 42

    async def test_intercepts_postgres_error_and_returns_none(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=PostgresError("constraint violation"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool
        session.logger = MagicMock()

        async def my_func(conn):
            return 42

        result = await session.execute(my_func, catch_errors=True)
        assert result is None
        session.logger.assert_called_once()

    async def test_re_raises_when_catch_errors_false(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=PostgresError("constraint violation"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool

        async def my_func(conn):
            return 42

        with pytest.raises(PostgresError, match="constraint violation"):
            await session.execute(my_func, catch_errors=False)

    async def test_intercepts_generic_exception_and_returns_none(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("something went wrong"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool
        session.logger = MagicMock()

        async def my_func(conn):
            return 42

        result = await session.execute(my_func, catch_errors=True)
        assert result is None
        session.logger.assert_called_once()

    async def test_re_raises_generic_exception_when_catch_errors_false(self, session):
        mock_pool = MagicMock()
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("boom"))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = acquire_cm
        session._pool = mock_pool

        async def my_func(conn):
            return 42

        with pytest.raises(RuntimeError, match="boom"):
            await session.execute(my_func, catch_errors=False)


class TestClose:
    async def test_closes_pool(self, session):
        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()
        session._pool = mock_pool
        await session.close()
        mock_pool.close.assert_awaited_once()
        assert session._pool is None
