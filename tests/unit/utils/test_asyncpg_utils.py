# tests/unit/utils/test_asyncpg_utils.py
from unittest.mock import AsyncMock, patch

import pytest
from asyncpg import Connection
from asyncpg.pool import PoolConnectionProxy

from kronicle.utils import asyncpg_utils

# --------------------------------------------------------------------------------------
# parse_pg_dsn
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dsn,expected",
    [
        ("postgresql://alice:secret@localhost:5432/mydb", ("alice", "localhost:5432", "mydb")),
        ("postgresql://bob:pass@dbserver/myotherdb", ("bob", "dbserver:5432", "myotherdb")),  # default port
    ],
)
def test_parse_pg_dsn_valid(dsn, expected):
    assert asyncpg_utils.parse_pg_dsn(dsn) == expected


@pytest.mark.parametrize(
    "dsn",
    [
        "",  # empty string
        "postgres://alice@localhost/db",  # wrong scheme
        "postgresql://alice@/db",  # missing host
        "invalid_string",
    ],
)
def test_parse_pg_dsn_invalid(dsn):
    with pytest.raises(ValueError):
        asyncpg_utils.parse_pg_dsn(dsn)


# --------------------------------------------------------------------------------------
# verify_connection
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verify_connection_success():
    dsn = "postgresql://alice:secret@localhost:5432/mydb"
    mock_conn = AsyncMock()
    with (
        patch("kronicle.utils.asyncpg_utils.connect", new=mock_conn),
        patch("kronicle.utils.asyncpg_utils.log_d") as mock_log,
    ):
        mock_conn.return_value.close = AsyncMock()
        result = await asyncpg_utils.verify_connection(dsn)
        assert result == ("alice", "localhost:5432", "mydb")
        mock_conn.return_value.close.assert_awaited_once()
        mock_log.assert_called_once()


@pytest.mark.asyncio
async def test_verify_connection_empty_dsn():
    with pytest.raises(ValueError, match="Input connection string should not be null"):
        await asyncpg_utils.verify_connection("")


@pytest.mark.asyncio
async def test_verify_connection_malformed_dsn():
    bad_dsn = "postgresql://alice@localhost"  # missing dbname
    with pytest.raises(ValueError, match="Input connection string is malformed"):
        await asyncpg_utils.verify_connection(bad_dsn)


@pytest.mark.asyncio
async def test_verify_connection_cannot_connect():
    dsn = "postgresql://alice:secret@localhost:5432/mydb"
    with patch("kronicle.utils.asyncpg_utils.connect", new_callable=AsyncMock) as mock_connect:
        mock_connect.side_effect = Exception("DB unreachable")
        with pytest.raises(ConnectionError, match="Cannot connect to DB 'mydb' as user 'alice'"):
            await asyncpg_utils.verify_connection(dsn)


# --------------------------------------------------------------------------------------
# table_exists
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_table_exists_true():
    db = AsyncMock(spec=Connection)
    db.fetchval.return_value = True
    result = await asyncpg_utils.table_exists(db, "public", "mytable")
    assert result is True
    db.fetchval.assert_awaited_once()


@pytest.mark.asyncio
async def test_table_exists_false():
    db = AsyncMock(spec=Connection)
    db.fetchval.return_value = False
    result = await asyncpg_utils.table_exists(db, "public", "mytable")
    assert result is False
    db.fetchval.assert_awaited_once()


@pytest.mark.asyncio
async def test_table_exists_with_pool_connection_proxy():
    db = AsyncMock(spec=PoolConnectionProxy)
    db.fetchval.return_value = True
    result = await asyncpg_utils.table_exists(db, "public", "mytable")
    assert result is True
    db.fetchval.assert_awaited_once()
