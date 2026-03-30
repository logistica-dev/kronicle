# tests/db/data/models/test_channel_timeseries.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pytest import raises

from kronicle.db.data.models.channel_timeseries import ChannelTimeseries
from kronicle.errors.error_types import BadRequestError, NotFoundError

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


@pytest.fixture
def fake_schema():
    schema = MagicMock()
    schema.user_columns = {
        "temperature": MagicMock(db_type="FLOAT", optional=False),
        "humidity": MagicMock(db_type="FLOAT", optional=True),
    }

    def validate_row(row, from_user=True):
        if "temperature" not in row:
            raise ValueError("Missing temperature")
        # emulate normalized DB row
        return {
            "time": row["time"],
            "temperature": row["temperature"],
            "humidity": row.get("humidity"),
            "received_at": row.get("received_at", "now"),
        }

    schema.validate_row.side_effect = validate_row
    schema.get_usr_col_name.side_effect = lambda x: x
    return schema


@pytest.fixture
def ts(fake_schema):
    return ChannelTimeseries(uuid4(), fake_schema)


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.fetch = AsyncMock()
    conn.execute = AsyncMock()
    return conn


# --------------------------------------------------------------------------------------
# Basic behavior
# --------------------------------------------------------------------------------------


def test_table_name_format(ts):
    assert ts.table_name.startswith("channel_")
    assert "-" not in ts.table_name


def test_len_and_clear(ts):
    ts._rows = [{"a": 1}]
    assert len(ts) == 1
    ts.clear_rows()
    assert len(ts) == 0


# --------------------------------------------------------------------------------------
# add_rows
# --------------------------------------------------------------------------------------


def test_add_rows_success(ts):
    rows = [{"time": "t1", "temperature": 10}]
    ts.add_rows(rows)
    assert len(ts.rows) == 1


def test_add_rows_with_warnings(ts):
    rows = [
        {"time": "t1", "temperature": 10},
        {"time": "t2"},  # invalid
    ]
    ts.add_rows(rows)
    feedback = ts.op_feedback
    assert feedback.has_details
    assert len(feedback.details) > 0
    assert "row_2" == feedback.details[0].subfield
    assert len(ts.rows) == 1


def test_add_rows_strict_raises(ts):
    rows = [{"time": "t2"}]  # invalid
    with raises(BadRequestError):
        ts.add_rows(rows, strict=True)


def test_add_rows_empty_raises(ts):
    with raises(BadRequestError):
        ts.add_rows([])


def test_add_rows_all_invalid_raises(ts):
    rows = [{"time": "t2"}]
    with raises(BadRequestError):
        ts.add_rows(rows, strict=True)


# --------------------------------------------------------------------------------------
# get_db_tuples
# --------------------------------------------------------------------------------------


def test_get_db_tuples(ts):
    ts._rows = [
        {
            "time": "t1",
            "temperature": 10,
            "humidity": None,
            "received_at": "now",
        }
    ]
    tuples = ts.get_db_tuples()
    assert isinstance(tuples, list)
    assert len(tuples[0]) == len(ts._sql_insert_columns)


# --------------------------------------------------------------------------------------
# verify_db_schema
# --------------------------------------------------------------------------------------


def test_verify_db_schema_success(ts):
    db_columns = {
        "temperature": "FLOAT",
        "humidity": "FLOAT",
    }
    ts.verify_db_schema(db_columns)


def test_verify_db_schema_missing_column(ts):
    db_columns = {"temperature": "FLOAT"}
    with raises(ValueError):
        ts.verify_db_schema(db_columns)


def test_verify_db_schema_type_mismatch(ts):
    db_columns = {
        "temperature": "INTEGER",
        "humidity": "FLOAT",
    }
    with raises(ValueError):
        ts.verify_db_schema(db_columns)


def test_verify_db_schema_extra_column(ts):
    db_columns = {
        "temperature": "FLOAT",
        "humidity": "FLOAT",
        "extra": "TEXT",
    }
    with raises(ValueError):
        ts.verify_db_schema(db_columns)


# --------------------------------------------------------------------------------------
# to_user_rows
# --------------------------------------------------------------------------------------


def test_to_user_rows_skip_received(ts):
    ts._rows = [
        {
            "time": "t1",
            "temperature": 10,
            "humidity": 20,
            "received_at": "now",
        }
    ]
    rows = ts.to_user_rows(skip_received=True)
    assert "received_at" not in rows[0]


def test_to_user_rows_include_received(ts):
    ts._rows = [
        {
            "time": "t1",
            "temperature": 10,
            "humidity": 20,
            "received_at": "now",
        }
    ]
    rows = ts.to_user_rows(skip_received=False)
    assert "received_at" in rows[0]


# --------------------------------------------------------------------------------------
# create_table_sql
# --------------------------------------------------------------------------------------


def test_create_table_sql_contains_columns(ts):
    sql = ts.create_table_sql()
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "temperature FLOAT NOT NULL" in sql


# --------------------------------------------------------------------------------------
# ensure_table
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_table_creates_when_missing(ts, mock_conn):
    with patch(
        "kronicle.db.data.models.channel_timeseries.table_exists",
        new=AsyncMock(return_value=False),
    ):
        await ts.ensure_table(mock_conn)
        mock_conn.execute.assert_called_once()


@pytest.mark.asyncio
async def test_ensure_table_skips_when_exists(ts, mock_conn):
    with patch(
        "kronicle.db.data.models.channel_timeseries.table_exists",
        new=AsyncMock(return_value=True),
    ):
        await ts.ensure_table(mock_conn)
        mock_conn.execute.assert_not_called()


# --------------------------------------------------------------------------------------
# fetch
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_table_missing_returns_empty(ts, mock_conn):
    with patch.object(ts, "table_exists", new=AsyncMock(return_value=False)):
        result = await ts.fetch(mock_conn)
        assert result is ts
        assert len(ts.rows) == 0


@pytest.mark.asyncio
async def test_fetch_success(ts, mock_conn):
    fake_row = {
        "time": "t1",
        "temperature": 10,
        "humidity": 20,
        "received_at": "now",
    }

    with patch.object(ts, "table_exists", new=AsyncMock(return_value=True)):
        mock_conn.fetch.return_value = [fake_row]
        result = await ts.fetch(mock_conn, filter=RequestFilter())

        assert result is ts
        assert len(ts.rows) == 1


# --------------------------------------------------------------------------------------
# insert
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insert_no_rows_returns_self(ts, mock_conn):
    result = await ts.insert(mock_conn)
    assert result is ts


@pytest.mark.asyncio
async def test_insert_success(ts, mock_conn):
    ts._rows = [
        {
            "time": "t1",
            "temperature": 10,
            "humidity": 20,
            "received_at": "now",
        }
    ]

    with patch.object(ts, "ensure_table", new=AsyncMock()):
        result = await ts.insert(mock_conn)

    mock_conn.execute.assert_called_once()
    assert result is ts


@pytest.mark.asyncio
async def test_insert_strict_failure(ts, mock_conn):
    ts._rows = [
        {
            "time": "t1",
            "temperature": 10,
            "humidity": 20,
            "received_at": "now",
        }
    ]

    mock_conn.execute.side_effect = Exception("DB error")

    with patch.object(ts, "ensure_table", new=AsyncMock()):
        with raises(BadRequestError):
            await ts.insert(mock_conn, strict=True)


# --------------------------------------------------------------------------------------
# delete
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_table_missing_raises(ts, mock_conn):
    with patch.object(ts, "table_exists", new=AsyncMock(return_value=False)):
        with raises(NotFoundError):
            await ts.delete(mock_conn)


@pytest.mark.asyncio
async def test_delete_success(ts, mock_conn):
    fake_row = {
        "time": "t1",
        "temperature": 10,
        "humidity": 20,
        "received_at": "now",
    }

    with patch.object(ts, "table_exists", new=AsyncMock(return_value=True)):
        mock_conn.fetch.return_value = [fake_row]
        result = await ts.delete(mock_conn)

        assert result is ts
        assert len(ts.rows) == 1
