import json
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from pytest import raises

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import ConflictError
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.types.iso_datetime import IsoDateTime

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


@pytest.fixture
def sample_schema():
    user_schema = {
        "time": "time",
        "temperature": "float",
    }
    return ChannelSchema.from_user_json(user_schema)


@pytest.fixture
def sample_metadata(sample_schema):
    return ChannelMetadata(
        channel_id=uuid4(),
        channel_schema=sample_schema,
        name="My Channel Name",
        user_metadata={"location": "lab"},
        tags={"room": 101},
    )


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    conn.execute = AsyncMock()
    return conn


# --------------------------------------------------------------------------------------
# Validation tests
# --------------------------------------------------------------------------------------


def test_name_is_normalized(sample_schema):
    m = ChannelMetadata(
        channel_id=uuid4(),
        channel_schema=sample_schema,
        name="My Channel Name",
    )
    assert m.name == "my_channel_name"


def test_metadata_none_becomes_empty_dict(sample_schema):
    m = ChannelMetadata(
        channel_id=uuid4(),
        channel_schema=sample_schema,
        user_metadata=None,
        tags=None,
    )
    assert m.user_metadata == {}
    assert m.tags == {}


def test_metadata_invalid_type_raises(sample_schema):
    with raises(TypeError):
        ChannelMetadata(
            channel_id=uuid4(),
            channel_schema=sample_schema,
            user_metadata="not_a_dict",  # type: ignore
        )


def test_empty_tag_key_raises(sample_schema):
    with raises(ValueError):
        ChannelMetadata(
            channel_id=uuid4(),
            channel_schema=sample_schema,
            tags={"": "bad"},
        )


def test_received_at_tzinfo(sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": IsoDateTime.now_local(),
    }
    obj = ChannelMetadata.from_db(row)
    assert isinstance(obj.received_at, IsoDateTime)
    assert obj.received_at.tzinfo is not None


# --------------------------------------------------------------------------------------
# Class helpers
# --------------------------------------------------------------------------------------


def test_namespace_and_table_name():
    assert ChannelMetadata.namespace() == "data"
    assert ChannelMetadata.table_name() == "channel_metadata"


def test_table_schema_is_copy():
    schema1 = ChannelMetadata.table_schema()
    schema1["new_col"] = "TEXT"
    schema2 = ChannelMetadata.table_schema()
    assert "new_col" not in schema2


def test_create_table_sql_contains_expected_parts():
    sql = ChannelMetadata.create_table_sql()
    assert "CREATE TABLE data.channel_metadata" in sql
    assert "channel_id UUID PRIMARY KEY" in sql


# --------------------------------------------------------------------------------------
# db_ready_values
# --------------------------------------------------------------------------------------


def test_db_ready_values(sample_metadata):
    values = sample_metadata.db_ready_values()

    assert values[0] == sample_metadata.channel_id
    assert isinstance(values[1], dict)  # schema JSON
    assert values[2] == sample_metadata.name
    assert values[3] == sample_metadata.user_metadata
    assert values[4] == sample_metadata.tags
    assert isinstance(values[5], IsoDateTime)


# --------------------------------------------------------------------------------------
# from_db
# --------------------------------------------------------------------------------------


def test_from_db_with_dict_json(sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "user_metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": IsoDateTime.now_local(),
    }

    obj = ChannelMetadata.from_db(row)
    assert obj.channel_id == sample_metadata.channel_id
    assert obj.user_metadata == sample_metadata.user_metadata
    assert obj.tags == sample_metadata.tags


def test_from_db_with_stringified_json(sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": json.dumps(sample_metadata.channel_schema.to_db_json()),
        "name": sample_metadata.name,
        "user_metadata": json.dumps(sample_metadata.user_metadata),
        "tags": json.dumps(sample_metadata.tags),
        "received_at": IsoDateTime.now_local(),
    }

    obj = ChannelMetadata.from_db(row)
    assert obj.user_metadata == sample_metadata.user_metadata
    assert obj.tags == sample_metadata.tags


# --------------------------------------------------------------------------------------
# from_processed
# --------------------------------------------------------------------------------------


def test_from_processed(sample_schema):
    processed = ProcessedPayload(
        channel_id=str(uuid4()),  # type: ignore
        channel_schema=sample_schema,
        name="Test Name",
        metadata={"a": 1},
        tags={"b": 2},
    )

    obj = ChannelMetadata.from_processed(processed)

    assert obj.name == "test_name"
    assert obj.user_metadata == {"a": 1}
    assert obj.tags == {"b": 2}


# --------------------------------------------------------------------------------------
# Async DB operations
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_success(sample_metadata, mock_conn):
    # fetch_by_id returns None (no existing row)
    # insert fetchrow returns a dict (fake DB row)
    mock_conn.fetchrow.side_effect = [
        None,  # fetch_by_id
        {
            "channel_id": sample_metadata.channel_id,
            "channel_schema": sample_metadata.channel_schema.to_db_json(),
            "name": sample_metadata.name,
            "metadata": sample_metadata.user_metadata,
            "tags": sample_metadata.tags,
            "received_at": sample_metadata.received_at,
        },
    ]

    result = await sample_metadata.create(mock_conn)
    assert result.channel_id == sample_metadata.channel_id


@pytest.mark.asyncio
async def test_create_conflict(sample_metadata, mock_conn, sample_schema):
    mock_conn.fetchrow.return_value = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": {"temp": "DOUBLE PRECISION", "room_id": "TEXT"},
    }

    with raises(ConflictError):
        await sample_metadata.create(mock_conn)


@pytest.mark.asyncio
async def test_update_success(sample_metadata, mock_conn):
    mock_conn.fetchrow.return_value = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "user_metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }

    await sample_metadata.update(mock_conn)

    assert mock_conn.fetchrow.called


@pytest.mark.asyncio
async def test_update_missing_row(sample_metadata, mock_conn):
    mock_conn.fetchrow.return_value = None

    with raises(ValueError):
        await sample_metadata.update(mock_conn)


@pytest.mark.asyncio
async def test_fetch_by_id_returns_none(mock_conn):
    mock_conn.fetchrow.return_value = None

    with patch.object(ChannelMetadata, "ensure_table", new=AsyncMock()):
        result = await ChannelMetadata.fetch_by_id(mock_conn, uuid4())

    assert result is None


@pytest.mark.asyncio
async def test_fetch_all(mock_conn, sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }

    mock_conn.fetch.return_value = [row]

    with patch.object(ChannelMetadata, "ensure_table", new=AsyncMock()):
        results = await ChannelMetadata.fetch_all(mock_conn)

    assert len(results) == 1
    assert isinstance(results[0], ChannelMetadata)
