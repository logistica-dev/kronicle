# tests/unit/db/data/models/test_channel_metadata.py
from json import dumps
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from asyncpg import UniqueViolationError
from pytest import raises

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, ConflictError, DatabaseInstructionError
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.str_utils import uuid_to_str

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


def test_namespace_and_tablename():
    assert ChannelMetadata.namespace() == "data"
    assert ChannelMetadata.tablename() == "channel_metadata"


def test_table_schema_is_copy():
    schema1 = ChannelMetadata.table_schema()
    schema1["new_col"] = "TEXT"
    schema2 = ChannelMetadata.table_schema()
    assert "new_col" not in schema2


def test_create_table_sql_contains_expected_parts():
    sql = ChannelMetadata.create_table_sql()
    assert "CREATE TABLE IF NOT EXISTS data.channel_metadata" in sql
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
        "channel_schema": dumps(sample_metadata.channel_schema.to_db_json()),
        "name": sample_metadata.name,
        "user_metadata": dumps(sample_metadata.user_metadata),
        "tags": dumps(sample_metadata.tags),
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


def _full_row(meta):
    return {
        "channel_id": meta.channel_id,
        "channel_schema": meta.channel_schema.to_db_json(),
        "name": meta.name,
        "user_metadata": meta.user_metadata,
        "tags": meta.tags,
        "received_at": meta.received_at,
    }


# --------------------------------------------------------------------------------------
# to_json
# --------------------------------------------------------------------------------------


def test_to_json_returns_strings_and_filters_none(sample_metadata):
    result = sample_metadata.to_json()
    assert result["channel_id"] == uuid_to_str(sample_metadata.channel_id)
    assert result["name"] == sample_metadata.name
    assert result["user_metadata"] == sample_metadata.user_metadata


def test_to_json_none_metadata_becomes_empty_dicts(sample_schema):
    m = ChannelMetadata(
        channel_id=uuid4(),
        channel_schema=sample_schema,
        name=None,
        user_metadata=None,
        tags=None,
    )
    result = m.to_json()
    assert "name" not in result
    assert result["user_metadata"] == {}
    assert result["tags"] == {}


def test_str_is_json_string(sample_metadata):
    assert str(sample_metadata) == str(sample_metadata.to_json())


# --------------------------------------------------------------------------------------
# from_processed with channel_truth
# --------------------------------------------------------------------------------------


def test_from_processed_uses_channel_truth_when_schema_is_none(sample_schema):
    processed = ProcessedPayload.model_construct(
        channel_id=str(uuid4()),
        channel_schema=None,
        name="Test Name",
        metadata={"a": 1},
        tags={"b": 2},
        received_at=IsoDateTime.now_local(),
        rows=[],
        op_feedback=OpFeedback(),
    )
    obj = ChannelMetadata.from_processed(processed, channel_truth=sample_schema)
    assert obj.channel_schema is sample_schema
    assert obj.name == "test_name"


def test_from_processed_with_channel_truth_uses_truth(sample_schema):
    other_schema = ChannelSchema.from_user_json({"other": "int"})
    processed = ProcessedPayload(
        channel_id=str(uuid4()),  # type: ignore
        channel_schema=other_schema,
        name=None,
    )
    obj = ChannelMetadata.from_processed(processed, channel_truth=sample_schema)
    assert obj.channel_schema is sample_schema
    assert obj.name is None


def test_from_processed_equivalent_schema_raises(sample_schema):
    processed = ProcessedPayload(
        channel_id=str(uuid4()),  # type: ignore
        channel_schema=sample_schema,
        name=None,
    )
    with raises(BadRequestError):
        ChannelMetadata.from_processed(processed, channel_truth=sample_schema)


# --------------------------------------------------------------------------------------
# fetch_by_id
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_by_id_returns_object(mock_conn, sample_metadata):
    mock_conn.fetchrow.return_value = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }

    result = await ChannelMetadata.fetch_by_id(mock_conn, sample_metadata.channel_id)
    assert result is not None
    assert result.channel_id == sample_metadata.channel_id


# --------------------------------------------------------------------------------------
# fetch_by_name
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_by_name_returns_none(mock_conn):
    mock_conn.fetchrow.return_value = None
    assert await ChannelMetadata.fetch_by_name(mock_conn, "My Name") is None


@pytest.mark.asyncio
async def test_fetch_by_name_returns_object(mock_conn, sample_metadata):
    mock_conn.fetchrow.return_value = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }
    result = await ChannelMetadata.fetch_by_name(mock_conn, "My Name")
    assert result is not None
    assert result.name == sample_metadata.name


@pytest.mark.asyncio
async def test_fetch_by_name_normalizes_strict(mock_conn):
    mock_conn.fetchrow.return_value = None
    assert await ChannelMetadata.fetch_by_name(mock_conn, "123abc") is None
    mock_conn.fetchrow.assert_awaited_once()
    sql, arg = mock_conn.fetchrow.await_args.args
    assert arg == "channel_123abc"
    mock_conn.fetchrow.reset_mock()
    assert await ChannelMetadata.fetch_by_name(mock_conn, "my__chan") is None
    mock_conn.fetchrow.assert_awaited_once()
    _, arg = mock_conn.fetchrow.await_args.args
    assert arg == "my_chan"


@pytest.mark.asyncio
async def test_fetch_by_name_returns_none_for_degenerate_name(mock_conn):
    assert await ChannelMetadata.fetch_by_name(mock_conn, "___") is None
    mock_conn.fetchrow.assert_not_awaited()


# --------------------------------------------------------------------------------------
# fetch_by_tags / fetch_by_user_meta
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_by_tags_empty_returns_empty(mock_conn):
    assert await ChannelMetadata.fetch_by_tags(mock_conn, {}) == []
    mock_conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_by_tags_returns_list(mock_conn, sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }
    mock_conn.fetch.return_value = [row]
    results = await ChannelMetadata.fetch_by_tags(mock_conn, {"room": "101"})
    assert len(results) == 1


@pytest.mark.asyncio
async def test_fetch_by_user_meta_empty_returns_empty(mock_conn):
    assert await ChannelMetadata.fetch_by_user_meta(mock_conn, {}) == []
    mock_conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_by_user_meta_returns_list(mock_conn, sample_metadata):
    row = {
        "channel_id": sample_metadata.channel_id,
        "channel_schema": sample_metadata.channel_schema.to_db_json(),
        "name": sample_metadata.name,
        "metadata": sample_metadata.user_metadata,
        "tags": sample_metadata.tags,
        "received_at": sample_metadata.received_at,
    }
    mock_conn.fetch.return_value = [row]
    results = await ChannelMetadata.fetch_by_user_meta(mock_conn, {"location": "lab"})
    assert len(results) == 1


# --------------------------------------------------------------------------------------
# exists / delete
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists_true(mock_conn, sample_metadata):
    mock_conn.fetchrow.return_value = _full_row(sample_metadata)
    assert await sample_metadata.exists(mock_conn) is True


@pytest.mark.asyncio
async def test_exists_false(mock_conn, sample_metadata):
    mock_conn.fetchrow.return_value = None
    assert await sample_metadata.exists(mock_conn) is False


@pytest.mark.asyncio
async def test_delete_missing_returns_none(mock_conn, sample_metadata):
    mock_conn.fetchrow.return_value = None
    assert await sample_metadata.delete(mock_conn) is None


@pytest.mark.asyncio
async def test_delete_success(mock_conn, sample_metadata):
    mock_conn.fetchrow.side_effect = [
        _full_row(sample_metadata),  # fetch_by_id
        _full_row(sample_metadata),  # DELETE RETURNING
    ]
    result = await sample_metadata.delete(mock_conn)
    assert result is not None
    assert result.channel_id == sample_metadata.channel_id


@pytest.mark.asyncio
async def test_delete_row_disappeared_returns_none(mock_conn, sample_metadata):
    mock_conn.fetchrow.side_effect = [
        _full_row(sample_metadata),  # fetch_by_id
        None,  # DELETE RETURNING - row already gone
    ]
    assert await sample_metadata.delete(mock_conn) is None


# --------------------------------------------------------------------------------------
# create / update unique constraint violations
# --------------------------------------------------------------------------------------


def _unique_violation(constraint_name):
    violation = UniqueViolationError("duplicate key value violates unique constraint")
    violation.constraint_name = constraint_name  # type: ignore[attr-defined]
    return violation


@pytest.mark.asyncio
async def test_create_pkey_violation_raises_conflict(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [None, _unique_violation("channel_metadata_pkey")]
    with raises(ConflictError):
        await sample_metadata.create(mock_conn)


@pytest.mark.asyncio
async def test_create_name_violation_raises_conflict(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [None, _unique_violation("channel_metadata_name_key")]
    with raises(ConflictError):
        await sample_metadata.create(mock_conn)


@pytest.mark.asyncio
async def test_create_unknown_violation_raises_conflict(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [None, _unique_violation("some_other_constraint")]
    with raises(ConflictError):
        await sample_metadata.create(mock_conn)


@pytest.mark.asyncio
async def test_create_insert_returns_no_row(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [None, None]
    with raises(DatabaseInstructionError):
        await sample_metadata.create(mock_conn)


@pytest.mark.asyncio
async def test_update_name_violation_raises_conflict(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [
        _full_row(sample_metadata),  # fetch_by_id
        _unique_violation("channel_metadata_name_key"),
    ]
    with raises(ConflictError):
        await sample_metadata.update(mock_conn)


@pytest.mark.asyncio
async def test_update_unknown_violation_raises_conflict(sample_metadata, mock_conn):
    mock_conn.fetchrow.side_effect = [
        _full_row(sample_metadata),  # fetch_by_id
        _unique_violation("channel_metadata_something"),
    ]
    with raises(ConflictError):
        await sample_metadata.update(mock_conn)
