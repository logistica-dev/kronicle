# tests/db/data/models/test_channel_resource.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pytest import raises

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_resource import ChannelResource
from kronicle.db.data.models.channel_timeseries import ChannelTimeseries
from kronicle.errors.error_types import (
    BadRequestError,
    ConflictError,
    NotFoundError,
)
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.schemas.payload.processed_payload import ProcessedPayload

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


@pytest.fixture
def mock_metadata():
    m = MagicMock(spec=ChannelMetadata)
    m.channel_id = uuid4()
    m.channel_schema = MagicMock()
    m.table_exists = AsyncMock(return_value=True)
    m.create = AsyncMock()
    m.exists = AsyncMock(return_value=False)
    return m


@pytest.fixture
def mock_timeseries(mock_metadata):
    ts = MagicMock(spec=ChannelTimeseries)
    ts.channel_id = mock_metadata.channel_id
    ts.channel_schema = mock_metadata.channel_schema
    ts.get_db_tuples.return_value = [("row",)]
    ts.verify_db_schema = MagicMock()
    ts.table_name = "ts_table"
    ts.table_exists = AsyncMock(return_value=False)
    ts.ensure_table = AsyncMock()
    ts.fetch = AsyncMock()
    ts.insert = AsyncMock()
    ts.op_feedback = OpFeedback()
    return ts


@pytest.fixture
def mock_payload():
    p = MagicMock(spec=ProcessedPayload)
    p.channel_id = str(uuid4())
    p.channel_schema = MagicMock()
    p.name = "My Channel"
    p.metadata = {"a": 1}
    p.tags = {"b": 2}
    p.rows = [{"time": "2024-01-01T00:00:00Z", "value": 1}]
    p.op_feedback = OpFeedback()  # ← ADD THIS
    return p


@pytest.fixture
def mock_conn():
    return AsyncMock()


# --------------------------------------------------------------------------------------
# Constructor
# --------------------------------------------------------------------------------------


def test_init_with_timeseries(mock_metadata, mock_timeseries):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    assert resource.metadata is mock_metadata
    assert resource.timeseries is mock_timeseries


def test_init_creates_timeseries_if_missing(mock_metadata):
    with patch("kronicle.db.data.models.channel_resource.ChannelTimeseries") as ts_cls:
        ts_instance = MagicMock()
        ts_cls.return_value = ts_instance

        resource = ChannelResource(mock_metadata)

        ts_cls.assert_called_once()
        assert resource.timeseries is ts_instance


# --------------------------------------------------------------------------------------
# from_processed
# --------------------------------------------------------------------------------------


def test_from_processed_success(mock_payload):
    fake_meta = MagicMock(spec=ChannelMetadata)
    fake_meta.channel_id = uuid4()
    fake_meta.channel_schema = MagicMock()

    with patch.object(ChannelMetadata, "from_processed", return_value=fake_meta):
        with patch("kronicle.db.data.models.channel_resource.ChannelTimeseries") as ts_cls:
            ts_instance = MagicMock()
            ts_cls.return_value = ts_instance

            resource = ChannelResource.from_processed(mock_payload)

            ts_instance.add_rows.assert_called_once()
            assert isinstance(resource, ChannelResource)


def test_from_processed_metadata_failure(mock_payload):
    with patch.object(ChannelMetadata, "from_processed", side_effect=Exception("bad meta")):
        with raises(BadRequestError):
            ChannelResource.from_processed(mock_payload)


def test_from_processed_strict_propagation(mock_payload):
    fake_meta = MagicMock(spec=ChannelMetadata)
    fake_meta.channel_id = uuid4()
    fake_meta.channel_schema = MagicMock()

    with patch.object(ChannelMetadata, "from_processed", return_value=fake_meta):
        with patch("kronicle.db.data.models.channel_resource.ChannelTimeseries") as ts_cls:
            ts_instance = MagicMock()
            ts_cls.return_value = ts_instance

            ChannelResource.from_processed(
                mock_payload,
                strict=True,
            )

            ts_instance.add_rows.assert_called_once_with(
                mock_payload.rows,
                strict=True,
            )


# --------------------------------------------------------------------------------------
# Property passthrough
# --------------------------------------------------------------------------------------


def test_schema_property(mock_metadata, mock_timeseries):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    assert resource.channel_schema == mock_metadata.channel_schema


def test_channel_id_property(mock_metadata, mock_timeseries):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    assert resource.channel_id == mock_metadata.channel_id


def test_get_db_tuples(mock_metadata, mock_timeseries):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    assert resource.get_db_tuples() == [("row",)]


def test_verify_db_schema(mock_metadata, mock_timeseries):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    resource.verify_db_schema({"col": "type"})
    mock_timeseries.verify_db_schema.assert_called_once()


# --------------------------------------------------------------------------------------
# Metadata DB access
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_table_exists(mock_metadata, mock_timeseries, mock_conn):
    resource = ChannelResource(mock_metadata, mock_timeseries)
    result = await resource.metadata_table_exists(mock_conn)
    assert result is True


@pytest.mark.asyncio
async def test_fetch_metadata_not_found(mock_conn):
    with patch.object(ChannelMetadata, "fetch_by_id", new=AsyncMock(return_value=None)):
        with raises(NotFoundError):
            await ChannelResource._fetch_metadata(mock_conn, uuid4())


# --------------------------------------------------------------------------------------
# Timeseries DB access
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insert_rows(mock_metadata, mock_timeseries, mock_conn):
    resource = ChannelResource(mock_metadata, mock_timeseries)

    await resource.insert_rows(mock_conn)

    mock_timeseries.ensure_table.assert_called_once()
    mock_timeseries.insert.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_rows(mock_metadata, mock_timeseries, mock_conn):
    resource = ChannelResource(mock_metadata, mock_timeseries)

    result = await resource.fetch_rows(mock_conn)

    mock_timeseries.fetch.assert_called_once()
    assert result is resource


# --------------------------------------------------------------------------------------
# create()
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_conflict_metadata_exists(mock_metadata, mock_timeseries, mock_conn):
    mock_metadata.exists = AsyncMock(return_value=True)
    resource = ChannelResource(mock_metadata, mock_timeseries)

    with raises(ConflictError):
        await resource.create(mock_conn)


@pytest.mark.asyncio
async def test_create_success(mock_metadata, mock_timeseries, mock_conn):
    mock_metadata.exists = AsyncMock(return_value=False)
    mock_timeseries.table_exists = AsyncMock(return_value=False)

    resource = ChannelResource(mock_metadata, mock_timeseries)

    result = await resource.create(mock_conn)

    mock_metadata.create.assert_called_once_with(mock_conn)
    mock_timeseries.insert.assert_called_once()
    assert result is resource


# --------------------------------------------------------------------------------------
# fetch()
# --------------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_fetch_success(mock_conn):
    fake_meta = MagicMock(spec=ChannelMetadata)
    fake_meta.channel_id = uuid4()
    fake_meta.channel_schema = MagicMock()

    resource = ChannelResource(fake_meta)
    resource.row_nb = 1  # <-- must be truthy so fetch_rows is called

    # create a mock for _fetch_metadata that returns your resource
    resource.fetch_rows = AsyncMock(return_value=resource)
    mock_fetch_meta = AsyncMock(return_value=resource)

    # patch _fetch_metadata
    with patch.object(ChannelResource, "_fetch_metadata", mock_fetch_meta):
        # patch fetch_rows on the resource returned by _fetch_metadata
        result = await ChannelResource.fetch(mock_conn, fake_meta.channel_id)

        # assert _fetch_metadata was awaited
        mock_fetch_meta.assert_awaited_once_with(mock_conn, fake_meta.channel_id)

        # assert fetch_rows was awaited
        resource.fetch_rows.assert_awaited_once_with(mock_conn, filter=None)

        # result is the resource instance
        assert result is resource
