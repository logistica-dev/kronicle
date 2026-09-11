# tests/unit/api/test_setup_routes.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import asyncio
import pytest

from kronicle.api.setup_routes import (
    ChannelBatchDeletePayload,
    batch_delete_channels,
    clone_channel,
    delete_channel,
    delete_channel_rows,
    get_column_types,
)
from kronicle.db.data.models.schema_registry import SchemaRegistry
from kronicle.schemas.filters.row_query_filter import RowQueryFilter


@pytest.fixture
def async_mock():
    return AsyncMock()


@patch("kronicle.api.setup_routes.RowRequestFilter.from_query", return_value="request_filter")
@pytest.mark.asyncio
async def test_delete_channel_rows_converts_filter(mock_from_query):
    channel_id = uuid4()
    core = MagicMock()
    data_service = AsyncMock()
    filter_param = RowQueryFilter()
    await delete_channel_rows(channel_id=channel_id, filter=filter_param, data_service=data_service)
    mock_from_query.assert_called_once_with(filter_param)
    data_service.delete_rows_for_channel.assert_awaited_once_with(channel_id, filter="request_filter")


@pytest.mark.asyncio
async def test_clone_channel():
    payload = MagicMock()
    data_service = AsyncMock()
    data_service.clone_channel.return_value = "cloned"
    assert await clone_channel(payload=payload, data_service=data_service) == "cloned"
    data_service.clone_channel.assert_awaited_once_with(payload)


@pytest.mark.asyncio
async def test_delete_channel():
    channel_id = uuid4()
    core = MagicMock()
    data_service = AsyncMock()
    data_service.delete_channel.return_value = "deleted"
    assert await delete_channel(channel_id=channel_id, data_service=data_service, core=core) == "deleted"
    core.delete_core_channel.assert_called_once_with(channel_id)
    data_service.delete_channel.assert_awaited_once_with(channel_id)


@pytest.mark.asyncio
async def test_batch_delete_channels():
    core = MagicMock()
    data_service = AsyncMock()
    data_service.delete_channels.return_value = ["deleted1", "deleted2"]
    ids = [uuid4(), uuid4()]
    payload = ChannelBatchDeletePayload(channel_ids=ids)
    result = await batch_delete_channels(payload=payload, data_service=data_service, core=core)
    assert core.delete_core_channel.call_count == 2
    data_service.delete_channels.assert_awaited_once_with(ids)
    assert result == ["deleted1", "deleted2"]


def test_get_column_types():
    assert asyncio.run(get_column_types()) == SchemaRegistry().allowed_types
