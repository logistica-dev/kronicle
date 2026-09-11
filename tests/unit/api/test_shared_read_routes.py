# tests/unit/api/test_shared_read_routes.py
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from kronicle.api.shared_read_routes import (
    fetch_all_channels_metadata,
    fetch_channel,
    fetch_channel_columns,
    fetch_channel_rows,
)
from kronicle.schemas.filters.row_query_filter import RowQueryFilter


class TestFetchAllChannelsMetadata:
    @pytest.mark.asyncio
    async def test_filters_by_name_priority(self):
        data_service = AsyncMock()
        data_service.fetch_metadata_by_name.return_value = "by-name"
        result = await fetch_all_channels_metadata(name="myname", tags=None, metadata=None, data_service=data_service)
        data_service.fetch_metadata_by_name.assert_awaited_once_with(name="myname")
        data_service.fetch_all_metadata.assert_not_awaited()
        assert result == "by-name"

    @pytest.mark.asyncio
    async def test_filters_by_tags(self):
        data_service = AsyncMock()
        data_service.fetch_metadata_by_tags.return_value = "by-tags"
        result = await fetch_all_channels_metadata(
            name=None, tags=["color:red"], metadata=None, data_service=data_service
        )
        data_service.fetch_metadata_by_tags.assert_awaited_once_with(tags=["color:red"])
        assert result == "by-tags"

    @pytest.mark.asyncio
    async def test_filters_by_user_metadata(self):
        data_service = AsyncMock()
        data_service.fetch_metadata_by_user_meta.return_value = "by-meta"
        result = await fetch_all_channels_metadata(
            name=None, tags=None, metadata=["location:lab"], data_service=data_service
        )
        data_service.fetch_metadata_by_user_meta.assert_awaited_once_with(user_meta=["location:lab"])
        assert result == "by-meta"

    @pytest.mark.asyncio
    async def test_returns_all_when_no_filter(self):
        data_service = AsyncMock()
        data_service.fetch_all_metadata.return_value = "all"
        result = await fetch_all_channels_metadata(name=None, tags=None, metadata=None, data_service=data_service)
        data_service.fetch_all_metadata.assert_awaited_once()
        assert result == "all"


class TestFetchChannel:
    @pytest.mark.asyncio
    async def test_fetches_metadata(self):
        data_service = AsyncMock()
        data_service.fetch_metadata.return_value = "meta"
        channel_id = uuid4()
        assert await fetch_channel(channel_id=channel_id, data_service=data_service) == "meta"
        data_service.fetch_metadata.assert_awaited_once_with(channel_id)


class TestFetchChannelRows:
    @patch("kronicle.api.shared_read_routes.RowRequestFilter.from_query", return_value="request_filter")
    @pytest.mark.asyncio
    async def test_fetches_rows(self, mock_from_query):
        data_service = AsyncMock()
        data_service.fetch_rows.return_value = "rows"
        channel_id = uuid4()
        filter_param = RowQueryFilter()
        result = await fetch_channel_rows(channel_id=channel_id, filter=filter_param, data_service=data_service)
        mock_from_query.assert_called_once_with(filter_param)
        data_service.fetch_rows.assert_awaited_once_with(channel_id, filter="request_filter")
        assert result == "rows"


class TestFetchChannelColumns:
    @patch("kronicle.api.shared_read_routes.RowRequestFilter.from_query", return_value="request_filter")
    @pytest.mark.asyncio
    async def test_fetches_columns(self, mock_from_query):
        data_service = AsyncMock()
        data_service.fetch_columns.return_value = "columns"
        channel_id = uuid4()
        filter_param = RowQueryFilter()
        result = await fetch_channel_columns(channel_id=channel_id, filter=filter_param, data_service=data_service)
        mock_from_query.assert_called_once_with(filter_param)
        data_service.fetch_columns.assert_awaited_once_with(channel_id, filter="request_filter")
        assert result == "columns"
