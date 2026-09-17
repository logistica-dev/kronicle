# tests/unit/api/test_shared_read_routes.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.api.shared_read_routes import (
    fetch_all_channels_metadata,
    fetch_channel,
    fetch_channel_columns,
    fetch_channel_rows,
)
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.schemas.filters.row_query_filter import RowQueryFilter
from kronicle.schemas.payload.response_payload import ResponsePayload


def _metadata_payload() -> ResponsePayload:
    return ResponsePayload(id=uuid4(), channel_schema=ChannelSchema.from_user_json({"time": "time"}))


class TestFetchAllChannelsMetadata:
    @pytest.mark.asyncio
    async def test_filters_by_name_priority(self):
        data_service = AsyncMock()
        payload = _metadata_payload()
        data_service.fetch_metadata_by_name.return_value = payload
        zone_id = uuid4()
        core = MagicMock()
        core_channel = MagicMock()
        core_channel.zone.id = zone_id
        core.get_core_channel.return_value = core_channel
        result = await fetch_all_channels_metadata(
            name="myname", tags=None, metadata=None, data_service=data_service, core=core
        )
        data_service.fetch_metadata_by_name.assert_awaited_once_with(name="myname")
        data_service.fetch_all_metadata.assert_not_awaited()
        assert isinstance(result, ResponsePayload)
        assert result.zone_id == zone_id

    @pytest.mark.asyncio
    async def test_filters_by_tags(self):
        data_service = AsyncMock()
        data_service.fetch_metadata_by_tags.return_value = "by-tags"
        core = MagicMock()
        result = await fetch_all_channels_metadata(
            name=None, tags=["color:red"], metadata=None, data_service=data_service, core=core
        )
        data_service.fetch_metadata_by_tags.assert_awaited_once_with(tags=["color:red"])
        assert result == "by-tags"

    @pytest.mark.asyncio
    async def test_filters_by_user_metadata(self):
        data_service = AsyncMock()
        data_service.fetch_metadata_by_user_meta.return_value = "by-meta"
        core = MagicMock()
        result = await fetch_all_channels_metadata(
            name=None, tags=None, metadata=["location:lab"], data_service=data_service, core=core
        )
        data_service.fetch_metadata_by_user_meta.assert_awaited_once_with(user_meta=["location:lab"])
        assert result == "by-meta"

    @pytest.mark.asyncio
    async def test_returns_all_when_no_filter(self):
        data_service = AsyncMock()
        data_service.fetch_all_metadata.return_value = "all"
        core = MagicMock()
        result = await fetch_all_channels_metadata(
            name=None, tags=None, metadata=None, data_service=data_service, core=core
        )
        data_service.fetch_all_metadata.assert_awaited_once()
        assert result == "all"


class TestFetchChannel:
    @pytest.mark.asyncio
    async def test_fetches_metadata_with_zone(self):
        data_service = AsyncMock()
        payload = _metadata_payload()
        data_service.fetch_metadata.return_value = payload
        zone_id = uuid4()
        core = MagicMock()
        core_channel = MagicMock()
        core_channel.zone.id = zone_id
        core.get_core_channel.return_value = core_channel
        result = await fetch_channel(channel_id=payload.id, data_service=data_service, core=core)
        data_service.fetch_metadata.assert_awaited_once_with(payload.id)
        core.get_core_channel.assert_called_once_with(payload.id)
        assert result.zone_id == zone_id

    @pytest.mark.asyncio
    async def test_leaves_zone_id_empty_when_no_core_channel(self):
        data_service = AsyncMock()
        payload = _metadata_payload()
        data_service.fetch_metadata.return_value = payload
        core = MagicMock()
        core.get_core_channel.return_value = None
        result = await fetch_channel(channel_id=payload.id, data_service=data_service, core=core)
        core.get_core_channel.assert_called_once_with(payload.id)
        assert result.zone_id is None


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
