# tests/unit/services/test_channel_service.py
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from kronicle.db.data.channel_repository import ChannelRepository
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.services.channel_service import ChannelService
from kronicle.types.iso_datetime import IsoDateTime

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_repo():
    repo = AsyncMock(spec=ChannelRepository)
    return repo


@pytest.fixture
def service(mock_repo):
    return ChannelService(channel_repository=mock_repo)


def make_payload():
    return InputPayload(
        channel_id=uuid4(),
        name="test-channel",
        rows=[{"time": IsoDateTime(), "value": 42}],
        channel_schema={"time": "int", "value": "int"},
    )


@pytest.fixture
def input_payload():
    return make_payload()


@pytest.mark.asyncio
async def test_ping(service, mock_repo):
    mock_repo.ping.return_value = True
    result = await service.ping()
    assert result is True
    mock_repo.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_channel(service, mock_repo, input_payload):
    fake_processed = AsyncMock(spec=ProcessedPayload)
    fake_response = AsyncMock(spec=ResponsePayload)

    with (
        patch(
            "kronicle.services.channel_service.ProcessedPayload.from_input",
            return_value=fake_processed,
        ),
        patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ),
    ):
        mock_repo.create_channel.return_value = fake_processed
        result = await service.create_channel(input_payload)
        assert result == fake_response
        mock_repo.create_channel.assert_awaited_once_with(fake_processed)


@pytest.mark.asyncio
async def test_update_metadata(service, mock_repo, input_payload):
    fake_channel = AsyncMock()
    fake_response = AsyncMock()

    mock_repo.fetch_metadata.return_value = fake_channel
    with (
        patch(
            "kronicle.services.channel_service.ProcessedPayload.from_input",
            return_value=fake_channel,
        ),
        patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ),
    ):
        mock_repo.update_metadata.return_value = fake_channel
        result = await service.update_metadata(input_payload)
        assert result == fake_response
        mock_repo.fetch_metadata.assert_awaited_once()
        mock_repo.update_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_upsert_metadata_creates_if_missing(service, mock_repo, input_payload):
    fake_response = AsyncMock()

    # simulate fetch_metadata raising exception to trigger creation
    mock_repo.fetch_metadata.side_effect = Exception("not found")

    with patch(
        "kronicle.services.channel_service.ChannelService.create_channel",
        return_value=fake_response,
    ):
        result = await service.upsert_metadata(input_payload)
        assert result == fake_response
        mock_repo.fetch_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_upsert_metadata_updates_if_exists(service, mock_repo, input_payload):
    fake_channel = AsyncMock()
    fake_response = AsyncMock()

    mock_repo.fetch_metadata.return_value = fake_channel
    mock_repo.update_metadata.return_value = fake_channel

    with (
        patch(
            "kronicle.services.channel_service.ProcessedPayload.from_input",
            return_value=fake_channel,
        ),
        patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ),
    ):
        result = await service.upsert_metadata(input_payload)
        assert result == fake_response
        mock_repo.fetch_metadata.assert_awaited_once()
        mock_repo.update_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_metadata(service, mock_repo):
    channel_id = uuid4()
    fake_channel = AsyncMock()
    fake_response = AsyncMock()
    mock_repo.fetch_channel.return_value = fake_channel

    with patch(
        "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
        return_value=fake_response,
    ):
        result = await service.fetch_metadata(channel_id)
        assert result == fake_response
        mock_repo.fetch_channel.assert_awaited_once_with(channel_id)


@pytest.mark.asyncio
async def test_insert_channel_rows(service, mock_repo, input_payload):
    # fake_processed = AsyncMock(spec=ProcessedPayload)
    fake_channel = AsyncMock()
    fake_response = AsyncMock()

    fake_channel.timeseries.to_user_rows.return_value = [{"time": "2026-04-02T11:53:27", "value": 42}]
    fake_channel.metadata = AsyncMock()  # just so ResponsePayload accesses metadata
    fake_channel.channel_schema = AsyncMock()

    mock_repo.fetch_channel.return_value = fake_channel
    mock_repo.insert_rows.return_value = fake_channel

    with patch(
        "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
        return_value=fake_response,
    ):
        result = await service.insert_channel_rows(input_payload)
        assert result == fake_response
        mock_repo.insert_rows.assert_awaited_once()


@pytest.mark.asyncio
async def test_insert_channel_rows_raises_if_no_rows(service, input_payload):
    input_payload.rows = []
    with pytest.raises(BadRequestError):
        await service._process_payload_for_insertion(input_payload)
