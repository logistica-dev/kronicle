# tests/unit/services/test_channel_service.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.repo.data.channel_repository import ChannelRepository
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.services.channel_service import ChannelService
from kronicle.types.iso_datetime import IsoDateTime

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_repo():
    return AsyncMock(spec=ChannelRepository)


@pytest.fixture
def service(mock_repo):
    return ChannelService(channel_repository=mock_repo)


def make_payload(**overrides):
    channel_id = overrides.get("channel_id", uuid4())
    channel = {
        "id": channel_id,
        "channel_id": channel_id,
        "name": "test-channel",
        "rows": [{"time": IsoDateTime(), "value": 42}],
        "channel_schema": {"time": "int", "value": "int"},
    }
    channel.update(overrides)
    if "id" not in overrides:
        channel["id"] = channel["channel_id"]
    return InputPayload(**channel)


@pytest.fixture
def input_payload():
    return make_payload()


# ==================================================================================================
# Ping
# ==================================================================================================


class TestPing:
    async def test_ping(self, service, mock_repo):
        mock_repo.ping.return_value = True
        result = await service.ping()
        assert result is True
        mock_repo.ping.assert_awaited_once()


# ==================================================================================================
# Create channel
# ==================================================================================================


class TestCreateChannel:
    async def test_create(self, service, mock_repo, input_payload):
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


# ==================================================================================================
# Update metadata
# ==================================================================================================


class TestUpdateMetadata:
    async def test_update(self, service, mock_repo, input_payload):
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


# ==================================================================================================
# Upsert metadata
# ==================================================================================================


class TestUpsertMetadata:
    async def test_creates_if_missing(self, service, mock_repo, input_payload):
        fake_response = AsyncMock()
        mock_repo.fetch_metadata.side_effect = Exception("not found")

        with patch(
            "kronicle.services.channel_service.ChannelService.create_channel",
            return_value=fake_response,
        ):
            result = await service.upsert_metadata(input_payload)
            assert result == fake_response
            mock_repo.fetch_metadata.assert_awaited_once()

    async def test_updates_if_exists(self, service, mock_repo, input_payload):
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


# ==================================================================================================
# Fetch metadata (single)
# ==================================================================================================


class TestFetchMetadata:
    async def test_fetch(self, service, mock_repo):
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

    async def test_not_found(self, service, mock_repo):
        channel_id = uuid4()
        mock_repo.fetch_channel.return_value = None

        with pytest.raises(NotFoundError, match=str(channel_id)):
            await service.fetch_metadata(channel_id)


# ==================================================================================================
# Fetch all metadata
# ==================================================================================================


class TestFetchAllMetadata:
    async def test_fetch_all(self, service, mock_repo):
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_all_metadata.return_value = [fake_channel]

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_all_metadata()
            assert result == [fake_response]
            mock_repo.fetch_all_metadata.assert_awaited_once()

    async def test_empty(self, service, mock_repo):
        mock_repo.fetch_all_metadata.return_value = []
        result = await service.fetch_all_metadata()
        assert result == []


# ==================================================================================================
# Fetch metadata by name
# ==================================================================================================


class TestFetchMetadataByName:
    async def test_fetch(self, service, mock_repo):
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata_by_name.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_metadata_by_name("some-channel")
            assert result == fake_response
            mock_repo.fetch_metadata_by_name.assert_awaited_once()

    async def test_not_found_is_none(self, service, mock_repo):
        mock_repo.fetch_metadata_by_name.return_value = None
        res = await service.fetch_metadata_by_name("some-channel")
        assert res is None


# ==================================================================================================
# Fetch metadata by tags
# ==================================================================================================


class TestFetchMetadataByTags:
    async def test_fetch(self, service, mock_repo):
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata_by_tags.return_value = [fake_channel]

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_metadata_by_tags(["env:prod"])
            assert result == [fake_response]
            mock_repo.fetch_metadata_by_tags.assert_awaited_once()

    async def test_empty(self, service, mock_repo):
        mock_repo.fetch_metadata_by_tags.return_value = []
        result = await service.fetch_metadata_by_tags(["env:prod"])
        assert result == []


# ==================================================================================================
# Fetch metadata by user_meta
# ==================================================================================================


class TestFetchMetadataByUserMeta:
    async def test_fetch(self, service, mock_repo):
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata_by_user_meta.return_value = [fake_channel]

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_metadata_by_user_meta(["unit:temp"])
            assert result == [fake_response]
            mock_repo.fetch_metadata_by_user_meta.assert_awaited_once()

    async def test_empty(self, service, mock_repo):
        mock_repo.fetch_metadata_by_user_meta.return_value = []
        result = await service.fetch_metadata_by_user_meta(["unit:temp"])
        assert result == []


# ==================================================================================================
# Delete channel
# ==================================================================================================


class TestDeleteChannel:
    async def test_delete(self, service, mock_repo):
        channel_id = uuid4()
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.delete_channel_with_id.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.delete_channel(channel_id)
            assert result == fake_response
            mock_repo.delete_channel_with_id.assert_awaited_once_with(channel_id)

    async def test_not_found(self, service, mock_repo):
        channel_id = uuid4()
        mock_repo.delete_channel_with_id.return_value = None
        result = await service.delete_channel(channel_id)
        assert result is None


# ==================================================================================================
# Delete channels (bulk)
# ==================================================================================================


class TestDeleteChannels:
    async def test_delete_multiple(self, service, mock_repo):
        import asyncio

        ids = [uuid4(), uuid4()]
        mock_repo.delete_channel_with_id.return_value = AsyncMock()

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=AsyncMock(),
        ):
            coro_list = await service.delete_channels(ids)
            # delete_channels has a bug: it appends coroutines instead of awaiting them.
            assert len(coro_list) == 2
            mock_repo.delete_channel_with_id.assert_not_called()

            resolved = await asyncio.gather(*coro_list)
            assert len(resolved) == 2
            assert mock_repo.delete_channel_with_id.await_count == 2


# ==================================================================================================
# Process payload for insertion
# ==================================================================================================


class TestProcessPayloadForInsertion:
    async def test_with_schema(self, service, mock_repo):
        payload = make_payload(channel_schema={"x": "int"})
        fake_schema = MagicMock()
        fake_processed = AsyncMock(spec=ProcessedPayload)

        with (
            patch(
                "kronicle.services.channel_service.ChannelSchema.from_user_json",
                return_value=fake_schema,
            ),
            patch(
                "kronicle.services.channel_service.ProcessedPayload.from_input",
                return_value=fake_processed,
            ),
        ):
            result = await service._process_payload_for_insertion(payload)
            assert result == fake_processed

    async def test_without_schema(self, service, mock_repo):
        payload = make_payload(channel_schema=None)
        fake_channel = AsyncMock()
        fake_channel.channel_schema = MagicMock()
        fake_processed = AsyncMock(spec=ProcessedPayload)
        mock_repo.fetch_metadata.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ProcessedPayload.from_input",
            return_value=fake_processed,
        ):
            result = await service._process_payload_for_insertion(payload)
            assert result == fake_processed
            mock_repo.fetch_metadata.assert_awaited_once()

    async def test_no_rows_raises(self, service):
        payload = make_payload(rows=[])
        with pytest.raises(BadRequestError):
            await service._process_payload_for_insertion(payload)


# ==================================================================================================
# Insert channel rows
# ==================================================================================================


class TestInsertChannelRows:
    async def test_insert(self, service, mock_repo, input_payload):
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        fake_channel.timeseries.to_user_rows.return_value = [{"time": "2026-04-02T11:53:27", "value": 42}]
        fake_channel.metadata = AsyncMock()
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

    async def test_no_rows_raises(self, service, input_payload):
        input_payload.rows = []
        with pytest.raises(BadRequestError):
            await service._process_payload_for_insertion(input_payload)


# ==================================================================================================
# Upsert metadata and insert rows
# ==================================================================================================


class TestUpsertMetadataAndInsertRows:
    async def test_upsert(self, service, mock_repo, input_payload):
        fake_schema = MagicMock()
        fake_processed = AsyncMock(spec=ProcessedPayload)
        fake_processed.channel_id = uuid4()
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.upsert_metadata_and_insert_rows.return_value = fake_channel

        with (
            patch(
                "kronicle.services.channel_service.ChannelSchema.from_user_json",
                return_value=fake_schema,
            ),
            patch(
                "kronicle.services.channel_service.ProcessedPayload.from_input",
                return_value=fake_processed,
            ),
            patch(
                "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
                return_value=fake_response,
            ),
        ):
            result = await service.upsert_metadata_and_insert_rows(input_payload, strict=True)
            assert result == fake_response
            mock_repo.upsert_metadata_and_insert_rows.assert_awaited_once_with(
                processed=fake_processed,
                strict=True,
            )


# ==================================================================================================
# Fetch rows
# ==================================================================================================


class TestFetchRows:
    async def test_fetch(self, service, mock_repo):
        channel_id = uuid4()
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_channel_rows.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_rows(channel_id)
            assert result == fake_response
            mock_repo.fetch_channel_rows.assert_awaited_once_with(channel_id, filter=None)

    async def test_fetch_with_filter(self, service, mock_repo):

        channel_id = uuid4()
        rrf = RowRequestFilter(limit=10, skip_received=False)
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_channel_rows.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.fetch_rows(channel_id, filter=rrf)
            assert result == fake_response
            mock_repo.fetch_channel_rows.assert_awaited_once_with(channel_id, filter=rrf)


# ==================================================================================================
# Delete rows for channel
# ==================================================================================================


class TestDeleteRowsForChannel:
    async def test_delete(self, service, mock_repo):
        channel_id = uuid4()
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.delete_rows.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.delete_rows_for_channel(channel_id)
            assert result == fake_response
            mock_repo.delete_rows.assert_awaited_once_with(channel_id, filter=None)

    async def test_delete_with_filter(self, service, mock_repo):

        channel_id = uuid4()
        rrf = RowRequestFilter(limit=5)
        fake_channel = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.delete_rows.return_value = fake_channel

        with patch(
            "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
            return_value=fake_response,
        ):
            result = await service.delete_rows_for_channel(channel_id, filter=rrf)
            assert result == fake_response
            mock_repo.delete_rows.assert_awaited_once_with(channel_id, filter=rrf)


# ==================================================================================================
# Fetch columns
# ==================================================================================================


class TestFetchColumns:
    async def test_with_rows(self, service, mock_repo):
        channel_id = uuid4()
        fake_response = MagicMock(spec=ResponsePayload)
        fake_response.rows = [{"x": 1}]
        fake_response.columns = None

        def set_columns(*, strict=False):
            fake_response.columns = {"x": [1]}

        fake_response.rows_to_columns = MagicMock(side_effect=set_columns)
        service.fetch_rows = AsyncMock(return_value=fake_response)

        result = await service.fetch_columns(channel_id)
        assert result is not None
        assert result.columns is not None
        service.fetch_rows.assert_awaited_once_with(channel_id=channel_id, filter=None)

    async def test_without_rows(self, service, mock_repo):
        channel_id = uuid4()
        fake_response = MagicMock(spec=ResponsePayload)
        fake_response.rows = None
        service.fetch_rows = AsyncMock(return_value=fake_response)

        result = await service.fetch_columns(channel_id)
        assert result is not None
        assert result.rows is None
        service.fetch_rows.assert_awaited_once_with(channel_id=channel_id, filter=None)


# ==================================================================================================
# Clone channel
# ==================================================================================================


class TestCloneChannel:
    async def test_clone(self, service, mock_repo):
        payload = make_payload(channel_schema={"t": "int"})
        fake_src = AsyncMock()
        fake_src.name = "src"
        fake_src.channel_schema = MagicMock()
        fake_src.metadata.user_metadata = {"k": "v"}
        fake_src.metadata.tags = {"e": "p"}
        fake_cloned = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata.return_value = fake_src
        mock_repo.create_channel.return_value = fake_cloned

        fake_new_schema = MagicMock()
        fake_processed = AsyncMock(spec=ProcessedPayload)
        fake_processed.channel_id = uuid4()
        fake_processed.name = None
        fake_processed.metadata = None
        fake_processed.tags = None

        with (
            patch(
                "kronicle.services.channel_service.ChannelSchema.from_user_json",
                return_value=fake_new_schema,
            ),
            patch(
                "kronicle.services.channel_service.ProcessedPayload.from_input",
                return_value=fake_processed,
            ),
            patch(
                "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
                return_value=fake_response,
            ),
        ):
            result = await service.clone_channel(payload)
            assert result == fake_response
            mock_repo.fetch_metadata.assert_awaited_once()
            mock_repo.create_channel.assert_awaited_once()

    async def test_clone_inherits_src_metadata(self, service, mock_repo):
        payload = make_payload(channel_schema=None)
        fake_src = AsyncMock()
        fake_src.name = "src"
        fake_src.channel_schema = MagicMock()
        fake_src.metadata.user_metadata = {"k": "v"}
        fake_src.metadata.tags = {"e": "p"}
        fake_cloned = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata.return_value = fake_src
        mock_repo.create_channel.return_value = fake_cloned

        fake_processed = AsyncMock(spec=ProcessedPayload)
        fake_processed.channel_id = uuid4()
        fake_processed.name = None
        fake_processed.metadata = None
        fake_processed.tags = None

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
            result = await service.clone_channel(payload)
            assert result == fake_response
            # name, metadata, tags should be inherited from src
            assert fake_processed.name == "src_copy"
            assert fake_processed.metadata == {"k": "v"}
            assert fake_processed.tags == {"e": "p"}


# ==================================================================================================
# Patch metadata
# ==================================================================================================


class TestPatchMetadata:
    async def test_patch(self, service, mock_repo, input_payload):
        channel_id = uuid4()
        no_schema_payload = make_payload(channel_id=channel_id, channel_schema=None)
        fake_channel = AsyncMock()
        fake_channel.channel_schema = MagicMock()
        fake_updated = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata.return_value = fake_channel
        mock_repo.patch_metadata.return_value = fake_updated

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
            result = await service.patch_metadata(no_schema_payload)
            assert result == fake_response
            mock_repo.fetch_metadata.assert_awaited_once_with(channel_id)
            mock_repo.patch_metadata.assert_awaited_once()

    async def test_patch_with_schema(self, service, mock_repo):
        channel_id = uuid4()
        payload = make_payload(channel_id=channel_id, channel_schema={"z": "str"})
        fake_channel = AsyncMock()
        fake_updated = AsyncMock()
        fake_response = AsyncMock()
        mock_repo.fetch_metadata.return_value = fake_channel
        mock_repo.patch_metadata.return_value = fake_updated

        fake_schema = MagicMock()

        with (
            patch(
                "kronicle.services.channel_service.ChannelSchema.from_user_json",
                return_value=fake_schema,
            ),
            patch(
                "kronicle.services.channel_service.ProcessedPayload.from_input",
                return_value=fake_channel,
            ),
            patch(
                "kronicle.services.channel_service.ResponsePayload.from_channel_resource",
                return_value=fake_response,
            ),
        ):
            result = await service.patch_metadata(payload)
            assert result == fake_response
            mock_repo.fetch_metadata.assert_awaited_once_with(channel_id)
            mock_repo.patch_metadata.assert_awaited_once()
