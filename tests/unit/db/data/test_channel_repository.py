# tests/unit/db/data/test_channel_repository.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.db.data.channel_repository import ChannelRepository
from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_resource import ChannelResource
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.schemas.payload.processed_payload import ProcessedPayload

pytestmark = pytest.mark.asyncio


# Helper to generate minimal ChannelSchema
def make_channel_schema():
    return ChannelSchema.from_user_json(
        {
            "channel_id": "uuid",
            "name": "str",
            "fields": "dict",
            "tags": "dict",
            "metadata": "dict",
        }
    )


@pytest.fixture
def channel_schema_fixture():
    return make_channel_schema()


class FakeTransactionCM:
    async def __aenter__(self):
        return self  # could also return db_session if needed

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


@pytest.fixture
def mock_db_session():
    db_session = MagicMock()
    db_session.transaction.return_value = FakeTransactionCM()
    db_session.ping = AsyncMock(return_value=True)
    return db_session


@pytest.fixture
def repo(mock_db_session):
    # Inject mock DB session into repository
    return ChannelRepository(db_session=mock_db_session)


@pytest.mark.asyncio
async def test_ping(repo, mock_db_session):
    result = await repo.ping()
    assert result is True
    mock_db_session.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_metadata_by_name_found(repo):
    fake_meta = AsyncMock(spec=ChannelMetadata)
    fake_channel = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_name",
            new_callable=AsyncMock,
            return_value=fake_meta,
        ),
        patch(
            "kronicle.db.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_channel,
        ),
    ):
        result = await repo.fetch_metadata_by_name("my-channel")
        assert result == fake_channel


@pytest.mark.asyncio
async def test_fetch_metadata_by_name_not_found(repo):
    with patch(
        "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_name",
        new_callable=AsyncMock,
        return_value=None,
    ):
        with pytest.raises(NotFoundError):
            await repo.fetch_metadata_by_name("nonexistent-channel")


@pytest.mark.asyncio
async def test_insert_rows_no_rows(repo, channel_schema_fixture):
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[])
    with pytest.raises(BadRequestError):
        await repo.insert_rows(processed)


@pytest.mark.asyncio
async def test_insert_rows_existing_channel(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = True
    channel_mock.insert_rows.return_value = None

    with patch(
        "kronicle.db.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.insert_rows(processed)
        channel_mock.insert_rows.assert_awaited_once()
        assert result == channel_mock


@pytest.mark.asyncio
async def test_create_channel_conflict(repo, channel_schema_fixture):
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[])
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = True
    channel_mock.metadata.create.return_value = None
    channel_mock.metadata.update.return_value = None

    with patch(
        "kronicle.db.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(ConflictError):
            await repo.create_channel(processed)
