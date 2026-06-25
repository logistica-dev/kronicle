# tests/unit/db/data/test_channel_repository.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_resource import ChannelResource
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.repo.data.channel_repository import ChannelRepository
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
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
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
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
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
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
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(ConflictError):
            await repo.create_channel(processed)


async def test_metadata_to_channel(repo):
    mock_meta = MagicMock(spec=ChannelMetadata)
    mock_meta.channel_id = uuid4()
    mock_meta.channel_schema = channel_schema_fixture

    with patch.object(ChannelResource, "count_rows", new_callable=AsyncMock) as mock_count:
        result = await repo._metadata_to_channel("db", mock_meta)
        assert isinstance(result, ChannelResource)
        assert result.metadata == mock_meta
        mock_count.assert_awaited_once_with("db")


async def test_list_metadata_to_channels(repo):
    meta1 = MagicMock(spec=ChannelMetadata)
    meta1.channel_id = uuid4()
    meta1.channel_schema = channel_schema_fixture
    meta2 = MagicMock(spec=ChannelMetadata)
    meta2.channel_id = uuid4()
    meta2.channel_schema = channel_schema_fixture

    with patch.object(ChannelResource, "count_rows", new_callable=AsyncMock) as mock_count:
        result = await repo._list_metadata_to_channels("db", [meta1, meta2])
        assert len(result) == 2
        assert mock_count.await_count == 2


async def test_fetch_metadata(repo):
    channel_id = uuid4()
    fake_channel = AsyncMock(spec=ChannelResource)

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.fetch",
        new_callable=AsyncMock,
        return_value=fake_channel,
    ):
        result = await repo.fetch_metadata(channel_id)
        assert result == fake_channel


async def test_fetch_all_metadata(repo):
    fake_meta = AsyncMock(spec=ChannelMetadata)
    fake_channel = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_all",
            new_callable=AsyncMock,
            return_value=[fake_meta],
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._list_metadata_to_channels",
            new_callable=AsyncMock,
            return_value=[fake_channel],
        ),
    ):
        result = await repo.fetch_all_metadata()
        assert result == [fake_channel]


async def test_fetch_metadata_by_tags_empty(repo):
    result = await repo.fetch_metadata_by_tags({})
    assert result == []


async def test_fetch_metadata_by_tags_found(repo):
    fake_channel = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_tags",
            new_callable=AsyncMock,
            return_value=[AsyncMock(spec=ChannelMetadata)],
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._list_metadata_to_channels",
            new_callable=AsyncMock,
            return_value=[fake_channel],
        ),
    ):
        result = await repo.fetch_metadata_by_tags({"env": "prod"})
        assert result == [fake_channel]


async def test_fetch_metadata_by_tags_not_found(repo):
    with patch(
        "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_tags",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await repo.fetch_metadata_by_tags({"env": "prod"})
        assert result == []


async def test_fetch_metadata_by_user_meta_empty(repo):
    result = await repo.fetch_metadata_by_user_meta({})
    assert result == []


async def test_fetch_metadata_by_user_meta_found(repo):
    fake_channel = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_user_meta",
            new_callable=AsyncMock,
            return_value=[AsyncMock(spec=ChannelMetadata)],
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._list_metadata_to_channels",
            new_callable=AsyncMock,
            return_value=[fake_channel],
        ),
    ):
        result = await repo.fetch_metadata_by_user_meta({"location": "lab"})
        assert result == [fake_channel]


async def test_fetch_metadata_by_user_meta_not_found(repo):
    with patch(
        "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_user_meta",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await repo.fetch_metadata_by_user_meta({"location": "lab"})
        assert result == []


async def test_update_metadata(repo, channel_schema_fixture):
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[])
    existing_meta = AsyncMock(spec=ChannelMetadata)
    existing_meta.channel_schema = channel_schema_fixture
    fake_channel = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_id",
            new_callable=AsyncMock,
            return_value=existing_meta,
        ),
        patch(
            "kronicle.db.data.models.channel_metadata.ChannelMetadata.from_processed",
            return_value=existing_meta,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_channel,
        ),
    ):
        result = await repo.update_metadata(processed)
        assert result == fake_channel
        existing_meta.update.assert_awaited_once()


async def test_update_metadata_not_found(repo, channel_schema_fixture):
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[])

    with patch(
        "kronicle.db.data.models.channel_metadata.ChannelMetadata.fetch_by_id",
        new_callable=AsyncMock,
        return_value=None,
    ):
        with pytest.raises(NotFoundError):
            await repo.update_metadata(processed)


async def test_patch_metadata_with_metadata_and_tags(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        metadata={"new_key": "new_val"},
        tags={"new_tag": "tag_val"},
        rows=[],
    )
    meta_mock = AsyncMock()
    meta_mock.user_metadata = MagicMock()
    meta_mock.tags = MagicMock()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = None
    channel_mock.metadata = meta_mock
    fake_result = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_result,
        ),
    ):
        result = await repo.patch_metadata(processed)
        assert result == fake_result
        meta_mock.user_metadata.update.assert_called_once_with({"new_key": "new_val"})
        meta_mock.tags.update.assert_called_once_with({"new_tag": "tag_val"})
        meta_mock.update.assert_awaited_once()


async def test_patch_metadata_no_metadata_no_tags(repo, channel_schema_fixture):
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[])
    meta_mock = AsyncMock()
    meta_mock.user_metadata = MagicMock()
    meta_mock.tags = MagicMock()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.metadata = meta_mock
    fake_result = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_result,
        ),
    ):
        result = await repo.patch_metadata(processed)
        assert result == fake_result
        meta_mock.update.assert_awaited_once()


async def test_patch_metadata_schema_update(repo):
    old_schema = ChannelSchema.from_user_json(
        {
            "channel_id": "uuid",
            "name": "str",
            "fields": "dict",
            "tags": "dict",
            "metadata": "dict",
        }
    )
    new_schema = ChannelSchema.from_user_json(
        {
            "channel_id": "uuid",
            "name": "str",
            "fields": "dict",
            "tags": "dict",
            "metadata": "dict",
            "extra": "str",
        }
    )
    processed = ProcessedPayload(channel_id=uuid4(), channel_schema=new_schema, rows=[])
    meta_mock = AsyncMock()
    meta_mock.user_metadata = {}
    meta_mock.tags = {}
    meta_mock.channel_schema = old_schema
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = None
    channel_mock.metadata = meta_mock
    fake_result = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_result,
        ),
    ):
        result = await repo.patch_metadata(processed)
        assert result == fake_result
        assert meta_mock.channel_schema == new_schema
        meta_mock.update.assert_awaited_once()


async def test_insert_rows_no_channel(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(NotFoundError):
            await repo.insert_rows(processed)


async def test_upsert_metadata_and_insert_rows_existing_with_rows(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = True

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.upsert_metadata_and_insert_rows(processed)
        assert result == channel_mock
        channel_mock.metadata.update.assert_awaited_once()
        channel_mock.insert_rows.assert_awaited_once()


async def test_upsert_metadata_and_insert_rows_new_with_rows(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.upsert_metadata_and_insert_rows(processed)
        assert result == channel_mock
        channel_mock.metadata.create.assert_awaited_once()
        channel_mock.insert_rows.assert_awaited_once()


async def test_upsert_metadata_and_insert_rows_new_no_rows(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.op_feedback = MagicMock()

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.upsert_metadata_and_insert_rows(processed)
        assert result == channel_mock
        channel_mock.metadata.create.assert_awaited_once()
        channel_mock.op_feedback.add_detail.assert_called_once_with("No rows to insert", "rows")


async def test_fetch_rows_found(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.fetch_rows = AsyncMock(return_value=channel_mock)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch("kronicle.repo.data.channel_repository.RowFetchContext"),
    ):
        result = await repo.fetch_rows(channel_id)
        assert result == channel_mock


async def test_fetch_rows_no_rows(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 0

    with patch(
        "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
        new_callable=AsyncMock,
        return_value=channel_mock,
    ):
        with pytest.raises(NotFoundError):
            await repo.fetch_rows(channel_id)


async def test_fetch_rows_with_filter(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.fetch_rows = AsyncMock(return_value=channel_mock)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch("kronicle.repo.data.channel_repository.RowFetchContext") as mock_ctx_cls,
    ):
        row_filter = RowRequestFilter(limit=10)
        result = await repo.fetch_rows(channel_id, filter=row_filter)
        assert result == channel_mock
        mock_ctx_cls.assert_called_once_with(column_types=channel_mock.column_types, in_filters=row_filter)


async def test_delete_rows_found(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.delete_rows = AsyncMock(return_value=channel_mock)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch("kronicle.repo.data.channel_repository.RowFetchContext"),
    ):
        result = await repo.delete_rows(channel_id)
        assert result == channel_mock


async def test_delete_rows_no_rows(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 0

    with patch(
        "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
        new_callable=AsyncMock,
        return_value=channel_mock,
    ):
        with pytest.raises(NotFoundError):
            await repo.delete_rows(channel_id)


async def test_fetch_channel(repo):
    channel_id = uuid4()
    fake_channel = AsyncMock(spec=ChannelResource)

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.fetch",
        new_callable=AsyncMock,
        return_value=fake_channel,
    ):
        result = await repo.fetch_channel(channel_id)
        assert result == fake_channel


async def test_fetch_channel_rows(repo):
    channel_id = uuid4()
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.fetch_rows = AsyncMock(return_value=channel_mock)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelResource.fetch",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch("kronicle.repo.data.channel_repository.RowFetchContext"),
    ):
        result = await repo.fetch_channel_rows(channel_id)
        assert result == channel_mock


async def test_create_channel_success(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.timeseries = AsyncMock()
    channel_mock.timeseries.table_exists.return_value = False

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.create_channel(processed)
        assert result == channel_mock
        channel_mock.timeseries.ensure_table.assert_awaited_once()
        channel_mock.metadata.create.assert_awaited_once()


async def test_create_channel_timeseries_conflict(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.timeseries = AsyncMock()
    channel_mock.timeseries.table_exists.return_value = True

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(ConflictError):
            await repo.create_channel(processed)


async def test_create_channel_with_rows(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.timeseries = AsyncMock()
    channel_mock.timeseries.table_exists.return_value = False
    channel_mock.timeseries.rows = [1]
    channel_mock.op_feedback = MagicMock()

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        result = await repo.create_channel(processed)
        assert result == channel_mock
        channel_mock.insert_rows.assert_awaited_once()


async def test_delete_channel_with_id(repo):
    channel_id = uuid4()
    fake_channel = AsyncMock(spec=ChannelResource)

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.delete_channel_with_id",
        new_callable=AsyncMock,
        return_value=fake_channel,
    ):
        result = await repo.delete_channel_with_id(channel_id)
        assert result == fake_channel


async def test_delete_channel_with_id_not_found(repo):
    channel_id = uuid4()

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.delete_channel_with_id",
        new_callable=AsyncMock,
        return_value=None,
    ):
        result = await repo.delete_channel_with_id(channel_id)
        assert result is None


async def test_patch_metadata_set_metadata_when_none(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        metadata={"new_key": "new_val"},
        tags={},
        rows=[],
    )
    meta_mock = AsyncMock()
    meta_mock.user_metadata = None
    meta_mock.tags = {"old_tag": "val"}
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.metadata = meta_mock
    fake_result = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_result,
        ),
    ):
        result = await repo.patch_metadata(processed)
        assert result == fake_result
        assert meta_mock.user_metadata == {"new_key": "new_val"}
        meta_mock.update.assert_awaited_once()


async def test_patch_metadata_set_tags_when_none(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        metadata={},
        tags={"new_tag": "tag_val"},
        rows=[],
    )
    meta_mock = AsyncMock()
    meta_mock.user_metadata = {"key": "val"}
    meta_mock.tags = None
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.row_nb = 5
    channel_mock.metadata = meta_mock
    fake_result = AsyncMock(spec=ChannelResource)

    with (
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._fetch_metadata",
            new_callable=AsyncMock,
            return_value=channel_mock,
        ),
        patch(
            "kronicle.repo.data.channel_repository.ChannelRepository._metadata_to_channel",
            new_callable=AsyncMock,
            return_value=fake_result,
        ),
    ):
        result = await repo.patch_metadata(processed)
        assert result == fake_result
        assert meta_mock.tags == {"new_tag": "tag_val"}
        meta_mock.update.assert_awaited_once()


async def test_upsert_metadata_and_insert_rows_metadata_upsert_fails(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.metadata.create.side_effect = RuntimeError("create failed")

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(RuntimeError):
            await repo.upsert_metadata_and_insert_rows(processed)


async def test_upsert_metadata_and_insert_rows_insert_fails(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = True
    channel_mock.insert_rows.side_effect = RuntimeError("insert failed")

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(RuntimeError):
            await repo.upsert_metadata_and_insert_rows(processed)


async def test_create_channel_metadata_create_fails(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.timeseries = AsyncMock()
    channel_mock.timeseries.table_exists.return_value = False
    channel_mock.metadata.create.side_effect = RuntimeError("meta create failed")

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(RuntimeError):
            await repo.create_channel(processed)


async def test_create_channel_insert_rows_fails(repo, channel_schema_fixture):
    processed = ProcessedPayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        rows=[{"time": 123, "value": 42}],
    )
    channel_mock = AsyncMock(spec=ChannelResource)
    channel_mock.metadata = AsyncMock()
    channel_mock.metadata.exists.return_value = False
    channel_mock.timeseries = AsyncMock()
    channel_mock.timeseries.table_exists.return_value = False
    channel_mock.timeseries.rows = [1]
    channel_mock.insert_rows.side_effect = RuntimeError("insert failed")
    channel_mock.op_feedback = MagicMock()

    with patch(
        "kronicle.repo.data.channel_repository.ChannelResource.from_processed",
        return_value=channel_mock,
    ):
        with pytest.raises(RuntimeError):
            await repo.create_channel(processed)
