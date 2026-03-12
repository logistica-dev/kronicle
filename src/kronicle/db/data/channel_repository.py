# kronicle/db/data/channel_repository.py
from __future__ import annotations

from uuid import UUID

from kronicle.db.data.channel_db_session import ChannelDbSession
from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_resource import ChannelResource
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.schemas.payload.request_filter import RequestFilter
from kronicle.types.tag_type import TagType
from kronicle.utils.dev_logs import log_d, log_e


class ChannelRepository:
    """
    Repository for Channel operations (metadata + timeseries).

    Responsibilities:
    - CRUD metadata
    - Fetch/create/delete ChannelResource shells
    - Fetch/insert/clear rows
    - Uses DBSession for connection/transaction management
    """

    def __init__(self, db_session: ChannelDbSession):
        self._db: ChannelDbSession = db_session

    # ----------------------------------------------------------------------------------------------
    # Health checkMetadata operations
    # ----------------------------------------------------------------------------------------------
    async def ping(self) -> bool:
        return await self._db.ping()

    # ----------------------------------------------------------------------------------------------
    # Helper
    # ----------------------------------------------------------------------------------------------
    async def _metadata_to_channel(self, db, metadata: ChannelMetadata) -> ChannelResource:
        channel = ChannelResource(metadata)
        await channel.count_rows(db)
        return channel

    async def _list_metadata_to_channels(self, db, metadata_list: list[ChannelMetadata]) -> list[ChannelResource]:
        channel_list = []
        for meta in metadata_list:
            channel_resource = ChannelResource(meta)
            await channel_resource.count_rows(db)
            channel_list.append(channel_resource)
        return channel_list

    # ----------------------------------------------------------------------------------------------
    # Pure ChannelMetadata operations
    # ----------------------------------------------------------------------------------------------
    async def fetch_metadata(self, channel_id: UUID) -> ChannelResource:
        async with self._db.transaction() as db:
            metadata = await ChannelMetadata.fetch_by_id(db, channel_id)
            if not metadata:
                raise NotFoundError("No metadata found", details={"channel_id": channel_id})
            channel = ChannelResource(metadata)
            await channel.count_rows(db)
            return channel

    async def fetch_all_metadata(self) -> list[ChannelResource]:
        async with self._db.transaction() as db:
            metadata_list = await ChannelMetadata.fetch_all(db)
            return await self._list_metadata_to_channels(db, metadata_list)

    async def fetch_metadata_by_name(self, name: str) -> ChannelResource:
        async with self._db.transaction() as db:
            metadata = await ChannelMetadata.fetch_by_name(db, name=name)
            if not metadata:
                raise NotFoundError("No channel was found", details={"name": name})
            return await self._metadata_to_channel(db, metadata)

    async def fetch_metadata_by_tags(self, tags: dict[str, TagType]) -> list[ChannelResource]:
        if not tags:
            return []
        async with self._db.transaction() as db:
            metadata_list = await ChannelMetadata.fetch_by_tags(db, tags)
            if not metadata_list:
                return []
            return await self._list_metadata_to_channels(db, metadata_list)

    async def create_metadata(self, processed: ProcessedPayload) -> ChannelResource:
        async with self._db.transaction() as db:
            metadata = ChannelMetadata.from_processed(processed)
            await metadata.create(db)
            return await self._metadata_to_channel(db, metadata)

    async def update_metadata(self, processed: ProcessedPayload) -> ChannelResource:
        async with self._db.transaction() as db:
            existing = await ChannelMetadata.fetch_by_id(db, processed.channel_id)
            if not existing:
                raise NotFoundError(f"No metadata for channel '{processed.channel_id}'")
            metadata = ChannelMetadata.from_processed(processed, channel_truth=existing.channel_schema)
            await metadata.update(db)
            return await self._metadata_to_channel(db, metadata)

    async def patch_metadata(self, processed: ProcessedPayload) -> ChannelResource:
        async with self._db.transaction() as db:
            channel = await self.fetch_metadata(processed.channel_id)

            # Update metadata fields if provided
            if processed.metadata:
                if channel.metadata.user_metadata:
                    channel.metadata.user_metadata.update(processed.metadata)
                else:
                    channel.metadata.user_metadata = processed.metadata

            if processed.tags:
                if channel.metadata.tags:
                    channel.metadata.tags.update(processed.tags)
                else:
                    channel.metadata.tags = processed.tags

            # Update schema if provided
            if not channel.row_nb and processed.channel_schema:
                if processed.channel_schema != channel.channel_schema:
                    channel.metadata.channel_schema = processed.channel_schema

            await channel.metadata.update(db)
            return await self._metadata_to_channel(db, channel.metadata)

    # ----------------------------------------------------------------------------------------------
    # ChannelResource operations (timeseries + metadata)
    # ----------------------------------------------------------------------------------------------
    async def insert_rows(self, processed: ProcessedPayload, *, strict: bool = False) -> ChannelResource:
        """
        Append rows to an existing channel
        """
        here = "insert_rows"
        log_d(here)
        if not processed.rows:
            raise BadRequestError("The payload contains no row to insert")
        channel = ChannelResource.from_processed(processed)
        async with self._db.transaction() as db:
            await channel.insert_rows(db, strict=strict)  # Here we want to get a list of the rows

        return channel

    async def upsert_metadata_and_insert_rows(
        self,
        processed: ProcessedPayload,
        *,
        strict: bool = False,
    ) -> ChannelResource:
        """
        Upsert metadata and insert rows in one operation.
        """
        here = "up_meta_add_rows"
        # log_d(here)
        # if not processed.rows:
        #     raise BadRequestError("The payload contains no row to insert")
        # log_d(here, "Payload has rows")
        channel = ChannelResource.from_processed(processed)
        async with self._db.transaction() as db:
            existing = await channel.metadata.exists(db=db)
            try:
                if existing:
                    await channel.metadata.update(db)
                else:
                    await channel.metadata.create(db)
            except Exception as e:
                log_e(here, "Metadata upsert failed", e)
                raise
            if processed.rows:
                try:
                    await channel.insert_rows(db, strict=strict)
                except Exception as e:
                    log_e(here, "Rows insertion failed", e)
                    raise
            else:
                channel.op_feedback.add_detail("No rows to insert", "rows")
        return channel

    async def delete_rows(self, channel: ChannelResource, *, filter: RequestFilter | None = None):
        async with self._db.transaction() as db:
            return await channel.delete_rows(db, filter=filter)

    # ----------------------------------------------------------------------------------------------
    # Channel operations (timeseries + metadata)
    # ----------------------------------------------------------------------------------------------

    async def fetch_channel(self, channel_id: UUID, *, filter: RequestFilter | None = None) -> ChannelResource:
        async with self._db.transaction() as db:
            channel = await ChannelResource.fetch(db, channel_id, filter=filter)
        if not channel:
            raise NotFoundError("No channel found", details={"channel_id": channel_id})
        return channel

    async def create_channel(self, processed: ProcessedPayload) -> ChannelResource:
        """
        Create metadata and ensure timeseries table exists.
        """
        channel = ChannelResource.from_processed(processed)
        async with self._db.transaction() as db:
            db_resource = await channel.create(db)
        return db_resource

    async def delete_channel_with_id(self, channel_id: UUID) -> ChannelResource | None:
        async with self._db.transaction() as db:
            return await ChannelResource.delete_channel_with_id(db, channel_id)

    async def fetch_all_channels(self, *, filter: RequestFilter | None = None) -> list[ChannelResource]:
        async with self._db.transaction() as db:
            return await ChannelResource.fetch_all(db, filter=filter)
