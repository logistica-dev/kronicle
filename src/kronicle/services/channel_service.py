# kronicle/services/channel_service.py

from uuid import UUID, uuid4

from kronicle.db.data.channel_repository import ChannelRepository
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.filters.request_filter import RequestFilter
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import ensure_uuid4, extract_tags

mod = "chan_srvc"


class ChannelService:
    """
    Orchestration layer between the user/FASTAPI layer and the DB layer:
    - sanitization & validation
    - CRUD operations
    - Atomic operations

    Control layer to orchestrate DB operations.
    - Rows can only be inserted (append-only)
    - Metadata is always upserted (create or update)
    - All inputs validated via Payload classes

    The pipeline InputPayload → ProcessedPayload → ChannelMetadata → ResponsePayload should stay
    coherent to avoid shortcutting the processed stage.

    ChannelService is also responsible, in the return flow, for presenting the data to the user
    according to the API defined.
    """

    def __init__(self, channel_repository: ChannelRepository):
        self._repo = channel_repository

    async def ping(self) -> bool:
        return await self._repo.ping()

    # ----------------------------------------------------------------------------------------------
    # Channel CRUD
    # ----------------------------------------------------------------------------------------------
    async def create_channel(self, payload: InputPayload) -> ResponsePayload:
        """
        Create new channel metadata (fail if already exists).
        """
        # These are done in `ProcessedPayload.from_input(payload)``
        #   ensure_uuid4(payload.channel_id)
        #   payload.ensure_channel_schema()
        # log_d("create_channel")
        processed = ProcessedPayload.from_input(payload)
        channel = await self._repo.create_channel(processed)
        return ResponsePayload.from_channel_resource(channel)

    async def update_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Update channel metadata.
        Schema must either be absent or identical to stored one.
        """
        channel = await self._repo.fetch_metadata(ensure_uuid4(payload.channel_id))
        processed_payload = ProcessedPayload.from_input(payload, channel_schema=channel.channel_schema)
        updated_channel = await self._repo.update_metadata(processed_payload)
        return ResponsePayload.from_channel_resource(updated_channel)

    async def upsert_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Create or update channel metadata (idempotent).
        - If channel does not exist -> require schema (create new).
        - If channel exists -> schema must either be absent or identical to stored one.
        """
        channel_id = ensure_uuid4(payload.channel_id)
        try:
            channel = await self._repo.fetch_metadata(channel_id)
        except Exception:
            log_d("upsert_metadata", "Channel does not exist, creating")
            return await self.create_channel(payload)

        processed_payload = ProcessedPayload.from_input(payload, channel_schema=channel.channel_schema)
        channel = await self._repo.update_metadata(processed_payload)
        return ResponsePayload.from_channel_resource(channel)

    async def fetch_metadata(self, channel_id: UUID) -> ResponsePayload:
        """
        Fetch metadata for a given channel_id.
        """
        channel = await self._repo.fetch_channel(ensure_uuid4(channel_id))
        return ResponsePayload.from_channel_resource(channel)

    async def fetch_all_metadata(self) -> list[ResponsePayload]:
        """
        Fetch all metadata entries and row counts.
        """
        all_channels = await self._repo.fetch_all_metadata()
        return [ResponsePayload.from_channel_resource(channel) for channel in all_channels]

    async def fetch_metadata_by_name(self, name: str) -> ResponsePayload:
        """
        Fetch all metadata entries and row counts.
        """
        channel = await self._repo.fetch_metadata_by_name(name=name)
        return ResponsePayload.from_channel_resource(channel)

    async def fetch_metadata_by_tags(self, tags: list[str]) -> list[ResponsePayload]:
        """
        Fetch all metadata entries and row counts.
        """
        tag_dict = extract_tags(tags)
        channels = await self._repo.fetch_metadata_by_tags(tag_dict)
        return [ResponsePayload.from_channel_resource(channel) for channel in channels]

    async def delete_channel(self, channel_id: UUID) -> ResponsePayload | None:
        """
        Delete a channel metadata and its data table.
        """
        channel_id = ensure_uuid4(channel_id)
        channel = await self._repo.delete_channel_with_id(channel_id)
        return ResponsePayload.from_channel_resource(channel) if channel else None

    async def delete_channels(self, channel_ids: list[UUID]) -> list[ResponsePayload]:
        res = []
        for channel_id in channel_ids:
            res.append(self.delete_channel(channel_id))
        return res

    # ----------------------------------------------------------------------------------------------
    # Row operations
    # ----------------------------------------------------------------------------------------------
    async def _process_payload_for_insertion(self, payload: InputPayload, *, strict: bool = False) -> ProcessedPayload:
        channel_id: UUID = payload.ensure_channel_id()
        rows = payload.rows
        if not rows:
            raise BadRequestError("No rows found in input payload", details={"channel_id": channel_id, "rows": rows})

        if payload.channel_schema:
            channel_schema = ChannelSchema.from_user_json(payload.channel_schema)
        else:
            # Channel schema is not mandatory for existing channel
            resource = await self._repo.fetch_metadata(channel_id)
            channel_schema = resource.channel_schema

        # Process input payload into ProcessedPayload
        processed: ProcessedPayload = ProcessedPayload.from_input(
            payload=payload,
            channel_schema=channel_schema,
            strict=strict,
        )
        return processed

    async def insert_channel_rows(self, payload: InputPayload, *, strict: bool = False) -> ResponsePayload:
        """
        Insert new rows for an existing_meta channel.
        Validates rows first; in strict mode, aborts on any validation errors.
        The metadata is not updated.
        """
        processed = await self._process_payload_for_insertion(payload, strict=strict)
        await self._repo.fetch_channel(ensure_uuid4(processed.channel_id))

        updated_resource = await self._repo.insert_rows(
            processed=processed,
            strict=strict,
        )
        return ResponsePayload.from_channel_resource(updated_resource)

    async def upsert_metadata_and_insert_rows(self, payload: InputPayload, strict: bool = False) -> ResponsePayload:
        """
        Upsert metadata and insert rows in one operation.
        - Metadata is validated first; if invalid, no rows are inserted.
        - Rows are validated; warnings collected if strict=False, error raised with every detail otherwise
        """
        here = "up_meta_add_rows"
        # log_d(here)
        processed = await self._process_payload_for_insertion(payload, strict=strict)
        updated_resource = await self._repo.upsert_metadata_and_insert_rows(
            processed=processed,
            strict=strict,
        )
        log_d(here, "updated resource", updated_resource.channel_id)

        return ResponsePayload.from_channel_resource(updated_resource)

    async def fetch_rows(
        self,
        channel_id: UUID,
        *,
        filter: RequestFilter | None = None,
    ) -> ResponsePayload:
        """
        Fetch rows for a given channel.
        """
        # here = "fetch_rows"
        channel = await self._repo.fetch_channel(ensure_uuid4(channel_id), filter=filter)
        return ResponsePayload.from_channel_resource(channel, skip_received=filter.skip_received if filter else True)

    async def fetch_all_rows(self, *, filter: RequestFilter | None = None) -> list[ResponsePayload]:
        """
        Fetch all rows for all channels.
        Returns a list of ResponsePayload objects.
        Each payload includes channel_id, metadata, tags, rows, and issued_at.
        """
        channel_list = await self._repo.fetch_all_channels(filter=filter)
        response_list = []
        skip_received = filter.skip_received if filter else True
        for channel in channel_list:
            response_list.append(ResponsePayload.from_channel_resource(channel, skip_received=skip_received))
        return response_list

    async def delete_rows_for_channel(
        self,
        channel_id: UUID,
        *,
        filter: RequestFilter | None = None,
    ) -> ResponsePayload:
        """
        Remove all data rows for a channel, keep its metadata, and return the metadata
        with available_rows set to 0.
        """
        channel_id = ensure_uuid4(channel_id)
        channel = await self._repo.fetch_metadata(channel_id)
        if not channel.row_nb:
            raise NotFoundError("No rows found for channel", details={"channel_id": channel_id})
        channel = await self._repo.delete_rows(channel, filter=filter)
        return ResponsePayload.from_channel_resource(channel)

    async def fetch_columns(self, channel_id: UUID, *, filter: RequestFilter | None = None) -> ResponsePayload:
        """
        Fetch rows for a given channel.
        """
        res = await self.fetch_rows(channel_id=channel_id, filter=filter)
        if not res or not res.rows:
            return res
        res.rows_to_columns(strict=True)
        return res

    # ----------------------------------------------------------------------------------------------
    # Optional setup operations
    # ----------------------------------------------------------------------------------------------

    async def clone_channel(self, payload: InputPayload) -> ResponsePayload:
        """
        Clone a channel's schema (and optionally metadata) into a new channel_id.
        - Does NOT copy data rows.
        - Generates a new channel_id automatically.
        - Schema/metadata/tags from payload override source if provided.
        """
        src_channel = await self._repo.fetch_metadata(ensure_uuid4(payload.channel_id))

        # Generate new channel_id automatically and assign to payload
        new_schema = (
            ChannelSchema.from_user_json(payload.channel_schema)
            if payload.channel_schema
            else src_channel.channel_schema
        )
        processed = ProcessedPayload.from_input(payload, new_schema)
        processed.channel_id = uuid4()
        if not processed.name:
            processed.name = f"{src_channel.name}_copy"
        if not processed.metadata:
            processed.metadata = src_channel.metadata.user_metadata
        if not processed.tags:
            processed.tags = src_channel.metadata.tags

        cloned_channel = await self._repo.create_channel(processed)
        return ResponsePayload.from_channel_resource(cloned_channel)

    async def patch_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Partially update metadata or tags for an existing_meta channel.
        If no rows were already uploaded, the schema may be updated.
        Only provided fields are updated.
        """
        channel_id = ensure_uuid4(payload.channel_id)
        channel = await self._repo.fetch_metadata(channel_id)
        schema = (
            ChannelSchema.from_user_json(payload.channel_schema) if payload.channel_schema else channel.channel_schema
        )
        processed = ProcessedPayload.from_input(payload, schema)
        updated_channel = await self._repo.patch_metadata(processed)
        return ResponsePayload.from_channel_resource(updated_channel)
