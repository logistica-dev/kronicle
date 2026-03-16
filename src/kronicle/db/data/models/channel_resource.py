# kronicle/db/data/models/channel_resources.py
from __future__ import annotations

from uuid import UUID

from asyncpg.pool import PoolConnectionProxy

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.db.data.models.channel_timeseries import ChannelTimeseries
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.filters.request_filter import RequestFilter
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.utils.dev_logs import log_e

mod = "chan_rsrc"


class ChannelResource:
    """
    High-level wrapper for a channel.

    Responsibilities:
    - Holds metadata (ChannelMetadata)
    - Holds runtime timeseries (ChannelTimeseries)
    - Provides row validation, insertion, and DB tuple access
    - Provides schema access
    """

    def __init__(
        self,
        metadata: ChannelMetadata,
        timeseries: ChannelTimeseries | None = None,
        op_feedback: OpFeedback | None = None,
    ):
        self.metadata = metadata
        if timeseries is not None:
            assert isinstance(timeseries, ChannelTimeseries)
            assert timeseries.channel_id == metadata.channel_id
            self.timeseries = timeseries
        else:
            self.timeseries = ChannelTimeseries(
                channel_id=metadata.channel_id,
                channel_schema=metadata.channel_schema,
            )
        self.row_nb: int | None = None  # None if it hasn't been checked in the DB yet.
        self.op_feedback: OpFeedback = op_feedback or OpFeedback()

    # ----------------------------------------------------------------------------------------------
    # Factory method
    # ----------------------------------------------------------------------------------------------

    @classmethod
    def from_processed(
        cls,
        processed: ProcessedPayload,
        *,
        strict: bool = False,
    ) -> ChannelResource:
        """
        Create a ChannelResource from an ProcessedPayload.

        Responsibilities:
        - Validate and normalize metadata and schema via ChannelMetadata.from_processed.
        - Create a ChannelTimeseries object bound to this channel.
        - Validate and store rows using the timeseries, respecting `strict` mode.

        Parameters:
        -----------
        payload : ProcessedPayload
            User-provided payload with metadata, optional schema, and optional rows.
        strict : bool, default=False
            If True, raises BadRequestError with the details if any row fails validation.
            If False, valid rows are stored, and warnings for invalid rows are stored in the op_feedback field

        Returns:
        --------
        ChannelResource
            Fully initialized resource with metadata and timeseries with updated op_feedback field

        Raises:
        -------
        BadRequestError
            - If metadata/schema is invalid.
            - If strict=True and some rows fail validation.
        """

        try:
            meta = ChannelMetadata.from_processed(processed)
        except Exception as e:
            raise BadRequestError(f"Incorrect metadata: {e}") from e

        resource = cls(meta, op_feedback=processed.op_feedback)
        if isinstance(processed, ProcessedPayload) and processed.rows:
            resource.timeseries.add_rows(processed.rows, strict=strict)
        return resource

    # ----------------------------------------------------------------------------------------------
    # Schema access
    # ----------------------------------------------------------------------------------------------
    @property
    def channel_schema(self) -> ChannelSchema:
        return self.metadata.channel_schema

    @property
    def channel_id(self) -> UUID:
        return self.metadata.channel_id

    @property
    def name(self) -> str | None:
        return self.metadata.name

    @property
    def user_metadata(self) -> dict | None:
        return self.metadata.user_metadata

    @property
    def tags(self) -> dict | None:
        return self.metadata.tags

    # ----------------------------------------------------------------------------------------------
    # Timeseries access
    # ----------------------------------------------------------------------------------------------
    def get_db_tuples(self) -> list[tuple]:
        """
        Return DB-ready tuples for insertion.
        """
        return self.timeseries.get_db_tuples()

    def verify_db_schema(self, db_columns: dict[str, str]) -> None:
        self.timeseries.verify_db_schema(db_columns)

    # ----------------------------------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------------------------------
    def to_json(self) -> dict:
        return {"meta": self.metadata.to_json(), "timeseries": self.timeseries.to_json()}

    def __str__(self) -> str:
        return f"ChannelResource {self.to_json()}"

    # ----------------------------------------------------------------------------------------------
    # DB table access: metadata
    # ----------------------------------------------------------------------------------------------
    @property
    def metadata_table_name(self) -> str:
        return ChannelMetadata.tablename()

    async def metadata_table_exists(self, db: PoolConnectionProxy) -> bool:
        """Return True if the ChannelMetadata table exists."""
        return await self.metadata.table_exists(db)

    @classmethod
    async def _fetch_metadata(cls, db: PoolConnectionProxy, channel_id: UUID) -> ChannelResource:
        metadata = await ChannelMetadata.fetch_by_id(db, channel_id)
        if not metadata:
            raise NotFoundError(f"No metadata found for UUID {channel_id}")
        resource = ChannelResource(metadata)
        await resource.count_rows(db)
        return resource

    # ----------------------------------------------------------------------------------------------
    # DB table access: timeseries
    # ----------------------------------------------------------------------------------------------
    @property
    def timeseries_table_name(self) -> str:
        return self.timeseries.table_name

    async def timeseries_table_exists(self, db: PoolConnectionProxy) -> bool:
        """Return True if the ChannelMetadata table exists."""
        return await self.timeseries.table_exists(db)

    async def ensure_timeseries_table(self, db: PoolConnectionProxy) -> None:
        await self.timeseries.ensure_table(db)

    async def count_rows(self, db: PoolConnectionProxy) -> int:
        self.row_nb = await self.timeseries.count_rows(db)
        return self.row_nb

    async def fetch_rows(self, db: PoolConnectionProxy, *, filter: RequestFilter | None = None) -> ChannelResource:
        await self.count_rows(db)
        if self.row_nb:
            await self.timeseries.fetch(db, filter=filter)
        return self

    async def insert_rows(
        self,
        db: PoolConnectionProxy,
        *,
        strict: bool = False,
    ):
        here = "insert_rows"
        try:
            await self.ensure_timeseries_table(db)
        except Exception as e:
            log_e(here, "'ensure_timeseries_table' raised an error", e)
            raise
        try:
            await self.timeseries.insert(db, strict=strict)
        except Exception:
            log_e(here, "'timeseries.insert' raised an error", self.timeseries.op_feedback)
            raise
        try:
            await self.count_rows(db)
        except Exception as e:
            log_e(here, "'count_rows' raised an error", e)
            raise
        return self

    async def delete_rows(self, db: PoolConnectionProxy, *, filter: RequestFilter | None = None):
        await self.timeseries.delete(db, filter=filter)
        await self.count_rows(db)
        return self

    # ----------------------------------------------------------------------------------------------
    # DB table access: metadata ant timeseries at the same time
    # ----------------------------------------------------------------------------------------------
    @classmethod
    async def fetch(
        cls, db: PoolConnectionProxy, channel_id: UUID, *, filter: RequestFilter | None = None
    ) -> ChannelResource:
        resource = await cls._fetch_metadata(db, channel_id)
        if resource.row_nb:
            await resource.fetch_rows(db, filter=filter)
        return resource

    async def _list_metadata_to_channels(self, db, metadata_list: list[ChannelMetadata]) -> list[ChannelResource]:
        channel_list = []
        for meta in metadata_list:
            channel_resource = ChannelResource(meta)
            await channel_resource.count_rows(db)
            channel_list.append(channel_resource)
        return channel_list

    @classmethod
    async def fetch_all(cls, db: PoolConnectionProxy, *, filter: RequestFilter | None = None) -> list[ChannelResource]:
        metadata_list = await ChannelMetadata.fetch_all(db, filter=filter)
        channel_list = []
        for metadata in metadata_list:
            channel = ChannelResource(metadata)
            await channel.fetch_rows(db, filter=filter)
            await channel.count_rows(db)
            channel_list.append(channel)
        return channel_list

    async def delete(self, db: PoolConnectionProxy) -> ChannelResource | None:
        existing = await self.metadata.delete(db)
        if not existing:
            return None
        await self.timeseries.drop(db)
        self.row_nb = 0
        return self

    @classmethod
    async def delete_channel_with_id(cls, db: PoolConnectionProxy, channel_id: UUID) -> ChannelResource | None:
        resource = await cls._fetch_metadata(db, channel_id)
        return await resource.delete(db)
