# kronicle/schemas/payload/response_payload.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ValidationInfo, field_serializer, field_validator, model_serializer

from kronicle.db.data.models.channel_resource import ChannelResource
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.types.tag_type import TagType
from kronicle.utils.dict_utils import ensure_dict_or_none, rows_to_columns, strip_nulls


# --------------------------------------------------------------------------------------------------
# ResponsePayload
# --------------------------------------------------------------------------------------------------
class ResponsePayload(BaseModel):
    """
    Payload returned in responses to the user.
    - issued_at: when the response was generated
    - available_rows: number of rows stored for this channel (None if unknown)
    """

    channel_id: UUID
    channel_schema: ChannelSchema
    name: str | None = None

    # labels
    metadata: dict[str, Any] | None = None
    tags: dict[str, TagType] | None = None

    # data
    rows: list[dict[str, Any]] | None = None
    columns: dict[str, list] | None = None

    # operation metadata
    op_details: dict = Field(default_factory=dict)

    @field_validator("metadata", "tags", mode="before")
    @classmethod
    def ensure_dict_or_none(cls, d, info: ValidationInfo) -> dict:
        return ensure_dict_or_none(d, info.field_name)

    # ----------------------------------------------------------------------------------------------
    # Factories
    # ----------------------------------------------------------------------------------------------
    @classmethod
    def from_channel_resource(
        cls,
        channel: ChannelResource,
        *,
        strict: bool = False,
        skip_received: bool = True,
    ) -> ResponsePayload:
        """
        Create a ResponsePayload from a ChannelResource.

        Responsibilities:
        - Extracts metadata from resource.metadata.
        - Converts timeseries rows to user-friendly format via ChannelSchema.
        - Computes available_rows.
        - Optional strict mode to remove raw rows if columns are generated.

        Parameters
        ----------
        resource : ChannelResource
            The channel resource containing metadata and timeseries.
        strict : bool, default=False
            If True, rows can be cleared after generating columns.
        skip_received : bool, default=True
            Whether to skip internal 'received_at' timestamps in rows conversion.

        Returns
        -------
        ResponsePayload
            Fully populated payload ready for response to user.
        """
        # here = "from_chan_resource"
        channel_metadata = channel.metadata
        channel_timeseries = channel.timeseries

        rows = channel_timeseries.to_user_rows(skip_received=skip_received)
        provided_rows = len(rows)
        payload = cls(
            channel_id=channel_metadata.channel_id,
            channel_schema=channel_metadata.channel_schema,
            name=channel_metadata.name,
            metadata=channel_metadata.user_metadata,
            tags=channel_metadata.tags,
            rows=rows,
            op_details={
                "provided_rows": provided_rows,
                "available_rows": channel.row_nb,
                **channel_timeseries.op_feedback.json(),
            },
        )

        # Optional: convert rows to columns and remove rows if strict
        if strict:
            payload.rows_to_columns(strict=True)
        return payload

    def with_op_status(self, status: str = "success", details: dict | None = None) -> ResponsePayload:
        """
        Set operation metadata (status and additional details).
        Automatically sets issued_at if not already set.
        """
        if status and status.lower() != "success":
            self.op_status = status
        if details:
            self.op_details.update(details)
        return self

    @field_serializer("channel_schema")
    def flatten_schema(self, channel_schema, _info):
        """Field serializer for channel_schema"""
        return channel_schema.model_dump(flatten=True)

    @field_serializer("name", "metadata", "tags", "rows", "columns")
    def skip_empty_fields(self, value, _info):
        return None if not value else value

    # @field_serializer("columns")
    # def serialize_cols(self, cols: dict[str, list] | None):
    #     """Ensure all datetime fields are expressed with local timezone."""
    #     if cols is None:
    #         return None
    #     return self.channel_schema.db_cols_to_user_cols(cols)

    @model_serializer(mode="wrap")
    def remove_nulls(
        self,
        serializer,
    ):
        return strip_nulls(serializer(self))

    def __str__(self) -> str:
        return self.model_dump_json(indent=2)

    def rows_to_columns(self, *, strict: bool = False) -> None:
        if not self.rows:
            return
        self.columns = rows_to_columns(self.rows)
        if strict:
            self.rows = None
