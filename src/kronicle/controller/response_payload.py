# kronicle/controller/output_payloads.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ValidationInfo, field_serializer, field_validator, model_serializer

from kronicle.controller.processed_payload import ProcessedPayload
from kronicle.db.channel_metadata import ChannelMetadata
from kronicle.db.channel_schema import ChannelSchema
from kronicle.types.iso_datetime import IsoDateTime
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
    tags: dict[str, str | int | float | list] | None = None

    # data
    rows: list[dict[str, Any]] | None = None
    columns: dict[str, list] | None = None
    available_rows: int | None = None

    # operation metadata
    op_status: str = Field(default="success", description="Overall operation status: success/warning/error")
    op_details: dict = Field(default_factory=dict)

    @field_validator("metadata", "tags", mode="before")
    @classmethod
    def ensure_dict_or_none(cls, d, info: ValidationInfo) -> dict:
        return ensure_dict_or_none(d, info.field_name)

    def model_post_init(self, __context=None):
        # Ensure op_details is always at least {'issued_at': ...}
        self.op_status = self.op_status or "success"
        self.op_details.setdefault("issued_at", IsoDateTime.now_local())

    @classmethod
    def from_metadata(
        cls,
        metadata: ChannelMetadata,
        available_rows: int | None = None,
    ) -> "ResponsePayload":
        res = cls(
            channel_id=metadata.channel_id,
            channel_schema=metadata.channel_schema,
            name=metadata.channel_name,
            metadata=metadata.metadata,
            tags=metadata.tags,
            rows=None,
            available_rows=available_rows,
        )
        if available_rows is not None:
            res.op_details["available_rows"] = available_rows
        return res

    @classmethod
    def from_db_data(
        cls, metadata: ChannelMetadata, rows: list | None = None, *, skip_received: bool = True
    ) -> "ResponsePayload":
        return cls(
            channel_id=metadata.channel_id,
            channel_schema=metadata.channel_schema,
            name=metadata.channel_name,
            metadata=metadata.metadata,
            tags=metadata.tags,
            rows=cls.normalize_rows(channel_schema=metadata.channel_schema, rows=rows, skip_received=skip_received),
            op_details={"available_rows": len(rows) if rows else 0},
            available_rows=len(rows) if rows else None,
        )

    @classmethod
    def from_processing_and_insertion(
        cls,
        processed: ProcessedPayload,
        metadata: ChannelMetadata,
        available_rows: int | None = None,
    ) -> "ResponsePayload":
        """
        Create a ResponsePayload from the result of processing and DB insertion.
        - `processed`: ProcessedPayload object (contains validated rows and op_status/op_details)
        - `metadata`: ChannelMetadata object from DB
        - `available_rows`: optional number of rows stored
        """
        res = cls(
            channel_id=metadata.channel_id,
            channel_schema=metadata.channel_schema,
            name=metadata.channel_name,
            metadata=metadata.metadata,
            tags=metadata.tags,
            rows=processed.rows,
            op_status=processed.op_status,
            op_details=processed.op_details.copy(),
            available_rows=available_rows if available_rows else None,
        )
        if available_rows is not None:
            res.op_details["available_rows"] = available_rows
        return res

    def with_op_status(self, status: str = "success", details: dict | None = None) -> "ResponsePayload":
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

    @classmethod
    def normalize_rows(
        cls,
        channel_schema: ChannelSchema,
        rows: list[dict] | None,
        *,
        skip_received: bool = True,
    ):
        """Ensure all datetime fields are expressed with local timezone."""
        return None if rows is None else channel_schema.db_rows_to_user_rows(rows, skip_received=skip_received)

    def __str__(self) -> str:
        return self.model_dump_json(indent=2)

    def rows_to_columns(self, *, strict: bool = False) -> None:
        if not self.rows:
            return
        self.columns = rows_to_columns(self.rows)
        if strict:
            self.rows = None


if __name__ == "__main__":
    here = "out_payload.test"

    from uuid import uuid4

    from kronicle.controller.input_payloads import InputPayload
    from kronicle.utils.dev_logs import log_d

    input_schema = {"temperature": "number", "humidity": "float", "time": "datetime"}

    # --- response from metadata only ---
    meta = ChannelMetadata(
        channel_id=uuid4(),
        channel_name=str(uuid4()),
        channel_schema=ChannelSchema.from_user_json(input_schema),
        metadata={"unit": "Celsius"},
        tags={"room": 101},
    )
    resp2 = ResponsePayload.from_metadata(meta)
    log_d(here, "Response from metadata :", resp2.model_dump())

    # --- response from processed payload ---
    payload = InputPayload(
        channel_id=uuid4(),
        name=str(uuid4()),
        channel_schema=input_schema,
        metadata={"location": "lab", "temperature_unit": "C", "humidity_unit": "g/m3"},
        tags={"room": 101},
        rows=[
            {"time": IsoDateTime.now_local().isoformat(), "temperature": 22.5, "humidity": 55.0},
            {"time": IsoDateTime.now_local(), "temperature": 23.0, "humidity": 53.0},
        ],
    )
    processed = ProcessedPayload.from_input(payload)
    resp3 = ResponsePayload.from_db_data(meta, processed.rows)
    log_d(here, "Response from processed :", resp3.model_dump())
