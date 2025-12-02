# kronicle/controller/output_payloads.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ValidationInfo, field_serializer, field_validator, model_serializer

from kronicle.controller.processed_payload import ProcessedPayload
from kronicle.db.sensor_metadata import SensorMetadata
from kronicle.db.sensor_schema import SensorSchema
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_d
from kronicle.utils.dict_utils import ensure_dict_or_none, rows_to_columns, strip_nulls


# --------------------------------------------------------------------------------------------------
# ResponsePayload
# --------------------------------------------------------------------------------------------------
class ResponsePayload(BaseModel):
    """
    Payload returned in responses to the user.
    - issued_at: when the response was generated
    - available_rows: number of rows stored for this sensor (None if unknown)
    """

    sensor_id: UUID
    sensor_schema: SensorSchema
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
        metadata: SensorMetadata,
        available_rows: int | None = None,
    ) -> "ResponsePayload":
        res = cls(
            sensor_id=metadata.sensor_id,
            sensor_schema=metadata.sensor_schema,
            name=metadata.sensor_name,
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
        cls, metadata: SensorMetadata, rows: list | None = None, *, skip_received: bool = True
    ) -> "ResponsePayload":
        return cls(
            sensor_id=metadata.sensor_id,
            sensor_schema=metadata.sensor_schema,
            name=metadata.sensor_name,
            metadata=metadata.metadata,
            tags=metadata.tags,
            rows=cls.normalize_rows(sensor_schema=metadata.sensor_schema, rows=rows, skip_received=skip_received),
            op_details={"available_rows": len(rows) if rows else 0},
            available_rows=len(rows) if rows else None,
        )

    @classmethod
    def from_processing_and_insertion(
        cls,
        processed: ProcessedPayload,
        metadata: SensorMetadata,
        available_rows: int | None = None,
    ) -> "ResponsePayload":
        """
        Create a ResponsePayload from the result of processing and DB insertion.
        - `processed`: ProcessedPayload object (contains validated rows and op_status/op_details)
        - `metadata`: SensorMetadata object from DB
        - `available_rows`: optional number of rows stored
        """
        res = cls(
            sensor_id=metadata.sensor_id,
            sensor_schema=metadata.sensor_schema,
            name=metadata.sensor_name,
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

    @field_serializer("sensor_schema")
    def flatten_schema(self, sensor_schema, _info):
        """Field serializer for sensor_schema"""
        return sensor_schema.model_dump(flatten=True)

    @field_serializer("name", "metadata", "tags", "rows", "columns")
    def skip_empty_fields(self, value, _info):
        return None if not value else value

    # @field_serializer("columns")
    # def serialize_cols(self, cols: dict[str, list] | None):
    #     """Ensure all datetime fields are expressed with local timezone."""
    #     if cols is None:
    #         return None
    #     return self.sensor_schema.db_cols_to_user_cols(cols)

    @model_serializer(mode="wrap")
    def remove_nulls(
        self,
        serializer,
    ):
        return strip_nulls(serializer(self))

    @classmethod
    def normalize_rows(
        cls,
        sensor_schema: SensorSchema,
        rows: list[dict] | None,
        *,
        skip_received: bool = True,
    ):
        """Ensure all datetime fields are expressed with local timezone."""
        return None if rows is None else sensor_schema.db_rows_to_user_rows(rows, skip_received=skip_received)

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

    input_schema = {"temperature": "number", "humidity": "float", "time": "datetime"}

    # --- response from metadata only ---
    meta = SensorMetadata(
        sensor_id=uuid4(),
        sensor_name=str(uuid4()),
        sensor_schema=SensorSchema.from_user_json(input_schema),
        metadata={"unit": "Celsius"},
        tags={"room": 101},
    )
    resp2 = ResponsePayload.from_metadata(meta)
    log_d(here, "Response from metadata :", resp2.model_dump())

    # --- response from processed payload ---
    payload = InputPayload(
        sensor_id=uuid4(),
        name=str(uuid4()),
        sensor_schema=input_schema,
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
