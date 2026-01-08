# kronicle/controller/processed_payloads.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError

from kronicle.controller.input_payloads import InputPayload
from kronicle.db.channel_metadata import ChannelMetadata
from kronicle.db.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import ensure_uuid4, normalize_to_snake_case


# --------------------------------------------------------------------------------------------------
# ProcessedMetadata (metadata-only)
# --------------------------------------------------------------------------------------------------
class ProcessedMetadata(BaseModel):
    """
    Processed metadata only, sanitized and normalized.
    Guaranteed to be valid metadata, tags, schema, and channel info.
    """

    channel_id: UUID
    channel_schema: ChannelSchema
    channel_name: str
    metadata: dict[str, Any]
    tags: dict[str, str | int | float | list]
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    @classmethod
    def sanitize_dict(
        cls, d: dict[str, Any] | None = None, field_name: str = "", cast_values: bool = True
    ) -> dict[str, str | int | float | list]:
        out: dict[str, str | int | float | list] = {}
        for k, v in (d or {}).items():
            if not isinstance(k, str) or not k.strip():
                raise ValueError(f"Invalid {field_name+' ' if field_name else ''}key: {k}")
            if cast_values and not isinstance(v, (str, int, float, list)):
                v = str(v)
            out[k] = v
        return out

    @classmethod
    def from_input(cls, payload: InputPayload, schema: ChannelSchema | None = None) -> ProcessedMetadata:
        channel_id = ensure_uuid4(payload.channel_id)
        channel_schema: ChannelSchema = schema if schema else payload.ensure_channel_schema()
        channel_name = normalize_to_snake_case(payload.name) if payload.name else ""
        metadata = cls.sanitize_dict(payload.metadata, "metadata", cast_values=False)
        tags = cls.sanitize_dict(payload.tags, "tags", cast_values=True)

        return cls(
            channel_id=channel_id,
            channel_schema=channel_schema,
            channel_name=channel_name,
            metadata=metadata,
            tags=tags,
        )

    def to_db_metadata(self) -> ChannelMetadata:
        return ChannelMetadata(
            channel_id=self.channel_id,
            channel_schema=self.channel_schema,
            channel_name=self.channel_name,
            metadata=self.metadata,
            tags=self.tags,
        )


# --------------------------------------------------------------------------------------------------
# ProcessedPayload (metadata + rows + operation status)
# --------------------------------------------------------------------------------------------------
class ProcessedPayload(ProcessedMetadata):
    """
    Processed payload including validated rows.
    Rows are validated against channel_schema.
    Stores operation metadata (status and warnings) in op_status/op_details.
    """

    rows: list[dict[str, Any]]
    op_status: str = Field(default="success", description="Overall operation status of the processing")
    op_details: dict[str, Any] = Field(default_factory=dict, description="Additional details/warnings from processing")

    @classmethod
    def from_input(
        cls, payload: InputPayload, schema: ChannelSchema | None = None, strict: bool = True
    ) -> ProcessedPayload:
        """
        Process input payload with rows.
        Strict mode: raises BadRequestError on any row validation error.
        Non-strict mode: stores warnings in op_status/op_details.
        """
        base = ProcessedMetadata.from_input(payload, schema)

        # Ensure rows exist
        if not payload.rows:
            raise BadRequestError("No rows to process", details={"channel_id": str(payload.channel_id)})

        processed = cls(**base.model_dump(), rows=payload.rows)

        # Validate rows
        warnings = processed._validate_rows(strict=strict)

        # Store warnings in op_status/op_details if not strict
        if warnings:
            processed.op_status = "warning"
            processed.op_details["rows"] = warnings
        else:
            processed.op_status = "success"

        return processed

    def _validate_rows(self, strict: bool = False) -> dict[str, str]:
        """
        Private helper to validate rows against channel_schema.
        Updates self.rows to validated rows.
        """
        if not self.rows:
            raise BadRequestError("No rows to validate", details={"channel_id": str(self.channel_id)})
        if not self.channel_schema:
            raise BadRequestError(
                "Cannot validate rows: no schema available.", details={"channel_id": str(self.channel_id)}
            )

        validated_rows = []
        warnings: dict[str, str] = {}
        pad_width = len(str(len(self.rows)))

        for idx, row in enumerate(self.rows, start=1):
            try:
                assert isinstance(self.channel_schema, ChannelSchema)
                validated_rows.append(self.channel_schema.validate_row(row, from_user=True))
            except ValueError as e:
                warnings[f"row_{str(idx).zfill(pad_width)}"] = str(e)

        if strict and warnings:
            raise BadRequestError(
                "Validation failed for some rows", details={"channel_id": str(self.channel_id), **warnings}
            )

        if not validated_rows:
            raise BadRequestError("No valid rows to insert", details={"channel_id": str(self.channel_id), **warnings})

        self.rows = validated_rows
        return warnings


# --------------------------------------------------------------------------------------------------
# Simple test / sanity check
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "in_payload.test"
    from uuid import uuid4

    from kronicle.utils.dev_logs import log_d

    log_d(here, "=== channel_payloads.py main test ===")

    # --- create sample schema from InputSchema ---
    input_schema = {"temperature": "number", "humidity": "float", "time": "datetime"}

    # --- create input payload ---
    payload = InputPayload(
        channel_id=uuid4(),
        channel_schema=input_schema,
        metadata={"location": "lab", "unit": "C"},
        tags={"room": 101},
        rows=[
            {"time": IsoDateTime.now_local().isoformat(), "temperature": 22.5, "humidity": 55.0},
            {"time": IsoDateTime.now_local(), "temperature": 23.0, "humidity": 53.0},
        ],
    )
    log_d(here, "InputPayload OK :", payload)

    # --- process payload ---
    processed_meta = ProcessedMetadata.from_input(payload)
    log_d(here, "ProcessedPayload OK :", processed_meta.model_dump())
    log_d(here, "Received_at:", processed_meta.received_at)

    # --- test sanitization: bad metadata key ---
    try:
        bad_payload = InputPayload(channel_id=uuid4(), metadata={"": "empty key"})
        ProcessedPayload.from_input(bad_payload)
    except ValueError as e:
        log_d(here, "Caught expected sanitization error :", e)

    # --- test validator: tags must be dict ---
    try:
        InputPayload(channel_id=uuid4(), tags=["not", "a", "dict"])  # type: ignore
    except (ValidationError, TypeError) as e:
        log_d(here, "Caught expected validation error :")
        log_d(here, e)

    # --- test validator: empty dicts default ---
    empty_payload = InputPayload(channel_id=uuid4())
    log_d(here, "Empty InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())

    log_d(here, "=== End of channel_payloads.py test ===")

    # --- test validator: no uuid ---
    empty_payload = InputPayload()  # type: ignore
    log_d(here, "No ID InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())
