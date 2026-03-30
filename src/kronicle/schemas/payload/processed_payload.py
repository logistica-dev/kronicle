# kronicle/schemas/payload/processed_payload.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.types.tag_type import TagType
from kronicle.utils.str_utils import ensure_uuid4, normalize_name, sanitize_dict


class ProcessedPayload(BaseModel):
    """
    Fully processed representation of an incoming payload.

    This model unifies:
    - Channel metadata processing (ID, schema, name, metadata, tags)
    - Optional row validation against the channel schema
    - Operation status tracking (success / warning)

    It guarantees:
    - A valid UUID4 channel_id
    - A resolved ChannelSchema instance
    - Sanitized metadata and tags
    - Validated rows (if provided)
    - A consistent operation status and warning structure

    The model can be used in two modes:
    1. Metadata-only (no rows provided)
    2. Full payload processing (rows validated against schema)

    In non-strict mode, row validation errors are stored as warnings.
    In strict mode, any row validation error raises BadRequestError.
    """

    # ----------------------------------------------------------------------------------
    # Channel / metadata fields
    # ----------------------------------------------------------------------------------

    channel_id: UUID
    channel_schema: ChannelSchema
    name: str | None = None
    metadata: dict[str, Any] | None = None
    tags: dict[str, TagType] | None = None
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    # ----------------------------------------------------------------------------------
    # Data rows + operation status
    # ----------------------------------------------------------------------------------

    rows: list[dict[str, Any]] = Field(default_factory=list)

    op_feedback: OpFeedback = Field(default_factory=OpFeedback)

    # op_status: str = Field(
    #     default="success",
    #     description="Overall operation status of the processing (success|warning).",
    # )
    # op_details: dict[str, Any] = Field(
    #     default_factory=dict,
    #     description="Additional validation details or warnings.",
    # )

    # ----------------------------------------------------------------------------------
    # Sanitization helpers
    # ----------------------------------------------------------------------------------

    @classmethod
    def normalize_name(cls, name: str) -> str:
        return normalize_name(name, prefix="channel_")

    @classmethod
    def sanitize_metadata(cls, d) -> dict[str, Any]:
        return sanitize_dict(d, "metadata", cast_values=False)

    @classmethod
    def sanitize_tags(cls, d) -> dict[str, TagType]:
        return sanitize_dict(d, "tags", cast_values=True)

    # ----------------------------------------------------------------------------------
    # Factory
    # ----------------------------------------------------------------------------------
    @classmethod
    def from_input(
        cls,
        payload: InputPayload,
        channel_schema: ChannelSchema | None = None,
        strict: bool = True,
    ) -> ProcessedPayload:
        """
        Create a ProcessedPayload from an InputPayload.

        Steps:
        1. Normalize and validate channel metadata.
        2. Sanitize metadata and tags.
        3. Attach rows (if provided).
        4. Validate rows against the channel schema.
        5. Set operation feedback accordingly.

        Args:
            payload: Incoming user payload.
            schema: Optional preloaded ChannelSchema.
            strict:
                - True  → raise BadRequestError on any row error.
                - False → collect row errors as warnings.

        Returns:
            ProcessedPayload

        Raises:
            BadRequestError:
                - If schema is missing when rows are provided.
                - If strict mode and row validation fails.
                - If no valid rows remain after validation.
        """
        processed = cls(
            channel_id=ensure_uuid4(payload.channel_id),
            channel_schema=channel_schema if channel_schema else payload.ensure_channel_schema(),
            name=normalize_name(payload.name, prefix="channel_") if payload.name else None,
            metadata=cls.sanitize_metadata(payload.metadata),
            tags=cls.sanitize_tags(payload.tags),
            rows=payload.rows or [],
            op_feedback=OpFeedback(),
        )

        if processed.rows:
            processed._validate_rows(strict=strict)
        return processed

    # ----------------------------------------------------------------------------------
    # Internal row validation
    # ----------------------------------------------------------------------------------

    def _validate_rows(self, strict: bool = False) -> ProcessedPayload:
        """
        Validate rows against the channel schema.

        - Updates self.rows to contain only validated rows.
        - Collects validation errors per row.

        Args:
            strict:
                If True, raises BadRequestError on first validation failure set.

        Returns:
            Dictionary mapping row index labels to validation error messages.
        """
        if not self.channel_schema:
            raise BadRequestError(
                "Cannot validate rows: no schema available.",
                details={"channel_id": str(self.channel_id)},
            )
        validated_rows: list[dict[str, Any]] = []

        pad_width = len(str(len(self.rows)))
        for idx, row in enumerate(self.rows, start=1):
            try:
                validated_rows.append(self.channel_schema.validate_row(row, from_user=True))
            except ValueError as e:
                row_label = f"row_{str(idx).zfill(pad_width)}"
                self.op_feedback.add_detail(message=str(e), field="rows", subfield=row_label)

        if strict and self.op_feedback.has_details:
            raise BadRequestError(
                "Validation failed for some rows",
                details=self.op_feedback.json(),
            )

        if self.rows and not validated_rows:
            # No valid rows survived
            self.op_feedback.add_detail("No valid rows to insert", field="rows")
            raise BadRequestError(
                "No valid rows to insert",
                details=self.op_feedback.json(),
            )

        self.rows = validated_rows
        return self
