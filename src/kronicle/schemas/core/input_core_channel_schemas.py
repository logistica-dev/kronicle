# kronicle/schemas/core/input_core_channel_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

# Channel name: First a letter, then only letters, digits and '_'
_EXTRA_CHARS = "_.- "
_CHAN_NAME_MIN_LENGTH = 4
_CHAN_NAME_MAX_LENGTH = 32


class InputCoreChannelPatch(BaseModel):
    name: str | None = Field(
        default=None,
        min_length=_CHAN_NAME_MIN_LENGTH,
        max_length=_CHAN_NAME_MAX_LENGTH,
        description="Core channel name",
    )
    zone_id: UUID | None = Field(default=None, description="Zone UUID to assign")
    details: dict[str, Any] | None = Field(default=None, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(
                v,
                extra_chars=_EXTRA_CHARS,
                min_length=_CHAN_NAME_MIN_LENGTH,
                max_length=_CHAN_NAME_MAX_LENGTH,
            )
        except ValueError as e:
            raise BadRequestError(f"CoreChannel {e}") from e


class InputCoreChannel(BaseModel):
    id: UUID
    name: str | None = None
    zone_id: UUID | None = None
    details: dict[str, Any] | None = None

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(
                v,
                extra_chars=_EXTRA_CHARS,
                min_length=_CHAN_NAME_MIN_LENGTH,
                max_length=_CHAN_NAME_MAX_LENGTH,
            )
        except ValueError as e:
            raise BadRequestError(f"CoreChannel {e}") from e
