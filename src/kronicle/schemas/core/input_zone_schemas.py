# kronicle/schemas/core/input_zone_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

# Zone name: First a letter, then only letters, digits and '_'
_EXTRA_CHARS = "_.- "
_ZONE_NAME_MIN_LENGTH = 4
_ZONE_NAME_MAX_LENGTH = 32


mod = "ingrp"


class InputZone(BaseModel):
    name: str = Field(
        ...,
        min_length=_ZONE_NAME_MIN_LENGTH,
        max_length=_ZONE_NAME_MAX_LENGTH,
        description="Unique zone name",
    )
    details: dict[str, Any] = Field(default_factory=dict, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(
                v,
                extra_chars=_EXTRA_CHARS,
                min_length=_ZONE_NAME_MIN_LENGTH,
                max_length=_ZONE_NAME_MAX_LENGTH,
            )
        except ValueError as e:
            raise BadRequestError(f"Zone {e}") from e


class InputZonePatch(BaseModel):
    name: str | None = Field(
        default=None,
        min_length=_ZONE_NAME_MIN_LENGTH,
        max_length=_ZONE_NAME_MAX_LENGTH,
        description="Zone name",
    )
    details: dict[str, Any] | None = Field(default=None, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(
                v,
                extra_chars=_EXTRA_CHARS,
                min_length=_ZONE_NAME_MIN_LENGTH,
                max_length=_ZONE_NAME_MAX_LENGTH,
            )
        except ValueError as e:
            raise BadRequestError(f"Zone {e}") from e
