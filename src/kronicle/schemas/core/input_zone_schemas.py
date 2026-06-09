# kronicle/schemas/core/input_zone_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

# Zone name: allowed characters after the first letter
_ALLOWED_CHARS = "A-Za-z0-9_ .-"
_ZONE_NAME_MIN_LENGTH = 4
_ZONE_NAME_MAX_LENGTH = 64

# Regex: first char is letter, rest are from ALLOWED_CHARS, total length 4–64
_ZONE_NAME_REGEX = rf"[A-Za-z][{_ALLOWED_CHARS}]{{{_ZONE_NAME_MIN_LENGTH - 1},{_ZONE_NAME_MAX_LENGTH - 1}}}"

mod = "ingrp"


class InputZone(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique zone name")
    details: dict[str, Any] = Field(default_factory=dict, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(v)
        except ValueError as e:
            raise BadRequestError(f"Zone {e}") from e


class InputZonePatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100, description="Zone name")
    details: dict[str, Any] | None = Field(default=None, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_zone_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(v)
        except ValueError as e:
            raise BadRequestError(f"Zone {e}") from e
