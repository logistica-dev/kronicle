# kronicle/schemas/rbac/input_zone_schemas.py
from __future__ import annotations

from re import fullmatch
from typing import Any

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError

from typing import Any

from pydantic import BaseModel, Field

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
        if v is None:
            return v
        if not fullmatch(_ZONE_NAME_REGEX, v):
            raise BadRequestError(
                "Zone name must start with a letter, be 4–64 characters long, "
                "and only contain letters, digits, '_', '.', '-', or space"
            )
        return v
