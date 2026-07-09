# kronicle/schemas/rbac/input_schema.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

mod = "input"


# Channel name: First a letter, then only letters, digits and '_'
_EXTRA_CHARS = "_.- "
_NAME_MIN_LENGTH = 4
_NAME_MAX_LENGTH = 64


class InputSchema(BaseModel):
    id: UUID | None = None
    name: str | None = Field(default=None, min_length=_NAME_MIN_LENGTH, max_length=_NAME_MAX_LENGTH)
    details: dict[str, Any] | None = Field(default=None, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_group_name_syntax(cls, v: str | None) -> str | None:
        if not v:
            return None
        try:
            return validate_name_syntax(
                v,
                extra_chars=_EXTRA_CHARS,
                min_length=_NAME_MIN_LENGTH,
                max_length=_NAME_MAX_LENGTH,
            )
        except ValueError as e:
            raise BadRequestError(f"{cls.__name__} {e}") from e


class NamedInputSchema(InputSchema):
    name: str = Field(default=None, min_length=_NAME_MIN_LENGTH, max_length=_NAME_MAX_LENGTH)  # type: ignore
