# kronicle/schemas/rbac/input_schema.py
from __future__ import annotations

from json import dumps
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

mod = "input"


# Channel name: First a letter, then only letters, digits and '_'
_EXTRA_CHARS = "_.- "
_NAME_MIN_LENGTH = 4
_NAME_MAX_LENGTH = 64

_MAX_JSON_SIZE = 64 * 1024  # 64KB


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

    @model_validator(mode="before")
    @classmethod
    def validate_json_size(cls, data: Any) -> Any:
        if isinstance(data, dict) and "details" in data:
            details = data["details"]
            # Approximate size by converting to JSON string
            if len(dumps(details).encode("utf-8")) > _MAX_JSON_SIZE:
                raise ValueError("details field is too large")
        return data

    @field_validator("details")
    def validate_details_dict(cls, v: dict | None) -> dict | None:
        if not v:
            return None

        return v


class NamedInputSchema(InputSchema):
    name: str = Field(default=None, min_length=_NAME_MIN_LENGTH, max_length=_NAME_MAX_LENGTH)  # type: ignore
