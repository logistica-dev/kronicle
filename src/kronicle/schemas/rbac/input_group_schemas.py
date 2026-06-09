# kronicle/schemas/rbac/input_group_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

mod = "ingrp"


class InputGroup(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique group name")
    details: dict[str, Any] = Field(default_factory=dict, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_group_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(v)
        except ValueError as e:
            raise BadRequestError(f"Group {e}") from e
