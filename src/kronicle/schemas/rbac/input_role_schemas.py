# kronicle/schemas/rbac/input_role_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax


class InputRole(BaseModel):
    """Schema for creating or updating a role."""

    name: str = Field(..., min_length=1, max_length=100, description="Unique role name")
    description: str | None = Field(default=None, description="Human-readable description")
    permissions: list[str] = Field(default_factory=list, description="List of permission strings granted by this role")
    restrictions: list[str] = Field(default_factory=list, description="List of permission strings denied by this role")
    details: dict[str, Any] = Field(default_factory=dict, description="Optional JSONB metadata")

    @field_validator("name")
    def validate_user_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(v)
        except ValueError as e:
            raise BadRequestError(f"Role {e}") from e
