# kronicle/schemas/rbac/input_role_schemas.py
from __future__ import annotations

from pydantic import Field

from kronicle.schemas.input_schema import NamedInputSchema


class InputRole(NamedInputSchema):
    """Schema for creating or updating a role."""

    description: str | None = Field(default=None, description="Human-readable description")
    permissions: list[str] = Field(default_factory=list, description="List of permission strings granted by this role")
    restrictions: list[str] = Field(default_factory=list, description="List of permission strings denied by this role")
