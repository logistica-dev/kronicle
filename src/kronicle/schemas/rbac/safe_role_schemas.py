# kronicle/schemas/rbac/safe_role_schemas.py
from __future__ import annotations

from kronicle.schemas.output_schema import OutputSchema


class OutputRole(OutputSchema):
    description: str | None = None
    permissions: list[str] = []
    restrictions: list[str] = []
