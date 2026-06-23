# kronicle/schemas/rbac/safe_role_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.db.rbac.models.rbac_role import RbacRole


class OutputRole(BaseModel):
    id: UUID
    name: str
    description: str | None = None
    permissions: list[str] = []
    restrictions: list[str] = []
    details: dict[str, Any] | None = None

    @classmethod
    def from_db_role(cls, db_role: RbacRole) -> OutputRole:
        return cls(
            id=db_role.id,
            name=db_role.name,
            description=db_role.description or None,
            permissions=db_role.permissions or [],
            restrictions=db_role.restrictions or [],
            details=db_role.details or None,
        )

    def model_dump(self, *args, **kwargs) -> dict:
        d = super().model_dump(*args, **kwargs)
        if not self.details:
            d.pop("details", None)
        if not self.description:
            d.pop("description", None)
        return d
