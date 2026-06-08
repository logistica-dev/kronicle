# kronicle/schemas/rbac/safe_group_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.db.rbac.models.rbac_group import RbacGroup


class OutputGroup(BaseModel):
    id: UUID
    name: str
    details: dict[str, Any] | None = None

    @classmethod
    def from_db_group(cls, db_group: RbacGroup) -> OutputGroup:
        details = db_group.details if db_group.details else None
        return cls(
            id=db_group.id,
            name=db_group.name,
            details=details,
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        if not self.details:
            d.pop("details", None)
        return d
