# kronicle/schemas/core/safe_zone_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.db.core.models.core_zone import CoreZone


class OutputZone(BaseModel):
    id: UUID
    name: str
    details: dict[str, Any] | None = None

    @classmethod
    def from_db_zone(cls, db_zone: CoreZone) -> OutputZone:
        details = db_zone.details if db_zone.details else None
        return cls(
            id=db_zone.id,
            name=db_zone.name,
            details=details,
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        if not self.details:
            d.pop("details", None)
        return d
