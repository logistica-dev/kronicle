# kronicle/schemas/core/safe_core_channel_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.db.core.models.core_channel import CoreChannel


class OutputCoreChannel(BaseModel):
    id: UUID
    name: str
    zone_id: UUID | None = None
    details: dict[str, Any] | None = None

    @classmethod
    def from_db_core_channel(cls, db_channel: CoreChannel) -> OutputCoreChannel:
        details = db_channel.details if db_channel.details else None
        return cls(
            id=db_channel.id,
            name=db_channel.name,
            zone_id=db_channel.zone_id,
            details=details,
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        if not self.details:
            d.pop("details", None)
        return d
