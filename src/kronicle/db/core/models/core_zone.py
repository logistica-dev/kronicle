# kronicle/db/core/models/core_zone.py
from __future__ import annotations

from typing import Any

from kronicle.db.core.models.core_entity import CoreEntity


class CoreZone(CoreEntity):
    __tablename__ = "zones"

    # ----------------------------------------------------------------------------------------------
    # Snapshot
    # ----------------------------------------------------------------------------------------------
    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
        }
