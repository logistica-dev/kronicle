# kronicle/db/core/models/core_zone.py
from __future__ import annotations

from kronicle.db.core.models.core_entity import CoreEntity


class CoreZone(CoreEntity):
    __tablename__ = "zones"
