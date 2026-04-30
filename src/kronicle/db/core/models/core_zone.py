# kronicle/db/core/models/core_zone.py
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Mapped, relationship

from kronicle.db.core.links.core_link import CoreLink
from kronicle.db.core.models.core_entity import CoreEntity


class Zone(CoreEntity):
    __tablename__ = "zones"

    # ---------------------------------------------------------------------
    # Hierarchy relationships
    # ---------------------------------------------------------------------

    parent_links: Mapped[Zone] = relationship(
        "Zone",
        secondary=f"{CoreLink.namespace()}.zone_hierarchy",
        primaryjoin="Zone.id == zone_hierarchy.c.child_id",
        secondaryjoin="Zone.id == zone_hierarchy.c.parent_id",
        backref="child_links",
    )

    # ----------------------------------------------------------------------------------------------
    # Snapshot
    # ----------------------------------------------------------------------------------------------
    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "channel_ids": [str(chan.id) for chan in getattr(self, "channels", [])],
        }
