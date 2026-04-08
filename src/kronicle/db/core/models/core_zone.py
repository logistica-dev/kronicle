# kronicle/db/core/models/core_zone.py
from __future__ import annotations

from typing import Any

from sqlalchemy import UUID, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.core.models.core_entity import CoreEntity


class Zone(CoreEntity, KronicleHierarchyMixin):
    __tablename__ = "zones"

    parent_zone_id: Mapped[UUID] = mapped_column(ForeignKey("zones.id"), ondelete="SET NULL", nullable=True)

    parent: Mapped[Zone] = relationship("Zone", remote_side="Zone.id", backref="children")

    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
        }


# Setup the hierarchy table and children relationship dynamically
# Zone._setup_hierarchy()
