# kronicle/db/core/models/core_zone.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.core.models.core_entity import CoreEntity


class Zone(CoreEntity, KronicleHierarchyMixin):
    __tablename__ = "zones"

    # ----------------------------------------------------------------------------------------------
    # Hierarchy
    # ----------------------------------------------------------------------------------------------
    parent_zone_id: Mapped[UUID] = mapped_column(
        ForeignKey(f"{CoreEntity.namespace()}.{__tablename__}.id"),
        ondelete="SET NULL",
        nullable=True,
    )

    parent: Mapped[Zone] = relationship("Zone", remote_side=lambda: Zone.id, backref="children")

    # ----------------------------------------------------------------------------------------------
    # Snapshot
    # ----------------------------------------------------------------------------------------------
    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
        }


# Setup the hierarchy table and children relationship dynamically
# Zone._setup_hierarchy()
