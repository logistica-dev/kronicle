# kronicle/db/core/links/zone_hierarchy.py
from __future__ import annotations

from sqlalchemy import UniqueConstraint

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchy
from kronicle.db.core.links.core_link import CoreLink
from kronicle.db.core.models.core_zone import CoreZone


class ZoneHierarchy(KronicleHierarchy[CoreZone]):
    """
    Zone hierarchy definition.

    - Strict tree structure
    - Each zone has at most one parent
    - Used for physical or logical partitioning
    """

    node_model = CoreZone

    UQ_CONSTRAINT = "uq_zone_parent"

    __tablename__ = "zone_hierarchy"
    __table_args__ = (
        UniqueConstraint(
            KronicleHierarchy.PARENT_ID,
            KronicleHierarchy.CHILD_ID,
            name=UQ_CONSTRAINT,
        ),  # Tuple of constraints first
        {"schema": CoreLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    @classmethod
    def namespace(cls) -> str:
        return CoreLink.namespace()
