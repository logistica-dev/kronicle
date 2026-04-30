# kronicle/db/core/links/zone_hierarchy.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.core.links.core_link import CoreLink
from kronicle.db.core.models.core_zone import Zone


class ZoneHierarchy(CoreLink):
    """
    Zone hierarchy definition.

    - Strict tree structure
    - Each zone has at most one parent
    - Used for physical or logical partitioning
    """

    UQ_CONSTRAINT = "uq_zone_parent"

    __tablename__ = "zone_hierarchy"
    __table_args__ = (
        UniqueConstraint(CoreLink.PARENT_ID, CoreLink.CHILD_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": CoreLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    # Note: ondelete=CASCADE
    # => If a referenced RbacGroup row is deleted, automatically delete the rows in this table that point to it.
    parent_id: Mapped[UUID] = mapped_column(ForeignKey(Zone.id, ondelete="CASCADE"), primary_key=True)
    child_id: Mapped[UUID] = mapped_column(ForeignKey(Zone.id, ondelete="CASCADE"), primary_key=True)

    parent = relationship(Zone, foreign_keys=[parent_id], backref="child_links")
    child = relationship(Zone, foreign_keys=[child_id], backref="parent_links")
