# kronicle/db/base/hierarchy/zone_hierarchy.py

from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.core.models.core_zone import Zone

"""
Zone hierarchy definition.

- Strict tree structure
- Each zone has at most one parent
- Used for physical or logical partitioning
"""


class ZoneHierarchy(KronicleBase):
    __tablename__ = "zone_hierarchy"
    __table_args__ = {"schema": Zone.namespace()}

    parent_id = Column(ForeignKey(Zone.id, ondelete="CASCADE"), primary_key=True)

    child_id = Column(ForeignKey(Zone.id, ondelete="CASCADE"), primary_key=True)

    parent = relationship(Zone, foreign_keys=[parent_id], backref="child_links")
    child = relationship(Zone, foreign_keys=[child_id], backref="parent_links")

    # --- Minimal persistence API ---
    @classmethod
    def add(cls, session, parent: Zone, child: Zone):
        session.add(cls(parent_id=parent.id, child_id=child.id))

    @classmethod
    def remove(cls, session, parent: Zone, child: Zone):
        session.query(cls).filter_by(parent_id=parent.id, child_id=child.id).delete()
