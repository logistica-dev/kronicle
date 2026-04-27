# kronicle/db/base/hierarchy/group_hierarchy.py

from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import Session, relationship

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.rbac.models.rbac_group import RbacGroup

"""
RBAC group hierarchy definition.

- Can be a DAG or tree depending on configuration
- Used for permission inheritance
"""


class GroupHierarchy(KronicleBase):
    __tablename__ = "group_hierarchy"
    __table_args__ = {"schema": RbacGroup.namespace()}

    parent_id = Column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True)
    child_id = Column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True)

    # ORM navigation (important for traversal)
    parent = relationship(RbacGroup, foreign_keys=[parent_id], backref="child_links")
    child = relationship(RbacGroup, foreign_keys=[child_id], backref="parent_links")

    # --- Minimal persistence API ---
    @classmethod
    def add(cls, session: Session, parent: RbacGroup, child: RbacGroup):
        session.add(cls(parent_id=parent.id, child_id=child.id))

    @classmethod
    def remove(cls, session, parent: RbacGroup, child: RbacGroup):
        session.query(cls).filter_by(parent_id=parent.id, child_id=child.id).delete()
