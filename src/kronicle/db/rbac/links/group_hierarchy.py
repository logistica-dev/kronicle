# kronicle/db/rbac/links/group_hierarchy.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_group import RbacGroup

"""
RBAC group hierarchy definition.

- Can be a DAG or tree depending on configuration
- Used for permission inheritance
"""


class RbacGroupHierarchy(RbacLink):

    UQ_CONSTRAINT = "uq_group_parent"

    __tablename__ = "group_hierarchy"
    __table_args__ = (
        UniqueConstraint(RbacLink.CHILD_ID, RbacLink.PARENT_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    parent_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True)
    child_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True)

    # ORM navigation (important for traversal)
    parents = relationship(RbacGroup, foreign_keys=[parent_id], backref="children")
