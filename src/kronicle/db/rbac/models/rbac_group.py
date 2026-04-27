# kronicle/db/rbac/models/rbac_group.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import Mapped, Session, relationship

from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacGroup(RbacEntity):
    """
    RBAC Group model.

    Groups are hierarchical nodes used for permission inheritance.

    Structure:
        - A group can have one parent (tree structure)
        - A group can have multiple children
        - Hierarchy logic is handled by HierarchyService, not here
    """

    __tablename__ = "groups"

    # ---------------------------------------------------------------------
    # Hierarchy relationships (used by HierarchyDescriptor)
    # ---------------------------------------------------------------------

    parent_links: Mapped[list[RbacGroup]] = relationship(
        "RbacGroup",
        secondary="rbac.groups_hierarchy",
        primaryjoin="RbacGroup.id == groups_hierarchy.c.child_id",
        secondaryjoin="RbacGroup.id == groups_hierarchy.c.parent_id",
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
            "user_ids": [str(u.id) for u in getattr(self, "users", [])],
        }

    # ----------------------------------------------------------------------------------------------
    # Read table
    # ----------------------------------------------------------------------------------------------
    @classmethod
    def fetch(
        cls,
        db: Session,
        id: UUID | None = None,
        name: str | None = None,
    ) -> RbacGroup | list[RbacGroup]:
        q = db.query(RbacGroup)
        if id:
            return q.filter(cls.id == id).first()
        if name:
            return q.filter(cls.name == name).first()
        else:
            return q.all()
