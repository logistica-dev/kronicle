# kronicle/db/rbac/models/rbac_group.py
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Mapped, relationship

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
    # Hierarchy relationships
    # ---------------------------------------------------------------------

    parent_links: Mapped[list[RbacGroup]] = relationship(
        "RbacGroup",
        secondary="rbac.group_hierarchy",
        primaryjoin="RbacGroup.id == group_hierarchy.c.child_id",
        secondaryjoin="RbacGroup.id == group_hierarchy.c.parent_id",
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
