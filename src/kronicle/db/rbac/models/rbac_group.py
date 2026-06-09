# kronicle/db/rbac/models/rbac_group.py
from __future__ import annotations

from typing import Any

from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.utils.str_utils import uuid_to_str


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

    # ----------------------------------------------------------------------------------------------
    # Snapshot
    # ----------------------------------------------------------------------------------------------
    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": uuid_to_str(self.id),
            "name": self.name,
            "user_ids": [str(u.id) for u in getattr(self, "users", [])],
        }
