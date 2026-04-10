# kronicle/db/rbac/models/rbac_group.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacGroup(RbacEntity, KronicleHierarchyMixin):
    __tablename__ = "groups"

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


# Setup the hierarchy table and children relationship dynamically
# RbacGroup._setup_hierarchy()
