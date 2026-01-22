# kronicle/db/rbac/models/rbac_group.py
from typing import Any

from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacGroup(RbacEntity, KronicleHierarchyMixin):
    __tablename__ = "groups"

    name: Mapped[str] = mapped_column(nullable=False, unique=True)

    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "user_ids": [str(u.id) for u in getattr(self, "users", [])],
        }


# Setup the hierarchy table and children relationship dynamically
# RbacGroup._setup_hierarchy()
