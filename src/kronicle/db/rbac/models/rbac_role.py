# kronicle/db/rbac/models/rbac_role.py
from typing import Any

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacRole(RbacEntity):
    """
    Represents a Role in the Kronicle RBAC system.

    A Role is an abstract permission template, independent of any resource instance.

    Attributes:
        name: Unique name of the role (e.g., "Reader", "Writer", "Admin").
        permissions: List of permission strings granted by this role.
        restrictions: List of permission strings explicitly denied by this role.
    """

    __tablename__ = "roles"

    description: Mapped[str] = mapped_column(nullable=True)
    permissions: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    restrictions: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)

    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "permissions": self.permissions,
            "restrictions": self.restrictions,
            "is_global": self.is_global,
        }

    def __repr__(self) -> str:
        return f"<Role {self.name}>"


if __name__ == "__main__":  # pragma: no cover
    print(RbacRole.namespace())  # prints rbac => OK
    try:
        RbacRole.namespace = "testsing"  # type: ignore
    except AttributeError:
        print("Caught AttributeError: OK")
    print(RbacRole.namespace())  # prints rbac => OK
    print(RbacRole.table_args())  # prints rbac => OK
