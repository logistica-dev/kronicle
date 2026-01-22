# kronicle/db/rbac/rbac_engine.py
from __future__ import annotations

from uuid import UUID

from pydantic import EmailStr
from sqlalchemy.orm import Session

from kronicle.db.core.models.zone import Zone
from kronicle.db.rbac.models.rbac_policy import ZonePolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacEngine:
    """Orchestrates business logic for RBAC operations."""

    # ----------------------------------------------------------------------------------------------
    # Read-only: fetch user info
    # ----------------------------------------------------------------------------------------------
    @staticmethod
    def fetch_user_by_email(db: Session, email: EmailStr) -> RbacUser | None:
        return RbacUser.fetch_by_email(db, email)

    @staticmethod
    def fetch_user_by_name(db: Session, name: str) -> RbacUser | None:
        return RbacUser.fetch_by_name(db, name)

    @staticmethod
    def list_users(db: Session) -> list[RbacUser]:
        return RbacUser.fetch_all(db)

    @staticmethod
    def get_effective_role(db: Session, user_id: UUID, zone: Zone) -> RbacRole | None:
        """
        Walks the Zone hierarchy to determine the highest role assigned to a user.
        Considers inherited roles via RbacHierarchy.
        """
        candidate_roles: list[RbacRole] = []

        def collect_role(z: Zone):
            assignment = db.query(ZonePolicy).filter_by(subject_id=user_id, zone_id=z.id).first()
            if assignment:
                candidate_roles.append(assignment.role)

        visited = set()
        stack = [zone]
        while stack:
            node = stack.pop()
            if node.id in visited:
                continue
            visited.add(node.id)
            collect_role(node)
            stack.extend(node.children)

        if not candidate_roles:
            return None

        # Return the "highest" role
        return max(candidate_roles, key=lambda r: getattr(r, "level", 0))

    # ----------------------------------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------------------------------
    @staticmethod
    def create_user(db: Session, user: RbacUser) -> RbacUser:
        db.add(user)
        db.flush()  # ensures id is populated
        return user
