# kronicle/services/rbac_authorization_service.py
from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.models.rbac_policy import ZonePolicy
from kronicle.db.rbac.models.rbac_role import RbacRole


class RbacAuthorizationService:
    """Orchestrates business logic for RBAC operations."""

    @staticmethod
    def get_effective_role(db: Session, user_id: UUID, zone: CoreZone) -> RbacRole | None:
        """
        Walks the Zone hierarchy to determine the highest role assigned to a user.
        Considers inherited roles via RbacHierarchy.
        """
        candidate_roles: list[RbacRole] = []

        def collect_role(z: CoreZone):
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
