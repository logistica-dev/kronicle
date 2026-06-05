# kronicle/db/rbac/repo/links/rbac_group_roles_repo.py
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository


class RbacGroupRolesRepository(KronicleLinkRepository[RbacGroupRoles]):
    model = RbacGroupRoles

    # ----------------------------------------------------------------------------------------------
    # Group → Roles
    # ----------------------------------------------------------------------------------------------
    def get_role_ids_for_group(self, db: Session, *, group_id: UUID) -> set[UUID]:
        stmt = select(self.model.role_id).where(self.model.group_id == group_id)
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Role → Groups
    # ----------------------------------------------------------------------------------------------
    def get_group_ids_for_role(self, db: Session, *, role_id: UUID) -> set[UUID]:
        stmt = select(self.model.group_id).where(self.model.role_id == role_id)
        return set(db.execute(stmt).scalars().all())

    def get_group_ids_for_roles(self, db: Session, *, role_ids: set[UUID]) -> set[UUID]:
        if not role_ids:
            return set()
        stmt = select(self.model.group_id).where(self.model.role_id.in_(role_ids))
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    def assign_role_to_group(self, db: Session, group_id: UUID, role_id: UUID):
        self.ensure_link(db, {self.model.GROUP_ID: group_id, self.model.ROLE_ID: role_id})

    def remove_role_from_group(self, db: Session, group_id: UUID, role_id: UUID):
        self.remove_link(db, {self.model.GROUP_ID: group_id, self.model.ROLE_ID: role_id})
