# kronicle/db/rbac/repo/links/rbac_user_roles_repo.py
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository

"""
This repo should only:
read edges (User ↔ Role)
return either IDs or explicit edge rows
never perform hierarchy logic
never load full Role/User objects unless explicitly asked
"""


class RbacUserRolesRepository(KronicleLinkRepository[RbacUserRoles]):
    model = RbacUserRoles

    # ----------------------------------------------------------------------------------------------
    # User → Roles
    # ----------------------------------------------------------------------------------------------
    def get_role_ids_for_user(self, db: Session, *, user_id: UUID) -> set[UUID]:
        stmt = select(self.model.role_id).where(self.model.user_id == user_id)
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Role → Users
    # ----------------------------------------------------------------------------------------------
    def get_user_ids_for_role(self, db: Session, *, role_id: UUID) -> set[UUID]:
        stmt = select(self.model.user_id).where(self.model.role_id == role_id)
        return set(db.execute(stmt).scalars().all())

    def get_user_ids_for_roles(self, db: Session, *, role_ids: set[UUID]) -> set[UUID]:
        if not role_ids:
            return set()
        stmt = select(self.model.user_id).where(self.model.role_id.in_(role_ids))
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    def assign_role_to_user(self, db: Session, *, user: RbacUser, role: RbacUserRoles):
        self.ensure_link(db, {self.model.USER_ID: user.id, self.model.ROLE_ID: role.id})

    def remove_role_from_user(self, db: Session, *, user: RbacUser, role: RbacUserRoles):
        self.remove_link(db, {self.model.USER_ID: user.id, self.model.ROLE_ID: role.id})
