# kronicle/repo/rbac/links/rbac_group_roles_repo.py
from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import delete, select

from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository

"""
This repo should only:
read edges (Group ↔ Role)
return either IDs or explicit edge rows
never perform hierarchy logic
never load full Group/Role objects unless explicitly asked
"""


class RbacGroupRolesRepository(KronicleLinkRepository[RbacGroupRoles]):
    model = RbacGroupRoles

    # ----------------------------------------------------------------------------------------------
    # Group → Roles
    # ----------------------------------------------------------------------------------------------
    def get_role_ids_for_group(self, db: Session, *, group_id: UUID) -> set[UUID]:
        stmt = select(self.model.role_id).where(self.model.group_id == group_id)
        return set(db.execute(stmt).scalars().all())

    def list_roles_for_group(self, db: Session, *, group_id: UUID) -> list[RbacGroupRoles]:
        stmt = select(self.model).where(self.model.group_id == group_id)
        return list(db.execute(stmt).scalars().all())

    def list_roles_for_groups(self, db: Session, *, group_ids: set[UUID]) -> list[RbacGroupRoles]:
        if not group_ids:
            return []
        stmt = select(self.model).where(self.model.group_id.in_(group_ids))
        return list(db.execute(stmt).scalars().all())

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
    # Single edge rows
    # ----------------------------------------------------------------------------------------------
    def get_role_link(self, db: Session, *, group_id: UUID, role_id: UUID) -> RbacGroupRoles | None:
        stmt = select(self.model).where(self.model.group_id == group_id, self.model.role_id == role_id)
        return db.execute(stmt).scalars().first()

    def get_role_link_for_groups(self, db: Session, *, group_ids: set[UUID], role_id: UUID) -> RbacGroupRoles | None:
        if not group_ids:
            return None
        stmt = select(self.model).where(self.model.group_id.in_(group_ids), self.model.role_id == role_id)
        return db.execute(stmt).scalars().first()

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    def assign_role_to_group(self, db: Session, *, group_id: UUID, role_id: UUID) -> RbacGroupRoles | None:
        return self.ensure_link_returning(db, {RbacLink.GROUP_ID: group_id, RbacLink.ROLE_ID: role_id})

    def remove_role_from_group(self, db: Session, *, group_id: UUID, role_id: UUID) -> RbacGroupRoles | None:
        return self.remove_link_returning(db, {RbacLink.GROUP_ID: group_id, RbacLink.ROLE_ID: role_id})

    def delete_all_for_group(self, db: Session, *, group_id: UUID) -> list[RbacGroupRoles]:
        stmt = delete(self.model).where(self.model.group_id == group_id).returning(self.model)
        return list(db.execute(stmt).scalars().all())

    def delete_all_for_role(self, db: Session, *, role_id: UUID) -> list[RbacGroupRoles]:
        stmt = delete(self.model).where(self.model.role_id == role_id).returning(self.model)
        return list(db.execute(stmt).scalars().all())
