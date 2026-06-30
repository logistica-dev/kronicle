# kronicle/repo/rbac/links/rbac_user_group_repo.py
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository

"""
This repo should only:
read edges (User ↔ Group)
return either IDs or explicit edge rows
never perform hierarchy logic
never load full Group/User objects unless explicitly asked
"""


class RbacUserGroupRepository(KronicleLinkRepository[RbacUserGroups]):
    model = RbacUserGroups

    # ----------------------------------------------------------------------------------------------
    # User → Groups
    # ----------------------------------------------------------------------------------------------
    def get_group_ids_for_user(self, db: Session, *, user_id: UUID) -> set[UUID]:
        stmt = select(self.model.group_id).where(self.model.user_id == user_id)
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Group → Users
    # ----------------------------------------------------------------------------------------------
    def get_user_ids_for_group(self, db: Session, *, group_id: UUID) -> set[UUID]:
        stmt = select(self.model.user_id).where(self.model.group_id == group_id)
        return set(db.execute(stmt).scalars().all())

    def get_user_ids_for_groups(self, db: Session, *, group_ids: set[UUID]) -> set[UUID]:
        if not group_ids:
            return set()
        stmt = select(self.model.user_id).where(self.model.group_id.in_(group_ids))
        return set(db.execute(stmt).scalars().all())

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    def add_user_to_group(self, db: Session, *, user: RbacUser, group: RbacGroup):
        self.ensure_link(db, {self.model.USER_ID: user.id, self.model.GROUP_ID: group.id})

    def remove_user_from_group(self, db: Session, *, user: RbacUser, group: RbacGroup):
        self.remove_link(db, {self.model.USER_ID: user.id, self.model.GROUP_ID: group.id})

    def delete_all_for_user(self, db: Session, *, user_id: UUID) -> None:
        stmt = delete(self.model).where(self.model.user_id == user_id)
        db.execute(stmt)

    def delete_all_for_group(self, db: Session, *, group_id: UUID) -> None:
        stmt = delete(self.model).where(self.model.group_id == group_id)
        db.execute(stmt)
