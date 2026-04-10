# kronicle/db/rbac/associations/user_groups.py
from __future__ import annotations

from typing import Any, Callable
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from kronicle.db.rbac.associations.rbac_association import RbacAssociation
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.utils.dev_logs import log_w


class RbacUserGroups(RbacAssociation):
    """
    Association table linking Users and Groups.
    Supports many-to-many relationships between users and groups,
    and provides helper methods to traverse group hierarchies.
    """

    __tablename__ = "user_groups"
    __table_args__ = (
        UniqueConstraint("user_id", "group_id", name="uq_user_groups"),  # Tuple of constraints first
        {"schema": RbacAssociation.namespace(), "extend_existing": True},  # Options dictionary last
    )

    # Foreign keys to User and Group
    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id), primary_key=True)
    group_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id), primary_key=True)

    # ORM convenience relationships
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__)
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)

    @classmethod
    def namespace(cls) -> str:
        return RbacAssociation.namespace()

    @classmethod
    def direct_groups_for_user(cls, session: Session, user_id: UUID) -> list[RbacGroup]:
        """
        Return the groups a user belongs to directly (without considering hierarchy).
        """
        return session.query(RbacGroup).join(cls, cls.group_id == RbacGroup.id).filter(cls.user_id == user_id).all()

    @classmethod
    def all_groups_for_user(
        cls,
        session: Session,
        user_id: UUID,
        *,
        sort_key: Callable[[RbacGroup], Any] | None = None,
        reverse: bool = False,
    ) -> list[RbacGroup]:
        """
        Return all groups a user belongs to, including parent groups in the hierarchy.
        Args:
            session: SQLAlchemy session.
            user_id: The user UUID.
            sort_key: Optional callable to sort groups (e.g., lambda g: g.name.lower()).
            reverse: If True, sort in descending order.

        Note:
            - Uses the `parent` relationship from `RbacGroup` to walk up the hierarchy.
            - Duplicates are removed using a `set`.
            - Converting ORM objects to a set works if identity equality is maintained (same session).
        """
        # First, fetch the direct groups
        direct_groups = cls.direct_groups_for_user(session, user_id)

        # Remove duplicates
        user_groups: dict[UUID, RbacGroup] = {g.id: g for g in direct_groups}
        # Collect all parent groups recursively
        for group in list(user_groups.values()):
            lambda ancestor: user_groups.setdefault(ancestor.id, ancestor)
            group._walk_ancestors(lambda ancestor: user_groups.setdefault(ancestor.id, ancestor))

        groups_in_hierarchy = list(user_groups.values())

        # Optional sorting
        if sort_key:
            try:
                groups_in_hierarchy.sort(key=sort_key, reverse=reverse)
            except Exception as e:
                log_w("all_groups_for_user", f"Could not sort with such key: {sort_key}", e)

        return groups_in_hierarchy

    @classmethod
    def direct_users_for_group(cls, session: Session, group_id: UUID) -> list[RbacUser]:
        return session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.group_id == group_id).all()

    @classmethod
    def all_users_for_group(
        cls,
        session: Session,
        group: RbacGroup,
        *,
        sort_key: Callable | None = None,
        reverse: bool = False,
    ) -> list[RbacUser]:
        """
        Return all users in this group and all descendant groups.
        """
        group_ids: set[UUID] = {group.id, *group.descendants}  # descendants comes from KronicleHierarchyMixin
        direct_users = (
            session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.group_id.in_(group_ids)).all()
        )
        # Deduplicate users by ID
        users: dict[UUID, RbacUser] = {u.id: u for u in direct_users}
        users_in_hierarchy = list(users.values())

        # Deduplicate users by ID
        if sort_key:
            try:
                users_in_hierarchy.sort(key=sort_key, reverse=reverse)
            except Exception as e:
                log_w("all_users_for_group", f"Could not sort with such key: {sort_key}", e)

        return users_in_hierarchy
