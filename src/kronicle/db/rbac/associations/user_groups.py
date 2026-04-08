# kronicle/db/rbac/associations/user_groups.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from kronicle.db.rbac.associations.rbac_association import RbacAssociation
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser


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
    def all_groups_for_user(cls, session: Session, user_id: UUID) -> list[RbacGroup]:
        """
        Return all groups a user belongs to, including parent groups in the hierarchy.

        Note:
            - Uses the `parent` relationship from `RbacGroup` to walk up the hierarchy.
            - Duplicates are removed using a `set`.
            - Converting ORM objects to a set works if identity equality is maintained (same session).
        """
        # First, fetch the direct groups
        direct_groups = cls.direct_groups_for_user(session, user_id)

        # Cast as set to remove duplicates
        all_groups: set[RbacGroup] = set(direct_groups)
        # Collect all parent groups recursively
        for group in direct_groups:
            group._walk_ancestors(lambda ancestor: all_groups.add(ancestor))

        return list(all_groups)

    @classmethod
    def direct_users_for_group(cls, session: Session, group_id: UUID) -> list[RbacUser]:
        return session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.group_id == group_id).all()

    @classmethod
    def all_users_for_group(cls, session: Session, group: RbacGroup) -> list[RbacUser]:
        """
        Return all users in this group and all descendant groups.
        """
        group_ids: set[UUID] = {group.id, *group.descendants}  # descendants comes from KronicleHierarchyMixin
        return session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.group_id.in_(group_ids)).all()
