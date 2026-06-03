# kronicle/db/rbac/links/user_groups.py
from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacUserGroups(RbacLink):
    """
    Association table linking Users and Groups.
    Supports many-to-many relationships between users and groups.
    """

    UQ_CONSTRAINT = "uq_user_groups"

    __tablename__ = "user_groups"
    __table_args__ = (
        UniqueConstraint(RbacLink.USER_ID, RbacLink.GROUP_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    # Foreign keys to User and Group
    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id, ondelete="CASCADE"), primary_key=True)
    group_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True)

    # ORM convenience relationships
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__)
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)
