# kronicle/db/rbac/links/user_roles.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacUserRoles(RbacLink):
    """
    Association table linking Users and Roles.
    Supports many-to-many relationships between users and roles.
    """

    UQ_CONSTRAINT = "uq_user_roles"

    __tablename__ = "user_roles"
    __table_args__ = (
        UniqueConstraint(RbacLink.USER_ID, RbacLink.ROLE_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id, ondelete="CASCADE"), primary_key=True)
    role_id: Mapped[UUID] = mapped_column(ForeignKey(RbacRole.id, ondelete="CASCADE"), primary_key=True)

    # Optional ORM helpers
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__, passive_deletes=True)
    role: Mapped[RbacRole] = relationship(RbacRole, backref=__tablename__, passive_deletes=True)
