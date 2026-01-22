# kronicle/db/rbac/associations/user_roles.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.associations.rbac_association import RbacAssociation
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacUserRoles(RbacAssociation):
    __tablename__ = "user_roles"
    __table_args__ = (
        UniqueConstraint("user_id", "role_id", name="uq_user_roles"),  # Tuple of constraints first
        {"schema": RbacAssociation.namespace(), "extend_existing": True},  # Options dictionary last
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id), primary_key=True)
    role_id: Mapped[UUID] = mapped_column(ForeignKey(RbacRole.id), primary_key=True)

    # Optional ORM helpers
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__)
    role: Mapped[RbacRole] = relationship(RbacRole, backref=__tablename__)

    @classmethod
    def namespace(cls):
        return RbacAssociation.namespace()

    @classmethod
    def roles_for_user(cls, session, user_id: UUID) -> list[RbacRole]:
        return session.query(RbacRole).join(cls, cls.role_id == RbacRole.id).filter(cls.user_id == user_id).all()

    @classmethod
    def users_for_role(cls, session, role_id: UUID) -> list[RbacUser]:
        return session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.role_id == role_id).all()
