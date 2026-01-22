# kronicle/db/rbac/associations/user_groups.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.associations.rbac_association import RbacAssociation
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacUserGroups(RbacAssociation):
    __tablename__ = "user_groups"
    __table_args__ = (
        UniqueConstraint("user_id", "group_id", name="uq_user_groups"),  # Tuple of constraints first
        {"schema": RbacAssociation.namespace(), "extend_existing": True},  # Options dictionary last
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id), primary_key=True)
    group_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id), primary_key=True)

    # Optional ORM helpers
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__)
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)

    @classmethod
    def namespace(cls):
        return RbacAssociation.namespace()

    @classmethod
    def groups_for_user(cls, session, user_id: UUID) -> list[RbacGroup]:
        return session.query(RbacGroup).join(cls, cls.group_id == RbacGroup.id).filter(cls.user_id == user_id).all()

    @classmethod
    def users_for_group(cls, session, group_id: UUID) -> list[RbacUser]:
        return session.query(RbacUser).join(cls, cls.user_id == RbacUser.id).filter(cls.group_id == group_id).all()
