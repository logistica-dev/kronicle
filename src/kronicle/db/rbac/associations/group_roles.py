# kronicle/db/rbac/associations/group_roles.py
from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.associations.rbac_association import RbacAssociation
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole


class RbacGroupRoles(RbacAssociation):
    __tablename__ = "group_roles"
    __table_args__ = (
        UniqueConstraint("group_id", "role_id", name="uq_group_roles"),  # Tuple of constraints first
        {"schema": RbacAssociation.namespace(), "extend_existing": True},  # Options dictionary last
    )

    group_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id), primary_key=True)
    role_id: Mapped[UUID] = mapped_column(ForeignKey(RbacRole.id), primary_key=True)

    # Optional ORM helpers
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)
    role: Mapped[RbacRole] = relationship(RbacRole, backref=__tablename__)

    @classmethod
    def groups_for_role(cls, session, role_id: UUID) -> list[RbacGroup]:
        return session.query(RbacGroup).join(cls, cls.group_id == RbacGroup.id).filter(cls.role_id == role_id).all()

    @classmethod
    def roles_for_group(cls, session, group_id: UUID) -> list[RbacRole]:
        return session.query(RbacRole).join(cls, cls.role_id == RbacRole.id).filter(cls.group_id == group_id).all()
