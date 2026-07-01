# kronicle/db/rbac/models/rbac_subject.py

from enum import Enum
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser


class SubjectType(str, Enum):
    user = "user"
    group = "group"


class RbacSubject(RbacEntity):
    """
    Represents a Subject in the RBAC system.
    Can be either a User or a Group.
    Used as the target of Policies.
    """

    __tablename__ = "subjects"
    __table_args__ = (
        CheckConstraint(f"type IN ('{SubjectType.user.value}', '{SubjectType.group.value}')", name="chk_subject_type"),
        Index("ix_subject_type", "type"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )

    # Type of subject: 'users' or 'groups'
    type: Mapped[SubjectType] = mapped_column(String(16), nullable=False)

    # Foreign keys to User and Group
    user_id: Mapped[UUID] = mapped_column(ForeignKey(RbacUser.id, ondelete="CASCADE"), primary_key=True, nullable=True)
    group_id: Mapped[UUID] = mapped_column(
        ForeignKey(RbacGroup.id, ondelete="CASCADE"), primary_key=True, nullable=True
    )

    # ORM convenience relationships
    user: Mapped[RbacUser] = relationship(RbacUser, backref=__tablename__)
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)
