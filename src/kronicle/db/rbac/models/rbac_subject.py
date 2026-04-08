# kronicle/db/rbac/models/rbac_subject.py

from enum import Enum

from sqlalchemy import CheckConstraint, Index, String
from sqlalchemy.orm import Mapped, foreign, mapped_column, relationship

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

    user = relationship(RbacUser, primaryjoin=lambda: foreign(RbacSubject.id) == RbacUser.id, viewonly=True)
    group = relationship(RbacGroup, primaryjoin=lambda: foreign(RbacSubject.id) == RbacGroup.id, viewonly=True)
