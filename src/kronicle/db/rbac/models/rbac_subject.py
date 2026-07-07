# kronicle/db/rbac/models/rbac_subject.py

from enum import Enum
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, backref, mapped_column, relationship

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

    Exactly one of user_id or group_id must be set (exclusive arc).
    """

    __tablename__ = "subjects"
    __table_args__ = (
        CheckConstraint(f"type IN ('{SubjectType.user.value}', '{SubjectType.group.value}')", name="chk_subject_type"),
        CheckConstraint("num_nonnulls(user_id, group_id) = 1", name="chk_subject_one_owner"),
        UniqueConstraint("id", name="uq_subject_id"),
        UniqueConstraint("user_id", name="uq_subject_user_id"),
        UniqueConstraint("group_id", name="uq_subject_group_id"),
        Index("ix_subject_type", "type"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )

    # Type of subject: 'users' or 'groups'
    type: Mapped[SubjectType] = mapped_column(String(16), nullable=False)

    # Exclusive-arc FK columns — exactly one must be non-null (enforced by chk_subject_one_owner)
    user_id: Mapped[UUID | None] = mapped_column(ForeignKey(RbacUser.id, ondelete="CASCADE"), nullable=True)
    group_id: Mapped[UUID | None] = mapped_column(ForeignKey(RbacGroup.id, ondelete="CASCADE"), nullable=True)

    # ORM convenience relationships
    # passive_deletes=True on BOTH sides — let DB-level ON DELETE CASCADE handle cleanup;
    # without it SQLAlchemy nulls the FK first, violating chk_subject_one_owner.
    user: Mapped[RbacUser] = relationship(
        RbacUser,
        backref=backref(__tablename__, passive_deletes=True),
        passive_deletes=True,
    )
    group: Mapped[RbacGroup] = relationship(
        RbacGroup,
        backref=backref(__tablename__, passive_deletes=True),
        passive_deletes=True,
    )
