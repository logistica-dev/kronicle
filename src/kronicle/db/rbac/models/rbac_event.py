# kronicle/db/rbac/models/rbac_event.py
from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, backref, mapped_column, relationship

from kronicle.db.core.models.zone import Zone
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_user import RbacUser


class RbacEvent(RbacEntity):
    """
    Immutable audit log of RBAC-related actions, including role assignments,
    revocations, delegations, and policy changes.

    Attributes:
        verb: The action performed, e.g., "ASSIGN", "REVOKE", "DELEGATE".
        actor_id: UUID of the User who performed the action.
        actor_snapshot: Serialized snapshot of the actor at the time of action.
        subject_type: "user" or "group" for the entity affected.
        subject_id: UUID of the affected User/Group.
        subject_snapshot: Serialized snapshot of the affected entity.
        target_type: "zone" or "resource".
        target_id: UUID of the affected Zone or Resource.
        target_snapshot: Serialized snapshot of the target at the time of action.
        role_id: UUID of the Role involved (if applicable).
    """

    __tablename__ = "rbac_events"

    # ----------------------------
    # Core action
    # ----------------------------
    verb: Mapped[str] = mapped_column(String(16), nullable=False)  # ASSIGN, REVOKE, DELEGATE

    # ----------------------------
    # Actor
    # ----------------------------
    actor_id: Mapped[UUID] = mapped_column(ForeignKey("rbac.users.id"), nullable=False)
    actor: Mapped[RbacUser] = relationship(RbacUser, backref=backref("performed_events", lazy="select"))
    actor_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # ----------------------------
    # Subject
    # ----------------------------
    subject_type: Mapped[str] = mapped_column(String(16), nullable=False)  # "user" or "group"
    subject_id: Mapped[UUID] = mapped_column(nullable=False)
    subject_user: Mapped[RbacUser | None] = relationship(
        RbacUser,
        primaryjoin="RbacUser.id==RbacEvent.subject_id",
        viewonly=True,
    )
    subject_group: Mapped[RbacGroup | None] = relationship(
        RbacGroup,
        primaryjoin="RbacGroup.id==RbacEvent.subject_id",
        viewonly=True,
    )
    subject_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=True, default=dict)

    # ----------------------------
    # Target (Zone or Resource)
    # ----------------------------
    target_type: Mapped[str] = mapped_column(String(16), nullable=False)
    target_id: Mapped[UUID] = mapped_column(nullable=False)
    target_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=True, default=dict)
    zone: Mapped[Zone | None] = relationship(Zone, primaryjoin="Zone.id==RbacEvent.target_id", viewonly=True)

    # ----------------------------
    # Role involved
    # ----------------------------
    role_id: Mapped[UUID | None] = mapped_column(ForeignKey("rbac.roles.id"), nullable=True)
    role: Mapped[RbacRole | None] = relationship(RbacRole, primaryjoin="RbacRole.id==RbacEvent.role_id", viewonly=True)
