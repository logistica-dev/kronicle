# kronicle/db/rbac/models/rbac_policy.py
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

from kronicle.db.rbac.models.rbac_access_profile import ChannelAccessProfile, ResourceAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_subject import RbacSubject


class RbacPolicy(RbacEntity):
    """
    Abstract base class for any active Policy: binding a Scoped Role (AccessProfile)
    to a Subject (User or Group).

    Concrete subclasses must define `access_profile_id` and `access_profile` to
    point to the proper AccessProfile subclass (ChannelAccessProfile, ZoneAccessProfile, etc.).

    Attributes:
        subject_type: "user" or "group"
        subject_id: UUID of the User or Group
        role_id: UUID of the Role assigned
        is_delegation: Whether this policy is a temporary delegated assignment
        delegation_start: Optional start datetime of delegation
        delegation_end: Optional end datetime of delegation
    """

    __abstract__ = True

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        raise NotImplementedError("Concrete subclass must define FK to its AccessProfile")

    @declared_attr
    def access_profile(cls) -> Mapped[ResourceAccessProfile]:
        raise NotImplementedError("Concrete subclass must define relationship to AccessProfile")

    @declared_attr
    def subject_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(RbacSubject.id), nullable=False)

    @declared_attr
    def subject(cls) -> Mapped[RbacSubject]:
        """Subject can be either a User or a Group. View-only relationship."""
        return relationship(RbacSubject, viewonly=True)

    @declared_attr
    def is_delegation(cls) -> Mapped[bool]:
        """Indicates if this policy is a temporary delegated assignment."""
        return mapped_column(Boolean, nullable=False, default=False, server_default="false")

    @declared_attr
    def delegation_start(cls) -> Mapped[datetime | None]:
        """Optional start datetime for delegated policy."""
        return mapped_column(DateTime(timezone=True), nullable=True)

    @declared_attr
    def delegation_end(cls) -> Mapped[datetime | None]:
        """Optional end datetime for delegated policy."""
        return mapped_column(DateTime(timezone=True), nullable=True)


class ChannelPolicy(RbacPolicy):
    """
    Policy for a Channel instance. Links a ChannelAccessProfile to a Subject.
    """

    __tablename__ = "channel_policies"
    __table_args__ = {"schema": RbacEntity.namespace(), "extend_existing": True}

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(ChannelAccessProfile.id), nullable=False)

    @declared_attr
    def access_profile(cls) -> Mapped[ChannelAccessProfile]:  # type: ignore[reportIncompatibleVariableOverride]
        return relationship(ChannelAccessProfile)


class ZonePolicy(RbacPolicy):
    """
    Policy for a Zone instance. Links a ZoneAccessProfile to a Subject.
    """

    __tablename__ = "zone_policies"
    __table_args__ = {"schema": RbacEntity.namespace(), "extend_existing": True}

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(ZoneAccessProfile.id), nullable=False)

    @declared_attr
    def access_profile(cls) -> Mapped[ZoneAccessProfile]:  # type: ignore[reportIncompatibleVariableOverride]
        return relationship(ZoneAccessProfile)
