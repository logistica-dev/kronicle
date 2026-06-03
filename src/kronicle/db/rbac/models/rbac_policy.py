# kronicle/db/rbac/models/rbac_policy.py
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

from kronicle.db.rbac.links.rbac_access_profile import (
    ChannelAccessProfile,
    ResourceAccessProfile,
    RowAccessProfile,
    ZoneAccessProfile,
)
from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_subject import RbacSubject


class RbacPolicy(RbacLink):
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

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

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
        return mapped_column(Boolean, default=False, server_default="false", nullable=False)

    @declared_attr
    def delegation_start(cls) -> Mapped[datetime | None]:
        """Optional start datetime for delegated policy."""
        return mapped_column(DateTime(timezone=True), nullable=True)

    @declared_attr
    def delegation_end(cls) -> Mapped[datetime | None]:
        """Optional end datetime for delegated policy."""
        return mapped_column(DateTime(timezone=True), nullable=True)


class ZonePolicy(RbacPolicy):
    """
    Policy for a Zone instance. Links a ZoneAccessProfile to a Subject.
    """

    UQ_CONSTRAINT = "uq_zone_policy"

    __tablename__ = "zone_policies"
    __table_args__ = (
        UniqueConstraint(
            RbacLink.SUBJECT_ID, RbacLink.ACCESS_PROFILE_ID, name=UQ_CONSTRAINT
        ),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},
    )

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(ZoneAccessProfile.id), nullable=False)

    @declared_attr
    def access_profile(cls) -> Mapped[ZoneAccessProfile]:  # type: ignore[reportIncompatibleVariableOverride]
        return relationship(ZoneAccessProfile)


class ChannelPolicy(RbacPolicy):
    """
    Policy for a Channel instance. Links a ChannelAccessProfile to a Subject (User or Group).
    """

    UQ_CONSTRAINT = "uq_channel_policies"

    __tablename__ = "channel_policies"
    __table_args__ = (
        UniqueConstraint(
            RbacLink.SUBJECT_ID, RbacLink.ACCESS_PROFILE_ID, name=UQ_CONSTRAINT
        ),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},
    )

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(ChannelAccessProfile.id), nullable=False)

    @declared_attr
    def access_profile(cls) -> Mapped[ChannelAccessProfile]:  # type: ignore[reportIncompatibleVariableOverride]
        return relationship(ChannelAccessProfile)


class RowPolicy(RbacPolicy):
    """
    Policy for a single Channel's timeseries row. Links a RowAccessProfile to a Subject.
    """

    UQ_CONSTRAINT = "uq_row_policies"

    __tablename__ = "row_policies"
    __table_args__ = (
        UniqueConstraint(
            RbacLink.SUBJECT_ID, RbacLink.ACCESS_PROFILE_ID, name=UQ_CONSTRAINT
        ),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},
    )

    @declared_attr
    def access_profile_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(RowAccessProfile.id), nullable=False)

    @declared_attr
    def access_profile(cls) -> Mapped[RowAccessProfile]:  # type: ignore[reportIncompatibleVariableOverride]
        return relationship(RowAccessProfile)
