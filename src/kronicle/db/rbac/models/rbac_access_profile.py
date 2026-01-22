# kronicle/db/rbac/models/rbac_access_profile.py
from uuid import UUID

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

from kronicle.db.core.models.channel import Channel
from kronicle.db.core.models.zone import Zone
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_role import RbacRole


class ResourceAccessProfile(RbacEntity):
    """
    Abstract base for a Scoped Role, i.e. a Role applied to a specific Resource instance.
    Automatically created when a new Resource is added.
    Contains shared fields and FK to Role.
    """

    __abstract__ = True

    @declared_attr
    def role_id(cls) -> Mapped[UUID]:
        return mapped_column(PgUUID(as_uuid=True), ForeignKey(RbacRole.id), nullable=False)

    @declared_attr
    def role(cls) -> Mapped[RbacRole]:
        return relationship(RbacRole)

    @declared_attr
    def description(cls) -> Mapped[str | None]:
        return mapped_column(String(255), nullable=True)


class ChannelAccessProfile(ResourceAccessProfile):
    __tablename__ = "channel_access_profiles"
    __table_args__ = (
        UniqueConstraint("role_id", "channel_id", name="uq_channel_access_profile"),  # Tuple of constraints first
        {"schema": RbacEntity.namespace(), "extend_existing": True},  # Options dictionary last
    )

    channel_id: Mapped[str] = mapped_column(ForeignKey(Channel.id), nullable=False)
    channel: Mapped[Channel] = relationship(Channel)


class ZoneAccessProfile(ResourceAccessProfile):
    """
    Scoped Role for a Zone instance.
    """

    __tablename__ = "zone_access_profiles"
    __table_args__ = (
        UniqueConstraint("role_id", "zone_id", name="uq_zone_access_profile"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )

    zone_id: Mapped[str] = mapped_column(ForeignKey(Zone.id), nullable=False)
    zone: Mapped[Zone] = relationship(Zone)
