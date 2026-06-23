# kronicle/db/rbac/links/rbac_access_profile.py
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_row import CoreRow
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_role import RbacRole


class ResourceAccessProfile(RbacLink):
    """
    Abstract base for a Scoped Role, i.e. a Role applied to a specific Resource instance.
    Automatically created when a new Resource is added.
    Contains shared fields and FK to Role.
    """

    __abstract__ = True

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    @declared_attr
    def role_id(cls) -> Mapped[UUID]:
        return mapped_column(PgUUID(as_uuid=True), ForeignKey(RbacRole.id), nullable=False)

    @declared_attr
    def role(cls) -> Mapped[RbacRole]:
        return relationship(RbacRole)

    @declared_attr
    def description(cls) -> Mapped[str | None]:
        return mapped_column(String(255), nullable=True)


class ZoneAccessProfile(ResourceAccessProfile):
    """
    Scoped Role for a Zone instance.
    """

    UQ_CONSTRAINT = "uq_zone_access_profile"

    __tablename__ = "zone_access_profiles"
    __table_args__ = (
        UniqueConstraint(RbacLink.ROLE_ID, RbacLink.ZONE_ID, name=UQ_CONSTRAINT),
        {"schema": RbacLink.namespace(), "extend_existing": True},
    )

    zone_id: Mapped[UUID] = mapped_column(ForeignKey(CoreZone.id), nullable=False)
    zone: Mapped[CoreZone] = relationship(CoreZone, backref="access_profiles")


class ChannelAccessProfile(ResourceAccessProfile):
    __tablename__ = "channel_access_profiles"
    UQ_CONSTRAINT = "uq_channel_access_profile"

    __table_args__ = (
        UniqueConstraint(RbacLink.ROLE_ID, RbacLink.CHANNEL_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    channel_id: Mapped[UUID] = mapped_column(ForeignKey(CoreChannel.id), nullable=False)
    channel: Mapped[CoreChannel] = relationship(CoreChannel, backref="access_profiles")


class RowAccessProfile(ResourceAccessProfile):
    __tablename__ = "row_access_profiles"
    UQ_CONSTRAINT = "uq_row_access_profile"

    __table_args__ = (
        UniqueConstraint(RbacLink.ROLE_ID, RbacLink.ROW_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    # Reminder: row_id is based on ChannelTimeseries.row_id which is a BIGSERIAL int
    row_id: Mapped[UUID] = mapped_column(ForeignKey(CoreRow.id), nullable=False)
    row: Mapped[CoreRow] = relationship(CoreRow, backref="access_profiles")
