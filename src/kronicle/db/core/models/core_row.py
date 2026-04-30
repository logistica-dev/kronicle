# kronicle/db/core/models/core_row.py

from datetime import datetime

from sqlalchemy import UUID, Boolean, DateTime, ForeignKey, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.core.models.core_channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity


class Row(CoreEntity):
    """
    Represents a row from a ChannelTimeseries as an object of a Policy.
    Note: the core.row.id (inherited from KronicleBase) = data.timeseries.row_id
    """

    UQ_CONSTRAINT = "uq_channel_row"

    __tablename__ = "rows"

    # Index for fast lookup when given channel + timeseries row
    __table_args__ = (
        UniqueConstraint("channel_id", "timeseries_row_id", name=UQ_CONSTRAINT),
        {"schema": CoreEntity.namespace(), "extend_existing": True},
    )
    # Reminder: id is inherited from KronicleBase
    # id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # The channel this row belongs to
    channel_id: Mapped[UUID] = mapped_column(ForeignKey(Channel.id), nullable=False)
    channel: Mapped[Channel] = relationship(Channel, backref=__tablename__)

    # Link to Timescale row_id (BIGSERIAL)
    timeseries_row_id: Mapped[int] = mapped_column(nullable=False)

    # User-friendly name is made optional because it makes no sense at the row level.
    name: Mapped[str] = mapped_column(String(36), unique=True, nullable=True)

    # optional: store metadata like origin_user or public flag if convenient
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    release_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
