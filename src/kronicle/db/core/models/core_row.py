# kronicle/db/core/models/core_row.py
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.core.models.core_channel import CoreChannel


class CoreRow(KronicleBase):
    """
    Represents a row from a ChannelTimeseries as an object of a Policy.
    The PK is timeseries_row_id matching the BIGSERIAL in the timeseries table.
    """

    UQ_CONSTRAINT = "uq_channel_row"

    __tablename__ = "rows"

    __table_args__ = (
        UniqueConstraint("channel_id", "timeseries_row_id", name=UQ_CONSTRAINT),
        {"schema": "core", "extend_existing": True},
    )

    @classmethod
    def namespace(cls) -> str:
        return "core"

    # Unique timeseries row ID (BIGSERIAL from upstream), not PK — id from KronicleBase is the PK.
    # Scoped uniqueness is enforced by the composite (channel_id, timeseries_row_id) constraint.
    timeseries_row_id: Mapped[int] = mapped_column()

    # The channel this row belongs to
    channel_id: Mapped[UUID] = mapped_column(ForeignKey(CoreChannel.id, ondelete="CASCADE"), nullable=False)

    channel: Mapped[CoreChannel] = relationship(CoreChannel, back_populates="rows")

    # User-friendly name is made optional because it makes no sense at the row level.
    name: Mapped[str] = mapped_column(String(36), nullable=True)

    # optional: store metadata like origin_user or public flag if convenient
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    release_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
