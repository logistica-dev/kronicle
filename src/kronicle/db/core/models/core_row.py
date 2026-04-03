# kronicle/db/core/models/core_row.py

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.core.models.core_channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity


class Row(CoreEntity):
    __tablename__ = "row"

    # The channel this row belongs to
    channel_id = Column(ForeignKey("channel.id"))
    channel: Mapped["Channel"] = relationship("Channel")

    # Link to Timescale row_id (BIGSERIAL)
    timeseries_row_id: Mapped[int] = mapped_column(nullable=False, unique=True)

    # optional: store metadata like origin_user or public flag if convenient
    owner = Column(ForeignKey("rbac_user.id"), nullable=True)
    is_public: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    release_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
