# kronicle/db/core/models/core_channel.py
from __future__ import annotations

from sqlalchemy import UUID, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_zone import CoreZone


class CoreChannel(CoreEntity):
    __tablename__ = "channels"
    zone_id: Mapped[UUID] = mapped_column(ForeignKey(CoreZone.id), nullable=True)

    # ORM convenience only (not ownership)
    zone: Mapped[CoreZone] = relationship(CoreZone, backref=__tablename__)
