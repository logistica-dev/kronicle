# kronicle/db/core/models/core_channel.py
# pyright: reportImportCycles=false
# -> CoreRow is imported only under TYPE_CHECKING (erased at runtime, so no actual cycle)
# but pyright's reportImportCycles can't be suppressed per-line, so we relax it for this file only.
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_zone import CoreZone

if TYPE_CHECKING:  # noqa: PLC0415 - TYPE_CHECKING import erased at runtime; no actual cycle
    from kronicle.db.core.models.core_row import CoreRow


class CoreChannel(CoreEntity):
    __tablename__ = "channels"
    zone_id: Mapped[UUID] = mapped_column(ForeignKey(CoreZone.id), nullable=True)

    # ORM convenience only (not ownership)
    zone: Mapped[CoreZone] = relationship(CoreZone, backref=__tablename__)

    # One-to-many backref. passive_deletes="all" defers child deletion to the DB
    # (FK has ondelete="CASCADE"), so deleting a channel does NOT try to null out
    # channel_id on the child rows (which is NOT NULL).
    rows: Mapped[list[CoreRow]] = relationship("CoreRow", back_populates="channel", passive_deletes="all")
