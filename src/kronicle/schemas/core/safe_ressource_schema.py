# kronicle/schemas/core/safe_core_channel_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.core.models.core_row import CoreRow
from kronicle.schemas.output_schema import OutputSchema


class OutputZone(OutputSchema):
    pass


class OutputCoreChannel(OutputSchema):
    zone: OutputZone | None = None

    @classmethod
    def from_db(cls, db_obj: KronicleBase) -> OutputCoreChannel:
        return cls.model_validate(db_obj, from_attributes=True)


class OutputCoreRow(BaseModel):
    # Timeseries BIGSERIAL row_id of the targeted row (data.channel_<uuid>.<row_id>).
    id: int
    # Internal CoreRow PK in core.rows.
    core_row_id: UUID | None = None
    channel_id: UUID | None = None
    name: str | None = None
    details: dict[str, Any] | None = None
    channel: OutputCoreChannel | None = None

    @classmethod
    def from_db(cls, db_obj: CoreRow) -> OutputCoreRow:
        return cls(
            id=db_obj.timeseries_row_id,
            core_row_id=db_obj.id,
            channel_id=db_obj.channel_id,
            name=db_obj.name,
            details=db_obj.details,
            channel=OutputCoreChannel.from_db(db_obj.channel) if db_obj.channel else None,
        )
