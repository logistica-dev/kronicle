# kronicle/schemas/core/safe_core_channel_schemas.py
from __future__ import annotations

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.schemas.output_schema import OutputSchema


class OutputZone(OutputSchema):
    pass


class OutputCoreChannel(OutputSchema):
    zone: OutputZone | None = None

    @classmethod
    def from_db(cls, db_obj: KronicleBase) -> OutputCoreChannel:
        return cls.model_validate(db_obj, from_attributes=True)


class OutputCoreRow(OutputSchema):
    channel: OutputCoreChannel | None = None
