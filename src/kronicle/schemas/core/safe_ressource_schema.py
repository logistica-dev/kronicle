# kronicle/schemas/core/safe_core_channel_schemas.py
from __future__ import annotations

from uuid import UUID

from kronicle.schemas.output_schema import OutputSchema


class OutputZone(OutputSchema):
    pass


class OutputCoreChannel(OutputSchema):
    zone_id: UUID | None = None


class OutputCoreRow(OutputSchema):
    channel_id: UUID | None = None
