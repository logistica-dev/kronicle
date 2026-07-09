# kronicle/schemas/core/input_core_channel_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.schemas.input_schema import InputSchema, NamedInputSchema

mod = "in_zone"


class InputZone(NamedInputSchema):
    pass


class InputZonePatch(InputSchema):
    pass


class InputCoreChannel(InputSchema):
    id: UUID  # type: ignore
    zone_id: UUID | None = None


class InputCoreChannelPatch(InputSchema):
    zone_id: UUID | None = Field(default=None, description="Zone UUID to assign")


class InputRow(InputSchema):
    id: UUID  # type: ignore
    channel_id: UUID
