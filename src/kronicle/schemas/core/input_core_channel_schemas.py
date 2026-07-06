# kronicle/schemas/core/input_core_channel_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.schemas.input_schema import InputSchema


class InputCoreChannel(InputSchema):
    id: UUID  # type: ignore
    zone_id: UUID | None = None


class InputCoreChannelPatch(InputSchema):
    zone_id: UUID | None = Field(default=None, description="Zone UUID to assign")
