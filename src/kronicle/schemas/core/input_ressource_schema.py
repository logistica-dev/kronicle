# kronicle/schemas/core/input_core_channel_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.schemas.input_schema import InputSchema, NamedInputSchema
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.utils.str_utils import ensure_uuid4

mod = "in_zone"


class InputZone(NamedInputSchema):
    pass


class InputZonePatch(InputSchema):
    pass


class InputCoreChannel(InputSchema):
    id: UUID  # type: ignore
    zone: InputZone | None = None

    @classmethod
    def from_payload(cls, channel: InputPayload | ResponsePayload):
        return cls(id=ensure_uuid4(channel.id), name=channel.name)


class InputCoreChannelPatch(InputSchema):
    zone: InputZone | None = Field(default=None, description="Zone UUID to assign")


class InputRow(InputSchema):
    # Timeseries BIGSERIAL row_id of the targeted row (see data.channel_* tables).
    id: int  # type: ignore
    channel_id: UUID
