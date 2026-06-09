# kronicle/schemas/core/input_core_channel_schemas.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class InputCoreChannelPatch(BaseModel):
    name: str | None = Field(default=None, max_length=100, description="Core channel name")
    zone_id: UUID | None = Field(default=None, description="Zone UUID to assign")
    details: dict[str, Any] | None = Field(default=None, description="Optional JSONB metadata")
