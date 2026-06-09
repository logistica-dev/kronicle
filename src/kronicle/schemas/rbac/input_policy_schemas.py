# kronicle/schemas/rbac/input_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


class InputZonePolicy(BaseModel):
    """Assign a role to a subject (user or group) for a specific zone."""

    subject_id: UUID = Field(..., description="UUID of the user or group")
    role_id: UUID = Field(..., description="UUID of the role to assign")
    zone_id: UUID = Field(..., description="UUID of the zone")


class InputChannelPolicy(BaseModel):
    """Assign a role to a subject (user or group) for a specific channel."""

    subject_id: UUID = Field(..., description="UUID of the user or group")
    role_id: UUID = Field(..., description="UUID of the role to assign")
    channel_id: UUID = Field(..., description="UUID of the channel")
