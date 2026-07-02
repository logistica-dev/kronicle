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


class InputZoneAccessProfile(BaseModel):
    """Create a scoped role for a zone."""

    role_id: UUID = Field(..., description="UUID of the role")
    zone_id: UUID = Field(..., description="UUID of the zone")
    description: str | None = Field(default=None, description="Optional description")


class InputChannelAccessProfile(BaseModel):
    """Create a scoped role for a channel."""

    role_id: UUID = Field(..., description="UUID of the role")
    channel_id: UUID = Field(..., description="UUID of the channel")
    description: str | None = Field(default=None, description="Optional description")


class InputRowPolicy(BaseModel):
    """Assign a role to a subject (user or group) for a specific row."""

    subject_id: UUID = Field(..., description="UUID of the user or group")
    role_id: UUID = Field(..., description="UUID of the role to assign")
    row_id: UUID = Field(..., description="UUID of the row")


class InputRowAccessProfile(BaseModel):
    """Create a scoped role for a row."""

    role_id: UUID = Field(..., description="UUID of the role")
    row_id: UUID = Field(..., description="UUID of the row")
    description: str | None = Field(default=None, description="Optional description")
