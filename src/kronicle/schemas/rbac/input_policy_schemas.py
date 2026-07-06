# kronicle/schemas/rbac/input_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.schemas.input_schema import InputSchema


# --------------------------------------------------------------------------------------------------
# Input Policies
# --------------------------------------------------------------------------------------------------
class InputPolicy(InputSchema):
    subject_id: UUID = Field(..., description="UUID of the user or group")
    role_id: UUID = Field(..., description="UUID of the role to assign")


class InputZonePolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific zone."""

    zone_id: UUID = Field(..., description="UUID of the zone")


class InputChannelPolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific channel."""

    channel_id: UUID = Field(..., description="UUID of the channel")


class InputRowPolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific row."""

    row_id: UUID = Field(..., description="UUID of the row")


# --------------------------------------------------------------------------------------------------
# Input Access Profiles
# --------------------------------------------------------------------------------------------------
class InputAccessProfile(InputSchema):
    role_id: UUID = Field(..., description="UUID of the role")
    description: str | None = Field(default=None, description="Optional description")


class InputZoneAccessProfile(InputAccessProfile):
    """Create a scoped role for a zone."""

    zone_id: UUID = Field(..., description="UUID of the zone")


class InputChannelAccessProfile(InputAccessProfile):
    """Create a scoped role for a channel."""

    channel_id: UUID = Field(..., description="UUID of the channel")


class InputRowAccessProfile(InputAccessProfile):
    """Create a scoped role for a row."""

    row_id: UUID = Field(..., description="UUID of the row")
