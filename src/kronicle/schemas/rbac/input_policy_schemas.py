# kronicle/schemas/rbac/input_policy_schemas.py
from __future__ import annotations

from pydantic import Field

from kronicle.schemas.core.input_ressource_schema import InputRow, InputZonePatch
from kronicle.schemas.input_schema import InputSchema
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_subject_schemas import InputSubject


class InputPolicyPatch(InputSchema):
    """Fields that can be patched on a policy (name, details)."""

    pass


# --------------------------------------------------------------------------------------------------
# Input Access Profiles
# --------------------------------------------------------------------------------------------------
class InputAccessProfile(InputSchema):
    description: str | None = Field(default=None, description="Optional description")
    role: InputRole = Field(..., description="Role for this access profile")


class InputZoneAccessProfile(InputAccessProfile):
    """Create a scoped role for a zone."""

    zone: InputZonePatch = Field(..., description="Zone targeted by this access profile")


class InputChannelAccessProfile(InputAccessProfile):
    """Create a scoped role for a channel."""

    channel: InputPayload = Field(..., description="Channel targeted by this access profile")


class InputRowAccessProfile(InputAccessProfile):
    """Create a scoped role for a row."""

    row: InputRow = Field(..., description="ID of the row targeted by this access profile")


# --------------------------------------------------------------------------------------------------
# Input Policies
# --------------------------------------------------------------------------------------------------
class InputPolicy(InputSchema):
    subject: InputSubject = Field(..., description="UUID of the user or group")


class InputZonePolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific zone."""

    access_profile: InputZoneAccessProfile = Field(..., description="Access profile for the zone")


class InputChannelPolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific channel."""

    access_profile: InputChannelAccessProfile = Field(..., description="Access profile for the channel")


class InputRowPolicy(InputPolicy):
    """Assign a role to a subject (user or group) for a specific row."""

    access_profile: InputRowAccessProfile = Field(..., description="Access profile for the row")
