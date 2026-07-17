# kronicle/schemas/rbac/safe_introspect_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from kronicle.schemas.rbac.safe_link_schemas import OutputGroupRole, OutputUserRole
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelPolicy,
    OutputRowPolicy,
    OutputZonePolicy,
)

# --------------------------------------------------------------------------------------------------
# Permissions introspection
# --------------------------------------------------------------------------------------------------


class OutputUserPermissions(BaseModel):
    roles: list[OutputUserRole] = []
    zone_policies: list[OutputZonePolicy] = []
    channel_policies: list[OutputChannelPolicy] = []
    row_policies: list[OutputRowPolicy] = []


class OutputGroupPermissions(BaseModel):
    roles: list[OutputGroupRole] = []
    zone_policies: list[OutputZonePolicy] = []
    channel_policies: list[OutputChannelPolicy] = []
    row_policies: list[OutputRowPolicy] = []


# --------------------------------------------------------------------------------------------------
# Resource access introspection
# --------------------------------------------------------------------------------------------------


class ResourceAccess(BaseModel):
    resource: Any
    parent: Any = None
    policy: Any


class ResourceAccessList(BaseModel):
    zones: list[ResourceAccess] = []
    channels: list[ResourceAccess] = []
    rows: list[ResourceAccess] = []
