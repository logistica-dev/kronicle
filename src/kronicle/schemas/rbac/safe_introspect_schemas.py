# kronicle/schemas/rbac/safe_introspect_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_link_schemas import OutputGroupRole, OutputUserRole
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelPolicy,
    OutputRowPolicy,
    OutputZonePolicy,
)
from kronicle.schemas.rbac.safe_user_schemas import OutputUser

# --------------------------------------------------------------------------------------------------
# Permissions introspection
# --------------------------------------------------------------------------------------------------


class OutputUserPermissions(BaseModel):
    user: OutputUser | None = None
    roles: list[OutputUserRole] = []
    indirect_roles: list[OutputGroupRole] = []
    zone_policies: list[OutputZonePolicy] = []
    channel_policies: list[OutputChannelPolicy] = []
    row_policies: list[OutputRowPolicy] = []


class OutputGroupPermissions(BaseModel):
    group: OutputGroup | None = None
    roles: list[OutputGroupRole] = []
    indirect_roles: list[OutputGroupRole] = []
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
