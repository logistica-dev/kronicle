# kronicle/schemas/rbac/safe_introspect_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelPolicy,
    OutputRowPolicy,
    OutputZonePolicy,
)
from kronicle.schemas.rbac.safe_role_schemas import OutputRole

# --------------------------------------------------------------------------------------------------
# Permissions introspection
# --------------------------------------------------------------------------------------------------


class GroupRolePair(BaseModel):
    group: OutputGroup
    role: OutputRole


class SubjectPermissions(BaseModel):
    direct_roles: list[OutputRole] = []
    group_roles: list[GroupRolePair] = []
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
