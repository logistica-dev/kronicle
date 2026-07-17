# kronicle/schemas/rbac/safe_link_schemas.py
from __future__ import annotations

from pydantic import BaseModel

from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser

# --------------------------------------------------------------------------------------------------
# User ↔ Role
# --------------------------------------------------------------------------------------------------


class OutputUserRole(BaseModel):
    user: OutputUser
    role: OutputRole
    indirect: bool = False
    parent: OutputGroup | None = None


# --------------------------------------------------------------------------------------------------
# Group ↔ Role
# --------------------------------------------------------------------------------------------------


class OutputGroupRole(BaseModel):
    group: OutputGroup
    role: OutputRole
    indirect: bool = False
    parent: OutputGroup | None = None


# --------------------------------------------------------------------------------------------------
# User ↔ Group
# --------------------------------------------------------------------------------------------------


class OutputUserGroupMembership(BaseModel):
    user: OutputUser
    group: OutputGroup
    indirect: bool = False
    parent: OutputGroup | None = None


# --------------------------------------------------------------------------------------------------
# Role → Subjects
# --------------------------------------------------------------------------------------------------


class OutputRoleSubjects(BaseModel):
    users: list[OutputUser] = []
    groups: list[OutputGroup] = []
    indirect_users: list[OutputUser] = []
