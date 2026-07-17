# kronicle/schemas/rbac/safe_link_schemas.py
from __future__ import annotations

from pydantic import BaseModel

from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.schemas.output_schema import OutputSchema
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser

# --------------------------------------------------------------------------------------------------
# User ↔ Role
# --------------------------------------------------------------------------------------------------


class OutputUserRole(OutputSchema):
    # id: UUID | None = None
    user: OutputUser
    role: OutputRole

    # @classmethod
    # def from_db(cls, db_obj) -> OutputUserRole:
    #     return cls(user=OutputUser.from_db(db_obj.user), role=OutputRole.from_db(db_obj.role))


# --------------------------------------------------------------------------------------------------
# Group ↔ Role
# --------------------------------------------------------------------------------------------------


class OutputGroupRole(OutputSchema):
    # id: UUID | None = None
    group: OutputGroup
    role: OutputRole

    # @classmethod
    # def from_db(cls, db_obj) -> OutputGroupRole:
    #     return cls(group=OutputGroup.from_db(db_obj.group), role=OutputRole.from_db(db_obj.role))


# --------------------------------------------------------------------------------------------------
# User ↔ Group
# --------------------------------------------------------------------------------------------------


class OutputUserGroupMembership(BaseModel):
    user: OutputUser
    group: OutputGroup
    indirect: bool = False
    ancestors: list[OutputGroup] = []

    @classmethod
    def from_db(cls, db_obj: RbacUserGroups) -> OutputUserGroupMembership:
        return cls(user=OutputUser.from_db(db_obj.user), group=OutputGroup.from_db(db_obj.group))


# --------------------------------------------------------------------------------------------------
# Role → Subjects
# --------------------------------------------------------------------------------------------------


class OutputRoleSubjects(BaseModel):
    users: list[OutputUser] = []
    groups: list[OutputGroup] = []
    indirect_users: list[OutputUser] = []
