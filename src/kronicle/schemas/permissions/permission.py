# kronicle/schemas/permissions/permission.py
from __future__ import annotations

from enum import StrEnum


class PermTarget(StrEnum):
    USER = "user"
    ROLE = "role"
    GROUP = "group"
    POLICY = "policy"
    ZONE = "zone"
    CHANNEL = "channel"
    ROW = "row"
    RBAC = "rbac"
    DATA = "data"
    SETUP = "setup"


class PermAction(StrEnum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    ASSIGN = "assign"
    SYNC = "sync"
    WRITE = "write"
    DELEGATE = "delegate"
    ACCESS = "access"


Tgt = PermTarget
Act = PermAction


def perm(target: PermTarget, action: PermAction) -> str:
    return f"{target}:{action}"


class Permission:
    """Structured permission combining a target resource and an action.

    Supports construction from typed enums or string parsing.

    Usage:
        perm = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
        same = Permission.parse("zone:create")
        assert str(perm) == "zone:create"
        assert perm == same
    """

    __slots__ = ("_target", "_action")

    def __init__(self, target: PermTarget, action: PermAction) -> None:
        self._target = target if isinstance(target, PermTarget) else PermTarget(target)
        self._action = action if isinstance(action, PermAction) else PermAction(action)

    # -- read-only properties ------------------------------------------------

    @property
    def target(self) -> PermTarget:
        return self._target

    @property
    def action(self) -> PermAction:
        return self._action

    # -- string serialisation ------------------------------------------------

    def __str__(self) -> str:
        return f"{self._target.value}:{self._action.value}"

    def to_str(self) -> str:
        return self.__str__()

    def __repr__(self) -> str:
        return f"Permission({self._target.value!r}, {self._action.value!r})"

    # -- equality / hashing (for set membership) -----------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Permission):
            return NotImplemented
        return self._target == other._target and self._action == other._action

    def __hash__(self) -> int:
        return hash((self._target, self._action))

    # -- parsing -------------------------------------------------------------

    @classmethod
    def parse(cls, raw: str) -> Permission:
        idx = raw.rfind(":")
        if idx <= 0 or idx >= len(raw) - 1:
            raise ValueError(f"Invalid permission string: {raw!r}")
        try:
            target = PermTarget(raw[:idx])
        except ValueError as e:
            raise ValueError(f"Unknown permission target: {raw[:idx]!r}") from e
        try:
            action = PermAction(raw[idx + 1 :])
        except ValueError as e:
            raise ValueError(f"Unknown permission action: {raw[idx + 1 :]!r}") from e
        return cls(target=target, action=action)


class PermStr(StrEnum):
    """Every known permission in the Kronicle system.

    Usage:
        require_permission(PermStr.CHANNEL_READ)
        PermStr.parse("channel:read")   # → PermStr.CHANNEL_READ
        PermStr.CHANNEL_READ.target     # → PermTarget.CHANNEL
        PermStr.CHANNEL_READ.action     # → PermAction.READ
    """

    ROLE_CREATE = perm(Tgt.ROLE, Act.CREATE)  # Create a role
    ROLE_READ = perm(Tgt.ROLE, Act.READ)  #     List roles
    ROLE_UPDATE = perm(Tgt.ROLE, Act.UPDATE)  # Update a role
    ROLE_DELETE = perm(Tgt.ROLE, Act.DELETE)  # Delete a role
    ROLE_ASSIGN = perm(Tgt.ROLE, Act.ASSIGN)  # Assign a role to a user or a group

    GROUP_CREATE = perm(Tgt.GROUP, Act.CREATE)  # Create a group
    GROUP_READ = perm(Tgt.GROUP, Act.READ)  #     List groups
    GROUP_UPDATE = perm(Tgt.GROUP, Act.UPDATE)  # Update a group
    GROUP_DELETE = perm(Tgt.GROUP, Act.DELETE)  # Delete a group
    GROUP_ASSIGN = perm(Tgt.GROUP, Act.ASSIGN)  # Assign a user to a group

    USER_CREATE = perm(Tgt.USER, Act.CREATE)  # Create a user
    USER_READ = perm(Tgt.USER, Act.READ)  #     List users
    USER_UPDATE = perm(Tgt.USER, Act.UPDATE)  # Update a user's information
    USER_DELETE = perm(Tgt.USER, Act.DELETE)  # Delete a user

    ZONE_CREATE = perm(Tgt.ZONE, Act.CREATE)  # Create a zone
    ZONE_READ = perm(Tgt.ZONE, Act.READ)  #     List zones
    ZONE_UPDATE = perm(Tgt.ZONE, Act.UPDATE)  # Update a zone
    ZONE_DELETE = perm(Tgt.ZONE, Act.DELETE)  # Delete a zone

    CHANNEL_CREATE = perm(Tgt.CHANNEL, Act.CREATE)  # Create a channel in a zone
    CHANNEL_READ = perm(Tgt.CHANNEL, Act.READ)  #     List channels in a zone
    CHANNEL_UPDATE = perm(Tgt.CHANNEL, Act.UPDATE)  # Update a channel
    CHANNEL_DELETE = perm(Tgt.CHANNEL, Act.DELETE)  # Delete a channel
    CHANNEL_SYNC = perm(Tgt.CHANNEL, Act.SYNC)  # (admin-only) sync CoreChannels to existing ChannelResources

    ROW_READ = perm(Tgt.ROW, Act.READ)
    ROW_CREATE = perm(Tgt.ROW, Act.CREATE)
    ROW_UPDATE = perm(Tgt.ROW, Act.UPDATE)
    ROW_DELETE = perm(Tgt.ROW, Act.DELETE)

    POLICY_CREATE = perm(Tgt.POLICY, Act.CREATE)
    POLICY_READ = perm(Tgt.POLICY, Act.READ)
    POLICY_UPDATE = perm(Tgt.POLICY, Act.UPDATE)
    POLICY_DELETE = perm(Tgt.POLICY, Act.DELETE)

    RBAC_ACCESS = perm(Tgt.RBAC, Act.ACCESS)
    RBAC_READ = perm(Tgt.RBAC, Act.READ)
    RBAC_DELEGATE = perm(Tgt.RBAC, Act.DELEGATE)
    DATA_ACCESS = perm(Tgt.DATA, Act.ACCESS)
    SETUP_ACCESS = perm(Tgt.SETUP, Act.ACCESS)

    @property
    def target(self) -> PermTarget:
        target_str, _ = self.value.split(":", 1)
        return PermTarget(target_str)

    @property
    def action(self) -> PermAction:
        _, action_str = self.value.split(":", 1)
        return PermAction(action_str)

    def to_permission(self) -> Permission:
        return Permission(self.target, self.action)

    @classmethod
    def parse(cls, raw: str) -> PermStr:
        return cls(raw)
