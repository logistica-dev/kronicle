from __future__ import annotations

from enum import StrEnum


class PermissionTarget(StrEnum):
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


class PermissionAction(StrEnum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    ASSIGN = "assign"
    SYNC = "sync"
    WRITE = "write"
    DELEGATE = "delegate"
    ACCESS = "access"


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

    def __init__(self, target: PermissionTarget, action: PermissionAction) -> None:
        self._target = target if isinstance(target, PermissionTarget) else PermissionTarget(target)
        self._action = action if isinstance(action, PermissionAction) else PermissionAction(action)

    # -- read-only properties ------------------------------------------------

    @property
    def target(self) -> PermissionTarget:
        return self._target

    @property
    def action(self) -> PermissionAction:
        return self._action

    # -- string serialisation ------------------------------------------------

    def __str__(self) -> str:
        return f"{self._target.value}:{self._action.value}"

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
            target = PermissionTarget(raw[:idx])
        except ValueError as e:
            raise ValueError(f"Unknown permission target: {raw[:idx]!r}") from e
        try:
            action = PermissionAction(raw[idx + 1 :])
        except ValueError as e:
            raise ValueError(f"Unknown permission action: {raw[idx + 1 :]!r}") from e
        return cls(target=target, action=action)
