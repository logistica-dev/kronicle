# tests/unit/schemas/permissions/test_permission.py
import pytest

from kronicle.schemas.permissions.permission import PermAction, Permission, PermTarget


class TestPermissionTarget:
    def test_enum_values(self):
        assert PermTarget.USER.value == "user"
        assert PermTarget.ROLE.value == "role"
        assert PermTarget.GROUP.value == "group"
        assert PermTarget.ZONE.value == "zone"
        assert PermTarget.CHANNEL.value == "channel"
        assert PermTarget.POLICY.value == "policy"
        assert PermTarget.ROW.value == "row"
        assert PermTarget.RBAC.value == "rbac"
        assert PermTarget.DATA.value == "data"
        assert PermTarget.SETUP.value == "setup"


class TestPermissionAction:
    def test_enum_values(self):
        assert PermAction.CREATE.value == "create"
        assert PermAction.READ.value == "read"
        assert PermAction.UPDATE.value == "update"
        assert PermAction.DELETE.value == "delete"
        assert PermAction.ASSIGN.value == "assign"
        assert PermAction.SYNC.value == "sync"
        assert PermAction.WRITE.value == "write"
        assert PermAction.DELEGATE.value == "delegate"
        assert PermAction.ACCESS.value == "access"


class TestPermissionConstruct:
    def test_two_arg_constructor(self):
        p = Permission(PermTarget.ZONE, PermAction.CREATE)
        assert p.target == PermTarget.ZONE
        assert p.action == PermAction.CREATE

    def test_parse_simple(self):
        p = Permission.parse("zone:create")
        assert p.target == PermTarget.ZONE
        assert p.action == PermAction.CREATE

    def test_parse_compound_target_no_longer_supported(self):
        with pytest.raises(ValueError, match="Unknown permission target"):
            Permission.parse("core:channel:update")

    def test_parse_simple_channel(self):
        p = Permission.parse("channel:sync")
        assert p.target == PermTarget.CHANNEL
        assert p.action == PermAction.SYNC

    def test_parse_invalid_no_colon(self):
        with pytest.raises(ValueError, match="Invalid permission string"):
            Permission.parse("invalid")

    def test_parse_invalid_empty_target(self):
        with pytest.raises(ValueError):
            Permission.parse(":read")

    def test_parse_invalid_empty_action(self):
        with pytest.raises(ValueError):
            Permission.parse("zone:")

    def test_parse_invalid_target(self):
        with pytest.raises(ValueError, match="Unknown permission target"):
            Permission.parse("unknown:read")

    def test_parse_invalid_action(self):
        with pytest.raises(ValueError, match="Unknown permission action"):
            Permission.parse("zone:unknown")


class TestPermissionStr:
    def test_str_simple(self):
        p = Permission(PermTarget.ZONE, PermAction.CREATE)
        assert str(p) == "zone:create"

    def test_str_channel(self):
        p = Permission(PermTarget.CHANNEL, PermAction.UPDATE)
        assert str(p) == "channel:update"

    def test_str_roundtrip(self):
        raw = "channel:read"
        assert str(Permission.parse(raw)) == raw


class TestPermissionEquality:
    def test_equal_same_target_action(self):
        a = Permission(PermTarget.USER, PermAction.CREATE)
        b = Permission(PermTarget.USER, PermAction.CREATE)
        assert a == b

    def test_not_equal_different_target(self):
        a = Permission(PermTarget.USER, PermAction.CREATE)
        b = Permission(PermTarget.ROLE, PermAction.CREATE)
        assert a != b

    def test_not_equal_different_action(self):
        a = Permission(PermTarget.USER, PermAction.CREATE)
        b = Permission(PermTarget.USER, PermAction.DELETE)
        assert a != b

    def test_equal_parse_vs_construct(self):
        assert Permission.parse("zone:create") == Permission(PermTarget.ZONE, PermAction.CREATE)

    def test_not_equal_wrong_type(self):
        p = Permission(PermTarget.ZONE, PermAction.CREATE)
        assert p != "zone:create"


class TestPermissionHash:
    def test_hashable(self):
        p = Permission(PermTarget.ZONE, PermAction.READ)
        s = {p}
        assert p in s

    def test_set_membership(self):
        p1 = Permission(PermTarget.ZONE, PermAction.READ)
        p2 = Permission(PermTarget.ZONE, PermAction.WRITE)
        s = {p1, p2}
        assert Permission(PermTarget.ZONE, PermAction.READ) in s
        assert Permission(PermTarget.ZONE, PermAction.WRITE) in s
        assert Permission(PermTarget.CHANNEL, PermAction.CREATE) not in s

    def test_hash_consistent_with_equality(self):
        a = Permission.parse("zone:create")
        b = Permission(PermTarget.ZONE, PermAction.CREATE)
        assert hash(a) == hash(b)


class TestPermissionAllStrings:
    def test_all_known_permissions_roundtrip(self):
        cases = [
            ("user:create", PermTarget.USER, PermAction.CREATE),
            ("user:read", PermTarget.USER, PermAction.READ),
            ("user:update", PermTarget.USER, PermAction.UPDATE),
            ("user:delete", PermTarget.USER, PermAction.DELETE),
            ("role:assign", PermTarget.ROLE, PermAction.ASSIGN),
            ("role:create", PermTarget.ROLE, PermAction.CREATE),
            ("role:read", PermTarget.ROLE, PermAction.READ),
            ("role:update", PermTarget.ROLE, PermAction.UPDATE),
            ("role:delete", PermTarget.ROLE, PermAction.DELETE),
            ("group:create", PermTarget.GROUP, PermAction.CREATE),
            ("group:read", PermTarget.GROUP, PermAction.READ),
            ("group:update", PermTarget.GROUP, PermAction.UPDATE),
            ("group:delete", PermTarget.GROUP, PermAction.DELETE),
            ("group:assign", PermTarget.GROUP, PermAction.ASSIGN),
            ("zone:create", PermTarget.ZONE, PermAction.CREATE),
            ("zone:read", PermTarget.ZONE, PermAction.READ),
            ("zone:update", PermTarget.ZONE, PermAction.UPDATE),
            ("zone:delete", PermTarget.ZONE, PermAction.DELETE),
            ("channel:create", PermTarget.CHANNEL, PermAction.CREATE),
            ("channel:read", PermTarget.CHANNEL, PermAction.READ),
            ("channel:update", PermTarget.CHANNEL, PermAction.UPDATE),
            ("channel:delete", PermTarget.CHANNEL, PermAction.DELETE),
            ("channel:sync", PermTarget.CHANNEL, PermAction.SYNC),
            ("policy:create", PermTarget.POLICY, PermAction.CREATE),
            ("policy:read", PermTarget.POLICY, PermAction.READ),
            ("policy:delete", PermTarget.POLICY, PermAction.DELETE),
            ("row:read", PermTarget.ROW, PermAction.READ),
            ("row:create", PermTarget.ROW, PermAction.CREATE),
            ("row:update", PermTarget.ROW, PermAction.UPDATE),
            ("row:delete", PermTarget.ROW, PermAction.DELETE),
            ("rbac:access", PermTarget.RBAC, PermAction.ACCESS),
            ("rbac:delegate", PermTarget.RBAC, PermAction.DELEGATE),
            ("data:access", PermTarget.DATA, PermAction.ACCESS),
            ("setup:access", PermTarget.SETUP, PermAction.ACCESS),
        ]
        for raw_str, expected_target, expected_action in cases:
            p = Permission.parse(raw_str)
            assert p.target == expected_target, f"Target mismatch for {raw_str}"
            assert p.action == expected_action, f"Action mismatch for {raw_str}"
            assert str(p) == raw_str, f"Roundtrip failed for {raw_str}"
