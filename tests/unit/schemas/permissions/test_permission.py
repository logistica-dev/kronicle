import pytest

from kronicle.schemas.permissions.permission import Permission, PermissionAction, PermissionTarget


class TestPermissionTarget:
    def test_enum_values(self):
        assert PermissionTarget.USER.value == "user"
        assert PermissionTarget.ROLE.value == "role"
        assert PermissionTarget.GROUP.value == "group"
        assert PermissionTarget.DATA.value == "data"
        assert PermissionTarget.ZONE.value == "zone"
        assert PermissionTarget.CORE_CHANNEL.value == "core:channel"
        assert PermissionTarget.CHANNEL.value == "channel"
        assert PermissionTarget.POLICY.value == "policy"


class TestPermissionAction:
    def test_enum_values(self):
        assert PermissionAction.CREATE.value == "create"
        assert PermissionAction.READ.value == "read"
        assert PermissionAction.UPDATE.value == "update"
        assert PermissionAction.DELETE.value == "delete"
        assert PermissionAction.ASSIGN.value == "assign"
        assert PermissionAction.SYNC.value == "sync"
        assert PermissionAction.WRITE.value == "write"


class TestPermissionConstruct:
    def test_two_arg_constructor(self):
        p = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
        assert p.target == PermissionTarget.ZONE
        assert p.action == PermissionAction.CREATE

    def test_parse_simple(self):
        p = Permission.parse("zone:create")
        assert p.target == PermissionTarget.ZONE
        assert p.action == PermissionAction.CREATE

    def test_parse_compound_target(self):
        p = Permission.parse("core:channel:update")
        assert p.target == PermissionTarget.CORE_CHANNEL
        assert p.action == PermissionAction.UPDATE

    def test_parse_compound_target_sync(self):
        p = Permission.parse("core:channel:sync")
        assert p.target == PermissionTarget.CORE_CHANNEL
        assert p.action == PermissionAction.SYNC

    def test_parse_invalid_no_colon(self):
        with pytest.raises(ValueError, match="Invalid permission string"):
            Permission.parse("invalid")

    def test_parse_invalid_empty_target(self):
        with pytest.raises(ValueError):
            Permission.parse(":read")

    def test_parse_invalid_empty_action(self):
        with pytest.raises(ValueError):
            Permission.parse("data:")

    def test_parse_invalid_target(self):
        with pytest.raises(ValueError, match="Unknown permission target"):
            Permission.parse("unknown:read")

    def test_parse_invalid_action(self):
        with pytest.raises(ValueError, match="Unknown permission action"):
            Permission.parse("data:unknown")


class TestPermissionStr:
    def test_str_simple(self):
        p = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
        assert str(p) == "zone:create"

    def test_str_compound_target(self):
        p = Permission(PermissionTarget.CORE_CHANNEL, PermissionAction.UPDATE)
        assert str(p) == "core:channel:update"

    def test_str_roundtrip(self):
        raw = "data:write"
        assert str(Permission.parse(raw)) == raw


class TestPermissionEquality:
    def test_equal_same_target_action(self):
        a = Permission(PermissionTarget.USER, PermissionAction.CREATE)
        b = Permission(PermissionTarget.USER, PermissionAction.CREATE)
        assert a == b

    def test_not_equal_different_target(self):
        a = Permission(PermissionTarget.USER, PermissionAction.CREATE)
        b = Permission(PermissionTarget.ROLE, PermissionAction.CREATE)
        assert a != b

    def test_not_equal_different_action(self):
        a = Permission(PermissionTarget.USER, PermissionAction.CREATE)
        b = Permission(PermissionTarget.USER, PermissionAction.DELETE)
        assert a != b

    def test_equal_parse_vs_construct(self):
        assert Permission.parse("zone:create") == Permission(PermissionTarget.ZONE, PermissionAction.CREATE)

    def test_not_equal_wrong_type(self):
        p = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
        assert p != "zone:create"


class TestPermissionHash:
    def test_hashable(self):
        p = Permission(PermissionTarget.DATA, PermissionAction.READ)
        s = {p}
        assert p in s

    def test_set_membership(self):
        p1 = Permission(PermissionTarget.DATA, PermissionAction.READ)
        p2 = Permission(PermissionTarget.DATA, PermissionAction.WRITE)
        s = {p1, p2}
        assert Permission(PermissionTarget.DATA, PermissionAction.READ) in s
        assert Permission(PermissionTarget.DATA, PermissionAction.WRITE) in s
        assert Permission(PermissionTarget.ZONE, PermissionAction.CREATE) not in s

    def test_hash_consistent_with_equality(self):
        a = Permission.parse("zone:create")
        b = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
        assert hash(a) == hash(b)


class TestPermissionAllStrings:
    def test_all_known_permissions_roundtrip(self):
        cases = [
            ("user:create", PermissionTarget.USER, PermissionAction.CREATE),
            ("user:update", PermissionTarget.USER, PermissionAction.UPDATE),
            ("user:delete", PermissionTarget.USER, PermissionAction.DELETE),
            ("role:assign", PermissionTarget.ROLE, PermissionAction.ASSIGN),
            ("role:create", PermissionTarget.ROLE, PermissionAction.CREATE),
            ("role:update", PermissionTarget.ROLE, PermissionAction.UPDATE),
            ("role:delete", PermissionTarget.ROLE, PermissionAction.DELETE),
            ("group:create", PermissionTarget.GROUP, PermissionAction.CREATE),
            ("group:update", PermissionTarget.GROUP, PermissionAction.UPDATE),
            ("group:delete", PermissionTarget.GROUP, PermissionAction.DELETE),
            ("group:assign", PermissionTarget.GROUP, PermissionAction.ASSIGN),
            ("data:read", PermissionTarget.DATA, PermissionAction.READ),
            ("data:write", PermissionTarget.DATA, PermissionAction.WRITE),
            ("zone:create", PermissionTarget.ZONE, PermissionAction.CREATE),
            ("zone:update", PermissionTarget.ZONE, PermissionAction.UPDATE),
            ("zone:delete", PermissionTarget.ZONE, PermissionAction.DELETE),
            ("core:channel:update", PermissionTarget.CORE_CHANNEL, PermissionAction.UPDATE),
            ("core:channel:sync", PermissionTarget.CORE_CHANNEL, PermissionAction.SYNC),
            ("channel:create", PermissionTarget.CHANNEL, PermissionAction.CREATE),
            ("channel:update", PermissionTarget.CHANNEL, PermissionAction.UPDATE),
            ("channel:delete", PermissionTarget.CHANNEL, PermissionAction.DELETE),
            ("policy:create", PermissionTarget.POLICY, PermissionAction.CREATE),
            ("policy:delete", PermissionTarget.POLICY, PermissionAction.DELETE),
        ]
        for raw_str, expected_target, expected_action in cases:
            p = Permission.parse(raw_str)
            assert p.target == expected_target, f"Target mismatch for {raw_str}"
            assert p.action == expected_action, f"Action mismatch for {raw_str}"
            assert str(p) == raw_str, f"Roundtrip failed for {raw_str}"
