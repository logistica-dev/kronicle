# tests/unit/services/test_rbac_permissions.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, NotFoundError, UnauthorizedError
from kronicle.schemas.permissions.permission import PermAction, Permission, PermTarget
from kronicle.schemas.rbac.input_subject_schemas import InputSubject
from tests.unit.services.conftest import fake_group, fake_user


class TestPermissions:
    def test_user_has_permission_direct(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_via_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        # Direct check: returns something that has .first() -> None
        mock_direct_res = MagicMock()
        mock_direct_res.first.return_value = None

        # Group check: returns something that has .first() -> (role_id,)
        mock_group_res = MagicMock()
        mock_group_res.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct_res, mock_group_res]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_via_policy_direct(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        # 1. Direct: None
        mock_direct_res = MagicMock()
        mock_direct_res.first.return_value = None

        # 2. Group check: None
        mock_group_res = MagicMock()
        mock_group_res.first.return_value = None

        # 3. Policy check: Found
        mock_policy_res = MagicMock()
        mock_policy_res.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct_res, mock_group_res, mock_policy_res]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_anonymous(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(name="anonymous"))
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(None, "zone:read")
        assert result is True

    def test_user_has_permission_anonymous_no_group(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        result = rbac_service.user_has_permission(None, "zone:read")
        assert result is False

    def test_user_has_permission_denied(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is False


# ==================================================================================================


class TestSubject:
    def test_ensure_subject_user(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subject = MagicMock()
        subject.type = "user"
        subject.name = "usr"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subject)
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))

        result = rbac_service._ensure_subject(db, InputSubject(id=uid, type="user", user_id=uid))

        assert result.type == "user"
        assert result.name == "usr"

    def test_ensure_subject_group(self, rbac_service):
        gid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subject = MagicMock()
        subject.type = "group"
        subject.name = "grp"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))

        result = rbac_service._ensure_subject(db, InputSubject(id=gid, type="group", group_id=gid))

        assert result.type == "group"
        assert result.name == "grp"

    def test_ensure_subject_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)

        with pytest.raises(NotFoundError, match="User|Group"):
            rbac_service._ensure_subject(db, InputSubject(id=uuid4(), type="user", user_id=uuid4()))

    def test_ensure_subject_by_id_inactive_user(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = fake_user(id=uid)
        user.is_active = False
        rbac_service._user_repo.get_by_id = MagicMock(return_value=user)

        with pytest.raises(UnauthorizedError, match="inactive"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uid, type="user", user_id=uid))

    def test_ensure_subject_by_name_user_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service._ensure_subject_by_name(db, "user", "missing")

    def test_ensure_subject_by_name_group_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service._ensure_subject_by_name(db, "group", "missing")

    def test_ensure_subject_id_missing(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._ensure_subject(db, InputSubject(id=None, name=None, type="user"))


# ==================================================================================================
# Access profiles
# ==================================================================================================


class TestHierarchy:
    def test_get_group_ancestor_ids(self, rbac_service):
        gid, pid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.return_value = [(pid,)]

        result = rbac_service._get_group_ancestor_ids(db, gid)
        assert pid in result

    def test_get_group_ancestor_ids_multi_level(self, rbac_service):
        gid, p1, p2 = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [[(p1,)], [(p2,)], []]

        result = rbac_service._get_group_ancestor_ids(db, gid)
        assert p1 in result and p2 in result

    def test_get_group_descendant_ids(self, rbac_service):
        gid, cid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.return_value = [(cid,)]

        result = rbac_service._get_group_descendant_ids(db, gid)
        assert cid in result


# ==================================================================================================
# user_has_permission (from original test file)
# ==================================================================================================


class TestUserHasPermission:
    def test_direct_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is True
        db.execute.assert_called_once()

    def test_group_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, (uuid4(),)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

        result = rbac_service.user_has_permission(user_id, "zone:create")
        assert result is True
        assert db.execute.call_count == 2

    def test_no_match(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is False
        assert db.execute.call_count == 4

    def test_zone_policy_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, None, (uuid4(),)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(user_id, "group:read")
        assert result is True

    def test_empty_groups_no_match(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, None] + [None] * 12
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is False

    def test_with_permission_object(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        perm = Permission(PermTarget.ZONE, PermAction.READ)
        result = rbac_service.user_has_permission(user_id, perm)
        assert result is True
        db.execute.assert_called_once()


# ==================================================================================================
# Name reservation
# ==================================================================================================


class TestEnsureSubjectByIdEdges:
    def test_user_subject_missing_user_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="user_id must be provided"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="user", user_id=None))

    def test_user_subject_user_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="user", user_id=uuid4()))

    def test_group_subject_missing_group_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="group_id must be provided"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="group", group_id=None))

    def test_group_subject_group_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="group", group_id=uuid4()))


# ==================================================================================================
# _ensure_subject_by_name success paths + _ensure_subject name path
# ==================================================================================================


class TestEnsureSubjectByNameSuccess:
    def test_ensure_subject_by_name_user_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = fake_user(name="alice")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=user)
        subject = MagicMock()
        subject.name = "alice"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject_by_name(db, "user", "alice")
        assert result.name == "alice"
        rbac_service._subject_repo.ensure_from_user.assert_called_once_with(db, user=user)

    def test_ensure_subject_by_name_group_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        group = fake_group(name="team-a")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=group)
        subject = MagicMock()
        subject.name = "team-a"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject_by_name(db, "group", "team-a")
        assert result.name == "team-a"
        rbac_service._subject_repo.ensure_from_group.assert_called_once_with(db, group=group)

    def test_ensure_subject_name_path(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        group = fake_group(name="mygroup")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=group)
        subject = MagicMock()
        subject.name = "mygroup"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject(db, InputSubject(id=None, name="mygroup", type="group"))
        assert result.name == "mygroup"


# ==================================================================================================
# _ensure_zone_access_profile internal logic
# ==================================================================================================


class TestCheckPolicyPermGroupMatch:
    def test_group_policy_perm_found_via_user_has_permission(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        mock_direct = MagicMock()
        mock_direct.first.return_value = None
        mock_group_roles = MagicMock()
        mock_group_roles.first.return_value = None
        mock_zone_user = MagicMock()
        mock_zone_user.first.return_value = None
        mock_zone_group = MagicMock()
        mock_zone_group.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct, mock_group_roles, mock_zone_user, mock_zone_group]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True


# ==================================================================================================
# user_has_permission row policy (line 391)
# ==================================================================================================


class TestUserHasPermissionRowPolicy:
    def test_via_row_policy(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        mock_direct = MagicMock()
        mock_direct.first.return_value = None
        mock_zone = MagicMock()
        mock_zone.first.return_value = None
        mock_channel = MagicMock()
        mock_channel.first.return_value = None
        mock_row = MagicMock()
        mock_row.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct, mock_zone, mock_channel, mock_row]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True


# ==================================================================================================
# patch_zone_policy extra edge
# ==================================================================================================


class TestAnonymousPermissionLeak:
    """Verify that anonymous users (user_id=None) do not inherit permissions
    from arbitrary group policies.

    Regression test for a bug where ``RbacSubject.user_id == None`` became
    ``IS NULL`` in SQL, matching every group subject instead of only the
    anonymous group.
    """

    def test_anonymous_does_not_inherit_other_group_policy(self, rbac_service):
        """Zone policy: another group has USER_READ but anonymous group does not."""
        anon_gid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(id=anon_gid, name="anonymous"))

        # For each of zone/channel/row: user-subject query (skipped) + group-subject query
        mock_zone_user = MagicMock()
        mock_zone_user.first.return_value = None
        mock_zone_group = MagicMock()
        mock_zone_group.first.return_value = None  # anonymous group has no zone policy
        mock_channel_user = MagicMock()
        mock_channel_user.first.return_value = None
        mock_channel_group = MagicMock()
        mock_channel_group.first.return_value = None
        mock_row_user = MagicMock()
        mock_row_user.first.return_value = None
        mock_row_group = MagicMock()
        mock_row_group.first.return_value = None

        db.execute.side_effect = [
            mock_zone_user,
            mock_zone_group,
            mock_channel_user,
            mock_channel_group,
            mock_row_user,
            mock_row_group,
        ]

        result = rbac_service.user_has_permission(None, "user:read")
        assert result is False

    def test_anonymous_inherits_own_group_policy(self, rbac_service):
        """Zone policy: anonymous group has a matching policy."""
        anon_gid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(id=anon_gid, name="anonymous"))

        mock_zone_user = MagicMock()
        mock_zone_user.first.return_value = None  # skipped, user_id=None
        mock_zone_group = MagicMock()
        mock_zone_group.first.return_value = (uuid4(),)  # anonymous group matches
        mock_channel_user = MagicMock()
        mock_channel_user.first.return_value = None
        mock_channel_group = MagicMock()
        mock_channel_group.first.return_value = None
        mock_row_user = MagicMock()
        mock_row_user.first.return_value = None
        mock_row_group = MagicMock()
        mock_row_group.first.return_value = None

        db.execute.side_effect = [
            mock_zone_user,
            mock_zone_group,
            mock_channel_user,
            mock_channel_group,
            mock_row_user,
            mock_row_group,
        ]

        result = rbac_service.user_has_permission(None, "zone:read")
        assert result is True
